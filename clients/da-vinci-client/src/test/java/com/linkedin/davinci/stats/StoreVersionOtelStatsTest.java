package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.VeniceVersionedStatsOtelMetricEntity.STORE_VERSION;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreVersionOtelStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String STORE_VERSION_METRIC_NAME = STORE_VERSION.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  // Dedicated AsyncGauge executor: another test class calling MetricsRepository.close()
  // in the same JVM shuts down the static default executor, which would make
  // AsyncGauge.measure() return 0.0 in this test forever.
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;
  private StoreVersionOtelStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(new MetricConfig(asyncGaugeExecutor))
            .build());
    stats = new StoreVersionOtelStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      // Closes the dedicated executor we injected via setTehutiMetricConfig — does NOT touch
      // the static AsyncGauge.DEFAULT_ASYNC_GAUGE_EXECUTOR shared with other test classes.
      metricsRepository.close();
    }
  }

  @Test
  public void testReportsCurrentAndFutureVersion() {
    Store store = createMockStore(
        TEST_STORE_NAME,
        5,
        createVersion(5, VersionStatus.ONLINE),
        createVersion(6, VersionStatus.STARTED));
    stats.handleStoreChanged(store);

    validateGauge(5, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(6, TEST_STORE_NAME, VersionRole.FUTURE);
  }

  @Test
  public void testGaugeUpdatesOnVersionChange() {
    Store store = createMockStore(TEST_STORE_NAME, 3, createVersion(3, VersionStatus.ONLINE));
    stats.handleStoreChanged(store);
    validateGauge(3, TEST_STORE_NAME, VersionRole.CURRENT);

    // Version swap: 3 → 7
    Store updated = createMockStore(TEST_STORE_NAME, 7, createVersion(7, VersionStatus.ONLINE));
    stats.handleStoreChanged(updated);
    validateGauge(7, TEST_STORE_NAME, VersionRole.CURRENT);
  }

  @Test
  public void testMultiStoreIsolation() {
    String storeA = "store-a";
    String storeB = "store-b";

    stats.handleStoreChanged(createMockStore(storeA, 5, createVersion(5, VersionStatus.ONLINE)));
    stats.handleStoreChanged(createMockStore(storeB, 10, createVersion(10, VersionStatus.ONLINE)));

    validateGauge(5, storeA, VersionRole.CURRENT);
    validateGauge(10, storeB, VersionRole.CURRENT);
  }

  @Test
  public void testStoreDeletedResetsToNonExistingVersion() {
    Store store = createMockStore(TEST_STORE_NAME, 5, createVersion(5, VersionStatus.ONLINE));
    stats.handleStoreChanged(store);
    validateGauge(5, TEST_STORE_NAME, VersionRole.CURRENT);

    // Deletion resets versions to NON_EXISTING_VERSION (state kept for callback reuse)
    stats.handleStoreDeleted(TEST_STORE_NAME);
    validateGauge(NON_EXISTING_VERSION, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(NON_EXISTING_VERSION, TEST_STORE_NAME, VersionRole.FUTURE);

    // Re-creation reuses the existing callback — no duplicate registration
    Store reCreated = createMockStore(TEST_STORE_NAME, 8, createVersion(8, VersionStatus.ONLINE));
    stats.handleStoreChanged(reCreated);
    validateGauge(8, TEST_STORE_NAME, VersionRole.CURRENT);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    AsyncGauge.AsyncGaugeExecutor dedicatedExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setEmitOtelMetrics(false)
            .setTehutiMetricConfig(new MetricConfig(dedicatedExecutor))
            .build())) {
      StoreVersionOtelStats disabledStats = new StoreVersionOtelStats(disabledRepo, TEST_CLUSTER_NAME);
      Store store = createMockStore(TEST_STORE_NAME, 1, createVersion(1, VersionStatus.ONLINE));
      disabledStats.handleStoreChanged(store);
      disabledStats.handleStoreDeleted(TEST_STORE_NAME);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    StoreVersionOtelStats plainStats = new StoreVersionOtelStats(new MetricsRepository(), TEST_CLUSTER_NAME);
    Store store = createMockStore(TEST_STORE_NAME, 1, createVersion(1, VersionStatus.ONLINE));
    plainStats.handleStoreChanged(store);
    plainStats.handleStoreDeleted(TEST_STORE_NAME);
  }

  @Test
  public void testHandleStoreCreatedInitializesGauge() {
    Store store = createMockStore(TEST_STORE_NAME, 3, createVersion(3, VersionStatus.ONLINE));
    stats.handleStoreCreated(store);

    validateGauge(3, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(NON_EXISTING_VERSION, TEST_STORE_NAME, VersionRole.FUTURE);
  }

  @Test
  public void testFutureVersionComputedFromPushedStatus() {
    Store store = createMockStore(
        TEST_STORE_NAME,
        3,
        createVersion(3, VersionStatus.ONLINE),
        createVersion(4, VersionStatus.PUSHED),
        createVersion(5, VersionStatus.STARTED));
    stats.handleStoreChanged(store);

    // Future is the max of STARTED/PUSHED versions
    validateGauge(3, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(5, TEST_STORE_NAME, VersionRole.FUTURE);
  }

  @Test
  public void testNoFutureVersionWhenAllOnline() {
    Store store = createMockStore(
        TEST_STORE_NAME,
        3,
        createVersion(2, VersionStatus.ONLINE),
        createVersion(3, VersionStatus.ONLINE));
    stats.handleStoreChanged(store);

    validateGauge(3, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(NON_EXISTING_VERSION, TEST_STORE_NAME, VersionRole.FUTURE);
  }

  @Test
  public void testRegisterInitializesPreExistingStores() {
    ReadOnlyStoreRepository mockRepo = mock(ReadOnlyStoreRepository.class);
    Store preExisting = createMockStore(TEST_STORE_NAME, 5, createVersion(5, VersionStatus.ONLINE));
    when(mockRepo.getAllStores()).thenReturn(Collections.singletonList(preExisting));

    stats.register(mockRepo);

    validateGauge(5, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(NON_EXISTING_VERSION, TEST_STORE_NAME, VersionRole.FUTURE);
  }

  /**
   * Sequential test of the {@code computeIfAbsent}-only semantic: if an entry already exists
   * for a store, {@code register()} must not overwrite it with snapshot data. This is the
   * invariant {@link StoreVersionOtelStats#initializeStoreIfAbsent} relies on to make the
   * register-then-snapshot path safe against races with ZK events.
   *
   * <p>Note: this test is single-threaded — it pre-populates an entry via
   * {@code handleStoreChanged} before calling {@code register()}. A real concurrency test
   * with latched threads racing the snapshot against the listener-dispatch path would be a
   * stronger guard; left as a follow-up.
   */
  @Test
  public void testRegisterDoesNotOverwriteExistingEntry() {
    ReadOnlyStoreRepository mockRepo = mock(ReadOnlyStoreRepository.class);
    // Snapshot has stale version 3
    Store snapshot = createMockStore(TEST_STORE_NAME, 3, createVersion(3, VersionStatus.ONLINE));
    when(mockRepo.getAllStores()).thenReturn(Collections.singletonList(snapshot));

    // Pre-populate the entry with version 5 (simulating a ZK event that arrived earlier)
    stats.handleStoreChanged(createMockStore(TEST_STORE_NAME, 5, createVersion(5, VersionStatus.ONLINE)));

    // register() should NOT overwrite v5 with stale v3 from the snapshot
    stats.register(mockRepo);

    validateGauge(5, TEST_STORE_NAME, VersionRole.CURRENT);
  }

  @Test
  public void testDeleteForNeverSeenStoreIsNoOp() {
    // handleStoreDeleted for a store that was never created should not throw
    stats.handleStoreDeleted("never-seen-store");
  }

  @Test
  public void testCreateFactoryRegistersListenerAndInitializesPreExistingStores() {
    // Exercises the production entry point: StoreVersionOtelStats.create(repo, cluster, metadataRepo).
    // Verifies (1) the listener is registered on the metadata repository and (2) gauges are
    // initialized for stores already present at registration time.
    ReadOnlyStoreRepository mockRepo = mock(ReadOnlyStoreRepository.class);
    Store preExisting = createMockStore(TEST_STORE_NAME, 7, createVersion(7, VersionStatus.ONLINE));
    when(mockRepo.getAllStores()).thenReturn(Collections.singletonList(preExisting));

    StoreVersionOtelStats created = StoreVersionOtelStats.create(metricsRepository, TEST_CLUSTER_NAME, mockRepo);

    verify(mockRepo).registerStoreDataChangedListener(created);
    validateGauge(7, TEST_STORE_NAME, VersionRole.CURRENT);
    validateGauge(NON_EXISTING_VERSION, TEST_STORE_NAME, VersionRole.FUTURE);
  }

  @Test
  public void testCreateFactoryDoesNotRegisterListenerWhenOtelDisabled() throws Exception {
    // When OTel is disabled, registering the listener would only add no-op dispatch overhead
    // for every store create/change/delete event. Verify the optimization holds.
    AsyncGauge.AsyncGaugeExecutor dedicatedExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setEmitOtelMetrics(false)
            .setTehutiMetricConfig(new MetricConfig(dedicatedExecutor))
            .build())) {
      ReadOnlyStoreRepository mockRepo = mock(ReadOnlyStoreRepository.class);
      StoreVersionOtelStats.create(disabledRepo, TEST_CLUSTER_NAME, mockRepo);
      verify(mockRepo, never()).registerStoreDataChangedListener(any());
    }
  }

  private void validateGauge(long expectedValue, String storeName, VersionRole role) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        buildAttributes(storeName, role),
        STORE_VERSION_METRIC_NAME,
        TEST_METRIC_PREFIX);
  }

  private static Attributes buildAttributes(String storeName, VersionRole role) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .build();
  }

  private static Store createMockStore(String storeName, int currentVersion, Version... versions) {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn(storeName);
    when(store.getCurrentVersion()).thenReturn(currentVersion);
    when(store.getVersions()).thenReturn(Arrays.asList(versions));
    return store;
  }

  private static Version createVersion(int number, VersionStatus status) {
    VersionImpl version = new VersionImpl(TEST_STORE_NAME, number);
    version.setStatus(status);
    return version;
  }
}
