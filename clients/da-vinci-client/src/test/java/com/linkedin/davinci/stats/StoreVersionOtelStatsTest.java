package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.VeniceVersionedStatsOtelMetricEntity.STORE_VERSION;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
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
  private StoreVersionOtelStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new StoreVersionOtelStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
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
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
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
