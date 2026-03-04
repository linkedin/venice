package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelMetricEntity.INGESTION_HEARTBEAT_DELAY;
import static com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelMetricEntity.INGESTION_RECORD_DELAY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for HeartbeatVersionedStats that verify Tehuti heartbeat metrics and
 * OTel metrics (heartbeat + record-level) receive consistent data when recording delays.
 */
public class HeartbeatVersionedStatsTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String REGION = "us-west";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String TEST_PREFIX = "test_prefix";
  private static final long FIXED_CURRENT_TIME = 1000000L;

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ReadOnlyStoreRepository mockMetadataRepository;
  private HeartbeatVersionedStats heartbeatVersionedStats;
  private Map<HeartbeatKey, IngestionTimestampEntry> leaderMonitors;
  private Map<HeartbeatKey, IngestionTimestampEntry> followerMonitors;
  private Set<String> regions;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    mockMetadataRepository = mock(ReadOnlyStoreRepository.class);

    // Setup store and versions
    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(STORE_NAME);
    when(mockStore.getCurrentVersion()).thenReturn(CURRENT_VERSION);

    List<Version> versions = new ArrayList<>();
    Version currentVersion = new VersionImpl(STORE_NAME, CURRENT_VERSION, "push1");
    currentVersion.setStatus(VersionStatus.ONLINE);
    Version futureVersion = new VersionImpl(STORE_NAME, FUTURE_VERSION, "push2");
    futureVersion.setStatus(VersionStatus.STARTED);
    versions.add(currentVersion);
    versions.add(futureVersion);
    when(mockStore.getVersions()).thenReturn(versions);

    when(mockMetadataRepository.getStoreOrThrow(STORE_NAME)).thenReturn(mockStore);
    when(mockMetadataRepository.getAllStores()).thenReturn(Collections.singletonList(mockStore));

    leaderMonitors = new VeniceConcurrentHashMap<>();
    followerMonitors = new VeniceConcurrentHashMap<>();
    // Add a dummy entry so isStoreAssignedToThisNode returns true for STORE_NAME
    leaderMonitors
        .put(new HeartbeatKey(STORE_NAME, CURRENT_VERSION, 0, REGION), new IngestionTimestampEntry(0, false, false));

    regions = new HashSet<>();
    regions.add(REGION);

    MetricConfig metricConfig = new MetricConfig();
    Supplier<HeartbeatStat> statsInitiator = () -> new HeartbeatStat(metricConfig, regions);
    StatsSupplier<HeartbeatStatReporter> reporterSupplier =
        (repo, storeName, clusterName) -> new HeartbeatStatReporter(repo, storeName, regions);

    heartbeatVersionedStats = new HeartbeatVersionedStats(
        metricsRepository,
        mockMetadataRepository,
        statsInitiator,
        reporterSupplier,
        leaderMonitors,
        followerMonitors,
        CLUSTER_NAME);
  }

  @Test
  public void testRecordLeaderLag() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record multiple leader lags (delays: 100ms, 200ms, 150ms)
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 200);
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 150);

    // Verify Tehuti accumulated correctly
    HeartbeatStat tehutiStats = heartbeatVersionedStats.getStatsForTesting(STORE_NAME, CURRENT_VERSION);
    assertEquals(tehutiStats.getReadyToServeLeaderLag(REGION).getMax(), 200.0, "Tehuti max should be 200ms");

    // Verify OTel accumulated correctly (min=100, max=200, count=3, sum=450)
    validateOtelHistogram(ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, 100.0, 200.0, 3, 450.0);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordFollowerLag(boolean isReadyToServe) {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record multiple follower lags (delays: 100ms, 200ms, 150ms)
    heartbeatVersionedStats
        .recordFollowerLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100, isReadyToServe);
    heartbeatVersionedStats
        .recordFollowerLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 200, isReadyToServe);
    heartbeatVersionedStats
        .recordFollowerLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 150, isReadyToServe);

    // Verify Tehuti metrics
    HeartbeatStat tehutiStats = heartbeatVersionedStats.getStatsForTesting(STORE_NAME, CURRENT_VERSION);
    double readyToServeMax = tehutiStats.getReadyToServeFollowerLag(REGION).getMax();
    double catchingUpMax = tehutiStats.getCatchingUpFollowerLag(REGION).getMax();

    // Active metric should have max=200ms, squelched metric should be 0
    assertEquals(readyToServeMax, isReadyToServe ? 200.0 : 0.0);
    assertEquals(catchingUpMax, isReadyToServe ? 0.0 : 200.0);

    // Verify OTel metrics: active has min=100, max=200, count=3, sum=450; squelched has all 0s
    validateOtelHistogram(
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        isReadyToServe ? 100.0 : 0.0,
        isReadyToServe ? 200.0 : 0.0,
        3,
        isReadyToServe ? 450.0 : 0.0);
    validateOtelHistogram(
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        isReadyToServe ? 0.0 : 100.0,
        isReadyToServe ? 0.0 : 200.0,
        3,
        isReadyToServe ? 0.0 : 450.0);
  }

  @Test
  public void testHandleStoreDeleted() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record some metrics to create OTel stats for the store
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);

    HeartbeatOtelStats otelStatsBefore = heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME);
    assertNotNull(otelStatsBefore, "OTel stats should exist before deletion");

    // handleStoreDeleted should clean up OTel stats without throwing exceptions
    heartbeatVersionedStats.handleStoreDeleted(STORE_NAME);

    // Verify OTel stats are cleaned up after deletion
    assertNull(heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME), "OTel stats should be null after deletion");

    // After deletion, recording new metrics should still work (creates new stats)
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 200);

    // Verify a fresh OTel stats instance was created (different object from before deletion)
    HeartbeatOtelStats otelStatsAfter = heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME);
    assertNotNull(otelStatsAfter, "OTel stats should be recreated after recording post-deletion");
  }

  @Test
  public void testVersionInfoInitializedCorrectly() {
    // Create a new store with specific version configuration
    String newStoreName = "new_test_store";
    int newCurrentVersion = 5;
    int newFutureVersion = 6;

    Store newMockStore = mock(Store.class);
    when(newMockStore.getName()).thenReturn(newStoreName);
    when(newMockStore.getCurrentVersion()).thenReturn(newCurrentVersion);

    List<Version> versions = new ArrayList<>();
    Version currentVer = new VersionImpl(newStoreName, newCurrentVersion, "push1");
    currentVer.setStatus(VersionStatus.ONLINE);
    Version futureVer = new VersionImpl(newStoreName, newFutureVersion, "push2");
    futureVer.setStatus(VersionStatus.STARTED); // STARTED status makes it a future version
    versions.add(currentVer);
    versions.add(futureVer);
    when(newMockStore.getVersions()).thenReturn(versions);
    when(mockMetadataRepository.getStoreOrThrow(newStoreName)).thenReturn(newMockStore);

    // Add store to leader monitors so isStoreAssignedToThisNode returns true
    leaderMonitors.put(
        new HeartbeatKey(newStoreName, newCurrentVersion, 0, REGION),
        new IngestionTimestampEntry(0, false, false));

    // Record a metric to trigger store initialization via getVersionedStats -> addStore
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);
    heartbeatVersionedStats.recordLeaderLag(newStoreName, newCurrentVersion, REGION, FIXED_CURRENT_TIME - 100);

    // Verify version info was initialized correctly
    HeartbeatStat stats = heartbeatVersionedStats.getStatsForTesting(newStoreName, newCurrentVersion);
    assertNotNull(stats, "Stats should be created for the new store");

    HeartbeatOtelStats otelStats = heartbeatVersionedStats.getOtelStatsForTesting(newStoreName);
    assertNotNull(otelStats, "OTel stats should be created for the new store");
    assertEquals(otelStats.getVersionInfo().getCurrentVersion(), newCurrentVersion);
    assertEquals(otelStats.getVersionInfo().getFutureVersion(), newFutureVersion);
  }

  @Test
  public void testOnVersionInfoUpdatedCalledOnStoreChange() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // First, record a metric to ensure the store is tracked
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);

    // Get the OTel stats and verify initial version info
    HeartbeatOtelStats otelStats = heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME);
    assertNotNull(otelStats, "OTel stats should exist for the store");
    OtelVersionedStatsUtils.VersionInfo initialVersionInfo = otelStats.getVersionInfo();
    assertEquals(initialVersionInfo.getCurrentVersion(), CURRENT_VERSION);
    assertEquals(initialVersionInfo.getFutureVersion(), FUTURE_VERSION);

    // Now simulate a store change - new version becomes current, new future version
    int newCurrentVersion = FUTURE_VERSION; // 3 becomes current
    int newFutureVersion = 4;

    Store updatedMockStore = mock(Store.class);
    when(updatedMockStore.getName()).thenReturn(STORE_NAME);
    when(updatedMockStore.getCurrentVersion()).thenReturn(newCurrentVersion);

    List<Version> updatedVersions = new ArrayList<>();
    Version currentVer = new VersionImpl(STORE_NAME, newCurrentVersion, "push2");
    currentVer.setStatus(VersionStatus.ONLINE);
    Version futureVer = new VersionImpl(STORE_NAME, newFutureVersion, "push3");
    futureVer.setStatus(VersionStatus.PUSHED); // PUSHED status also makes it a future version
    updatedVersions.add(currentVer);
    updatedVersions.add(futureVer);
    when(updatedMockStore.getVersions()).thenReturn(updatedVersions);

    // Trigger handleStoreChanged
    heartbeatVersionedStats.handleStoreChanged(updatedMockStore);

    // Verify onVersionInfoUpdated was called and OTel stats were updated
    OtelVersionedStatsUtils.VersionInfo updatedVersionInfo = otelStats.getVersionInfo();
    assertEquals(updatedVersionInfo.getCurrentVersion(), newCurrentVersion);
    assertEquals(updatedVersionInfo.getFutureVersion(), newFutureVersion);
  }

  @Test
  public void testFutureVersionComputedFromStartedAndPushedStatus() {
    // Test that future version is correctly identified from STARTED status
    String storeName = "future_version_test_store";

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(storeName);
    when(mockStore.getCurrentVersion()).thenReturn(1);

    List<Version> versions = new ArrayList<>();
    // Version 1 is ONLINE (current)
    Version v1 = new VersionImpl(storeName, 1, "push1");
    v1.setStatus(VersionStatus.ONLINE);
    // Version 2 is PUSHED (should be detected as future)
    Version v2 = new VersionImpl(storeName, 2, "push2");
    v2.setStatus(VersionStatus.PUSHED);
    // Version 3 is STARTED (should be detected as future, and should win as it's higher)
    Version v3 = new VersionImpl(storeName, 3, "push3");
    v3.setStatus(VersionStatus.STARTED);
    versions.add(v1);
    versions.add(v2);
    versions.add(v3);
    when(mockStore.getVersions()).thenReturn(versions);
    when(mockMetadataRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);

    // Add to monitors
    leaderMonitors.put(new HeartbeatKey(storeName, 1, 0, REGION), new IngestionTimestampEntry(0, false, false));

    // Record metric to trigger initialization
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);
    heartbeatVersionedStats.recordLeaderLag(storeName, 1, REGION, FIXED_CURRENT_TIME - 100);

    // Verify future version is the highest STARTED/PUSHED version (3)
    HeartbeatOtelStats otelStats = heartbeatVersionedStats.getOtelStatsForTesting(storeName);
    assertNotNull(otelStats);
    OtelVersionedStatsUtils.VersionInfo versionInfo = otelStats.getVersionInfo();
    assertEquals(versionInfo.getCurrentVersion(), 1);
    assertEquals(versionInfo.getFutureVersion(), 3, "Future version should be highest STARTED/PUSHED version");
  }

  @Test
  public void testNoFutureVersionWhenAllOnline() {
    // Test that future version is NON_EXISTING_VERSION when no versions are STARTED/PUSHED
    String storeName = "no_future_version_store";

    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(storeName);
    when(mockStore.getCurrentVersion()).thenReturn(2);

    List<Version> versions = new ArrayList<>();
    Version v1 = new VersionImpl(storeName, 1, "push1");
    v1.setStatus(VersionStatus.ONLINE);
    Version v2 = new VersionImpl(storeName, 2, "push2");
    v2.setStatus(VersionStatus.ONLINE);
    versions.add(v1);
    versions.add(v2);
    when(mockStore.getVersions()).thenReturn(versions);
    when(mockMetadataRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);

    leaderMonitors.put(new HeartbeatKey(storeName, 2, 0, REGION), new IngestionTimestampEntry(0, false, false));

    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);
    heartbeatVersionedStats.recordLeaderLag(storeName, 2, REGION, FIXED_CURRENT_TIME - 100);

    HeartbeatOtelStats otelStats = heartbeatVersionedStats.getOtelStatsForTesting(storeName);
    assertNotNull(otelStats);
    OtelVersionedStatsUtils.VersionInfo versionInfo = otelStats.getVersionInfo();
    assertEquals(versionInfo.getCurrentVersion(), 2);
    assertEquals(
        versionInfo.getFutureVersion(),
        com.linkedin.venice.meta.Store.NON_EXISTING_VERSION,
        "Future version should be NON_EXISTING_VERSION when no versions are STARTED/PUSHED");
  }

  private Attributes buildAttributes(ReplicaType replicaType, ReplicaState replicaState) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), REGION)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.CURRENT.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), replicaType.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), replicaState.getDimensionValue())
        .build();
  }

  private void validateOtelHistogram(
      ReplicaType replicaType,
      ReplicaState replicaState,
      double expectedMin,
      double expectedMax,
      int expectedCount,
      double expectedSum) {
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        buildAttributes(replicaType, replicaState),
        INGESTION_HEARTBEAT_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordLeaderRecordLag() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record multiple leader record lags (delays: 100ms, 200ms, 150ms)
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 200);
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 150);

    // Verify OTel accumulated correctly (min=100, max=200, count=3, sum=450)
    validateRecordOtelHistogram(ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, 100.0, 200.0, 3, 450.0);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordFollowerRecordLag(boolean isReadyToServe) {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record multiple follower record lags (delays: 100ms, 200ms, 150ms)
    heartbeatVersionedStats
        .recordFollowerRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100, isReadyToServe);
    heartbeatVersionedStats
        .recordFollowerRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 200, isReadyToServe);
    heartbeatVersionedStats
        .recordFollowerRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 150, isReadyToServe);

    // Verify OTel metrics: active has min=100, max=200, count=3, sum=450; squelched has all 0s
    validateRecordOtelHistogram(
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        isReadyToServe ? 100.0 : 0.0,
        isReadyToServe ? 200.0 : 0.0,
        3,
        isReadyToServe ? 450.0 : 0.0);
    validateRecordOtelHistogram(
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        isReadyToServe ? 0.0 : 100.0,
        isReadyToServe ? 0.0 : 200.0,
        3,
        isReadyToServe ? 0.0 : 450.0);
  }

  private void validateRecordOtelHistogram(
      ReplicaType replicaType,
      ReplicaState replicaState,
      double expectedMin,
      double expectedMax,
      int expectedCount,
      double expectedSum) {
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        buildAttributes(replicaType, replicaState),
        INGESTION_RECORD_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testEmitPerRecordLeaderOtelMetric() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Initialize the OTel stats for this store by calling recordLeaderRecordLag first
    // This simulates the normal flow where periodic emission initializes the stats
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 50);

    // Now emit per-record leader OTel metrics immediately (delays: 100ms, 200ms, 150ms)
    heartbeatVersionedStats.emitPerRecordLeaderOtelMetric(STORE_NAME, CURRENT_VERSION, REGION, 100);
    heartbeatVersionedStats.emitPerRecordLeaderOtelMetric(STORE_NAME, CURRENT_VERSION, REGION, 200);
    heartbeatVersionedStats.emitPerRecordLeaderOtelMetric(STORE_NAME, CURRENT_VERSION, REGION, 150);

    // Verify OTel accumulated correctly: initial 50 + 100 + 200 + 150 = 500 sum, count=4
    validateRecordOtelHistogram(ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, 50.0, 200.0, 4, 500.0);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testEmitPerRecordFollowerOtelMetric(boolean isReadyToServe) {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Initialize the OTel stats for this store by calling recordFollowerRecordLag first
    // Note: recordFollowerRecordLag records to BOTH states (one with actual value, one with 0)
    heartbeatVersionedStats
        .recordFollowerRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 50, isReadyToServe);

    // Now emit per-record follower OTel metrics (delays: 100ms, 200ms, 150ms)
    // Note: emitPerRecordFollowerOtelMetric only records to the active state (no squelching)
    heartbeatVersionedStats.emitPerRecordFollowerOtelMetric(STORE_NAME, CURRENT_VERSION, REGION, 100, isReadyToServe);
    heartbeatVersionedStats.emitPerRecordFollowerOtelMetric(STORE_NAME, CURRENT_VERSION, REGION, 200, isReadyToServe);
    heartbeatVersionedStats.emitPerRecordFollowerOtelMetric(STORE_NAME, CURRENT_VERSION, REGION, 150, isReadyToServe);

    // Verify OTel metrics:
    // - recordFollowerRecordLag: records 50 to active state, 0 to inactive state (count=1 each)
    // - emitPerRecordFollowerOtelMetric x3: records 100, 200, 150 ONLY to active state (count=3)
    // Active state: 50 + 100 + 200 + 150 = 500, count=4
    // Inactive state: 0, count=1 (only from initial recordFollowerRecordLag)
    if (isReadyToServe) {
      validateRecordOtelHistogram(ReplicaType.FOLLOWER, ReplicaState.READY_TO_SERVE, 50.0, 200.0, 4, 500.0);
      validateRecordOtelHistogram(ReplicaType.FOLLOWER, ReplicaState.CATCHING_UP, 0.0, 0.0, 1, 0.0);
    } else {
      validateRecordOtelHistogram(ReplicaType.FOLLOWER, ReplicaState.CATCHING_UP, 50.0, 200.0, 4, 500.0);
      validateRecordOtelHistogram(ReplicaType.FOLLOWER, ReplicaState.READY_TO_SERVE, 0.0, 0.0, 1, 0.0);
    }
  }

  @Test
  public void testEmitPerRecordOtelMetricWhenStoreNotInitialized() {
    // Test that emitting metrics for an unknown store doesn't throw exception
    // This tests the null check fast path - should be a graceful no-op
    heartbeatVersionedStats.emitPerRecordLeaderOtelMetric("unknown_store", 1, REGION, 100);
    heartbeatVersionedStats.emitPerRecordFollowerOtelMetric("unknown_store", 1, REGION, 100, true);
    // No exception should be thrown - graceful no-op since recordLevelDelayOtelStatsMap.get() returns null
  }

  // ==================================================================================
  // Tests covering the full lifecycle: creation, version initialization, version update,
  // and cleanup for both heartbeat and record-level delay OTel stats.
  // These validate invariants that must hold regardless of how getOrCreate* and
  // onVersionInfoUpdated are implemented internally.
  // ==================================================================================

  /**
   * Verifies that OTel stats are created lazily: a heartbeat creates only heartbeat OTel stats,
   * not record-level stats. Record-level stats are created on the first record-level method call.
   * Both are initialized with correct version info from the metadata repository.
   */
  @Test
  public void testOtelStatsCreatedLazilyWithCorrectVersionInfo() {
    String newStoreName = "fresh_store";
    int currentVer = 10;
    int futureVer = 11;

    Store newMockStore = createMockStore(newStoreName, currentVer, futureVer);
    when(mockMetadataRepository.getStoreOrThrow(newStoreName)).thenReturn(newMockStore);
    leaderMonitors
        .put(new HeartbeatKey(newStoreName, currentVer, 0, REGION), new IngestionTimestampEntry(0, false, false));

    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Before any heartbeat, neither map should have entries for this store
    assertNull(heartbeatVersionedStats.getOtelStatsForTesting(newStoreName));
    assertNull(heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(newStoreName));

    // Record a heartbeat — creates heartbeat OTel stats only (not record-level)
    heartbeatVersionedStats.recordLeaderLag(newStoreName, currentVer, REGION, FIXED_CURRENT_TIME - 100);

    HeartbeatOtelStats heartbeatOtelStats = heartbeatVersionedStats.getOtelStatsForTesting(newStoreName);
    assertNotNull(heartbeatOtelStats, "Heartbeat OTel stats should be created on first heartbeat");
    assertEquals(heartbeatOtelStats.getVersionInfo().getCurrentVersion(), currentVer);
    assertEquals(heartbeatOtelStats.getVersionInfo().getFutureVersion(), futureVer);

    // Record-level stats not created yet — only created on first record-level call
    assertNull(
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(newStoreName),
        "Record-level stats should not be created by a heartbeat call");

    // Now record a record-level lag — creates record-level OTel stats
    heartbeatVersionedStats.recordLeaderRecordLag(newStoreName, currentVer, REGION, FIXED_CURRENT_TIME - 50);

    RecordLevelDelayOtelStats recordStats =
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(newStoreName);
    assertNotNull(recordStats, "Record-level OTel stats should be created on first record-level call");
    assertEquals(recordStats.getVersionInfo().getCurrentVersion(), currentVer);
    assertEquals(recordStats.getVersionInfo().getFutureVersion(), futureVer);
  }

  /**
   * Verifies that handleStoreChanged propagates version updates to BOTH
   * heartbeat and record-level delay OTel stats via onVersionInfoUpdated.
   */
  @Test
  public void testHandleStoreChangedUpdatesVersionInfoInBothMaps() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Initialize both OTel stats by recording both types
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 50);

    // Verify initial version info in both maps
    HeartbeatOtelStats heartbeatStats = heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME);
    RecordLevelDelayOtelStats recordStats = heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(STORE_NAME);
    assertNotNull(heartbeatStats);
    assertNotNull(recordStats);
    assertEquals(heartbeatStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(heartbeatStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);
    assertEquals(recordStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(recordStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);

    // Simulate version change: FUTURE_VERSION becomes current, new future = 4
    int newCurrentVersion = FUTURE_VERSION;
    int newFutureVersion = 4;
    Store updatedStore = createMockStore(STORE_NAME, newCurrentVersion, newFutureVersion);
    heartbeatVersionedStats.handleStoreChanged(updatedStore);

    // Both maps should reflect the updated versions
    assertEquals(heartbeatStats.getVersionInfo().getCurrentVersion(), newCurrentVersion);
    assertEquals(heartbeatStats.getVersionInfo().getFutureVersion(), newFutureVersion);
    assertEquals(recordStats.getVersionInfo().getCurrentVersion(), newCurrentVersion);
    assertEquals(recordStats.getVersionInfo().getFutureVersion(), newFutureVersion);
  }

  /**
   * Verifies that handleStoreDeleted cleans up BOTH heartbeat and record-level delay stats.
   */
  @Test
  public void testHandleStoreDeletedCleansBothMaps() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Initialize both OTel stats
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 50);

    assertNotNull(heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME));
    assertNotNull(heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(STORE_NAME));

    // Delete the store
    heartbeatVersionedStats.handleStoreDeleted(STORE_NAME);

    // Both maps should be cleaned up
    assertNull(heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME), "Heartbeat stats should be removed");
    assertNull(
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(STORE_NAME),
        "Record-level stats should be removed");
  }

  /**
   * Verifies that after handleStoreDeleted, recording new metrics re-creates both stats
   * with fresh version info from the metadata repository.
   */
  @Test
  public void testRecreationAfterDeletionInitializesVersionInfo() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Create and then delete
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 50);
    heartbeatVersionedStats.handleStoreDeleted(STORE_NAME);

    // Recreate by recording again
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 200);
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 75);

    // Verify fresh stats have correct version info
    HeartbeatOtelStats newHeartbeatStats = heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME);
    RecordLevelDelayOtelStats newRecordStats =
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(STORE_NAME);
    assertNotNull(newHeartbeatStats);
    assertNotNull(newRecordStats);
    assertEquals(newHeartbeatStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(newHeartbeatStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);
    assertEquals(newRecordStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(newRecordStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);
  }

  /**
   * Verifies that handleStoreChanged does NOT create OTel stats for a store that hasn't
   * had any metrics recorded yet. OTel stats are created lazily on first metric recording,
   * not proactively by version info updates.
   */
  @Test
  public void testHandleStoreChangedDoesNotCreateStatsForNewStore() {
    String trackedStore = "tracked_store";
    int currentVer = 5;
    int futureVer = 6;
    Store mockStore = createMockStore(trackedStore, currentVer, futureVer);
    when(mockMetadataRepository.getStoreOrThrow(trackedStore)).thenReturn(mockStore);

    // Add to monitors so handleStoreChanged doesn't short-circuit
    leaderMonitors
        .put(new HeartbeatKey(trackedStore, currentVer, 0, REGION), new IngestionTimestampEntry(0, false, false));

    // handleStoreChanged triggers onVersionInfoUpdated, but computeIfPresent is a no-op
    // since no OTel stats exist yet for this store
    heartbeatVersionedStats.handleStoreChanged(mockStore);

    assertNull(
        heartbeatVersionedStats.getOtelStatsForTesting(trackedStore),
        "OTel stats should not be created by handleStoreChanged alone");
    assertNull(
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(trackedStore),
        "Record-level stats should not be created by handleStoreChanged alone");
  }

  /**
   * Verifies that when handleStoreChanged fires before any OTel stats exist for a store
   * (computeIfPresent is a no-op), the lazily created OTel stats still get the correct
   * version info from the metadata repository at the time of creation.
   */
  @Test
  public void testVersionInfoFromHandleStoreChangedUsedByLazyCreation() {
    String trackedStore = "tracked_store";
    int initialCurrentVersion = 3;
    int initialFutureVersion = 4;
    int updatedCurrentVersion = 5;
    int updatedFutureVersion = 6;

    // Set up metadata repository with initial versions
    Store initialMockStore = createMockStore(trackedStore, initialCurrentVersion, initialFutureVersion);
    when(mockMetadataRepository.getStoreOrThrow(trackedStore)).thenReturn(initialMockStore);

    // Add to monitors so isStoreAssignedToThisNode returns true
    leaderMonitors.put(
        new HeartbeatKey(trackedStore, initialCurrentVersion, 0, REGION),
        new IngestionTimestampEntry(0, false, false));

    // Simulate handleStoreChanged with updated versions BEFORE any metrics are recorded
    Store updatedMockStore = createMockStore(trackedStore, updatedCurrentVersion, updatedFutureVersion);
    when(mockMetadataRepository.getStoreOrThrow(trackedStore)).thenReturn(updatedMockStore);
    heartbeatVersionedStats.handleStoreChanged(updatedMockStore);

    // Verify OTel stats are NOT created yet (lazy)
    assertNull(
        heartbeatVersionedStats.getOtelStatsForTesting(trackedStore),
        "OTel stats should not be created by handleStoreChanged alone");
    assertNull(
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(trackedStore),
        "Record-level stats should not be created by handleStoreChanged alone");

    // Now record a metric — triggers lazy OTel stats creation
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);
    heartbeatVersionedStats.recordLeaderLag(trackedStore, updatedCurrentVersion, REGION, FIXED_CURRENT_TIME - 100);

    // Verify OTel stats were created with version info from the metadata repository
    HeartbeatOtelStats heartbeatStats = heartbeatVersionedStats.getOtelStatsForTesting(trackedStore);
    assertNotNull(heartbeatStats, "Heartbeat OTel stats should be created on first metric recording");
    assertEquals(
        heartbeatStats.getVersionInfo().getCurrentVersion(),
        updatedCurrentVersion,
        "Current version should match the metadata repository");
    assertEquals(
        heartbeatStats.getVersionInfo().getFutureVersion(),
        updatedFutureVersion,
        "Future version should match the metadata repository");

    // Also verify record-level stats pick up the same version info
    heartbeatVersionedStats.recordLeaderRecordLag(trackedStore, updatedCurrentVersion, REGION, FIXED_CURRENT_TIME - 50);
    RecordLevelDelayOtelStats recordStats =
        heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(trackedStore);
    assertNotNull(recordStats, "Record-level OTel stats should be created on first record-level call");
    assertEquals(
        recordStats.getVersionInfo().getCurrentVersion(),
        updatedCurrentVersion,
        "Record-level current version should match the metadata repository");
    assertEquals(
        recordStats.getVersionInfo().getFutureVersion(),
        updatedFutureVersion,
        "Record-level future version should match the metadata repository");
  }

  /**
   * Verifies version role tagging across current, future, and backup versions for heartbeat metrics.
   */
  @Test
  public void testVersionRoleTaggingInHeartbeatMetrics() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record for current version
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);

    // Verify CURRENT role tagging
    Attributes currentAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), REGION)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.CURRENT.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), ReplicaType.LEADER.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), ReplicaState.READY_TO_SERVE.getDimensionValue())
        .build();
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        100.0,
        100.0,
        1,
        100.0,
        currentAttributes,
        INGESTION_HEARTBEAT_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);

    // Record for future version
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, FUTURE_VERSION, REGION, FIXED_CURRENT_TIME - 200);

    // Verify FUTURE role tagging
    Attributes futureAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), REGION)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.FUTURE.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), ReplicaType.LEADER.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), ReplicaState.READY_TO_SERVE.getDimensionValue())
        .build();
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        200.0,
        200.0,
        1,
        200.0,
        futureAttributes,
        INGESTION_HEARTBEAT_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  /**
   * Verifies version role tagging in record-level delay metrics.
   */
  @Test
  public void testVersionRoleTaggingInRecordLevelDelayMetrics() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record for current version
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, CURRENT_VERSION, REGION, FIXED_CURRENT_TIME - 100);

    // Verify CURRENT role tagging
    Attributes currentAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), REGION)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.CURRENT.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), ReplicaType.LEADER.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), ReplicaState.READY_TO_SERVE.getDimensionValue())
        .build();
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        100.0,
        100.0,
        1,
        100.0,
        currentAttributes,
        INGESTION_RECORD_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);

    // Record for future version
    heartbeatVersionedStats.recordLeaderRecordLag(STORE_NAME, FUTURE_VERSION, REGION, FIXED_CURRENT_TIME - 200);

    // Verify FUTURE role tagging
    Attributes futureAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), REGION)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.FUTURE.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), ReplicaType.LEADER.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), ReplicaState.READY_TO_SERVE.getDimensionValue())
        .build();
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        200.0,
        200.0,
        1,
        200.0,
        futureAttributes,
        INGESTION_RECORD_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  /**
   * Verifies that multiple stores maintain independent version info and OTel stats.
   */
  @Test
  public void testMultipleStoresHaveIndependentVersionInfo() {
    String storeA = "store_a";
    String storeB = "store_b";

    Store mockStoreA = createMockStore(storeA, 5, 6);
    Store mockStoreB = createMockStore(storeB, 10, 11);
    when(mockMetadataRepository.getStoreOrThrow(storeA)).thenReturn(mockStoreA);
    when(mockMetadataRepository.getStoreOrThrow(storeB)).thenReturn(mockStoreB);
    leaderMonitors.put(new HeartbeatKey(storeA, 5, 0, REGION), new IngestionTimestampEntry(0, false, false));
    leaderMonitors.put(new HeartbeatKey(storeB, 10, 0, REGION), new IngestionTimestampEntry(0, false, false));

    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Initialize both stores
    heartbeatVersionedStats.recordLeaderLag(storeA, 5, REGION, FIXED_CURRENT_TIME - 100);
    heartbeatVersionedStats.recordLeaderLag(storeB, 10, REGION, FIXED_CURRENT_TIME - 200);

    // Verify independent version info
    HeartbeatOtelStats statsA = heartbeatVersionedStats.getOtelStatsForTesting(storeA);
    HeartbeatOtelStats statsB = heartbeatVersionedStats.getOtelStatsForTesting(storeB);
    assertEquals(statsA.getVersionInfo().getCurrentVersion(), 5);
    assertEquals(statsA.getVersionInfo().getFutureVersion(), 6);
    assertEquals(statsB.getVersionInfo().getCurrentVersion(), 10);
    assertEquals(statsB.getVersionInfo().getFutureVersion(), 11);

    // Update only store A's version
    Store updatedStoreA = createMockStore(storeA, 6, 7);
    heartbeatVersionedStats.handleStoreChanged(updatedStoreA);

    // Store A should be updated, store B unchanged
    assertEquals(statsA.getVersionInfo().getCurrentVersion(), 6);
    assertEquals(statsA.getVersionInfo().getFutureVersion(), 7);
    assertEquals(statsB.getVersionInfo().getCurrentVersion(), 10);
    assertEquals(statsB.getVersionInfo().getFutureVersion(), 11);
  }

  /**
   * Verifies that version role tagging updates correctly AFTER a version change.
   * Records a metric as FUTURE, then promotes it to CURRENT, and records again —
   * the same version number should now be tagged as CURRENT.
   */
  @Test
  public void testVersionRoleChangesAfterPromotion() {
    heartbeatVersionedStats.setCurrentTimeSupplier(() -> FIXED_CURRENT_TIME);

    // Record for FUTURE_VERSION — should be tagged as FUTURE
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, FUTURE_VERSION, REGION, FIXED_CURRENT_TIME - 100);

    HeartbeatOtelStats otelStats = heartbeatVersionedStats.getOtelStatsForTesting(STORE_NAME);
    assertEquals(otelStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(otelStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);

    // Promote: FUTURE_VERSION (3) becomes CURRENT, new future = 4
    int newFutureVersion = 4;
    Store promotedStore = createMockStore(STORE_NAME, FUTURE_VERSION, newFutureVersion);
    heartbeatVersionedStats.handleStoreChanged(promotedStore);

    // Verify version info updated
    assertEquals(otelStats.getVersionInfo().getCurrentVersion(), FUTURE_VERSION);
    assertEquals(otelStats.getVersionInfo().getFutureVersion(), newFutureVersion);

    // Record again for the SAME version (3) — now tagged as CURRENT
    heartbeatVersionedStats.recordLeaderLag(STORE_NAME, FUTURE_VERSION, REGION, FIXED_CURRENT_TIME - 200);

    // Verify the metric was tagged with CURRENT role (not FUTURE) for the second recording
    Attributes currentAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), REGION)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), VersionRole.CURRENT.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), ReplicaType.LEADER.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), ReplicaState.READY_TO_SERVE.getDimensionValue())
        .build();
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        200.0,
        200.0,
        1,
        200.0,
        currentAttributes,
        INGESTION_HEARTBEAT_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  /**
   * Verifies that handleStoreChanged is a no-op for stores not assigned to this node.
   */
  @Test
  public void testHandleStoreChangedIgnoresUnassignedStores() {
    String unassignedStore = "unassigned_store";

    Store mockStore = createMockStore(unassignedStore, 1, 2);
    when(mockMetadataRepository.getStoreOrThrow(unassignedStore)).thenReturn(mockStore);
    // Do NOT add to leaderMonitors/followerMonitors — store is not assigned

    // handleStoreChanged should be a no-op since isStoreAssignedToThisNode returns false
    heartbeatVersionedStats.handleStoreChanged(mockStore);

    // No stats should have been created
    assertNull(heartbeatVersionedStats.getOtelStatsForTesting(unassignedStore));
    assertNull(heartbeatVersionedStats.getRecordLevelDelayOtelStatsForTesting(unassignedStore));
  }

  private Store createMockStore(String storeName, int currentVersion, int futureVersion) {
    Store mockStore = mock(Store.class);
    when(mockStore.getName()).thenReturn(storeName);
    when(mockStore.getCurrentVersion()).thenReturn(currentVersion);

    List<Version> versions = new ArrayList<>();
    Version currentVer = new VersionImpl(storeName, currentVersion, "push_current");
    currentVer.setStatus(VersionStatus.ONLINE);
    versions.add(currentVer);

    if (futureVersion > currentVersion) {
      Version futureVer = new VersionImpl(storeName, futureVersion, "push_future");
      futureVer.setStatus(VersionStatus.STARTED);
      versions.add(futureVer);
    }

    when(mockStore.getVersions()).thenReturn(versions);
    return mockStore;
  }
}
