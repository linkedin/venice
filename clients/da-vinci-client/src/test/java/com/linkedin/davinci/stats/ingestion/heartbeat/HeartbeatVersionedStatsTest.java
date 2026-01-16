package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_HEARTBEAT_DELAY;
import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelStats.SERVER_METRIC_ENTITIES;
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

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VersionRole;
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
 * Unit tests for HeartbeatVersionedStats that verify both Tehuti and OTel metrics
 * receive consistent data when recording heartbeat delays.
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
  private Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> leaderMonitors;
  private Map<String, Map<Integer, Map<Integer, Map<String, HeartbeatTimeStampEntry>>>> followerMonitors;
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
    leaderMonitors.put(STORE_NAME, new VeniceConcurrentHashMap<>());

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
}
