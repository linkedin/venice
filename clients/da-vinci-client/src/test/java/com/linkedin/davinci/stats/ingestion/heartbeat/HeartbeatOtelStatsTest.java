package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_HEARTBEAT_DELAY;
import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelStats.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static org.testng.Assert.*;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HeartbeatOtelStatsTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String REGION_US_WEST = "us-west";
  private static final String REGION_US_EAST = "us-east";
  private static final int BACKUP_VERSION = 1;
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String TEST_PREFIX = "test_prefix";

  private InMemoryMetricReader inMemoryMetricReader;
  private HeartbeatOtelStats heartbeatOtelStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    heartbeatOtelStats = new HeartbeatOtelStats(metricsRepository, STORE_NAME, CLUSTER_NAME);
  }

  @Test
  public void testConstructorWithOtelEnabled() {
    // Verify OTel metrics are enabled
    assertTrue(heartbeatOtelStats.emitOtelMetrics(), "OTel metrics should be enabled");
  }

  @Test
  public void testConstructorWithOtelDisabled() {
    // Create with OTel disabled
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    HeartbeatOtelStats stats = new HeartbeatOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);

    // Verify OTel metrics are disabled
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled");
  }

  @Test
  public void testConstructorWithNonVeniceMetricsRepository() {
    // Create with regular MetricsRepository (not VeniceOpenTelemetryMetricsRepository)
    MetricsRepository regularRepository = new MetricsRepository();
    HeartbeatOtelStats stats = new HeartbeatOtelStats(regularRepository, STORE_NAME, CLUSTER_NAME);

    // Verify OTel metrics are disabled (default for non-Venice repository)
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled for non-Venice repository");
  }

  @Test
  public void testUpdateVersionInfo() {
    // Update version info
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record metrics - should work with updated version info
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Verify metric was recorded with CURRENT version type
    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);
  }

  @Test
  public void testRecordHeartbeatDelayOtelMetricsForCurrentVersion() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 150L;
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        delay);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        (double) delay,
        1);
  }

  @Test
  public void testRecordHeartbeatDelayOtelMetricsForFutureVersion() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 200L;
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        FUTURE_VERSION,
        REGION_US_WEST,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        delay);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.FUTURE,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        (double) delay,
        1);
  }

  @Test
  public void testRecordHeartbeatDelayOtelMetricsForBackupVersion() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 300L;
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        BACKUP_VERSION,
        REGION_US_WEST,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        delay);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.BACKUP,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        (double) delay,
        1);
  }

  @Test
  public void testRecordHeartbeatDelayOtelMetricsForNonExistingVersion() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record with NON_EXISTING_VERSION - should be classified as BACKUP
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        NON_EXISTING_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Verify metric was recorded with BACKUP version type
    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.BACKUP,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);
  }

  @Test
  public void testRecordHeartbeatDelayOtelMetricsWithMultipleRegions() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record for US-WEST
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Record for US-EAST
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_EAST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);

    // Verify both regions recorded metrics
    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);

    validateHeartbeatMetric(
        REGION_US_EAST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200.0,
        1);
  }

  @Test
  public void testRecordMultipleHeartbeatsForSameRegion() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record multiple heartbeats
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        150L);

    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);

    // Verify aggregate metrics (min=100, max=200, sum=450, count=3)
    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0, // min
        200.0, // max
        450.0, // sum
        3); // count
  }

  @DataProvider(name = "versionRoleProvider")
  public Object[][] versionRoleProvider() {
    return new Object[][] { { VersionRole.CURRENT, CURRENT_VERSION }, { VersionRole.FUTURE, FUTURE_VERSION },
        { VersionRole.BACKUP, BACKUP_VERSION } };
  }

  @Test(dataProvider = "versionRoleProvider")
  public void testAllVersionRoles(VersionRole expectedVersionRole, int version) {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        version,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    validateHeartbeatMetric(
        REGION_US_WEST,
        expectedVersionRole,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);
  }

  @DataProvider(name = "replicaTypeProvider")
  public Object[][] replicaTypeProvider() {
    return new Object[][] { { ReplicaType.LEADER }, { ReplicaType.FOLLOWER } };
  }

  @Test(dataProvider = "replicaTypeProvider")
  public void testAllReplicaTypes(ReplicaType replicaType) {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        replicaType,
        ReplicaState.READY_TO_SERVE,
        100L);

    validateHeartbeatMetric(REGION_US_WEST, VersionRole.CURRENT, replicaType, ReplicaState.READY_TO_SERVE, 100.0, 1);
  }

  @DataProvider(name = "replicaStateProvider")
  public Object[][] replicaStateProvider() {
    return new Object[][] { { ReplicaState.READY_TO_SERVE }, { ReplicaState.CATCHING_UP } };
  }

  @Test(dataProvider = "replicaStateProvider")
  public void testAllReplicaStates(ReplicaState replicaState) {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    heartbeatOtelStats
        .recordHeartbeatDelayOtelMetrics(CURRENT_VERSION, REGION_US_WEST, ReplicaType.FOLLOWER, replicaState, 100L);

    validateHeartbeatMetric(REGION_US_WEST, VersionRole.CURRENT, ReplicaType.FOLLOWER, replicaState, 100.0, 1);
  }

  @Test
  public void testVersionInfoUpdateChangesClassification() {
    // Initially set version 1 as current
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record for version 1 - should be CURRENT
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);

    // Update version info - version 1 is now backup
    heartbeatOtelStats.updateVersionInfo(FUTURE_VERSION, BACKUP_VERSION);

    // Record for version 1 again - should now be BACKUP
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        150L);

    // Verify version 1 is now classified as BACKUP
    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.BACKUP,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        150.0,
        1);
  }

  @Test
  public void testNoMetricsRecordedWhenOtelDisabled() {
    // Create stats with OTel disabled
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    HeartbeatOtelStats stats = new HeartbeatOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);

    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Try to record - should be no-op
    stats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Verify no metrics were recorded
    assertEquals(
        inMemoryMetricReader.collectAllMetrics().size(),
        0,
        "No metrics should be recorded when OTel disabled");
  }

  @Test
  public void testVersionInfoWithNonExistingVersions() {
    // Set both current and future as NON_EXISTING_VERSION
    heartbeatOtelStats.updateVersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);

    // Try to record for a valid version - should be classified as BACKUP
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.BACKUP,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);
  }

  @Test
  public void testRecordZeroDelay() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record with 0 delay (caught up)
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        0L);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.FOLLOWER,
        ReplicaState.READY_TO_SERVE,
        0.0,
        1);
  }

  @Test
  public void testRecordLargeDelay() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record with large delay
    long largeDelay = 10000L;
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        largeDelay);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        (double) largeDelay,
        1);
  }

  @Test
  public void testClassifyVersionWithNonExistingVersionInputReturnsBackup() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    assertSame(
        OtelVersionedStatsUtils.classifyVersion(NON_EXISTING_VERSION, heartbeatOtelStats.getVersionInfo()),
        VersionRole.BACKUP);
    assertSame(
        OtelVersionedStatsUtils.classifyVersion(CURRENT_VERSION, heartbeatOtelStats.getVersionInfo()),
        VersionRole.CURRENT);
    assertSame(
        OtelVersionedStatsUtils.classifyVersion(FUTURE_VERSION, heartbeatOtelStats.getVersionInfo()),
        VersionRole.FUTURE);
    assertSame(OtelVersionedStatsUtils.classifyVersion(10, heartbeatOtelStats.getVersionInfo()), VersionRole.BACKUP);
  }

  @Test
  public void testDifferentDimensionCombinations() {
    heartbeatOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record different combinations
    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        FUTURE_VERSION,
        REGION_US_WEST,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        200L);

    heartbeatOtelStats.recordHeartbeatDelayOtelMetrics(
        BACKUP_VERSION,
        REGION_US_EAST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        300L);

    // Verify each combination has its own metric
    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);

    validateHeartbeatMetric(
        REGION_US_WEST,
        VersionRole.FUTURE,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        200.0,
        1);

    validateHeartbeatMetric(
        REGION_US_EAST,
        VersionRole.BACKUP,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        300.0,
        1);
  }

  /**
   * Helper method to validate heartbeat histogram metrics
   * For single value recordings, min and max equal the value
   */
  private void validateHeartbeatMetric(
      String region,
      VersionRole versionRole,
      ReplicaType replicaType,
      ReplicaState replicaState,
      double expectedValue,
      long expectedCount) {
    // For single recordings, min/max/sum are all the same value
    validateHeartbeatMetric(
        region,
        versionRole,
        replicaType,
        replicaState,
        expectedValue,
        expectedValue,
        expectedValue,
        expectedCount);
  }

  /**
   * Helper method to validate heartbeat histogram metrics with explicit min/max/sum
   */
  private void validateHeartbeatMetric(
      String region,
      VersionRole versionRole,
      ReplicaType replicaType,
      ReplicaState replicaState,
      double expectedMin,
      double expectedMax,
      double expectedSum,
      long expectedCount) {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), region)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), replicaType.getDimensionValue())
        .put(VENICE_REPLICA_STATE.getDimensionNameInDefaultFormat(), replicaState.getDimensionValue())
        .build();

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        INGESTION_HEARTBEAT_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }
}
