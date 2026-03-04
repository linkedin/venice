package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelMetricEntity.INGESTION_RECORD_DELAY;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.Utils.setOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RecordLevelDelayOtelStatsTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String REGION_US_WEST = "us-west";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String TEST_PREFIX = "test_prefix";

  private InMemoryMetricReader inMemoryMetricReader;
  private RecordLevelDelayOtelStats recordLevelDelayOtelStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    recordLevelDelayOtelStats = new RecordLevelDelayOtelStats(metricsRepository, STORE_NAME, CLUSTER_NAME);
  }

  @Test
  public void testConstructorWithOtelEnabled() {
    // Verify OTel metrics are enabled
    assertTrue(recordLevelDelayOtelStats.emitOtelMetrics(), "OTel metrics should be enabled");
  }

  @Test
  public void testConstructorWithOtelDisabled() {
    // Create with OTel disabled
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    RecordLevelDelayOtelStats stats =
        new RecordLevelDelayOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);

    // Verify OTel metrics are disabled
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled");
  }

  @Test
  public void testConstructorWithNonVeniceMetricsRepository() {
    // Create with regular MetricsRepository (not VeniceOpenTelemetryMetricsRepository)
    MetricsRepository regularRepository = new MetricsRepository();
    RecordLevelDelayOtelStats stats = new RecordLevelDelayOtelStats(regularRepository, STORE_NAME, CLUSTER_NAME);

    // Verify OTel metrics are disabled (default for non-Venice repository)
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled for non-Venice repository");
  }

  @Test
  public void testUpdateVersionInfo() {
    // Update version info
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record metrics - should work with updated version info
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Verify metric was recorded with CURRENT version type
    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);
  }

  @Test
  public void testRecordRecordDelayOtelMetricsForCurrentVersion() {
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 150L;
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        delay);

    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        (double) delay,
        1);
  }

  @Test
  public void testRecordRecordDelayOtelMetricsForFutureVersion() {
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 200L;
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        FUTURE_VERSION,
        REGION_US_WEST,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        delay);

    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.FUTURE,
        ReplicaType.FOLLOWER,
        ReplicaState.CATCHING_UP,
        (double) delay,
        1);
  }

  @Test
  public void testRecordRecordDelayOtelMetricsMultipleRecords() {
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record three delays: 100ms, 200ms, 150ms
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        150L);

    // Validate aggregated metrics: min=100, max=200, sum=450, count=3
    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        200.0,
        450.0,
        3);
  }

  @Test
  public void testRecordRecordDelayOtelMetricsWhenOtelDisabled() {
    // Create stats with OTel disabled
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES).setEmitOtelMetrics(false).build());
    RecordLevelDelayOtelStats stats =
        new RecordLevelDelayOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);
    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record metrics - should be no-op
    stats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // No validation needed - metrics should not be recorded
  }

  /**
   * Helper method to validate record histogram metrics for a single value
   */
  private void validateRecordMetric(
      String region,
      VersionRole versionRole,
      ReplicaType replicaType,
      ReplicaState replicaState,
      double expectedValue,
      long expectedCount) {
    validateRecordMetric(
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
   * Helper method to validate record histogram metrics with explicit min/max/sum
   */
  private void validateRecordMetric(
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
        INGESTION_RECORD_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testMetricEntityDefinitions() {
    MetricEntity entity = INGESTION_RECORD_DELAY.getMetricEntity();
    assertEquals(entity.getMetricName(), "ingestion.replication.record.delay");
    assertEquals(entity.getMetricType(), MetricType.HISTOGRAM);
    assertEquals(entity.getUnit(), MetricUnit.MILLISECOND);
    assertEquals(entity.getDescription(), "Nearline ingestion record-level replication lag");
    Set<VeniceMetricsDimensions> expectedDimensions = setOf(
        VENICE_STORE_NAME,
        VENICE_CLUSTER_NAME,
        VENICE_REGION_NAME,
        VENICE_VERSION_ROLE,
        VENICE_REPLICA_TYPE,
        VENICE_REPLICA_STATE);
    assertEquals(entity.getDimensionsList(), expectedDimensions);

    assertEquals(RecordLevelDelayOtelMetricEntity.values().length, 1, "Expected 1 metric entity");
  }

  // ==================================================================================
  // Tests covering version lifecycle, version role classification, multi-region,
  // and close/reuse behavior. These validate invariants that must hold regardless
  // of how version info initialization is wired.
  // ==================================================================================

  /**
   * Verifies initial version info is NON_EXISTING_VERSION before updateVersionInfo is called.
   */
  @Test
  public void testInitialVersionInfoIsNonExisting() {
    VersionInfo versionInfo = recordLevelDelayOtelStats.getVersionInfo();
    assertEquals(
        versionInfo.getCurrentVersion(),
        NON_EXISTING_VERSION,
        "Initial current version should be NON_EXISTING_VERSION");
    assertEquals(
        versionInfo.getFutureVersion(),
        NON_EXISTING_VERSION,
        "Initial future version should be NON_EXISTING_VERSION");
  }

  /**
   * Verifies that recording a metric BEFORE updateVersionInfo tags the version as BACKUP
   * (neither current nor future), since both are NON_EXISTING_VERSION initially.
   */
  @Test
  public void testRecordBeforeUpdateVersionInfoTagsAsBackup() {
    // Do NOT call updateVersionInfo — version info remains NON_EXISTING_VERSION
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Version 2 doesn't match NON_EXISTING_VERSION for either current or future,
    // so it should be classified as BACKUP
    validateRecordMetric(REGION_US_WEST, VersionRole.BACKUP, ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, 100.0, 1);
  }

  /**
   * Verifies that a version matching neither current nor future is tagged as BACKUP.
   */
  @Test
  public void testBackupVersionRoleTagging() {
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    int backupVersion = 1; // Neither current (2) nor future (3)
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        backupVersion,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    validateRecordMetric(REGION_US_WEST, VersionRole.BACKUP, ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, 100.0, 1);
  }

  /**
   * Verifies that calling updateVersionInfo multiple times correctly updates the version
   * classification. A version that was FUTURE becomes CURRENT after promotion.
   */
  @Test
  public void testUpdateVersionInfoMultipleTimesReflectsLatest() {
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record for FUTURE_VERSION — should be FUTURE
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        FUTURE_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);
    validateRecordMetric(REGION_US_WEST, VersionRole.FUTURE, ReplicaType.LEADER, ReplicaState.READY_TO_SERVE, 100.0, 1);

    // Promote: FUTURE_VERSION (3) becomes CURRENT, new future = 4
    int newFutureVersion = 4;
    recordLevelDelayOtelStats.updateVersionInfo(FUTURE_VERSION, newFutureVersion);

    assertEquals(recordLevelDelayOtelStats.getVersionInfo().getCurrentVersion(), FUTURE_VERSION);
    assertEquals(recordLevelDelayOtelStats.getVersionInfo().getFutureVersion(), newFutureVersion);

    // Record same version (3) again — now tagged as CURRENT
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        FUTURE_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);

    // The CURRENT bucket now has the 200ms record
    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200.0,
        1);
  }

  /**
   * Verifies that different regions maintain independent metric states.
   */
  @Test
  public void testMultipleRegionsHaveIndependentMetrics() {
    String regionEast = "us-east";
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record 100ms in us-west
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);

    // Record 200ms in us-east
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        regionEast,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);

    // Validate us-west independently
    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        1);

    // Validate us-east independently
    Attributes eastAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), regionEast)
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
        eastAttributes,
        INGESTION_RECORD_DELAY.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  /**
   * Verifies that close() clears the per-region metric state and that recording
   * after close() re-creates metric state and continues to work.
   */
  @Test
  public void testCloseAndReuse() {
    recordLevelDelayOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record a metric, then close
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);
    recordLevelDelayOtelStats.close();

    // Version info should still be intact (close only clears metric state, not version info)
    assertEquals(recordLevelDelayOtelStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(recordLevelDelayOtelStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);

    // Recording after close should re-create metric state and work
    recordLevelDelayOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);

    // The histogram should have both the pre-close and post-close values since OTel
    // accumulates across the metric reader's collection cycle
    validateRecordMetric(
        REGION_US_WEST,
        VersionRole.CURRENT,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100.0,
        200.0,
        300.0,
        2);
  }
}
