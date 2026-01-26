package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_RECORD_DELAY;
import static com.linkedin.davinci.stats.ingestion.heartbeat.RecordOtelStats.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static org.testng.Assert.*;

import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RecordOtelStatsTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String REGION_US_WEST = "us-west";
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String TEST_PREFIX = "test_prefix";

  private InMemoryMetricReader inMemoryMetricReader;
  private RecordOtelStats recordOtelStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    recordOtelStats = new RecordOtelStats(metricsRepository, STORE_NAME, CLUSTER_NAME);
  }

  @Test
  public void testConstructorWithOtelEnabled() {
    // Verify OTel metrics are enabled
    assertTrue(recordOtelStats.emitOtelMetrics(), "OTel metrics should be enabled");
  }

  @Test
  public void testConstructorWithOtelDisabled() {
    // Create with OTel disabled
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    RecordOtelStats stats = new RecordOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);

    // Verify OTel metrics are disabled
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled");
  }

  @Test
  public void testConstructorWithNonVeniceMetricsRepository() {
    // Create with regular MetricsRepository (not VeniceOpenTelemetryMetricsRepository)
    MetricsRepository regularRepository = new MetricsRepository();
    RecordOtelStats stats = new RecordOtelStats(regularRepository, STORE_NAME, CLUSTER_NAME);

    // Verify OTel metrics are disabled (default for non-Venice repository)
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled for non-Venice repository");
  }

  @Test
  public void testUpdateVersionInfo() {
    // Update version info
    recordOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record metrics - should work with updated version info
    recordOtelStats.recordRecordDelayOtelMetrics(
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
    recordOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 150L;
    recordOtelStats.recordRecordDelayOtelMetrics(
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
    recordOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    long delay = 200L;
    recordOtelStats.recordRecordDelayOtelMetrics(
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
    recordOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record three delays: 100ms, 200ms, 150ms
    recordOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        100L);
    recordOtelStats.recordRecordDelayOtelMetrics(
        CURRENT_VERSION,
        REGION_US_WEST,
        ReplicaType.LEADER,
        ReplicaState.READY_TO_SERVE,
        200L);
    recordOtelStats.recordRecordDelayOtelMetrics(
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
    RecordOtelStats stats = new RecordOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);
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
}
