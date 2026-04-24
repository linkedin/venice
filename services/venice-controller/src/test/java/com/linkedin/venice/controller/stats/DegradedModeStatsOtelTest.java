package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DegradedModeStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String TEST_DC_NAME = "test-dc";
  private InMemoryMetricReader inMemoryMetricReader;
  private DegradedModeStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new DegradedModeStats(metricsRepository);
  }

  @Test
  public void testRecordRecoveryStoreSuccess() {
    stats.recordRecoveryStoreSuccess(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DegradedModeStats.DegradedModeOtelMetric.RECOVERY_STORE_SUCCESS_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordRecoveryStoreFailure() {
    stats.recordRecoveryStoreFailure(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DegradedModeStats.DegradedModeOtelMetric.RECOVERY_STORE_FAILURE_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordRecoveryVersionTransitioned() {
    stats.recordRecoveryVersionTransitioned(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DegradedModeStats.DegradedModeOtelMetric.RECOVERY_VERSION_TRANSITIONED_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordRecoveryProgress() {
    // Venice OTel gauges store as long internally, so fractional progress is truncated.
    // Use 1.0 (complete) to get a meaningful assertion.
    stats.recordRecoveryProgress(1.0);
    validateLongGauge(
        DegradedModeStats.DegradedModeOtelMetric.RECOVERY_PROGRESS.getMetricName(),
        1,
        Attributes.empty());
  }

  @Test
  public void testRecordPushAutoConverted() {
    stats.recordPushAutoConverted(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DegradedModeStats.DegradedModeOtelMetric.PUSH_AUTO_CONVERTED_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordPushBlockedIncremental() {
    stats.recordPushBlockedIncremental(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DegradedModeStats.DegradedModeOtelMetric.PUSH_BLOCKED_INCREMENTAL_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordDegradedDcActiveCount() {
    stats.recordDegradedDcActiveCount(3);
    validateLongGauge(
        DegradedModeStats.DegradedModeOtelMetric.DEGRADED_DC_ACTIVE_COUNT.getMetricName(),
        3,
        Attributes.empty());
  }

  @Test
  public void testRecordDegradedDcDurationMinutes() {
    stats.recordDegradedDcDurationMinutes(TEST_CLUSTER_NAME, TEST_DC_NAME, 42.0);
    validateLongGauge(
        DegradedModeStats.DegradedModeOtelMetric.DEGRADED_DC_DURATION_MINUTES.getMetricName(),
        42,
        clusterAndRegionAttributes());
  }

  @Test
  public void testRecordRecoveryStoreDurationMs() {
    stats.recordRecoveryStoreDurationMs(TEST_CLUSTER_NAME, TEST_STORE_NAME, 1500.0);
    validateLongGauge(
        DegradedModeStats.DegradedModeOtelMetric.RECOVERY_STORE_DURATION_MS.getMetricName(),
        1500,
        clusterAndStoreAttributes());
  }

  @Test
  public void testMultipleCounterIncrements() {
    stats.recordRecoveryStoreSuccess(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    stats.recordRecoveryStoreSuccess(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    stats.recordRecoveryStoreSuccess(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DegradedModeStats.DegradedModeOtelMetric.RECOVERY_STORE_SUCCESS_COUNT.getMetricName(),
        3,
        clusterAndStoreAttributes());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    DegradedModeStats disabledStats = new DegradedModeStats(disabledRepo);

    // Should execute without NPE
    disabledStats.recordRecoveryStoreSuccess(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordRecoveryStoreFailure(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordRecoveryVersionTransitioned(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordRecoveryProgress(0.5);
    disabledStats.recordPushAutoConverted(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordPushBlockedIncremental(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordDegradedDcActiveCount(2);
    disabledStats.recordDegradedDcDurationMinutes(TEST_CLUSTER_NAME, TEST_DC_NAME, 10.0);
    disabledStats.recordRecoveryStoreDurationMs(TEST_CLUSTER_NAME, TEST_STORE_NAME, 500.0);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    DegradedModeStats plainStats = new DegradedModeStats(plainRepo);

    // Should execute without NPE
    plainStats.recordRecoveryStoreSuccess(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordRecoveryStoreFailure(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordRecoveryVersionTransitioned(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordRecoveryProgress(0.5);
    plainStats.recordPushAutoConverted(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordPushBlockedIncremental(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordDegradedDcActiveCount(2);
    plainStats.recordDegradedDcDurationMinutes(TEST_CLUSTER_NAME, TEST_DC_NAME, 10.0);
    plainStats.recordRecoveryStoreDurationMs(TEST_CLUSTER_NAME, TEST_STORE_NAME, 500.0);
  }

  private static Attributes clusterAndStoreAttributes() {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .build();
  }

  private static Attributes clusterAndRegionAttributes() {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), TEST_DC_NAME)
        .build();
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateLongGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

}
