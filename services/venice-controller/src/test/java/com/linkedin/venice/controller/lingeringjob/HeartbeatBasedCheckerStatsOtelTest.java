package com.linkedin.venice.controller.lingeringjob;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.Consumer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HeartbeatBasedCheckerStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String STATS_NAME = "controller-batch-job-heartbeat-checker";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private HeartbeatBasedCheckerStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new HeartbeatBasedCheckerStats(metricsRepository);
  }

  @DataProvider(parallel = true)
  public static Object[][] singleRecordTestData() {
    return new Object[][] {
        { (Consumer<HeartbeatBasedCheckerStats>) HeartbeatBasedCheckerStats::recordCheckJobHasHeartbeatFailed,
            HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_CHECK_FAILURE_COUNT,
            HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.CHECK_JOB_HAS_HEARTBEAT_FAILED },
        { (Consumer<HeartbeatBasedCheckerStats>) HeartbeatBasedCheckerStats::recordTimeoutHeartbeatCheck,
            HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_TIMEOUT_COUNT,
            HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.TIMEOUT_HEARTBEAT_CHECK },
        { (Consumer<HeartbeatBasedCheckerStats>) HeartbeatBasedCheckerStats::recordNoTimeoutHeartbeatCheck,
            HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_ACTIVE_COUNT,
            HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.NON_TIMEOUT_HEARTBEAT_CHECK }, };
  }

  @Test(dataProvider = "singleRecordTestData")
  public void testSingleRecordAndValidate(
      Consumer<HeartbeatBasedCheckerStats> recorder,
      HeartbeatCheckerOtelMetricEntity otelMetric,
      HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum tehutiMetric) {
    // Each parallel invocation creates its own state to avoid thread-safety issues with shared fields
    InMemoryMetricReader localReader = InMemoryMetricReader.create();
    VeniceMetricsRepository localRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(localReader)
            .build());
    HeartbeatBasedCheckerStats localStats = new HeartbeatBasedCheckerStats(localRepo);

    recorder.accept(localStats);

    // OTel counter
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        localReader,
        1,
        Attributes.empty(),
        otelMetric.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti counter
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(STATS_NAME, tehutiMetric.getMetricName()) + ".Count";
    assertNotNull(localRepo.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        localRepo.getMetric(tehutiMetricName).value(),
        1.0,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }

  @Test
  public void testRecordMultipleHeartbeatFailures() {
    stats.recordCheckJobHasHeartbeatFailed();
    stats.recordCheckJobHasHeartbeatFailed();
    stats.recordCheckJobHasHeartbeatFailed();

    // OTel: accumulated = 3
    validateCounter(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_CHECK_FAILURE_COUNT.getMetricEntity().getMetricName(),
        3,
        Attributes.empty());

    // Tehuti: Count = 3
    validateTehutiMetric(
        HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.CHECK_JOB_HAS_HEARTBEAT_FAILED,
        "Count",
        3.0);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    verifyNoNpeWithRepository(disabledRepo);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    verifyNoNpeWithRepository(new MetricsRepository());
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateTehutiMetric(
      HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum tehutiEnum,
      String statSuffix,
      double expectedValue) {
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(STATS_NAME, tehutiEnum.getMetricName()) + "." + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }

  private void verifyNoNpeWithRepository(MetricsRepository repo) {
    HeartbeatBasedCheckerStats localStats = new HeartbeatBasedCheckerStats(repo);
    localStats.recordCheckJobHasHeartbeatFailed();
    localStats.recordTimeoutHeartbeatCheck();
    localStats.recordNoTimeoutHeartbeatCheck();
  }
}
