package com.linkedin.venice.controller.lingeringjob;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  @Test
  public void testHeartbeatCheckerTehutiMetricNameEnum() {
    Map<HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.CHECK_JOB_HAS_HEARTBEAT_FAILED,
        "check_job_has_heartbeat_failed");
    expectedNames.put(
        HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.TIMEOUT_HEARTBEAT_CHECK,
        "timeout_heartbeat_check");
    expectedNames.put(
        HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.NON_TIMEOUT_HEARTBEAT_CHECK,
        "non_timeout_heartbeat_check");

    assertEquals(
        HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New HeartbeatCheckerTehutiMetricNameEnum values were added but not included in this test");

    for (HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum enumValue: HeartbeatBasedCheckerStats.HeartbeatCheckerTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testHeartbeatCheckerOtelMetricEntity() {
    Map<HeartbeatCheckerOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_CHECK_FAILURE_COUNT,
        MetricEntity.createWithNoDimensions(
            "batch_job_heartbeat.check_failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed heartbeat check operations"));
    expectedMetrics.put(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_TIMEOUT_COUNT,
        MetricEntity.createWithNoDimensions(
            "batch_job_heartbeat.timeout_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Batch jobs timed out based on heartbeat"));
    expectedMetrics.put(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_ACTIVE_COUNT,
        MetricEntity.createWithNoDimensions(
            "batch_job_heartbeat.active_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Batch jobs with valid heartbeat"));

    assertEquals(
        HeartbeatCheckerOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New HeartbeatCheckerOtelMetricEntity values were added but not included in this test");

    for (HeartbeatCheckerOtelMetricEntity metric: HeartbeatCheckerOtelMetricEntity.values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }

    // Verify all HeartbeatCheckerOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
    for (MetricEntity expected: expectedMetrics.values()) {
      boolean found = false;
      for (MetricEntity actual: CONTROLLER_SERVICE_METRIC_ENTITIES) {
        if (Objects.equals(actual.getMetricName(), expected.getMetricName())
            && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
            && Objects.equals(actual.getDescription(), expected.getDescription())
            && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList())) {
          found = true;
          break;
        }
      }
      assertTrue(found, "MetricEntity not found in CONTROLLER_SERVICE_METRIC_ENTITIES: " + expected.getMetricName());
    }
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
