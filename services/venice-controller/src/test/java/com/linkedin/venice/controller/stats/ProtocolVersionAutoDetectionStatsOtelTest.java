package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ProtocolVersionAutoDetectionStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String RESOURCE_NAME =
      ".admin_operation_protocol_version_auto_detection_service_" + TEST_CLUSTER_NAME;
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ProtocolVersionAutoDetectionStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());

    stats = new ProtocolVersionAutoDetectionStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordConsecutiveFailure() {
    stats.recordProtocolVersionAutoDetectionErrorSensor(5);

    // OTel
    validateGauge(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT
            .getMetricName(),
        5,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_ERROR,
        "Gauge",
        5.0);
  }

  @Test
  public void testRecordDetectionLatency() {
    stats.recordProtocolVersionAutoDetectionLatencySensor(250.0);

    // OTel
    validateHistogram(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_TIME
            .getMetricName(),
        250.0,
        250.0,
        1,
        250.0,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_LATENCY,
        "Avg",
        250.0);
  }

  @Test
  public void testGaugeOverwriteSemantics() {
    stats.recordProtocolVersionAutoDetectionErrorSensor(5);
    validateGauge(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT
            .getMetricName(),
        5,
        clusterAttributes());

    // Record 0 and verify the gauge is overwritten, not accumulated
    stats.recordProtocolVersionAutoDetectionErrorSensor(0);
    validateGauge(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT
            .getMetricName(),
        0,
        clusterAttributes());
    validateTehutiMetric(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_ERROR,
        "Gauge",
        0.0);
  }

  @Test
  public void testMultipleLatencyRecordings() {
    stats.recordProtocolVersionAutoDetectionLatencySensor(100.0);
    stats.recordProtocolVersionAutoDetectionLatencySensor(300.0);

    // OTel: min=100, max=300, count=2, sum=400
    validateHistogram(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_TIME
            .getMetricName(),
        100.0,
        300.0,
        2,
        400.0,
        clusterAttributes());

    // Tehuti: Avg = (100 + 300) / 2 = 200
    validateTehutiMetric(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_LATENCY,
        "Avg",
        200.0);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    ProtocolVersionAutoDetectionStats disabledStats =
        new ProtocolVersionAutoDetectionStats(disabledRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    disabledStats.recordProtocolVersionAutoDetectionErrorSensor(3);
    disabledStats.recordProtocolVersionAutoDetectionLatencySensor(100.0);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    ProtocolVersionAutoDetectionStats plainStats = new ProtocolVersionAutoDetectionStats(plainRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    plainStats.recordProtocolVersionAutoDetectionErrorSensor(3);
    plainStats.recordProtocolVersionAutoDetectionLatencySensor(100.0);
  }

  private static Attributes clusterAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateHistogram(
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateTehutiMetric(
      ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum tehutiEnum,
      String statSuffix,
      double expectedValue) {
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(RESOURCE_NAME, tehutiEnum.getMetricName()) + "." + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }
}
