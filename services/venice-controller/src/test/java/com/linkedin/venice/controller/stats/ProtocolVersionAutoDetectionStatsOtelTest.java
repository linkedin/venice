package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
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
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
    MetricsRepository plainRepo = new MetricsRepository();
    ProtocolVersionAutoDetectionStats plainStats = new ProtocolVersionAutoDetectionStats(plainRepo, TEST_CLUSTER_NAME);

    // Should execute without NPE
    plainStats.recordProtocolVersionAutoDetectionErrorSensor(3);
    plainStats.recordProtocolVersionAutoDetectionLatencySensor(100.0);
  }

  @Test
  public void testProtocolVersionAutoDetectionTehutiMetricNameEnum() {
    Map<ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum, String> expectedNames =
        new HashMap<>();
    expectedNames.put(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_ERROR,
        "protocol_version_auto_detection_error");
    expectedNames.put(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_LATENCY,
        "protocol_version_auto_detection_latency");

    assertEquals(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New ProtocolVersionAutoDetectionTehutiMetricNameEnum values were added but not included in this test");

    for (ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum enumValue: ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testProtocolVersionAutoDetectionOtelMetricEntity() {
    Map<ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity, MetricEntity> expectedMetrics =
        new HashMap<>();
    expectedMetrics.put(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT,
        new MetricEntity(
            "protocol_version_auto_detection.consecutive_failure_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Consecutive failures in protocol version auto-detection",
            Utils.setOf(VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_TIME,
        new MetricEntity(
            "protocol_version_auto_detection.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Latency of protocol version auto-detection",
            Utils.setOf(VENICE_CLUSTER_NAME)));

    assertEquals(
        ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New ProtocolVersionAutoDetectionOtelMetricEntity values were added but not included in this test");

    for (ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity metric: ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionOtelMetricEntity
        .values()) {
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

    // Verify all entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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
