package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
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


public class ErrorPartitionStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String RESOURCE_NAME = ".test-cluster";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ErrorPartitionStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new ErrorPartitionStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordErrorPartitionResetAttempt() {
    stats.recordErrorPartitionResetAttempt(5.0, TEST_STORE_NAME);

    // OTel
    validateCounter(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ATTEMPT_COUNT.getMetricName(),
        5,
        clusterAndStoreAttributes());

    // Tehuti
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT,
        "Total",
        5.0);
  }

  @Test
  public void testRecordErrorPartitionResetAttemptErrored() {
    stats.recordErrorPartitionResetAttemptErrored(TEST_STORE_NAME);

    // OTel (store-scoped)
    validateCounter(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ERROR_COUNT.getMetricName(),
        1,
        clusterAndStoreAttributes());

    // Tehuti
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT_ERRORED,
        "Count",
        1.0);
  }

  @Test
  public void testRecordErrorPartitionProcessingError() {
    stats.recordErrorPartitionProcessingError();

    // OTel (cluster-only)
    validateCounter(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_ERROR_COUNT.getMetricName(),
        1,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_ERROR,
        "Count",
        1.0);
  }

  @Test
  public void testRecordErrorPartitionRecoveredFromReset() {
    stats.recordErrorPartitionRecoveredFromReset(TEST_STORE_NAME);

    // OTel
    validateCounter(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_RECOVERED_PARTITION_COUNT
            .getMetricName(),
        1,
        clusterAndStoreAttributes());

    // Tehuti
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RECOVERED_FROM_RESET,
        "Total",
        1.0);
  }

  @Test
  public void testRecordErrorPartitionUnrecoverableFromReset() {
    stats.recordErrorPartitionUnrecoverableFromReset(TEST_STORE_NAME);

    // OTel
    validateCounter(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_UNRECOVERABLE_PARTITION_COUNT
            .getMetricName(),
        1,
        clusterAndStoreAttributes());

    // Tehuti
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_UNRECOVERABLE_FROM_RESET,
        "Total",
        1.0);
  }

  @Test
  public void testRecordErrorPartitionProcessingTime() {
    stats.recordErrorPartitionProcessingTime(250.0);

    // OTel (cluster-only)
    validateHistogram(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_TIME.getMetricName(),
        250.0,
        250.0,
        1,
        250.0,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_TIME,
        "Avg",
        250.0);
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_TIME,
        "Max",
        250.0);
  }

  @Test
  public void testRecordMultipleResetAttempts() {
    stats.recordErrorPartitionResetAttempt(3.0, TEST_STORE_NAME);
    stats.recordErrorPartitionResetAttempt(2.0, TEST_STORE_NAME);

    // OTel: accumulated = 3 + 2 = 5
    validateCounter(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ATTEMPT_COUNT.getMetricName(),
        5,
        clusterAndStoreAttributes());

    // Tehuti: Total = 5
    validateTehutiMetric(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT,
        "Total",
        5.0);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    ErrorPartitionStats disabledStats = new ErrorPartitionStats(disabledRepo, TEST_CLUSTER_NAME);

    disabledStats.recordErrorPartitionResetAttempt(1.0, TEST_STORE_NAME);
    disabledStats.recordErrorPartitionResetAttemptErrored(TEST_STORE_NAME);
    disabledStats.recordErrorPartitionProcessingError();
    disabledStats.recordErrorPartitionRecoveredFromReset(TEST_STORE_NAME);
    disabledStats.recordErrorPartitionUnrecoverableFromReset(TEST_STORE_NAME);
    disabledStats.recordErrorPartitionProcessingTime(100.0);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    ErrorPartitionStats plainStats = new ErrorPartitionStats(plainRepo, TEST_CLUSTER_NAME);

    plainStats.recordErrorPartitionResetAttempt(1.0, TEST_STORE_NAME);
    plainStats.recordErrorPartitionResetAttemptErrored(TEST_STORE_NAME);
    plainStats.recordErrorPartitionProcessingError();
    plainStats.recordErrorPartitionRecoveredFromReset(TEST_STORE_NAME);
    plainStats.recordErrorPartitionUnrecoverableFromReset(TEST_STORE_NAME);
    plainStats.recordErrorPartitionProcessingTime(100.0);
  }

  @Test
  public void testErrorPartitionTehutiMetricNameEnum() {
    Map<ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT,
        "current_version_error_partition_reset_attempt");
    expectedNames.put(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT_ERRORED,
        "current_version_error_partition_reset_attempt_errored");
    expectedNames.put(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RECOVERED_FROM_RESET,
        "current_version_error_partition_recovered_from_reset");
    expectedNames.put(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_UNRECOVERABLE_FROM_RESET,
        "current_version_error_partition_unrecoverable_from_reset");
    expectedNames.put(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_ERROR,
        "error_partition_processing_error");
    expectedNames.put(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_TIME,
        "error_partition_processing_time");

    assertEquals(
        ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New ErrorPartitionTehutiMetricNameEnum values were added but not included in this test");

    for (ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum enumValue: ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testErrorPartitionOtelMetricEntity() {
    Map<ErrorPartitionStats.ErrorPartitionOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ATTEMPT_COUNT,
        new MetricEntity(
            "partition.error_partition.reset.attempt_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Total partitions reset across all operations",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    expectedMetrics.put(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_ERROR_COUNT,
        new MetricEntity(
            "partition.error_partition.reset.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed reset operations for a store",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    expectedMetrics.put(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_RECOVERED_PARTITION_COUNT,
        new MetricEntity(
            "partition.error_partition.reset.recovered_partition_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Partitions recovered after reset",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    expectedMetrics.put(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_RESET_UNRECOVERABLE_PARTITION_COUNT,
        new MetricEntity(
            "partition.error_partition.reset.unrecoverable_partition_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Partitions declared unrecoverable after hitting reset limit",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    expectedMetrics.put(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_ERROR_COUNT,
        new MetricEntity(
            "partition.error_partition.processing.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Unexpected failures in the error partition processing cycle",
            Utils.setOf(VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.ERROR_PARTITION_PROCESSING_TIME,
        new MetricEntity(
            "partition.error_partition.processing.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time for each complete error partition processing cycle",
            Utils.setOf(VENICE_CLUSTER_NAME)));

    assertEquals(
        ErrorPartitionStats.ErrorPartitionOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New ErrorPartitionOtelMetricEntity values were added but not included in this test");

    for (ErrorPartitionStats.ErrorPartitionOtelMetricEntity metric: ErrorPartitionStats.ErrorPartitionOtelMetricEntity
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

    // Verify all ErrorPartitionOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  private Attributes clusterAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private Attributes clusterAndStoreAttributes() {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
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
      ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum tehutiEnum,
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
