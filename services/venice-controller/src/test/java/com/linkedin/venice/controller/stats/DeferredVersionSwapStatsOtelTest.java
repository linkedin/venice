package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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


public class DeferredVersionSwapStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private InMemoryMetricReader inMemoryMetricReader;
  private DeferredVersionSwapStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new DeferredVersionSwapStats(metricsRepository);
  }

  @Test
  public void testRecordDeferredVersionSwapProcessingError() {
    // Both error and throwable flow to the same OTel metric
    stats.recordDeferredVersionSwapExceptionMetric(TEST_CLUSTER_NAME);
    stats.recordDeferredVersionSwapThrowableMetric(TEST_CLUSTER_NAME);
    validateCounter(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT
            .getMetricName(),
        2,
        clusterAttributes());
  }

  @Test
  public void testRecordDeferredVersionSwapFailedRollForward() {
    stats.recordDeferredVersionSwapFailedRollForwardMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_ROLL_FORWARD_FAILURE_COUNT
            .getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordDeferredVersionSwapStalledVersionSwap() {
    stats.recordDeferredVersionSwapStalledVersionSwapMetric(5);
    validateGauge(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_STALLED_COUNT
            .getMetricName(),
        5,
        Attributes.empty());
  }

  @Test
  public void testRecordDeferredVersionSwapParentChildStatusMismatch() {
    stats.recordDeferredVersionSwapParentChildStatusMismatchMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PARENT_STATUS_MISMATCH_COUNT
            .getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordDeferredVersionSwapChildStatusMismatch() {
    stats.recordDeferredVersionSwapChildStatusMismatchMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    validateCounter(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_COUNT
            .getMetricName(),
        1,
        clusterAndStoreAttributes());
  }

  @Test
  public void testRecordMultipleProcessingErrors() {
    stats.recordDeferredVersionSwapExceptionMetric(TEST_CLUSTER_NAME);
    stats.recordDeferredVersionSwapExceptionMetric(TEST_CLUSTER_NAME);
    stats.recordDeferredVersionSwapThrowableMetric(TEST_CLUSTER_NAME);
    validateCounter(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT
            .getMetricName(),
        3,
        clusterAttributes());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    DeferredVersionSwapStats disabledStats = new DeferredVersionSwapStats(disabledRepo);

    // Should execute without NPE
    disabledStats.recordDeferredVersionSwapExceptionMetric(TEST_CLUSTER_NAME);
    disabledStats.recordDeferredVersionSwapThrowableMetric(TEST_CLUSTER_NAME);
    disabledStats.recordDeferredVersionSwapFailedRollForwardMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordDeferredVersionSwapStalledVersionSwapMetric(3);
    disabledStats.recordDeferredVersionSwapParentChildStatusMismatchMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    disabledStats.recordDeferredVersionSwapChildStatusMismatchMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    DeferredVersionSwapStats plainStats = new DeferredVersionSwapStats(plainRepo);

    // Should execute without NPE
    plainStats.recordDeferredVersionSwapExceptionMetric(TEST_CLUSTER_NAME);
    plainStats.recordDeferredVersionSwapThrowableMetric(TEST_CLUSTER_NAME);
    plainStats.recordDeferredVersionSwapFailedRollForwardMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordDeferredVersionSwapStalledVersionSwapMetric(3);
    plainStats.recordDeferredVersionSwapParentChildStatusMismatchMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
    plainStats.recordDeferredVersionSwapChildStatusMismatchMetric(TEST_CLUSTER_NAME, TEST_STORE_NAME);
  }

  @Test
  public void testDeferredVersionSwapTehutiMetricNameEnum() {
    Map<DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_ERROR,
        "deferred_version_swap_error");
    expectedNames.put(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_THROWABLE,
        "deferred_version_swap_throwable");
    expectedNames.put(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_FAILED_ROLL_FORWARD,
        "deferred_version_swap_failed_roll_forward");
    expectedNames.put(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_STALLED_VERSION_SWAP,
        "deferred_version_swap_stalled_version_swap");
    expectedNames.put(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_PARENT_CHILD_STATUS_MISMATCH,
        "deferred_version_swap_parent_child_status_mismatch");
    expectedNames.put(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH,
        "deferred_version_swap_child_status_mismatch");

    assertEquals(
        DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New DeferredVersionSwapTehutiMetricNameEnum values were added but not included in this test");

    for (DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum enumValue: DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testDeferredVersionSwapOtelMetricEntity() {
    Map<DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT,
        new MetricEntity(
            "deferred_version_swap.processing_error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of unexpected failures in the deferred version swap processing loop",
            Utils.setOf(VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_ROLL_FORWARD_FAILURE_COUNT,
        new MetricEntity(
            "deferred_version_swap.roll_forward.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of deferred version swap roll forward failures",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    expectedMetrics.put(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_STALLED_COUNT,
        MetricEntity.createWithNoDimensions(
            "deferred_version_swap.stalled_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Count of stalled deferred version swaps across all clusters"));
    expectedMetrics.put(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PARENT_STATUS_MISMATCH_COUNT,
        new MetricEntity(
            "deferred_version_swap.parent_status_mismatch_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of deferred version swap parent-child status mismatches",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    expectedMetrics.put(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_COUNT,
        new MetricEntity(
            "deferred_version_swap.child_status_mismatch_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of deferred version swap child status mismatches",
            Utils.setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));

    assertEquals(
        DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New DeferredVersionSwapOtelMetricEntity values were added but not included in this test");

    for (DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity metric: DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity
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

    // Verify all DeferredVersionSwapOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  private static Attributes clusterAndStoreAttributes() {
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

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
