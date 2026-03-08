package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
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
