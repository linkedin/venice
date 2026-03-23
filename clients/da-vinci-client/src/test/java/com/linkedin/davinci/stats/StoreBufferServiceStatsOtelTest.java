package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_BUFFER_SERVICE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreBufferServiceStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_A = "store_a";
  private static final String TEST_STORE_B = "store_b";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;

  @BeforeMethod
  public void setUp() throws IOException {
    inMemoryMetricReader = InMemoryMetricReader.create();
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(new MetricConfig(asyncGaugeExecutor))
            .build());
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
    if (asyncGaugeExecutor != null) {
      asyncGaugeExecutor.close();
    }
  }

  @Test
  public void testAsyncGaugeMemoryMetrics() {
    AtomicLong totalUsage = new AtomicLong(1000L);
    AtomicLong totalRemaining = new AtomicLong(9000L);
    AtomicLong maxPerWriter = new AtomicLong(500L);
    AtomicLong minPerWriter = new AtomicLong(100L);

    new StoreBufferServiceStats(
        metricsRepository,
        "StoreBufferServiceSorted",
        TEST_CLUSTER_NAME,
        true,
        totalUsage::get,
        totalRemaining::get,
        maxPerWriter::get,
        minPerWriter::get);

    Attributes expectedAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "sorted")
        .build();

    validateGauge("store_buffer.memory.used", 1000, expectedAttrs);
    validateGauge("store_buffer.memory.remaining", 9000, expectedAttrs);
    validateGauge("store_buffer.memory.used_per_writer.max", 500, expectedAttrs);
    validateGauge("store_buffer.memory.used_per_writer.min", 100, expectedAttrs);

    // Test live value updates
    totalUsage.set(2000L);
    totalRemaining.set(8000L);
    validateGauge("store_buffer.memory.used", 2000, expectedAttrs);
    validateGauge("store_buffer.memory.remaining", 8000, expectedAttrs);
  }

  @Test
  public void testUnsortedDimensionValue() {
    new StoreBufferServiceStats(
        metricsRepository,
        "StoreBufferServiceUnsorted",
        TEST_CLUSTER_NAME,
        false,
        () -> 500L,
        () -> 9500L,
        () -> 250L,
        () -> 50L);

    Attributes expectedAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "unsorted")
        .build();

    validateGauge("store_buffer.memory.used", 500, expectedAttrs);
  }

  @Test
  public void testProcessingLatencyHistogram() {
    StoreBufferServiceStats stats = createSortedStats();

    stats.recordInternalProcessingLatency(10, TEST_STORE_A);
    stats.recordInternalProcessingLatency(20, TEST_STORE_A);

    Attributes expectedAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "sorted")
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_A)
        .build();

    // MIN_MAX_COUNT_SUM_AGGREGATIONS -> validateHistogramPointData
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        10.0, // min
        20.0, // max
        2, // count
        30.0, // sum
        expectedAttrs,
        "store_buffer.record.processing.time",
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testProcessingErrorCounter() {
    StoreBufferServiceStats stats = createSortedStats();

    stats.recordInternalProcessingError(TEST_STORE_A);
    stats.recordInternalProcessingError(TEST_STORE_A);
    stats.recordInternalProcessingError(TEST_STORE_A);

    Attributes expectedAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "sorted")
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_A)
        .build();

    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        3,
        expectedAttrs,
        "store_buffer.record.processing.error_count",
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testMultiStoreIsolation() {
    StoreBufferServiceStats stats = createSortedStats();

    // Record to store A
    stats.recordInternalProcessingLatency(10, TEST_STORE_A);
    stats.recordInternalProcessingError(TEST_STORE_A);

    // Record to store B
    stats.recordInternalProcessingLatency(20, TEST_STORE_B);
    stats.recordInternalProcessingLatency(30, TEST_STORE_B);
    stats.recordInternalProcessingError(TEST_STORE_B);
    stats.recordInternalProcessingError(TEST_STORE_B);

    Attributes storeAAttrs = buildStoreAttrs(TEST_STORE_A);
    Attributes storeBAttrs = buildStoreAttrs(TEST_STORE_B);

    // Store A: 1 latency recording
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        10.0,
        10.0,
        1,
        10.0,
        storeAAttrs,
        "store_buffer.record.processing.time",
        TEST_METRIC_PREFIX);

    // Store B: 2 latency recordings
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        20.0,
        30.0,
        2,
        50.0,
        storeBAttrs,
        "store_buffer.record.processing.time",
        TEST_METRIC_PREFIX);

    // Store A: 1 error
    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        storeAAttrs,
        "store_buffer.record.processing.error_count",
        TEST_METRIC_PREFIX);

    // Store B: 2 errors
    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        2,
        storeBAttrs,
        "store_buffer.record.processing.error_count",
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testBackwardsCompatibleNoArgRecording() {
    StoreBufferServiceStats stats = createSortedStats();

    // No-arg methods should use "unknown_store" as store name
    stats.recordInternalProcessingLatency(15);
    stats.recordInternalProcessingError();

    Attributes expectedAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "sorted")
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "unknown_store")
        .build();

    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        15.0,
        15.0,
        1,
        15.0,
        expectedAttrs,
        "store_buffer.record.processing.time",
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        expectedAttrs,
        "store_buffer.record.processing.error_count",
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository otelDisabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .build());
    try {
      exerciseAllRecordingPaths(otelDisabledRepo);
    } finally {
      otelDisabledRepo.close();
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() throws Exception {
    AsyncGauge.AsyncGaugeExecutor executor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    MetricsRepository plainRepo = new MetricsRepository(new MetricConfig(executor));
    try {
      exerciseAllRecordingPaths(plainRepo);
    } finally {
      plainRepo.close();
      executor.close();
    }
  }

  @Test
  public void testNullStoreNameSanitized() {
    StoreBufferServiceStats stats = createSortedStats();

    // null store name should be sanitized to "unknown_store"
    stats.recordInternalProcessingLatency(5, null);
    stats.recordInternalProcessingError(null);

    Attributes expectedAttrs = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "sorted")
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "unknown_store")
        .build();

    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        5.0,
        5.0,
        1,
        5.0,
        expectedAttrs,
        "store_buffer.record.processing.time",
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        expectedAttrs,
        "store_buffer.record.processing.error_count",
        TEST_METRIC_PREFIX);
  }

  /**
   * Cross-system consistency test: records via joint API and validates BOTH Tehuti sensor values
   * AND OTel metric values from the same recording calls. Catches bugs where joint API binding
   * is broken (Tehuti records but OTel doesn't, or vice versa).
   */
  @Test
  public void testTehutiAndOtelConsistency() {
    StoreBufferServiceStats stats = createSortedStats();

    stats.recordInternalProcessingLatency(25, TEST_STORE_A);
    stats.recordInternalProcessingLatency(75, TEST_STORE_A);
    stats.recordInternalProcessingError(TEST_STORE_A);

    // Verify Tehuti sensors (shared across all stores — single global sensor via registerSensorIfAbsent)
    // Format: .{statsName}--{tehutiMetricName}.{StatTypeSuffix}
    Metric tehutiAvg = metricsRepository.getMetric(".StoreBufferServiceSorted--internal_processing_latency.Avg");
    assertNotNull(tehutiAvg, "Tehuti Avg sensor should be registered");
    assertEquals(tehutiAvg.value(), 50.0, "Tehuti Avg should be (25+75)/2 = 50.0");

    Metric tehutiMax = metricsRepository.getMetric(".StoreBufferServiceSorted--internal_processing_latency.Max");
    assertNotNull(tehutiMax, "Tehuti Max sensor should be registered");
    assertEquals(tehutiMax.value(), 75.0, "Tehuti Max should be 75.0");

    // Tehuti OccurrenceRate is time-windowed events/sec; OTel COUNTER is cumulative count.
    // We verify the Tehuti sensor is registered (non-null) but don't assert an exact rate value
    // since it depends on wall-clock timing. OTel counter is validated with exact count below.
    Metric tehutiError =
        metricsRepository.getMetric(".StoreBufferServiceSorted--internal_processing_error.OccurrenceRate");
    assertNotNull(tehutiError, "Tehuti OccurrenceRate sensor should be registered for errors");

    // Verify OTel (per-store with dimensions)
    Attributes storeAAttrs = buildStoreAttrs(TEST_STORE_A);
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        25.0,
        75.0,
        2,
        100.0,
        storeAAttrs,
        "store_buffer.record.processing.time",
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        storeAAttrs,
        "store_buffer.record.processing.error_count",
        TEST_METRIC_PREFIX);
  }

  private static void exerciseAllRecordingPaths(MetricsRepository repo) {
    StoreBufferServiceStats stats = new StoreBufferServiceStats(
        repo,
        "StoreBufferServiceSorted",
        TEST_CLUSTER_NAME,
        true,
        () -> 100L,
        () -> 900L,
        () -> 50L,
        () -> 10L);
    stats.recordInternalProcessingLatency(10, "test-store");
    stats.recordInternalProcessingError("test-store");
    stats.recordInternalProcessingLatency(20);
    stats.recordInternalProcessingError();
  }

  private StoreBufferServiceStats createSortedStats() {
    return new StoreBufferServiceStats(
        metricsRepository,
        "StoreBufferServiceSorted",
        TEST_CLUSTER_NAME,
        true,
        () -> 100L,
        () -> 900L,
        () -> 50L,
        () -> 10L);
  }

  private Attributes buildStoreAttrs(String storeName) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_BUFFER_SERVICE_TYPE.getDimensionNameInDefaultFormat(), "sorted")
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .build();
  }

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttrs) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttrs,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
