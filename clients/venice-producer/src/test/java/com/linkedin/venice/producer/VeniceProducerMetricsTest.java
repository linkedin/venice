package com.linkedin.venice.producer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.Test;


/**
 * Tests for {@link VeniceProducerMetrics} to verify all sensors are registered
 * and recording correctly.
 */
public class VeniceProducerMetricsTest {
  private static final String TEST_STORE = "test-store";

  // Metric name patterns
  private static final String WRITE_OPERATION_METRIC = ".test-store--write_operation.OccurrenceRate";
  private static final String PUT_OPERATION_METRIC = ".test-store--put_operation.OccurrenceRate";
  private static final String DELETE_OPERATION_METRIC = ".test-store--delete_operation.OccurrenceRate";
  private static final String UPDATE_OPERATION_METRIC = ".test-store--update_operation.OccurrenceRate";
  private static final String SUCCESS_OPERATION_METRIC = ".test-store--success_write_operation.OccurrenceRate";
  private static final String FAILED_OPERATION_METRIC = ".test-store--failed_write_operation.OccurrenceRate";
  private static final String PENDING_OPERATION_GAUGE_METRIC = ".test-store--pending_write_operation.Gauge";
  private static final String PREPROCESSING_LATENCY_50_METRIC = ".test-store--preprocessing_latency.50thPercentile";
  private static final String PRODUCE_LATENCY_50_METRIC =
      ".test-store--produce_to_durable_buffer_latency.50thPercentile";
  private static final String END_TO_END_LATENCY_50_METRIC = ".test-store--end_to_end_latency.50thPercentile";

  @Test
  public void testAllSensorsRegistered() {
    MetricsRepository metricsRepository = new MetricsRepository();
    // Constructor registers sensors as side effect
    new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Verify existing sensors
    assertNotNull(metricsRepository.getMetric(WRITE_OPERATION_METRIC), "Write operation metric should be registered");
    assertNotNull(metricsRepository.getMetric(PUT_OPERATION_METRIC), "Put operation metric should be registered");
    assertNotNull(metricsRepository.getMetric(DELETE_OPERATION_METRIC), "Delete operation metric should be registered");
    assertNotNull(metricsRepository.getMetric(UPDATE_OPERATION_METRIC), "Update operation metric should be registered");
    assertNotNull(
        metricsRepository.getMetric(SUCCESS_OPERATION_METRIC),
        "Success operation metric should be registered");
    assertNotNull(metricsRepository.getMetric(FAILED_OPERATION_METRIC), "Failed operation metric should be registered");
    assertNotNull(
        metricsRepository.getMetric(PENDING_OPERATION_GAUGE_METRIC),
        "Pending operation gauge metric should be registered");
    assertNotNull(
        metricsRepository.getMetric(PREPROCESSING_LATENCY_50_METRIC),
        "Preprocessing latency metric should be registered");
    assertNotNull(
        metricsRepository.getMetric(PRODUCE_LATENCY_50_METRIC),
        "Produce latency metric should be registered");

    // Verify new sensors
    assertNotNull(
        metricsRepository.getMetric(END_TO_END_LATENCY_50_METRIC),
        "End-to-end latency metric should be registered");
  }

  @Test
  public void testPreprocessingLatencyRecorded() {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Record preprocessing latency
    metrics.recordPreprocessingLatency(100);
    metrics.recordPreprocessingLatency(200);
    metrics.recordPreprocessingLatency(300);

    // Verify latency is recorded (value depends on percentile calculation)
    double latency = metricsRepository.getMetric(PREPROCESSING_LATENCY_50_METRIC).value();
    assertTrue(latency >= 100 && latency <= 300, "Preprocessing latency should be in expected range");
  }

  @Test
  public void testProduceLatencyRecorded() {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Simulate successful request with latency
    metrics.recordPutRequest();
    metrics.recordSuccessfulRequestWithLatency(150);

    // Verify latency is recorded (percentile calculation may have slight variance)
    double latency = metricsRepository.getMetric(PRODUCE_LATENCY_50_METRIC).value();
    assertTrue(latency >= 140 && latency <= 160, "Produce latency should be approximately 150ms, got: " + latency);
  }

  @Test
  public void testEndToEndLatencyRecorded() {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Record end-to-end latency
    metrics.recordEndToEndLatency(500);
    metrics.recordEndToEndLatency(600);
    metrics.recordEndToEndLatency(700);

    // Verify latency is recorded
    double latency = metricsRepository.getMetric(END_TO_END_LATENCY_50_METRIC).value();
    assertTrue(latency >= 500 && latency <= 700, "End-to-end latency should be in expected range");
  }

  @Test
  public void testQueueSizeGaugesRegisteredWithExecutor() {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Create executor
    PartitionedProducerExecutor executor = new PartitionedProducerExecutor(4, 100, 2, 100, TEST_STORE, null);

    try {
      // Register queue metrics
      metrics.registerQueueMetrics(executor);

      // Verify queue size gauges are registered (AsyncGauge registers with Gauge suffix)
      assertNotNull(
          metricsRepository.getMetric(".test-store--total_worker_queue_size.Gauge"),
          "Total worker queue size metric should be registered");
      assertNotNull(
          metricsRepository.getMetric(".test-store--callback_queue_size.Gauge"),
          "Callback queue size metric should be registered");
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testPendingOperationsTracked() {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Record multiple requests
    metrics.recordPutRequest();
    metrics.recordPutRequest();
    metrics.recordDeleteRequest();

    // Gauge should show current pending count of 3
    double pendingCount = metricsRepository.getMetric(PENDING_OPERATION_GAUGE_METRIC).value();
    assertEquals(pendingCount, 3.0, "Pending operations gauge should be 3");

    // Complete requests
    metrics.recordSuccessfulRequestWithLatency(100);
    metrics.recordSuccessfulRequestWithLatency(100);
    metrics.recordFailedRequest();

    // Gauge should be 0 after completing all requests
    pendingCount = metricsRepository.getMetric(PENDING_OPERATION_GAUGE_METRIC).value();
    assertEquals(pendingCount, 0.0, "Pending operations gauge should be 0 after completing");
  }

  @Test
  public void testMetricsDisabledWhenNoRepository() {
    // Create metrics with null repository
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(null, TEST_STORE);

    // These should not throw exceptions
    metrics.recordPutRequest();
    metrics.recordDeleteRequest();
    metrics.recordUpdateRequest();
    metrics.recordSuccessfulRequestWithLatency(100);
    metrics.recordFailedRequest();
    metrics.recordPreprocessingLatency(50);
    metrics.recordEndToEndLatency(200);

    // Register queue metrics with null executor should also not throw
    metrics.registerQueueMetrics(null);
  }

  @Test
  public void testOperationRatesRecorded() {
    MetricsRepository metricsRepository = new MetricsRepository();
    VeniceProducerMetrics metrics = new VeniceProducerMetrics(metricsRepository, TEST_STORE);

    // Record various operations
    metrics.recordPutRequest();
    metrics.recordPutRequest();
    metrics.recordDeleteRequest();
    metrics.recordUpdateRequest();
    metrics.recordSuccessfulRequestWithLatency(100);
    metrics.recordSuccessfulRequestWithLatency(100);
    metrics.recordSuccessfulRequestWithLatency(100);
    metrics.recordFailedRequest();

    // Verify rates are being recorded (rates are non-zero)
    assertTrue(metricsRepository.getMetric(WRITE_OPERATION_METRIC).value() > 0, "Write operation rate should be > 0");
    assertTrue(metricsRepository.getMetric(PUT_OPERATION_METRIC).value() > 0, "Put operation rate should be > 0");
    assertTrue(metricsRepository.getMetric(DELETE_OPERATION_METRIC).value() > 0, "Delete operation rate should be > 0");
    assertTrue(metricsRepository.getMetric(UPDATE_OPERATION_METRIC).value() > 0, "Update operation rate should be > 0");
    assertTrue(
        metricsRepository.getMetric(SUCCESS_OPERATION_METRIC).value() > 0,
        "Success operation rate should be > 0");
    assertTrue(metricsRepository.getMetric(FAILED_OPERATION_METRIC).value() > 0, "Failed operation rate should be > 0");
  }
}
