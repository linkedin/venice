package com.linkedin.venice.producer;

import static com.linkedin.venice.producer.VeniceProducerMetrics.PRODUCER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.ONLINE_PRODUCER;
import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.DEFAULT_METRIC_PREFIX;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PRODUCER_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceProducerOperation.DELETE;
import static com.linkedin.venice.stats.dimensions.VeniceProducerOperation.PUT;
import static com.linkedin.venice.stats.dimensions.VeniceProducerOperation.UPDATE;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromGauge;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.producer.VeniceProducerMetrics.VeniceProducerTehutiMetricName;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceProducerOperation;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.function.BiConsumer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests that {@link VeniceProducerMetrics} emits both Tehuti and OpenTelemetry metrics when backed by an OpenTelemetry
 * enabled {@link VeniceMetricsRepository}.
 */
public class VeniceProducerMetricsOtelTest {
  private static final String STORE_NAME = "test_store";
  private static final String TEHUTI_PREFIX = "." + STORE_NAME + "--";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private VeniceProducerMetrics producerMetrics;
  private String otelMetricPrefix;

  @BeforeMethod
  public void setUp() {
    otelMetricPrefix = ONLINE_PRODUCER.getMetricsPrefix();
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository =
        getVeniceMetricsRepository(ONLINE_PRODUCER, PRODUCER_METRIC_ENTITIES, true, inMemoryMetricReader);
    producerMetrics = new VeniceProducerMetrics(metricsRepository, STORE_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testBaseAttributes() {
    assertEquals(
        producerMetrics.getBaseAttributes(),
        Attributes.builder().put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME).build());
  }

  @Test
  public void testCallCountByOperationAndOutcome() {
    producerMetrics.recordPutRequest();
    producerMetrics.recordPutRequest();
    producerMetrics.recordDeleteRequest();
    producerMetrics.recordUpdateRequest();
    producerMetrics.recordSuccessfulRequestWithLatency(PUT, 10);
    producerMetrics.recordSuccessfulRequestWithLatency(PUT, 20);
    producerMetrics.recordFailedRequest(DELETE);
    producerMetrics.recordFailedRequest(UPDATE);

    validateCallCount(PUT, SUCCESS, 2);
    validateCallCount(DELETE, FAIL, 1);
    validateCallCount(UPDATE, FAIL, 1);

    assertTehutiRateRecorded(VeniceProducerTehutiMetricName.SUCCESS_WRITE_OPERATION);
    assertTehutiRateRecorded(VeniceProducerTehutiMetricName.FAILED_WRITE_OPERATION);
    assertTehutiRateRecorded(VeniceProducerTehutiMetricName.WRITE_OPERATION);
    assertTehutiRateRecorded(VeniceProducerTehutiMetricName.PUT_OPERATION);
    assertTehutiRateRecorded(VeniceProducerTehutiMetricName.DELETE_OPERATION);
    assertTehutiRateRecorded(VeniceProducerTehutiMetricName.UPDATE_OPERATION);
  }

  @DataProvider(name = "latencyMetrics")
  public Object[][] latencyMetrics() {
    return new Object[][] {
        { VeniceProducerMetricEntity.PRODUCE_TO_DURABLE_BUFFER_TIME,
            VeniceProducerTehutiMetricName.PRODUCE_TO_DURABLE_BUFFER_LATENCY,
            (BiConsumer<VeniceProducerMetrics, Double>) (m, latency) -> m
                .recordSuccessfulRequestWithLatency(UPDATE, latency),
            null },
        { VeniceProducerMetricEntity.PREPROCESSING_TIME, VeniceProducerTehutiMetricName.PREPROCESSING_LATENCY,
            (BiConsumer<VeniceProducerMetrics, Double>) (m, latency) -> m.recordPreprocessingLatency(UPDATE, latency),
            null },
        { VeniceProducerMetricEntity.QUEUE_WAIT_TIME, VeniceProducerTehutiMetricName.QUEUE_WAIT_LATENCY,
            (BiConsumer<VeniceProducerMetrics, Double>) (m, latency) -> m.recordQueueWaitLatency(UPDATE, latency),
            null },
        { VeniceProducerMetricEntity.CALLER_TO_PUBSUB_PRODUCER_BUFFER_TIME,
            VeniceProducerTehutiMetricName.CALLER_TO_PUBSUB_PRODUCER_BUFFER_LATENCY,
            (BiConsumer<VeniceProducerMetrics, Double>) (m, latency) -> m
                .recordCallerToPubSubBufferLatency(UPDATE, latency),
            null },
        { VeniceProducerMetricEntity.CALL_TIME, VeniceProducerTehutiMetricName.END_TO_END_LATENCY,
            (BiConsumer<VeniceProducerMetrics, Double>) (m, latency) -> m.recordEndToEndLatency(UPDATE, latency),
            SUCCESS } };
  }

  @Test(dataProvider = "latencyMetrics")
  public void testLatencyMetrics(
      VeniceProducerMetricEntity metricEntity,
      VeniceProducerTehutiMetricName tehutiMetricName,
      BiConsumer<VeniceProducerMetrics, Double> recorder,
      VeniceResponseStatusCategory expectedStatusCategory) {
    recorder.accept(producerMetrics, 100.0);
    recorder.accept(producerMetrics, 300.0);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        100.0,
        300.0,
        2,
        400.0,
        expectedStatusCategory == null
            ? operationAttributes(UPDATE)
            : operationAndStatusAttributes(UPDATE, expectedStatusCategory),
        metricEntity.getMetricEntity().getMetricName(),
        otelMetricPrefix);

    // Tehuti percentiles are kept with the same names as before
    String tehutiMetric = TEHUTI_PREFIX + tehutiMetricName.getMetricName() + ".50thPercentile";
    assertNotNull(metricsRepository.getMetric(tehutiMetric), tehutiMetric + " should be registered");
    double p50 = metricsRepository.getMetric(tehutiMetric).value();
    assertTrue(p50 >= 100 && p50 <= 300, "Unexpected p50 for " + tehutiMetric + ": " + p50);
  }

  @Test
  public void testPendingCallCount() {
    producerMetrics.recordPutRequest();
    producerMetrics.recordDeleteRequest();
    producerMetrics.recordUpdateRequest();
    producerMetrics.recordSuccessfulRequestWithLatency(PUT, 10);

    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        2,
        producerMetrics.getBaseAttributes(),
        VeniceProducerMetricEntity.PENDING_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);
    assertEquals(
        metricsRepository.getMetric(TEHUTI_PREFIX + "pending_write_operation.Gauge").value(),
        2.0,
        "Tehuti pending gauge should be 2");
  }

  @Test
  public void testPendingCallCountAfterProducerIsRecreated() {
    producerMetrics.recordPutRequest();
    producerMetrics.recordPutRequest();
    producerMetrics.close();

    // A producer recreated for the same store on the same repository must report its own pending count
    VeniceProducerMetrics recreatedMetrics = new VeniceProducerMetrics(metricsRepository, STORE_NAME);
    recreatedMetrics.recordPutRequest();

    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        1,
        recreatedMetrics.getBaseAttributes(),
        VeniceProducerMetricEntity.PENDING_CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  @Test
  public void testOtelDisabled() {
    try (VeniceMetricsRepository tehutiOnlyRepository =
        getVeniceMetricsRepository(ONLINE_PRODUCER, PRODUCER_METRIC_ENTITIES, false)) {
      VeniceProducerMetrics metrics = new VeniceProducerMetrics(tehutiOnlyRepository, STORE_NAME);
      assertNull(metrics.getBaseAttributes());

      metrics.recordPutRequest();
      metrics.recordSuccessfulRequestWithLatency(PUT, 10);
      metrics.recordFailedRequest(PUT);
      metrics.recordEndToEndLatency(PUT, 10);

      assertTrue(tehutiOnlyRepository.getMetric(TEHUTI_PREFIX + "success_write_operation.OccurrenceRate").value() > 0);
      assertTrue(tehutiOnlyRepository.getMetric(TEHUTI_PREFIX + "failed_write_operation.OccurrenceRate").value() > 0);
      assertNotNull(tehutiOnlyRepository.getMetric(TEHUTI_PREFIX + "end_to_end_latency.50thPercentile"));
      metrics.close();
    }
  }

  @Test
  public void testOtelDisabledForRepositoryOfAnotherClientType() {
    InMemoryMetricReader sharedReader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository sharedRepository =
        getVeniceMetricsRepository(THIN_CLIENT, PRODUCER_METRIC_ENTITIES, true, sharedReader)) {
      VeniceProducerMetrics metrics = new VeniceProducerMetrics(sharedRepository, STORE_NAME);
      assertNull(metrics.getBaseAttributes());

      metrics.recordPutRequest();
      metrics.recordSuccessfulRequestWithLatency(PUT, 10);
      metrics.recordEndToEndLatency(PUT, 10);

      // Tehuti is still emitted, but no producer metric is mixed into the store client's OTel metrics
      assertTrue(sharedRepository.getMetric(TEHUTI_PREFIX + "success_write_operation.OccurrenceRate").value() > 0);
      assertFalse(
          sharedReader.collectAllMetrics()
              .stream()
              .anyMatch(
                  metricData -> metricData.getName()
                      .startsWith(DEFAULT_METRIC_PREFIX + THIN_CLIENT.getMetricsPrefix())),
          "Producer metrics should not be emitted under the store client's metric prefix");
      metrics.close();
    }
  }

  private void validateCallCount(
      VeniceProducerOperation operation,
      VeniceResponseStatusCategory statusCategory,
      long expectedCount) {
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedCount,
        operationAndStatusAttributes(operation, statusCategory),
        VeniceProducerMetricEntity.CALL_COUNT.getMetricEntity().getMetricName(),
        otelMetricPrefix);
  }

  private Attributes operationAttributes(VeniceProducerOperation operation) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_PRODUCER_OPERATION.getDimensionNameInDefaultFormat(), operation.getDimensionValue())
        .build();
  }

  private Attributes operationAndStatusAttributes(
      VeniceProducerOperation operation,
      VeniceResponseStatusCategory statusCategory) {
    return Attributes.builder()
        .putAll(operationAttributes(operation))
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), statusCategory.getDimensionValue())
        .build();
  }

  private void assertTehutiRateRecorded(VeniceProducerTehutiMetricName tehutiMetricName) {
    String metricName = TEHUTI_PREFIX + tehutiMetricName.getMetricName() + ".OccurrenceRate";
    assertNotNull(metricsRepository.getMetric(metricName), metricName + " should be registered");
    assertTrue(metricsRepository.getMetric(metricName).value() > 0, metricName + " should be > 0");
  }
}
