package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.client.stats.ClientStats.THIN_CLIENT_METRIC_PREFIX;
import static com.linkedin.venice.client.stats.ClientStats.THIN_CLIENT_SERVICE_NAME;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.getExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.getLongPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateLongPointData;
import static org.apache.http.HttpStatus.SC_OK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BasicClientStatsTest {
  @Test
  public void testMetricPrefix() {
    String storeName = "test_store";
    VeniceMetricsRepository metricsRepository1 =
        getVeniceMetricsRepository(THIN_CLIENT_SERVICE_NAME, THIN_CLIENT_METRIC_PREFIX, CLIENT_METRIC_ENTITIES, true);
    // Without prefix
    ClientConfig config1 = new ClientConfig(storeName);
    BasicClientStats.getClientStats(metricsRepository1, storeName, SINGLE_GET, config1);
    // Check metric name
    assertTrue(metricsRepository1.metrics().size() > 0);
    String metricPrefix1 = "." + storeName;
    metricsRepository1.metrics().forEach((k, v) -> {
      assertTrue(k.startsWith(metricPrefix1));
    });

    // With prefix
    String prefix = "test_prefix";
    VeniceMetricsRepository metricsRepository2 =
        getVeniceMetricsRepository(THIN_CLIENT_SERVICE_NAME, THIN_CLIENT_METRIC_PREFIX, CLIENT_METRIC_ENTITIES, true);
    ClientConfig config2 = new ClientConfig(storeName).setStatsPrefix(prefix);
    BasicClientStats.getClientStats(metricsRepository2, storeName, SINGLE_GET, config2);
    // Check metric name
    assertTrue(metricsRepository2.metrics().size() > 0);
    String metricPrefix2 = "." + prefix + "_" + storeName;
    metricsRepository2.metrics().forEach((k, v) -> {
      assertTrue(k.startsWith(metricPrefix2));
    });
  }

  @Test
  public void testEmitHealthyMetrics() {
    String storeName = "test_store";
    String otelPrefix = "test_prefix";
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(storeName, otelPrefix, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    BasicClientStats stats =
        BasicClientStats.getClientStats(metricsRepository, storeName, SINGLE_GET, new ClientConfig(storeName));

    String metricPrefix = "." + storeName;

    int keyCount = 2;
    double latency = 90.0;

    stats.emitHealthyRequestMetrics(latency, keyCount);

    // validate tehuti metrics
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--healthy_request.OccurrenceRate");
    Metric healthyLatencyMetric = metrics.get(metricPrefix + "--healthy_request_latency.Avg");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(healthyLatencyMetric.value(), 90.0);

    // validate otel metrics
    Attributes expectedAttributes = getExpectedAttributes(storeName, SC_OK, VeniceResponseStatusCategory.SUCCESS);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    Assert.assertFalse(metricsData.isEmpty(), "Metrics should not be empty");
    Assert.assertEquals(metricsData.size(), 2, "There should be two metrics recorded");
    LongPointData callCountData = getLongPointData(metricsData, "call_count", otelPrefix);
    validateLongPointData(callCountData, 1, expectedAttributes);
    ExponentialHistogramPointData callTimeData = getExponentialHistogramPointData(metricsData, "call_time", otelPrefix);
    validateExponentialHistogramPointData(callTimeData, 90.0, 90.0, 1, 90.0, expectedAttributes);
  }

  @Test
  public void testEmitUnhealthyMetrics() {
    String storeName = "test_store";
    String otelPrefix = "test_prefix";
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(storeName, otelPrefix, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    BasicClientStats stats =
        BasicClientStats.getClientStats(metricsRepository, storeName, SINGLE_GET, new ClientConfig(storeName));

    String metricPrefix = "." + storeName;

    int httpStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    double latency = 90.0;

    stats.emitUnhealthyRequestMetrics(latency, httpStatus);

    // validate tehuti metrics
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--unhealthy_request.OccurrenceRate");
    Metric unhealthyLatencyMetric = metrics.get(metricPrefix + "--unhealthy_request_latency.Avg");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(unhealthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyLatencyMetric.value(), 90.0);

    // validate otel metrics
    Attributes expectedAttributes = getExpectedAttributes(storeName, httpStatus, VeniceResponseStatusCategory.FAIL);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    Assert.assertFalse(metricsData.isEmpty(), "Metrics should not be empty");
    Assert.assertEquals(metricsData.size(), 2, "There should be two metrics recorded");
    LongPointData callCountData = getLongPointData(metricsData, "call_count", otelPrefix);
    validateLongPointData(callCountData, 1, expectedAttributes);
    ExponentialHistogramPointData callTimeData = getExponentialHistogramPointData(metricsData, "call_time", otelPrefix);
    validateExponentialHistogramPointData(callTimeData, 90.0, 90.0, 1, 90.0, expectedAttributes);
  }

  @Test
  public void testClientMetricEntities() {
    Map<BasicClientStats.BasicClientMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.CALL_COUNT,
        new MetricEntity(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests during response handling along with response codes",
            Utils.setOf(
                VENICE_STORE_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.CALL_TIME,
        new MetricEntity(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency based on all responses",
            Utils.setOf(
                VENICE_STORE_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));

    for (BasicClientStats.BasicClientMetricEntity metric: BasicClientStats.BasicClientMetricEntity.values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertNotNull(actual.getMetricName(), "Metric name should not be null for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertNotNull(actual.getMetricType(), "Metric type should not be null for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertNotNull(actual.getUnit(), "Metric unit should not be null for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertNotNull(actual.getDescription(), "Metric description should not be null for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertNotNull(actual.getDimensionsList(), "Metric dimensions should not be null for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }

    // Convert expectedMetrics to a Collection for comparison
    Collection<MetricEntity> expectedMetricEntities = expectedMetrics.values();

    // Assert size
    assertEquals(
        CLIENT_METRIC_ENTITIES.size(),
        expectedMetricEntities.size(),
        "Unexpected size of CLIENT_METRIC_ENTITIES");

    // Assert contents
    for (MetricEntity actual: CLIENT_METRIC_ENTITIES) {
      boolean found = false;
      for (MetricEntity expected: expectedMetricEntities) {
        if (metricEntitiesEqual(actual, expected)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Unexpected MetricEntity found: " + actual.getMetricName());
    }
  }

  private boolean metricEntitiesEqual(MetricEntity actual, MetricEntity expected) {
    return Objects.equals(actual.getMetricName(), expected.getMetricName())
        && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
        && Objects.equals(actual.getDescription(), expected.getDescription())
        && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList());
  }

  private Attributes getExpectedAttributes(
      String storeName,
      int httpStatus,
      VeniceResponseStatusCategory veniceStatusCategory) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(),
            transformIntToHttpResponseStatusEnum(httpStatus).getDimensionValue())
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            getVeniceHttpResponseStatusCodeCategory(httpStatus).getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            veniceStatusCategory.getDimensionValue())
        .build();
  }
}
