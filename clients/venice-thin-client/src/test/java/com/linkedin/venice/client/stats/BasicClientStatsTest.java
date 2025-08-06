package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.client.stats.ClientMetricEntity.RETRY_KEY_COUNT;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.DAVINCI_CLIENT;
import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum;
import static com.linkedin.venice.stats.dimensions.MessageType.*;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateLongPointDataFromCounter;
import static org.apache.http.HttpStatus.SC_OK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.httpclient.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BasicClientStatsTest {
  @Test
  public void testMetricPrefix() {
    String storeName = "test_store";
    VeniceMetricsRepository metricsRepository1 = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true);
    // Without prefix
    ClientConfig config1 = new ClientConfig(storeName);
    BasicClientStats.getClientStats(metricsRepository1, storeName, SINGLE_GET, config1, ClientType.THIN_CLIENT);
    // Check metric name
    assertTrue(metricsRepository1.metrics().size() > 0);
    String metricPrefix1 = "." + storeName;
    metricsRepository1.metrics().forEach((k, v) -> {
      assertTrue(k.startsWith(metricPrefix1));
    });

    // With prefix
    String prefix = "test_prefix";
    VeniceMetricsRepository metricsRepository2 = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true);
    ClientConfig config2 = new ClientConfig(storeName).setStatsPrefix(prefix);
    BasicClientStats.getClientStats(metricsRepository2, storeName, SINGLE_GET, config2, ClientType.THIN_CLIENT);
    // Check metric name
    assertTrue(metricsRepository2.metrics().size() > 0);
    String metricPrefix2 = "." + prefix + "_" + storeName;
    metricsRepository2.metrics().forEach((k, v) -> {
      assertTrue(k.startsWith(metricPrefix2));
    });
  }

  @Test
  public void testEmitHealthyMetrics() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    BasicClientStats stats = createStats(inMemoryMetricReader, THIN_CLIENT);
    stats.emitHealthyRequestMetrics(90.0, 2);

    validateTehutiMetrics(stats.getMetricsRepository(), ".test_store", true, 90.0);
    validateOtelMetrics(inMemoryMetricReader, "test_store", SC_OK, SUCCESS, 90.0, THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testEmitHealthyRequestMetricsForDavinciClient() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    BasicClientStats stats = createStats(inMemoryMetricReader, DAVINCI_CLIENT);
    stats.emitHealthyRequestMetricsForDavinciClient(90.0);

    validateTehutiMetrics(stats.getMetricsRepository(), ".test_store", true, 90.0);
    validateOtelMetrics(inMemoryMetricReader, "test_store", SUCCESS, 90.0, DAVINCI_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testEmitHealthyRequestMetricsForDavinciClientWithWrongClientType() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    BasicClientStats stats = createStats(inMemoryMetricReader, THIN_CLIENT);
    stats.emitHealthyRequestMetricsForDavinciClient(90.0);
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    Assert.assertFalse(metrics.get(".test_store--request.OccurrenceRate").value() > 0.0);
  }

  @Test
  public void testEmitUnhealthyMetrics() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    BasicClientStats stats = createStats(inMemoryMetricReader, THIN_CLIENT);
    stats.emitUnhealthyRequestMetrics(90.0, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    validateTehutiMetrics(stats.getMetricsRepository(), ".test_store", false, 90.0);
    validateOtelMetrics(
        inMemoryMetricReader,
        "test_store",
        HttpStatus.SC_INTERNAL_SERVER_ERROR,
        VeniceResponseStatusCategory.FAIL,
        90.0,
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testEmitUnhealthyMetricsForDavinciClient() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    BasicClientStats stats = createStats(inMemoryMetricReader, DAVINCI_CLIENT);
    stats.emitUnhealthyRequestMetricsForDavinciClient(90.0);

    validateTehutiMetrics(stats.getMetricsRepository(), ".test_store", false, 90.0);
    validateOtelMetrics(
        inMemoryMetricReader,
        "test_store",
        VeniceResponseStatusCategory.FAIL,
        90.0,
        DAVINCI_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testEmitUnhealthyRequestMetricsForDavinciClientWithWrongClientType() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    BasicClientStats stats = createStats(inMemoryMetricReader, THIN_CLIENT);
    stats.emitUnhealthyRequestMetricsForDavinciClient(90.0);
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    Assert.assertFalse(metrics.get(".test_store--request.OccurrenceRate").value() > 0.0);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testKeyCountMetrics(boolean isRequest) {
    for (ClientType client: ClientType.values()) {
      // verify that the following works for all client types.
      InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
      BasicClientStats stats = createStats(inMemoryMetricReader, client);

      int keyCount = 10;

      if (isRequest) {
        stats.recordRequestKeyCount(keyCount);
      } else {
        stats.recordResponseKeyCount(keyCount);
      }

      // Check Tehuti metrics
      Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
      String storeName = "test_store";
      if (isRequest) {
        Assert
            .assertEquals((int) metrics.get(String.format(".%s--request_key_count.Max", storeName)).value(), keyCount);
      } else {
        Assert.assertEquals(
            (int) metrics.get(String.format(".%s--success_request_key_count.Max", storeName)).value(),
            keyCount);
      }

      // Check OpenTelemetry metrics
      Attributes expectedAttr = getAttributes(storeName, isRequest ? REQUEST : RESPONSE);
      validateExponentialHistogramPointData(
          inMemoryMetricReader,
          keyCount,
          keyCount,
          1,
          keyCount,
          expectedAttr,
          "key_count",
          client.getMetricsPrefix());
    }
  }

  @Test
  public void testEmitRequestRetryMetrics() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    ClientStats stats = createClientStats(inMemoryMetricReader, THIN_CLIENT);
    stats.recordErrorRetryRequest();
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    Assert.assertTrue(metrics.get(".test_store--request_retry_count.OccurrenceRate").value() > 0);
    validateOtelMetrics(
        inMemoryMetricReader,
        "test_store",
        RequestRetryType.ERROR_RETRY,
        THIN_CLIENT.getMetricsPrefix(),
        1,
        "retry_count",
        1);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRetryKeyCountMetrics(boolean isRequest) {
    for (ClientType client: ClientType.values()) {
      // verify that the following works for all client types.
      InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
      ClientStats stats = createClientStats(inMemoryMetricReader, client);

      int keyCount = 10;

      if (isRequest) {
        stats.recordRetryRequestKeyCount(keyCount);
      } else {
        stats.recordRetryRequestSuccessKeyCount(keyCount);
      }

      // Check Tehuti metrics
      Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
      String storeName = "test_store";
      if (isRequest) {
        Assert.assertEquals(
            (int) metrics.get(String.format(".%s--retry_request_key_count.Max", storeName)).value(),
            keyCount);
        Assert.assertEquals(
            (int) metrics.get(String.format(".%s--retry_request_key_count.Avg", storeName)).value(),
            keyCount);
      } else {
        Assert.assertEquals(
            (int) metrics.get(String.format(".%s--retry_request_success_key_count.Max", storeName)).value(),
            keyCount);
        Assert.assertEquals(
            (int) metrics.get(String.format(".%s--retry_request_success_key_count.Avg", storeName)).value(),
            keyCount);
      }

      // Check OpenTelemetry metrics
      Attributes expectedAttr = getAttributes(storeName, isRequest ? REQUEST : RESPONSE);
      validateHistogramPointData(
          inMemoryMetricReader,
          keyCount,
          keyCount,
          1,
          keyCount,
          expectedAttr,
          RETRY_KEY_COUNT.getMetricEntity().getMetricName(),
          client.getMetricsPrefix());
    }
  }

  private BasicClientStats createStats(InMemoryMetricReader inMemoryMetricReader, ClientType clientType) {
    String storeName = "test_store";
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(clientType, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    return BasicClientStats
        .getClientStats(metricsRepository, storeName, SINGLE_GET, new ClientConfig(storeName), clientType);
  }

  private ClientStats createClientStats(InMemoryMetricReader inMemoryMetricReader, ClientType clientType) {
    String storeName = "test_store";
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(clientType, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    return ClientStats
        .getClientStats(metricsRepository, storeName, SINGLE_GET, new ClientConfig(storeName), clientType);
  }

  private void validateTehutiMetrics(
      MetricsRepository metricsRepository,
      String metricPrefix,
      boolean healthy,
      double expectedLatency) {
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    Assert.assertTrue(metrics.get(metricPrefix + "--request.OccurrenceRate").value() > 0.0);

    String type = healthy ? "healthy" : "unhealthy";
    Assert.assertTrue(metrics.get(metricPrefix + "--" + type + "_request.OccurrenceRate").value() > 0.0);
    Assert.assertEquals(metrics.get(metricPrefix + "--" + type + "_request_latency.Avg").value(), expectedLatency);
  }

  private void validateOtelMetrics(
      InMemoryMetricReader inMemoryMetricReader,
      String storeName,
      int httpStatus,
      VeniceResponseStatusCategory category,
      double latency,
      String otelPrefix) {
    Attributes expectedAttributes = getExpectedAttributes(storeName, httpStatus, category);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertEquals(metricsData.size(), 2, "There should be two metrics recorded: call_time and call_count");

    validateLongPointDataFromCounter(inMemoryMetricReader, 1, expectedAttributes, "call_count", otelPrefix);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        latency,
        latency,
        1,
        latency,
        expectedAttributes,
        "call_time",
        otelPrefix);
  }

  private void validateOtelMetrics(
      InMemoryMetricReader inMemoryMetricReader,
      String storeName,
      VeniceResponseStatusCategory category,
      double latency,
      String otelPrefix) {
    // Overload for Davinci client where httpStatus is not applicable
    validateOtelMetrics(inMemoryMetricReader, storeName, -1, category, latency, otelPrefix);
  }

  private void validateOtelMetrics(
      InMemoryMetricReader inMemoryMetricReader,
      String storeName,
      RequestRetryType retryType,
      String otelPrefix,
      int expectedDataSize,
      String expectedMetricName,
      long expectedValue) {
    Attributes expectedAttributes = getExpectedAttributes(storeName, retryType);
    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    assertEquals(
        metricsData.size(),
        expectedDataSize,
        String.format("There should be %d metrics recorded", expectedDataSize));

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        expectedMetricName,
        otelPrefix);
  }

  @Test
  public void testClientMetricEntities() {
    Map<ModuleMetricEntityInterface, MetricEntity> expectedMetrics = new HashMap<>();
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
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.CALL_COUNT_DVC,
        new MetricEntity(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all DaVinci Client requests",
            Utils.setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.CALL_TIME_DVC,
        new MetricEntity(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency for all DaVinci Client responses",
            Utils.setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    expectedMetrics.put(
        BasicClientStats.BasicClientMetricEntity.KEY_COUNT,
        new MetricEntity(
            "key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Count of keys for venice client request and response",
            Utils.setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)));
    expectedMetrics.put(
        ClientMetricEntity.RETRY_COUNT,
        new MetricEntity(
            "retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all retry requests for client",
            Utils.setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)));
    expectedMetrics.put(
        RETRY_KEY_COUNT,
        new MetricEntity(
            "retry_key_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Key count of retry requests for client",
            Utils.setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)));

    Set<String> uniqueMetricEntitiesNames = new HashSet<>();

    // Verify BasicClientMetricEntity.
    for (BasicClientStats.BasicClientMetricEntity metric: BasicClientStats.BasicClientMetricEntity.values()) {
      MetricEntity entity = metric.getMetricEntity();
      uniqueMetricEntitiesNames.add(entity.getMetricName());
      verifyMetricEntity(entity, expectedMetrics.get(metric), entity.getMetricName());
    }

    // Verify ClientMetricEntity.
    for (ClientMetricEntity metric: ClientMetricEntity.values()) {
      MetricEntity entity = metric.getMetricEntity();
      uniqueMetricEntitiesNames.add(entity.getMetricName());
      verifyMetricEntity(entity, expectedMetrics.get(metric), entity.getMetricName());
    }

    // Convert expectedMetrics to a Collection for comparison
    Collection<MetricEntity> expectedMetricEntities = expectedMetrics.values();

    // Assert size
    assertEquals(
        CLIENT_METRIC_ENTITIES.size(),
        uniqueMetricEntitiesNames.size(),
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

  private void verifyMetricEntity(MetricEntity actual, MetricEntity expected, String name) {
    assertNotNull(expected, "No expected definition for " + name);
    assertNotNull(actual.getMetricName(), "Metric name should not be null for " + name);
    assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + name);
    assertNotNull(actual.getMetricType(), "Metric type should not be null for " + name);
    assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + name);
    assertNotNull(actual.getUnit(), "Metric unit should not be null for " + name);
    assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + name);
    assertNotNull(actual.getDescription(), "Metric description should not be null for " + name);
    assertEquals(actual.getDescription(), expected.getDescription(), "Unexpected metric description for " + name);
    assertNotNull(actual.getDimensionsList(), "Metric dimensions should not be null for " + name);
    assertEquals(actual.getDimensionsList(), expected.getDimensionsList(), "Unexpected metric dimensions for " + name);
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
    return getExpectedAttributes(storeName, httpStatus, veniceStatusCategory, null);
  }

  private Attributes getExpectedAttributes(String storeName, RequestRetryType retryType) {
    return getExpectedAttributes(storeName, -1, null, retryType);
  }

  private Attributes getExpectedAttributes(
      String storeName,
      int httpStatus,
      VeniceResponseStatusCategory veniceStatusCategory,
      RequestRetryType retryType) {
    AttributesBuilder builder = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue());
    if (veniceStatusCategory != null) {
      builder.put(
          VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
          veniceStatusCategory.getDimensionValue());
    }
    if (httpStatus > -1) {
      builder
          .put(
              HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(),
              transformIntToHttpResponseStatusEnum(httpStatus).getDimensionValue())
          .put(
              HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
              getVeniceHttpResponseStatusCodeCategory(httpStatus).getDimensionValue());
    }
    if (retryType != null) {
      builder.put(VENICE_REQUEST_RETRY_TYPE.getDimensionNameInDefaultFormat(), retryType.getDimensionValue());
    }
    return builder.build();
  }

  private Attributes getAttributes(String storeName, MessageType type) {
    AttributesBuilder builder = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(VENICE_MESSAGE_TYPE.getDimensionNameInDefaultFormat(), type.getDimensionValue());
    return builder.build();
  }
}
