package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ClientStatsTest {
  @Test
  public void testAppTimedOutRequestResultRatio() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    ClientStats stats = createClientStats(inMemoryMetricReader);

    double ratio = 0.25;
    stats.recordAppTimedOutRequestResultRatio(ratio);

    // Validate Tehuti
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    String storeName = "test_store";
    assertEquals(metrics.get(String.format(".%s--app_timed_out_request_result_ratio.Avg", storeName)).value(), ratio);
    assertEquals(metrics.get(String.format(".%s--app_timed_out_request_result_ratio.Min", storeName)).value(), ratio);
    assertEquals(metrics.get(String.format(".%s--app_timed_out_request_result_ratio.Max", storeName)).value(), ratio);

    // Validate OpenTelemetry
    Attributes expectedAttr = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(VENICE_MESSAGE_TYPE.getDimensionNameInDefaultFormat(), MessageType.REQUEST.getDimensionValue())
        .build();

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        ratio,
        ratio,
        1,
        ratio,
        expectedAttr,
        ClientMetricEntity.KEY_TIMEOUT_RATIO.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testClientFutureTimeout() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    ClientStats stats = createClientStats(inMemoryMetricReader);

    long timeout = 150L;
    stats.recordClientFutureTimeout(timeout);

    // Validate Tehuti
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    String storeName = "test_store";
    assertEquals(metrics.get(String.format(".%s--client_future_timeout.Avg", storeName)).value(), (double) timeout);
    assertEquals(metrics.get(String.format(".%s--client_future_timeout.Min", storeName)).value(), (double) timeout);
    assertEquals(metrics.get(String.format(".%s--client_future_timeout.Max", storeName)).value(), (double) timeout);

    // Validate OpenTelemetry
    Attributes expectedAttr = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        timeout,
        timeout,
        1,
        timeout,
        expectedAttr,
        ClientMetricEntity.CLIENT_TIMEOUT.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testSuccessRequestDuplicateKeyCount() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    ClientStats stats = createClientStats(inMemoryMetricReader);

    int duplicateKeyCount = 7;
    stats.recordSuccessDuplicateRequestKeyCount(duplicateKeyCount);

    // Validate Tehuti (Rate should be > 0 when recorded)
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    String storeName = "test_store";
    Assert.assertTrue(
        metrics.get(String.format(".%s--success_request_duplicate_key_count.Rate", storeName)).value() > 0.0);

    // Validate OpenTelemetry counter value and attributes
    Attributes expectedAttr = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(VENICE_MESSAGE_TYPE.getDimensionNameInDefaultFormat(), MessageType.REQUEST.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        duplicateKeyCount,
        expectedAttr,
        ClientMetricEntity.DUPLICATE_KEY_COUNT.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  private ClientStats createClientStats(InMemoryMetricReader inMemoryMetricReader) {
    String storeName = "test_store";
    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    return ClientStats
        .getClientStats(metricsRepository, storeName, SINGLE_GET, new ClientConfig(storeName), THIN_CLIENT);
  }
}
