package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.StreamProgress;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ClientStatsTest {
  private static final String STORE = "test_store";
  private static final RequestType REQUEST_TYPE = RequestType.MULTI_GET_STREAMING;
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ClientStats clientStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = VeniceMetricsRepository
        .getVeniceMetricsRepository(THIN_CLIENT, BasicClientStats.CLIENT_METRIC_ENTITIES, true, inMemoryMetricReader);
    clientStats = ClientStats.getClientStats(metricsRepository, STORE, REQUEST_TYPE, null, THIN_CLIENT);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  private Attributes baseAttributes() {
    return Attributes.builder()
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STORE_NAME),
            STORE)
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_REQUEST_METHOD),
            REQUEST_TYPE.getDimensionValue())
        .build();
  }

  @Test
  public void testRequestSerializationTimeOtel() {
    clientStats.recordRequestSerializationTime(10);
    clientStats.recordRequestSerializationTime(20);
    clientStats.recordRequestSerializationTime(30);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        10,
        30,
        3,
        60,
        baseAttributes(),
        // entity name is enum name lowercased
        ClientMetricEntity.REQUEST_SERIALIZATION_TIME.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testResponseDecompressionTimeOtel() {
    clientStats.recordResponseDecompressionTime(7);
    clientStats.recordResponseDecompressionTime(13);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        7,
        13,
        2,
        20,
        baseAttributes(),
        ClientMetricEntity.RESPONSE_DECOMPRESSION_TIME.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testResponseDeserializationTimeOtel() {
    clientStats.recordResponseDeserializationTime(5);
    clientStats.recordResponseDeserializationTime(15);
    clientStats.recordResponseDeserializationTime(25);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        5,
        25,
        3,
        45,
        baseAttributes(),
        ClientMetricEntity.RESPONSE_DESERIALIZATION_TIME.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testCallSubmissionToHandlingTimeOtel() {
    clientStats.recordRequestSubmissionToResponseHandlingTime(3);
    clientStats.recordRequestSubmissionToResponseHandlingTime(9);

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        3,
        9,
        2,
        12,
        baseAttributes(),
        ClientMetricEntity.CALL_SUBMISSION_TO_HANDLING_TIME.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testResponseBatchStreamProgressTimeOtel() {
    // TTFR
    clientStats.recordStreamingResponseTimeToReceiveFirstRecord(11);
    // TT50PR
    clientStats.recordStreamingResponseTimeToReceive50PctRecord(22);
    // TT90PR
    clientStats.recordStreamingResponseTimeToReceive90PctRecord(33);

    // base attributes + delivery.progress for FIRST
    Attributes ttfrAttrs = Attributes.builder()
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STORE_NAME),
            STORE)
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_REQUEST_METHOD),
            REQUEST_TYPE.getDimensionValue())
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STREAM_PROGRESS),
            StreamProgress.FIRST.getDimensionValue())
        .build();

    Attributes tt50Attrs = Attributes.builder()
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STORE_NAME),
            STORE)
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_REQUEST_METHOD),
            REQUEST_TYPE.getDimensionValue())
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STREAM_PROGRESS),
            StreamProgress.PCT_50.getDimensionValue())
        .build();

    Attributes tt90Attrs = Attributes.builder()
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STORE_NAME),
            STORE)
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_REQUEST_METHOD),
            REQUEST_TYPE.getDimensionValue())
        .put(
            metricsRepository.getOpenTelemetryMetricsRepository()
                .getDimensionName(VeniceMetricsDimensions.VENICE_STREAM_PROGRESS),
            StreamProgress.PCT_90.getDimensionValue())
        .build();

    String metricName = ClientMetricEntity.RESPONSE_BATCH_STREAM_PROGRESS_TIME.getMetricEntity().getMetricName();

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        11,
        11,
        1,
        11,
        ttfrAttrs,
        metricName,
        THIN_CLIENT.getMetricsPrefix());
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        22,
        22,
        1,
        22,
        tt50Attrs,
        metricName,
        THIN_CLIENT.getMetricsPrefix());
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        33,
        33,
        1,
        33,
        tt90Attrs,
        metricName,
        THIN_CLIENT.getMetricsPrefix());
  }

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
        .build();

    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        ratio,
        ratio,
        1,
        ratio,
        expectedAttr,
        ClientMetricEntity.REQUEST_TIMEOUT_PARTIAL_RESPONSE_RATIO.getMetricEntity().getMetricName(),
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
        ClientMetricEntity.REQUEST_TIMEOUT_REQUESTED_DURATION.getMetricEntity().getMetricName(),
        THIN_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testAppTimedOutRequestCount() {
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    ClientStats stats = createClientStats(inMemoryMetricReader);

    stats.recordAppTimedOutRequest();

    // Validate Tehuti (Rate should be > 0 when recorded)
    Map<String, ? extends Metric> metrics = stats.getMetricsRepository().metrics();
    String storeName = "test_store";
    Assert.assertTrue(metrics.get(String.format(".%s--app_timed_out_request.OccurrenceRate", storeName)).value() > 0.0);

    // Validate OpenTelemetry counter value and attributes
    Attributes expectedAttr = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        expectedAttr,
        ClientMetricEntity.REQUEST_TIMEOUT_COUNT.getMetricEntity().getMetricName(),
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
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        duplicateKeyCount,
        expectedAttr,
        ClientMetricEntity.REQUEST_DUPLICATE_KEY_COUNT.getMetricEntity().getMetricName(),
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
