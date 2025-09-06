package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateHistogramPointData;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
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

    validateHistogramPointData(
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

    validateHistogramPointData(
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

    validateHistogramPointData(
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

    validateHistogramPointData(
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
                .getDimensionName(VeniceMetricsDimensions.VENICE_DELIVERY_PROGRESS),
            "first")
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
                .getDimensionName(VeniceMetricsDimensions.VENICE_DELIVERY_PROGRESS),
            "50pct")
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
                .getDimensionName(VeniceMetricsDimensions.VENICE_DELIVERY_PROGRESS),
            "90pct")
        .build();

    String metricName = ClientMetricEntity.RESPONSE_BATCH_STREAM_PROGRESS_TIME.getMetricEntity().getMetricName();

    validateHistogramPointData(
        inMemoryMetricReader,
        11,
        11,
        1,
        11,
        ttfrAttrs,
        metricName,
        THIN_CLIENT.getMetricsPrefix());
    validateHistogramPointData(
        inMemoryMetricReader,
        22,
        22,
        1,
        22,
        tt50Attrs,
        metricName,
        THIN_CLIENT.getMetricsPrefix());
    validateHistogramPointData(
        inMemoryMetricReader,
        33,
        33,
        1,
        33,
        tt90Attrs,
        metricName,
        THIN_CLIENT.getMetricsPrefix());
  }
}
