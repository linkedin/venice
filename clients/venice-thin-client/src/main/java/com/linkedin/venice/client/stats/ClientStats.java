package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.ClientMetricEntity.RETRY_COUNT;
import static com.linkedin.venice.stats.dimensions.MessageType.REQUEST;
import static com.linkedin.venice.stats.dimensions.MessageType.RESPONSE;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.ERROR_RETRY;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.dimensions.Granularity;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;


/**
 * This class is responsible for tracking client side metrics.
 */
public class ClientStats extends BasicClientStats {
  private final Map<Integer, Sensor> httpStatusSensorMap = new VeniceConcurrentHashMap<>();
  private final Sensor successRequestDuplicateKeyCountSensor;
  private final Sensor appTimedOutRequestSensor;
  private final Sensor appTimedOutRequestResultRatioSensor;
  private final Sensor clientFutureTimeoutSensor;
  private final Sensor retryKeySuccessRatioSensor;
  /**
   * Tracks the number of keys handled via MultiGet fallback mechanism for Client-Compute.
   */
  private final Sensor multiGetFallbackSensor;

  private final MetricEntityStateOneEnum<RequestRetryType> errorRetryRequest;
  private final MetricEntityStateOneEnum<MessageType> retryKeyCount;
  private final MetricEntityStateOneEnum<MessageType> retrySuccessKeyCount;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> requestSerializationTime;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> requestSubmissionToResponseHandlingTime;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> responseDeserializationTime;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> responseDecompressionTime;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> streamingResponseTimeToReceiveFirstRecord;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> streamingResponseTimeToReceive50PctRecord;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> streamingResponseTimeToReceive90PctRecord;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> streamingResponseTimeToReceive95PctRecord;
  private final MetricEntityStateTwoEnums<Granularity, MessageType> streamingResponseTimeToReceive99PctRecord;

  public static ClientStats getClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientConfig clientConfig,
      ClientType clientType) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ? storeName : prefix + "." + storeName;
    return new ClientStats(metricsRepository, metricName, requestType, clientType);
  }

  protected ClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientType clientType) {
    super(metricsRepository, storeName, requestType, clientType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate requestRetryCountRate = new OccurrenceRate();

    errorRetryRequest = MetricEntityStateOneEnum.create(
        RETRY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClientTehutiMetricName.REQUEST_RETRY_COUNT,
        Collections.singletonList(requestRetryCountRate),
        baseDimensionsMap,
        RequestRetryType.class);

    successRequestDuplicateKeyCountSensor = registerSensor("success_request_duplicate_key_count", new Rate());

    /**
     * Metrics to track the timed-out requests.
     * Just to be aware of that the timeout request here is not actually D2 timeout, but just the timeout when Venice
     * customers are retrieving Venice response in this way:
     * client.streamingBatchGet(keys).get(timeout, unit);
     *
     * This timeout behavior could actually happen before the D2 timeout, which is specified/configured in a different way.
     */
    appTimedOutRequestSensor = registerSensor("app_timed_out_request", new OccurrenceRate());
    appTimedOutRequestResultRatioSensor =
        registerSensorWithDetailedPercentiles("app_timed_out_request_result_ratio", new Avg(), new Min(), new Max());
    clientFutureTimeoutSensor = registerSensor("client_future_timeout", new Avg(), new Min(), new Max());
    /* Metrics relevant to track long tail retry efficacy for batch get*/
    Rate retryRequestKeyCount = new Rate();
    Rate retryRequestSuccessKeyCount = new Rate();

    retryKeyCount = MetricEntityStateOneEnum.create(
        ClientMetricEntity.RETRY_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClientTehutiMetricName.RETRY_REQUEST_KEY_COUNT,
        Arrays.asList(retryRequestKeyCount, new Avg(), new Max()),
        baseDimensionsMap,
        MessageType.class);

    retrySuccessKeyCount = MetricEntityStateOneEnum.create(
        ClientMetricEntity.RETRY_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ClientTehutiMetricName.RETRY_REQUEST_SUCCESS_KEY_COUNT,
        Arrays.asList(retryRequestSuccessKeyCount, new Avg(), new Max()),
        baseDimensionsMap,
        MessageType.class);

    requestSerializationTime = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.REQUEST_SERIALIZATION_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    // The total time it took to process the response.
    responseDeserializationTime = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_DESERIALIZATION_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    responseDecompressionTime = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_DECOMPRESSION_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    // The time it took between sending the request to the router and beginning to process the response.
    requestSubmissionToResponseHandlingTime = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.REQUEST_SUBMISSION_TO_RESPONSE_HANDLING_TIME,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    // Metrics to track the latency of each proportion of results received.
    streamingResponseTimeToReceiveFirstRecord = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TTFR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    streamingResponseTimeToReceive50PctRecord = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TT50PR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    streamingResponseTimeToReceive90PctRecord = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TT90PR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    streamingResponseTimeToReceive95PctRecord = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TT95PR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    streamingResponseTimeToReceive99PctRecord = MetricEntityStateTwoEnums.create(
        ClientMetricEntity.GRANULARITY_CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        ClientTehutiMetricName.RESPONSE_TT99PR,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        Granularity.class,
        MessageType.class);

    retryKeySuccessRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(
            retryRequestSuccessKeyCount,
            getSuccessRequestKeyCountRate(),
            "retry_key_success_ratio"));
    multiGetFallbackSensor = registerSensor("multiget_fallback", new OccurrenceRate());
  }

  public void recordHttpRequest(int httpStatus) {
    httpStatusSensorMap
        .computeIfAbsent(httpStatus, status -> registerSensor("http_" + httpStatus + "_request", new OccurrenceRate()))
        .record();
  }

  public void recordErrorRetryRequest() {
    errorRetryRequest.record(1, ERROR_RETRY);
  }

  public void recordSuccessDuplicateRequestKeyCount(int duplicateKeyCount) {
    successRequestDuplicateKeyCountSensor.record(duplicateKeyCount);
  }

  public void recordRequestSerializationTime(double latency) {
    requestSerializationTime.record(latency, Granularity.SERIALIZATION, REQUEST);
  }

  public void recordRequestSubmissionToResponseHandlingTime(double latency) {
    requestSubmissionToResponseHandlingTime.record(latency, Granularity.SUBMISSION_TO_RESPONSE, REQUEST);
  }

  public void recordResponseDeserializationTime(double latency) {
    responseDeserializationTime.record(latency, Granularity.DESERIALIZATION, RESPONSE);
  }

  public void recordResponseDecompressionTime(double latency) {
    responseDecompressionTime.record(latency, Granularity.DECOMPRESSION, RESPONSE);
  }

  public void recordStreamingResponseTimeToReceiveFirstRecord(double latency) {
    streamingResponseTimeToReceiveFirstRecord.record(latency, Granularity.FIRST_RECORD, RESPONSE);
  }

  public void recordStreamingResponseTimeToReceive50PctRecord(double latency) {
    streamingResponseTimeToReceive50PctRecord.record(latency, Granularity.PCT_50_RECORD, RESPONSE);
  }

  public void recordStreamingResponseTimeToReceive90PctRecord(double latency) {
    streamingResponseTimeToReceive90PctRecord.record(latency, Granularity.PCT_90_RECORD, RESPONSE);
  }

  public void recordStreamingResponseTimeToReceive95PctRecord(double latency) {
    streamingResponseTimeToReceive95PctRecord.record(latency, Granularity.PCT_95_RECORD, RESPONSE);
  }

  public void recordStreamingResponseTimeToReceive99PctRecord(double latency) {
    streamingResponseTimeToReceive99PctRecord.record(latency, Granularity.PCT_99_RECORD, RESPONSE);
  }

  public void recordAppTimedOutRequest() {
    appTimedOutRequestSensor.record();
  }

  public void recordAppTimedOutRequestResultRatio(double ratio) {
    appTimedOutRequestResultRatioSensor.record(ratio);
  }

  public void recordClientFutureTimeout(long clientFutureTimeout) {
    clientFutureTimeoutSensor.record(clientFutureTimeout);
  }

  public void recordRetryRequestKeyCount(int numberOfKeysSentInRetryRequest) {
    retryKeyCount.record(numberOfKeysSentInRetryRequest, REQUEST);
  }

  public void recordRetryRequestSuccessKeyCount(int numberOfKeysCompletedInRetryRequest) {
    retrySuccessKeyCount.record(numberOfKeysCompletedInRetryRequest, RESPONSE);
  }

  public void recordMultiGetFallback(int keyCount) {
    multiGetFallbackSensor.record(keyCount);
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum ClientTehutiMetricName implements TehutiMetricNameEnum {
    REQUEST_RETRY_COUNT, RETRY_REQUEST_KEY_COUNT, RETRY_REQUEST_SUCCESS_KEY_COUNT, REQUEST_SERIALIZATION_TIME,
    RESPONSE_DESERIALIZATION_TIME, RESPONSE_DECOMPRESSION_TIME, REQUEST_SUBMISSION_TO_RESPONSE_HANDLING_TIME,
    RESPONSE_TTFR, RESPONSE_TT50PR, RESPONSE_TT90PR, RESPONSE_TT95PR, RESPONSE_TT99PR;

    private final String metricName;

    ClientTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
