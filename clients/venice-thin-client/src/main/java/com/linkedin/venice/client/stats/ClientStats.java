package com.linkedin.venice.client.stats;

import static com.linkedin.venice.client.stats.ClientMetricEntity.RETRY_COUNT;
import static com.linkedin.venice.stats.dimensions.MessageType.REQUEST;
import static com.linkedin.venice.stats.dimensions.MessageType.RESPONSE;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.ERROR_RETRY;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
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


public class ClientStats extends BasicClientStats {
  private final Map<Integer, Sensor> httpStatusSensorMap = new VeniceConcurrentHashMap<>();
  private final Sensor successRequestDuplicateKeyCountSensor;
  private final Sensor requestSerializationTime;
  private final Sensor requestSubmissionToResponseHandlingTime;
  private final Sensor responseDeserializationTime;
  private final Sensor responseDecompressionTimeSensor;
  private final Sensor streamingResponseTimeToReceiveFirstRecord;
  private final Sensor streamingResponseTimeToReceive50PctRecord;
  private final Sensor streamingResponseTimeToReceive90PctRecord;
  private final Sensor streamingResponseTimeToReceive95PctRecord;
  private final Sensor streamingResponseTimeToReceive99PctRecord;
  private final Sensor appTimedOutRequestSensor;
  private final Sensor appTimedOutRequestResultRatioSensor;
  private final Sensor clientFutureTimeoutSensor;
  private final Sensor retryKeySuccessRatioSensor;
  /**
   * Tracks the number of keys handled via MultiGet fallback mechanism for Client-Compute.
   */
  private final Sensor multiGetFallbackSensor;

  private MetricEntityStateOneEnum<RequestRetryType> errorRetryRequest;
  private MetricEntityStateOneEnum<MessageType> retryKeyCount;
  private MetricEntityStateOneEnum<MessageType> retrySuccessKeyCount;

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
     * The time it took to serialize the request, to be sent to the router. This is done in a blocking fashion
     * on the caller's thread.
     */
    requestSerializationTime =
        registerSensorWithDetailedPercentiles("request_serialization_time", new Avg(), new Max());

    /**
     * The time it took between sending the request to the router and beginning to process the response.
     */
    requestSubmissionToResponseHandlingTime =
        registerSensorWithDetailedPercentiles("request_submission_to_response_handling_time", new Avg(), new Max());

    /**
     * The total time it took to process the response.
     */
    responseDeserializationTime =
        registerSensorWithDetailedPercentiles("response_deserialization_time", new Avg(), new Max());

    responseDecompressionTimeSensor =
        registerSensorWithDetailedPercentiles("response_decompression_time", new Avg(), new Max());

    /**
     * Metrics to track the latency of each proportion of results received.
     */
    streamingResponseTimeToReceiveFirstRecord = registerSensorWithDetailedPercentiles("response_ttfr", new Avg());
    streamingResponseTimeToReceive50PctRecord = registerSensorWithDetailedPercentiles("response_tt50pr", new Avg());
    streamingResponseTimeToReceive90PctRecord = registerSensorWithDetailedPercentiles("response_tt90pr", new Avg());
    streamingResponseTimeToReceive95PctRecord = registerSensorWithDetailedPercentiles("response_tt95pr", new Avg());
    streamingResponseTimeToReceive99PctRecord = registerSensorWithDetailedPercentiles("response_tt99pr", new Avg());

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
    requestSerializationTime.record(latency);
  }

  public void recordRequestSubmissionToResponseHandlingTime(double latency) {
    requestSubmissionToResponseHandlingTime.record(latency);
  }

  public void recordResponseDeserializationTime(double latency) {
    responseDeserializationTime.record(latency);
  }

  public void recordResponseDecompressionTime(double latency) {
    responseDecompressionTimeSensor.record(latency);
  }

  public void recordStreamingResponseTimeToReceiveFirstRecord(double latency) {
    streamingResponseTimeToReceiveFirstRecord.record(latency);
  }

  public void recordStreamingResponseTimeToReceive50PctRecord(double latency) {
    streamingResponseTimeToReceive50PctRecord.record(latency);
  }

  public void recordStreamingResponseTimeToReceive90PctRecord(double latency) {
    streamingResponseTimeToReceive90PctRecord.record(latency);
  }

  public void recordStreamingResponseTimeToReceive95PctRecord(double latency) {
    streamingResponseTimeToReceive95PctRecord.record(latency);
  }

  public void recordStreamingResponseTimeToReceive99PctRecord(double latency) {
    streamingResponseTimeToReceive99PctRecord.record(latency);
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
    REQUEST_RETRY_COUNT, RETRY_REQUEST_KEY_COUNT, RETRY_REQUEST_SUCCESS_KEY_COUNT;

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
