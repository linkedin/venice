package com.linkedin.venice.client.stats;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.Map;


public class ClientStats extends BasicClientStats {
  private final Lazy<Sensor> unhealthyRequestLatencySensor;
  private final Map<Integer, Sensor> httpStatusSensorMap = new VeniceConcurrentHashMap<>();
  private final Lazy<Sensor> requestRetryCountSensor;
  private final Lazy<Sensor> successRequestDuplicateKeyCountSensor;
  private final Lazy<Sensor> requestSerializationTime;
  private final Lazy<Sensor> requestSubmissionToResponseHandlingTime;
  private final Lazy<Sensor> responseDeserializationTime;
  private final Lazy<Sensor> responseDecompressionTimeSensor;
  private final Lazy<Sensor> streamingResponseTimeToReceiveFirstRecord;
  private final Lazy<Sensor> streamingResponseTimeToReceive50PctRecord;
  private final Lazy<Sensor> streamingResponseTimeToReceive90PctRecord;
  private final Lazy<Sensor> streamingResponseTimeToReceive95PctRecord;
  private final Lazy<Sensor> streamingResponseTimeToReceive99PctRecord;
  private final Lazy<Sensor> appTimedOutRequestSensor;
  private final Lazy<Sensor> appTimedOutRequestResultRatioSensor;
  private final Lazy<Sensor> clientFutureTimeoutSensor;
  private final Lazy<Sensor> retryRequestKeyCountSensor;
  private final Lazy<Sensor> retryRequestSuccessKeyCountSensor;
  private final Lazy<Sensor> retryKeySuccessRatioSensor;
  /**
   * Tracks the number of keys handled via MultiGet fallback mechanism for Client-Compute.
   */
  private final Lazy<Sensor> multiGetFallbackSensor;

  public static ClientStats getClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientConfig clientConfig) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ? storeName : prefix + "." + storeName;
    return new ClientStats(metricsRepository, metricName, requestType);
  }

  protected ClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate requestRetryCountRate = new OccurrenceRate();

    requestRetryCountSensor = Lazy.of(() -> registerSensor("request_retry_count", requestRetryCountRate));
    unhealthyRequestLatencySensor =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("unhealthy_request_latency", new Avg()));
    successRequestDuplicateKeyCountSensor =
        Lazy.of(() -> registerSensor("success_request_duplicate_key_count", new Rate()));
    /**
     * The time it took to serialize the request, to be sent to the router. This is done in a blocking fashion
     * on the caller's thread.
     */
    requestSerializationTime =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("request_serialization_time", new Avg(), new Max()));

    /**
     * The time it took between sending the request to the router and beginning to process the response.
     */
    requestSubmissionToResponseHandlingTime = Lazy.of(
        () -> registerSensorWithDetailedPercentiles(
            "request_submission_to_response_handling_time",
            new Avg(),
            new Max()));

    /**
     * The total time it took to process the response.
     */
    responseDeserializationTime =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_deserialization_time", new Avg(), new Max()));

    responseDecompressionTimeSensor =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_decompression_time", new Avg(), new Max()));

    /**
     * Metrics to track the latency of each proportion of results received.
     */
    streamingResponseTimeToReceiveFirstRecord =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_ttfr", new Avg()));
    streamingResponseTimeToReceive50PctRecord =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_tt50pr", new Avg()));
    streamingResponseTimeToReceive90PctRecord =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_tt90pr", new Avg()));
    streamingResponseTimeToReceive95PctRecord =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_tt95pr", new Avg()));
    streamingResponseTimeToReceive99PctRecord =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("response_tt99pr", new Avg()));

    /**
     * Metrics to track the timed-out requests.
     * Just to be aware of that the timeout request here is not actually D2 timeout, but just the timeout when Venice
     * customers are retrieving Venice response in this way:
     * client.streamingBatchGet(keys).get(timeout, unit);
     *
     * This timeout behavior could actually happen before the D2 timeout, which is specified/configured in a different way.
     */
    appTimedOutRequestSensor = Lazy.of(() -> registerSensor("app_timed_out_request", new OccurrenceRate()));
    appTimedOutRequestResultRatioSensor = Lazy.of(
        () -> registerSensorWithDetailedPercentiles(
            "app_timed_out_request_result_ratio",
            new Avg(),
            new Min(),
            new Max()));
    clientFutureTimeoutSensor = Lazy.of(() -> registerSensor("client_future_timeout", new Avg(), new Min(), new Max()));
    /* Metrics relevant to track long tail retry efficacy for batch get*/
    Rate retryRequestKeyCount = new Rate();
    retryRequestKeyCountSensor =
        Lazy.of(() -> registerSensor("retry_request_key_count", retryRequestKeyCount, new Avg(), new Max()));
    Rate retryRequestSuccessKeyCount = new Rate();
    retryRequestSuccessKeyCountSensor = Lazy
        .of(() -> registerSensor("retry_request_success_key_count", retryRequestSuccessKeyCount, new Avg(), new Max()));
    retryKeySuccessRatioSensor = Lazy.of(
        () -> registerSensor(
            new TehutiUtils.SimpleRatioStat(
                retryRequestSuccessKeyCount,
                getSuccessRequestKeyCountRate(),
                "retry_key_success_ratio")));
    multiGetFallbackSensor = Lazy.of(() -> registerSensor("multiget_fallback", new OccurrenceRate()));
  }

  public void recordHttpRequest(int httpStatus) {
    httpStatusSensorMap
        .computeIfAbsent(httpStatus, status -> registerSensor("http_" + httpStatus + "_request", new OccurrenceRate()))
        .record();
  }

  public void recordUnhealthyLatency(double latency) {
    unhealthyRequestLatencySensor.get().record(latency);
  }

  public void recordRequestRetryCount() {
    requestRetryCountSensor.get().record();
  }

  public void recordSuccessDuplicateRequestKeyCount(int duplicateKeyCount) {
    successRequestDuplicateKeyCountSensor.get().record(duplicateKeyCount);
  }

  public void recordRequestSerializationTime(double latency) {
    requestSerializationTime.get().record(latency);
  }

  public void recordRequestSubmissionToResponseHandlingTime(double latency) {
    requestSubmissionToResponseHandlingTime.get().record(latency);
  }

  public void recordResponseDeserializationTime(double latency) {
    responseDeserializationTime.get().record(latency);
  }

  public void recordResponseDecompressionTime(double latency) {
    responseDecompressionTimeSensor.get().record(latency);
  }

  public void recordStreamingResponseTimeToReceiveFirstRecord(double latency) {
    streamingResponseTimeToReceiveFirstRecord.get().record(latency);
  }

  public void recordStreamingResponseTimeToReceive50PctRecord(double latency) {
    streamingResponseTimeToReceive50PctRecord.get().record(latency);
  }

  public void recordStreamingResponseTimeToReceive90PctRecord(double latency) {
    streamingResponseTimeToReceive90PctRecord.get().record(latency);
  }

  public void recordStreamingResponseTimeToReceive95PctRecord(double latency) {
    streamingResponseTimeToReceive95PctRecord.get().record(latency);
  }

  public void recordStreamingResponseTimeToReceive99PctRecord(double latency) {
    streamingResponseTimeToReceive99PctRecord.get().record(latency);
  }

  public void recordAppTimedOutRequest() {
    appTimedOutRequestSensor.get().record();
  }

  public void recordAppTimedOutRequestResultRatio(double ratio) {
    appTimedOutRequestResultRatioSensor.get().record(ratio);
  }

  public void recordClientFutureTimeout(long clientFutureTimeout) {
    clientFutureTimeoutSensor.get().record(clientFutureTimeout);
  }

  public void recordRetryRequestKeyCount(int numberOfKeysSentInRetryRequest) {
    retryRequestKeyCountSensor.get().record(numberOfKeysSentInRetryRequest);
  }

  public void recordRetryRequestSuccessKeyCount(int numberOfKeysCompletedInRetryRequest) {
    retryRequestSuccessKeyCountSensor.get().record(numberOfKeysCompletedInRetryRequest);
  }

  public void recordMultiGetFallback(int keyCount) {
    multiGetFallbackSensor.get().record(keyCount);
  }
}
