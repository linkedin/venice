package com.linkedin.venice.client.stats;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.deserialization.BatchDeserializer;
import com.linkedin.venice.client.store.deserialization.BatchDeserializerType;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;

import io.tehuti.metrics.stats.Rate;
import java.util.Map;


public class ClientStats extends AbstractVeniceHttpStats {
  private final Sensor requestSensor;
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor healthyRequestLatencySensor;
  private final Sensor unhealthyRequestLatencySensor;
  private final Map<Integer, Sensor> httpStatusSensorMap = new VeniceConcurrentHashMap<>();
  private final Sensor requestKeyCountSensor;
  private final Sensor requestRetryCountSensor;
  private final Sensor successRequestKeyCountSensor;
  private final Sensor successRequestDuplicateKeyCountSensor;
  private final Sensor successRequestKeyRatioSensor;
  private final Sensor successRequestRatioSensor;
  private final Sensor requestSerializationTime;
  private final Sensor requestSubmissionToResponseHandlingTime;
  private final Sensor responseDeserializationTime;
  private final Sensor responseEnvelopeDeserializationTime;
  private final Sensor responseRecordsDeserializationTime;
  private final Sensor responseRecordsDeserializationSubmissionToStartTime;
  private final Sensor responseDecompressionTimeSensor;
  private final Sensor streamingResponseTimeToReceiveFirstRecord;
  private final Sensor streamingResponseTimeToReceive50PctRecord;
  private final Sensor streamingResponseTimeToReceive90PctRecord;
  private final Sensor streamingResponseTimeToReceive95PctRecord;
  private final Sensor streamingResponseTimeToReceive99PctRecord;
  private final Sensor appTimedOutRequestSensor;
  private final Sensor appTimedOutRequestResultRatioSensor;
  private final Sensor clientFutureTimeoutSensor;

  public static ClientStats getClientStats(MetricsRepository metricsRepository, String storeName,
      RequestType requestType, ClientConfig clientConfig) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ?  storeName : prefix + "." + storeName;
    return new ClientStats(metricsRepository, metricName, requestType);
  }

  private ClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate request = new OccurrenceRate();
    Rate healthyRequest = new OccurrenceRate();
    Rate requestRetryCount = new OccurrenceRate();

    requestSensor = registerSensor("request", request);
    healthySensor = registerSensor("healthy_request", healthyRequest);
    requestRetryCountSensor = registerSensor("request_retry_count", requestRetryCount);
    unhealthySensor = registerSensor("unhealthy_request", new OccurrenceRate());
    healthyRequestLatencySensor = registerSensorWithDetailedPercentiles("healthy_request_latency", new Avg());
    unhealthyRequestLatencySensor = registerSensorWithDetailedPercentiles("unhealthy_request_latency", new Avg());

    successRequestRatioSensor = registerSensor("success_request_ratio",
        new TehutiUtils.SimpleRatioStat(healthyRequest, request));
    Rate requestKeyCount = new Rate();
    Rate successRequestKeyCount = new Rate();
    requestKeyCountSensor = registerSensor("request_key_count", requestKeyCount, new Avg(), new Max());
    successRequestKeyCountSensor = registerSensor("success_request_key_count", successRequestKeyCount,
        new Avg(), new Max());
    successRequestDuplicateKeyCountSensor = registerSensor("success_request_duplicate_key_count", new Rate());
    successRequestKeyRatioSensor = registerSensor("success_request_key_ratio",
        new TehutiUtils.SimpleRatioStat(successRequestKeyCount, requestKeyCount));
    /**
     * The time it took to serialize the request, to be sent to the router. This is done in a blocking fashion
     * on the caller's thread.
     */
    requestSerializationTime = registerSensorWithDetailedPercentiles("request_serialization_time", new Avg(), new Max());

    /**
     * The time it took between sending the request to the router and beginning to process the response.
     */
    requestSubmissionToResponseHandlingTime = registerSensorWithDetailedPercentiles("request_submission_to_response_handling_time", new Avg(), new Max());

    /**
     * The total time it took to process the response.
     *
     * For {@link BatchDeserializer} implementations
     * other than {@link BatchDeserializerType.BLOCKING},
     * this metric is also further broken down into sub-steps (see below).
     */
    responseDeserializationTime = registerSensorWithDetailedPercentiles("response_deserialization_time", new Avg(), new Max());

    /**
     * The time it took to iterate over the {@link com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1}
     * envelopes of the raw response.
     */
    responseEnvelopeDeserializationTime = registerSensorWithDetailedPercentiles("response_envelope_deserialization_time", new Avg(), new Max());

    /**
     * The time it took to deserialize the user's records contained inside the raw envelopes. This is measured
     * starting from just before deserializing the first record, until finishing to deserialize the last one.
     */
    responseRecordsDeserializationTime = registerSensorWithDetailedPercentiles("response_records_deserialization_time", new Avg(), new Max());

    /**
     * The time it took between beginning to fork off asynchronous tasks and starting to deserialize a record.
     */
    responseRecordsDeserializationSubmissionToStartTime = registerSensorWithDetailedPercentiles("response_records_deserialization_submission_to_start_time", new Avg(), new Max());

    responseDecompressionTimeSensor = registerSensorWithDetailedPercentiles("response_decompression_time", new Avg(), new Max());

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
    appTimedOutRequestResultRatioSensor = registerSensorWithDetailedPercentiles("app_timed_out_request_result_ratio",
        new Avg(), new Min(), new Max());
    clientFutureTimeoutSensor = registerSensor("client_future_timeout", new Avg(), new Min(), new Max());
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordHealthyRequest() {
    recordRequest();
    healthySensor.record();
  }

  public void recordUnhealthyRequest() {
    recordRequest();
    unhealthySensor.record();
  }

  public void recordHttpRequest(int httpStatus) {
    httpStatusSensorMap.computeIfAbsent(httpStatus,
        status -> registerSensor("http_" + httpStatus + "_request", new OccurrenceRate()))
    .record();
  }

  public void recordHealthyLatency(double latency) {
    healthyRequestLatencySensor.record(latency);
  }

  public void recordUnhealthyLatency(double latency) {
    unhealthyRequestLatencySensor.record(latency);
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCountSensor.record(keyCount);
  }

  public void recordRequestRetryCount() {
    requestRetryCountSensor.record();
  }

  public void recordSuccessRequestKeyCount(int successKeyCount) {
    successRequestKeyCountSensor.record(successKeyCount);
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

  public void recordResponseEnvelopeDeserializationTime(double latency) {
    responseEnvelopeDeserializationTime.record(latency);
  }

  public void recordResponseRecordsDeserializationTime(double latency) {
    responseRecordsDeserializationTime.record(latency);
  }

  public void recordResponseRecordsDeserializationSubmissionToStartTime(double latency) {
    responseRecordsDeserializationSubmissionToStartTime.record(latency);
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

  public void recordClientFutureTimeout(long clientFutureTimeout){
    clientFutureTimeoutSensor.record(clientFutureTimeout);
  }
}
