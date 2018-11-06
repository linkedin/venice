package com.linkedin.venice.client.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;

import io.tehuti.metrics.stats.Rate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ClientStats extends AbstractVeniceHttpStats {
  private final Sensor requestSensor;
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor healthyRequestLatencySensor;
  private final Sensor unhealthyRequestLatencySensor;
  private final Map<Integer, Sensor> httpStatusSensorMap = new ConcurrentHashMap<>();
  private final Sensor requestKeyCountSensor;
  private final Sensor successRequestKeyCountSensor;
  private final Sensor successRequestKeyRatioSensor;
  private final Sensor successRequestRatioSensor;
  private final Sensor requestSerializationTime;
  private final Sensor requestSubmissionToResponseHandlingTime;
  private final Sensor responseDeserializationTime;

  public ClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate request = new OccurrenceRate();
    Rate healthyRequest = new OccurrenceRate();

    requestSensor = registerSensor("request", request);
    healthySensor = registerSensor("healthy_request", healthyRequest);
    unhealthySensor = registerSensor("unhealthy_request", new OccurrenceRate());
    healthyRequestLatencySensor = registerSensorWithDetailedPercentiles("healthy_request_latency");
    unhealthyRequestLatencySensor = registerSensorWithDetailedPercentiles("unhealthy_request_latency");

    successRequestRatioSensor = registerSensor("success_request_ratio",
        new TehutiUtils.SimpleRatioStat(healthyRequest, request));

    Rate requestKeyCount = new OccurrenceRate();
    Rate successRequestKeyCount = new OccurrenceRate();
    requestKeyCountSensor = registerSensor("request_key_count", requestKeyCount, new Avg(), new Max());
    successRequestKeyCountSensor = registerSensor("success_request_key_count", successRequestKeyCount,
        new Avg(), new Max());
    successRequestKeyRatioSensor = registerSensor("success_request_key_ratio",
        new TehutiUtils.SimpleRatioStat(successRequestKeyCount, requestKeyCount));
    requestSerializationTime = registerSensorWithDetailedPercentiles("request_serialization_time", new Avg(), new Max());
    requestSubmissionToResponseHandlingTime = registerSensorWithDetailedPercentiles("request_submission_to_response_handling_time", new Avg(), new Max());
    responseDeserializationTime = registerSensorWithDetailedPercentiles("response_deserialization_time", new Avg(), new Max());
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

  public void recordSuccessRequestKeyCount(int successKeyCount) {
    successRequestKeyCountSensor.record(successKeyCount);
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
}
