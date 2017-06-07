package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;

public class ServerHttpRequestStats extends AbstractVeniceStats{
  private final Sensor successRequestSensor;
  private final Sensor errorRequestSensor;
  private final Sensor successRequestLatencySensor;
  private final Sensor errorRequestLatencySensor;
  private final Sensor bdbQueryLatencySensor;
  private final Sensor requestKeyCountSensor;
  private final Sensor successRequestKeyCountSensor;
  private final Sensor successRequestKeyRatioSensor;

  private final Sensor successRequestRatioSensor;

  private RequestType requestType;


  public ServerHttpRequestStats(MetricsRepository metricsRepository,
                                String storeName, RequestType requestType) {
    super(metricsRepository, storeName);
    this.requestType = requestType;

    MeasurableStat successRequest = new Count();
    MeasurableStat errorRequest = new Count();
    successRequestSensor = registerSensor(getFullMetricName("success_request"), successRequest, new OccurrenceRate());
    errorRequestSensor = registerSensor(getFullMetricName( "error_request"), errorRequest, new OccurrenceRate());
    successRequestLatencySensor = registerSensor(getFullMetricName("success_request_latency"),
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("success_request_latency")));
    errorRequestLatencySensor = registerSensor(getFullMetricName("error_request_latency"),
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("error_request_latency")));
    successRequestRatioSensor = registerSensor(getFullMetricName("success_request_ratio"),
        new TehutiUtils.RatioStat(successRequest, errorRequest));

    //bdbQueryLatency is normally less than 1 ms. Record ns instead of ms for better readability.
    bdbQueryLatencySensor = registerSensor(getFullMetricName("bdb_query_latency_ns"),
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("bdb_query_latency")));

    MeasurableStat requestKeyCount = new Count();
    MeasurableStat successRequestKeyCount = new Count();
    requestKeyCountSensor = registerSensor(getFullMetricName("request_key_count"), requestKeyCount, new Avg(), new Max());
    successRequestKeyCountSensor = registerSensor(getFullMetricName("success_request_key_count"), successRequestKeyCount,
        new Avg(), new Max());
    successRequestKeyRatioSensor = registerSensor(getFullMetricName("success_request_key_ratio"),
        new TehutiUtils.SimpleRatioStat(successRequestKeyCount, requestKeyCount));
  }

  private String getFullMetricName(String metricName) {
    return requestType.getMetricPrefix() + metricName;
  }

  public void recordSuccessRequest() {
    successRequestSensor.record();
  }

  public void recordErrorRequest() {
    errorRequestSensor.record();
  }

  public void recordSuccessRequestLatency(double latency) {
    successRequestLatencySensor.record(latency);
  }

  public void recordErrorRequestLatency(double latency) {
    errorRequestLatencySensor.record(latency);
  }

  public void recordBdbQueryLatency(double latency) {
    bdbQueryLatencySensor.record(latency);
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCountSensor.record(keyCount);
  }

  public void recordSuccessRequestKeyCount(int successKeyCount) {
    successRequestKeyCountSensor.record(successKeyCount);
  }
}
