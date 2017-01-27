package com.linkedin.venice.stats;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;

public class ServerHttpRequestStats extends AbstractVeniceStats{

  private final Sensor successRequestSensor;
  private final Sensor errorRequestSensor;
  private final Sensor successRequestLatencySensor;
  private final Sensor errorRequestLatencySensor;
  private final Sensor bdbQueryLatencySensor;

  private final Sensor successRequestRatioSensor;


  public ServerHttpRequestStats(MetricsRepository metricsRepository,
                                String storeName) {
    super(metricsRepository, storeName);

    MeasurableStat successRequest = new Count();
    MeasurableStat errorRequest = new Count();
    successRequestSensor = registerSensor("success_request", successRequest, new OccurrenceRate());
    errorRequestSensor = registerSensor("error_request", errorRequest, new OccurrenceRate());
    successRequestLatencySensor = registerSensor("success_request_latency",
        TehutiUtils.getPercentileStat(getName() + "_" + "success_request_latency"));
    errorRequestLatencySensor = registerSensor("error_request_latency",
        TehutiUtils.getPercentileStat(getName() + "_" + "error_request_latency"));
    successRequestRatioSensor = registerSensor("success_request_ratio",
        new TehutiUtils.RatioStat(successRequest, errorRequest));

    //bdbQueryLatency is normally less than 1 ms. Record ns instead of ms for better readability.
    bdbQueryLatencySensor = registerSensor("bdb_query_latency_ns",
        TehutiUtils.getPercentileStat(getName() + "_" + "bdb_query_latency"));
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
}
