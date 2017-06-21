package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.SampledCount;
import io.tehuti.metrics.stats.SampledTotal;


public class ServerHttpRequestStats extends AbstractVeniceHttpStats{
  private final Sensor successRequestSensor;
  private final Sensor errorRequestSensor;
  private final Sensor successRequestLatencySensor;
  private final Sensor errorRequestLatencySensor;
  private final Sensor bdbQueryLatencySensor;
  private final Sensor requestKeyCountSensor;
  private final Sensor successRequestKeyCountSensor;
  private final Sensor successRequestKeyRatioSensor;
  private final Sensor successRequestRatioSensor;


  public ServerHttpRequestStats(MetricsRepository metricsRepository,
                                String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);

    /**
     * Check java doc of function: {@link TehutiUtils.RatioStat} to understand why choosing {@link Rate} instead of
     * {@link io.tehuti.metrics.stats.SampledStat}.
     */
    Rate successRequest = new OccurrenceRate();
    Rate errorRequest = new OccurrenceRate();
    successRequestSensor = registerSensor("success_request", successRequest);
    errorRequestSensor = registerSensor("error_request", errorRequest);
    successRequestLatencySensor = registerSensor("success_request_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("success_request_latency")));
    errorRequestLatencySensor = registerSensor("error_request_latency",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("error_request_latency")));
    successRequestRatioSensor = registerSensor("success_request_ratio",
        new TehutiUtils.RatioStat(successRequest, errorRequest));

    //bdbQueryLatency is normally less than 1 ms. Record ns instead of ms for better readability.
    bdbQueryLatencySensor = registerSensor("bdb_query_latency_ns",
        TehutiUtils.getPercentileStat(getName(), getFullMetricName("bdb_query_latency")));

    Rate requestKeyCount = new OccurrenceRate();
    Rate successRequestKeyCount = new OccurrenceRate();
    requestKeyCountSensor = registerSensor("request_key_count", requestKeyCount, new Avg(), new Max());
    successRequestKeyCountSensor = registerSensor("success_request_key_count", successRequestKeyCount,
        new Avg(), new Max());
    successRequestKeyRatioSensor = registerSensor("success_request_key_ratio",
        new TehutiUtils.SimpleRatioStat(successRequestKeyCount, requestKeyCount));
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
