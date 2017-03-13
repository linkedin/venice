package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;

public class RouterHttpRequestStats extends AbstractVeniceStats {

  final private Sensor requestSensor;
  final private Sensor healthySensor;
  final private Sensor unhealthySensor;
  final private Sensor latencySensor;
  final private Sensor keySizeSensor;
  final private Sensor valueSizeSensor;

  //QPS metrics
  public RouterHttpRequestStats(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    requestSensor = registerSensor("request", new Count(), new OccurrenceRate());
    healthySensor = registerSensor("healthy_request", new Count());
    unhealthySensor = registerSensor("unhealthy_request", new Count());

    //we have to explicitly pass the name again for PercentilesStat here.
    //TODO: remove the redundancy once Tehuti library is updated.
    latencySensor = registerSensor("latency", TehutiUtils.getPercentileStat(getName(), "latency"));
    keySizeSensor = registerSensor("key_size", TehutiUtils.getPercentileStat(getName(), "key_size"));
    valueSizeSensor = registerSensor("value_size", TehutiUtils.getPercentileStat(getName(), "value_size"));
  }

  public void recordRequest() {
    requestSensor.record();
  }

  public void recordHealthyRequest() {
    healthySensor.record();
  }

  public void recordUnhealthyRequest() {
    unhealthySensor.record();
  }

  public void recordLatency(double latency) {
    latencySensor.record(latency);
  }

  public void recordKeySize(double keySize) {
    keySizeSensor.record(keySize);
  }

  public void recordValueSize(double valueSize) {
    valueSizeSensor.record(valueSize);
  };
}
