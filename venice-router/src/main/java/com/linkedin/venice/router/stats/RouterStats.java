package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;

public class RouterStats extends AbstractVeniceStats {

  final private Sensor requestSensor;
  final private Sensor healthySensor;
  final private Sensor unhealthySensor;
  final private Sensor latencySensor;
  final private Sensor keySizeSensor;
  final private Sensor valueSizeSensor;

  //QPS metrics
  //TODO: implement a simple Tehuti class that just return the current value to calculate unhealthy ratio
  public RouterStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    requestSensor = registerSensor("request", new Count(), new OccurrenceRate());
    healthySensor = registerSensor("healthy_request", new Count());
    unhealthySensor = registerSensor("unhealthy_request", new Count());

    //we have to explicitly pass the anme again for PercentilesStat here.
    //TODO: remove the redundancy once Tehuti library is updated.
    latencySensor = registerSensor("latency", TehutiUtils.getPercentileStat(getName() + "_" + "latency"));
    keySizeSensor = registerSensor("key_size", TehutiUtils.getPercentileStat(getName() + "_" + "key_size"));
    valueSizeSensor = registerSensor("value_size", TehutiUtils.getPercentileStat(getName() + "_" + "value_size"));
  }

  public void addRequest() {
    record(requestSensor);
  }

  public void addHealthyRequest() {
    record(healthySensor);
  }

  public void addUnhealthyRequest() {
    record(unhealthySensor);
  }

  public void addLatency(double latency) {
    record(latencySensor, latency);
  }

  public void addKeySize(double keySize) {
    record(keySizeSensor, keySize);
  }

  public void addValueSize(double valueSize) {
    record(valueSizeSensor, valueSize);
  };
}
