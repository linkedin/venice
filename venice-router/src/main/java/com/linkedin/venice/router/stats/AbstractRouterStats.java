package com.linkedin.venice.router.stats;

import com.linkedin.venice.tehuti.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;


public abstract class AbstractRouterStats implements RouterStats {

  final protected MetricsRepository metricsRepository;
  final protected String name;

  //QPS metrics
  //TODO: implement a simple Tehuti class that just return the current value to calculate unhealthy ratio
  private Sensor healthyRequest;

  private Sensor unhealthyRequest;

  private Sensor requestSensor;

  private Sensor latencySensor;

  private Sensor keySizeSensor;

  private Sensor valueSizeSensor;

  public AbstractRouterStats(MetricsRepository metricsRepository, String name) {
    this.metricsRepository = metricsRepository;
    this.name = "." + name;

    String healthyRequestSensorName = this.name + "_healthy_request";
    healthyRequest = metricsRepository.sensor(healthyRequestSensorName);
    healthyRequest.add(healthyRequestSensorName + ".count", new Count());

    String unhealthyRequestSensorName = this.name + "_unhealthy_request";
    unhealthyRequest = metricsRepository.sensor(unhealthyRequestSensorName);
    unhealthyRequest.add(unhealthyRequestSensorName+ ".count", new Count());

    String requestSensorName = this.name + "_request";
    requestSensor = metricsRepository.sensor(name);
    requestSensor.add(requestSensorName + ".count", new Count());
    requestSensor.add(requestSensorName + ".QPS", new OccurrenceRate());

    //TODO: make the values configurable in Percentiles constructor
    String latencySensorName = this.name + "_latency";
    latencySensor = TehutiUtils.getPercentileSensor(metricsRepository, latencySensorName);

    String keySizeSensorName = this.name + "_key-size";
    keySizeSensor = TehutiUtils.getPercentileSensor(metricsRepository, keySizeSensorName);

    String valueSizeSensorName = this.name + "_value-size";
    valueSizeSensor = TehutiUtils.getPercentileSensor(metricsRepository, valueSizeSensorName);
  }

  @Override
  public void addRequest() {
    this.requestSensor.record();
  }

  @Override
  public void addHealthyRequest() {
    this.healthyRequest.record();
  }

  @Override
  public void addUnhealthyRequest() {
    this.unhealthyRequest.record();
  }

  @Override
  public void addLatency(double latency) {
    this.latencySensor.record(latency);
  }

  @Override
  public void addKeySize(double keySize) {
    this.keySizeSensor.record(keySize);
  }

  @Override
  public void addValueSize(double valueSize) {
    this.valueSizeSensor.record(valueSize);
  }

  @Override
  public void close() {
    metricsRepository.close();
  }
}
