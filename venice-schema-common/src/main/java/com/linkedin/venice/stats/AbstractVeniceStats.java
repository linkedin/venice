package com.linkedin.venice.stats;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Percentiles;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AbstractVeniceStats {

  final private MetricsRepository metricsRepository;
  final private String name;
  final private Map<String, Sensor> sensors;

  public AbstractVeniceStats(MetricsRepository metricsRepository, String name) {
    this.metricsRepository = metricsRepository;
    this.name = name;
    this.sensors = new ConcurrentHashMap<>();
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public String getName() {
    //add "." in front of the name because dot separator is a must in Tehuti to split package name and sensor name
    return "." + name;
  }

  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return registerSensor(sensorName, null, stats);
  }

  protected Sensor registerSensor(String sensorName, MetricConfig config, MeasurableStat... stats) {
    String sensorFullName = getName() + "." + sensorName;
    return sensors.computeIfAbsent(sensorName, key -> {
      Sensor sensor = metricsRepository.sensor(sensorFullName);
      for (MeasurableStat stat : stats) {
        if (stat instanceof Percentiles)
          sensor.add((Percentiles) stat, config);
        else
          sensor.add(sensorFullName + "." + stat.getClass().getSimpleName(), stat, config);
      }
      return sensor;
    });
  }

  protected void record(Sensor sensor) {
    sensor.record();
  }

  protected  void record(Sensor sensor, double value) {
    sensor.record(value);
  }

  public void close() {
    metricsRepository.close();
  }
}
