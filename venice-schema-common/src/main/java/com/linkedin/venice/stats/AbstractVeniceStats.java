package com.linkedin.venice.stats;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Percentiles;

import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.venice.stats.AbstractVeniceAggStats.*;

public class AbstractVeniceStats {
  public static final String DELIMITER = "--";

  private final MetricsRepository metricsRepository;
  private final String name;
  private final Map<String, Sensor> sensors;

  public AbstractVeniceStats(MetricsRepository metricsRepository, String name) {
    this.metricsRepository = metricsRepository;
    // N.B. colons are illegal characters in mbeans so they cause issues if we let them slip in...
    this.name = name == null ? name : name.replace(':', '_');
    this.sensors = new VeniceConcurrentHashMap<>();
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public String getName() {
    //add "." in front of the name because dot separator is a must in Tehuti to split package name and sensor name
    return "." + name;
  }

  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return registerSensor(getSensorFullName(getName(), sensorName), null, null, stats);
  }

  protected Sensor registerSensor(String sensorName, Sensor[] parents, MeasurableStat... stats) {
    return registerSensor(getSensorFullName(getName(), sensorName), null, parents, stats);
  }

  protected Sensor registerSensor(String sensorFullName, MetricConfig config, Sensor[] parents, MeasurableStat... stats) {
    return sensors.computeIfAbsent(sensorFullName, key -> {
      Sensor sensor = metricsRepository.sensor(sensorFullName, parents);
      for (MeasurableStat stat : stats) {
        if (stat instanceof Percentiles)
          sensor.add((Percentiles) stat, config);
        else
          sensor.add(sensorFullName + "." + stat.getClass().getSimpleName(), stat, config);
      }
      return sensor;
    });
  }

  protected Sensor registerSensorWithAggregate(String sensorName, Supplier<MeasurableStat[]> stats) {
    return registerSensorWithAggregate(sensorName, null, stats);
  }

  protected Sensor registerSensorWithAggregate(String sensorName, MetricConfig config, Supplier<MeasurableStat[]> stats) {
    synchronized (AbstractVeniceStats.class) {
      Sensor parent = registerSensorIfAbsent(STORE_NAME_FOR_TOTAL_STAT, sensorName, config,null, stats.get());
      return registerSensorIfAbsent(getName(), sensorName, config, new Sensor[]{parent}, stats.get());
    }
  }

  protected Sensor registerSensorIfAbsent(String sensorName, MeasurableStat... stats) {
    return registerSensorIfAbsent(getName(), sensorName, null, null, stats);
  }

  protected Sensor registerSensorIfAbsent(String resourceName, String sensorName, MetricConfig config, Sensor[] parents, MeasurableStat... stats) {
    String fullSensorName = getSensorFullName(resourceName, sensorName);
    Sensor sensor = metricsRepository.getSensor(fullSensorName);
    if (null == sensor) {
      sensor = registerSensor(fullSensorName, config, parents, stats);
    }
    return sensor;
  }

  protected String getSensorFullName(String sensorName) {
    return getSensorFullName(getName(), sensorName);
  }

  protected String getSensorFullName(String resourceName, String sensorName) {
    if (!resourceName.substring(0, 1).equals(".")) {
      resourceName = "." + resourceName;
    }
    return resourceName + AbstractVeniceStats.DELIMITER + sensorName;
  }
}
