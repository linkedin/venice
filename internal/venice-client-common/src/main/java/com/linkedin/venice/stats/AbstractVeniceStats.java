package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Percentiles;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


public class AbstractVeniceStats {
  public static final String DELIMITER = "--";

  private final MetricsRepository metricsRepository;
  private final String name;
  private final Map<String, Sensor> sensors;

  public AbstractVeniceStats(MetricsRepository metricsRepository, String name) {
    this.metricsRepository = metricsRepository;
    // N.B. colons are illegal characters in mbeans and Tehuti splits the metric name by dot character to get sensor
    // name and attribute name, so they cause issues if we let them slip in...
    this.name = name.replace(':', '_').replace(".", "_");
    this.sensors = new VeniceConcurrentHashMap<>();
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public String getName() {
    // add "." in front of the name because dot separator is a must in Tehuti to split package name and sensor name
    return "." + name;
  }

  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    return registerSensor(getSensorFullName(getName(), sensorName), null, null, stats);
  }

  protected Sensor registerSensorWithAttributeOverride(
      String sensorName,
      String attributeName,
      MeasurableStat... stats) {
    return registerSensor(getSensorFullName(getName(), sensorName), Optional.of(attributeName), null, null, stats);
  }

  protected Sensor registerSensor(String sensorName, Sensor[] parents, MeasurableStat... stats) {
    return registerSensor(getSensorFullName(getName(), sensorName), null, parents, stats);
  }

  protected Sensor registerSensor(
      String sensorFullName,
      MetricConfig config,
      Sensor[] parents,
      MeasurableStat... stats) {
    return registerSensor(sensorFullName, Optional.empty(), config, parents, stats);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  protected Sensor registerSensor(
      String sensorFullName,
      Optional<String> attributeName,
      MetricConfig config,
      Sensor[] parents,
      MeasurableStat... stats) {
    return sensors.computeIfAbsent(sensorFullName, key -> {
      /**
       * The sensors concurrentmap will not prevent other objects working on the same metrics repository to execute
       * this block. So it is possible for multiple threads to get here .
       * The metricsRepository.sensor method below will call {@link MetricsRepository#sensor(String, MetricConfig, Sensor...)}
       * which is synchronized and so it is guaranteed that only one thread will be able to create the sensor .
       * The sensor.add method will call {@link MetricsRepository#registerMetric(TehutiMetric)}  which is also synchronized but will throw
       * an error in case the metric already exists. The only way to avoid the error would be to atomically check
       * and add the metric. We lock on the sensor object . Since we are not expecting the same sensor to be registered
       * multiple times the contention will be minimal
       */
      Sensor sensor = metricsRepository.sensor(sensorFullName, parents);
      synchronized (sensor) {
        for (MeasurableStat stat: stats) {
          if (stat instanceof Percentiles) {
            Percentiles percentilesStat = (Percentiles) stat;
            if (percentilesStat.stats().size() > 0) { // Only checking one is enough to determine if we have already
                                                      // added this set
              String metricName = percentilesStat.stats().get(0).name();
              if (metricsRepository.getMetric(metricName) == null) {
                sensor.add(percentilesStat, config);
              }
            }
          } else {
            String metricName = sensorFullName + "." + attributeName.orElse(stat.getClass().getSimpleName());
            if (metricsRepository.getMetric(metricName) == null) {
              sensor.add(metricName, stat, config);
            }
          }
        }
      }
      return sensor;
    });
  }

  protected void unregisterAllSensors() {
    for (Sensor sensor: sensors.values()) {
      metricsRepository.removeSensor(sensor.name());
    }
    sensors.clear();
  }

  protected Sensor registerSensorWithAggregate(String sensorName, Supplier<MeasurableStat[]> stats) {
    return registerSensorWithAggregate(sensorName, null, stats);
  }

  protected Sensor registerSensorWithAggregate(
      String sensorName,
      MetricConfig config,
      Supplier<MeasurableStat[]> stats) {
    synchronized (AbstractVeniceStats.class) {
      Sensor parent = registerSensorIfAbsent(STORE_NAME_FOR_TOTAL_STAT, sensorName, config, null, stats.get());
      return registerSensorIfAbsent(getName(), sensorName, config, new Sensor[] { parent }, stats.get());
    }
  }

  protected Sensor registerSensorIfAbsent(String sensorName, MeasurableStat... stats) {
    return registerSensorIfAbsent(getName(), sensorName, null, null, stats);
  }

  protected Sensor registerSensorIfAbsent(
      String resourceName,
      String sensorName,
      MetricConfig config,
      Sensor[] parents,
      MeasurableStat... stats) {
    String fullSensorName = getSensorFullName(resourceName, sensorName);
    Sensor sensor = metricsRepository.getSensor(fullSensorName);
    if (sensor == null) {
      sensor = registerSensor(fullSensorName, config, parents, stats);
    }
    return sensor;
  }

  protected String getSensorFullName(String sensorName) {
    return getSensorFullName(getName(), sensorName);
  }

  protected String getSensorFullName(String resourceName, String sensorName) {
    if (resourceName.charAt(0) != '.') {
      resourceName = "." + resourceName;
    }
    return resourceName + AbstractVeniceStats.DELIMITER + sensorName;
  }
}
