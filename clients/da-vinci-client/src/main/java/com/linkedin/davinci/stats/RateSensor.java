package com.linkedin.davinci.stats;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;


/**
 * This class serves as a rate sensor and contains single stat type: Rate.
 */
public class RateSensor {
  private final Sensor sensor;
  private final MetricConfig metricConfig;
  private final Rate rateStat = new Rate();

  public RateSensor(MetricsRepository metricsRepo, MetricConfig metricConfig, String sensorName) {
    this.metricConfig = metricConfig;
    this.sensor = metricsRepo.sensor(sensorName);
    this.sensor.add(sensorName + Rate.class.getSimpleName(), rateStat);

  }

  /**
   * @return Rate stats of this sensor in millisecond unit.
   */
  public double getRate() {
    return rateStat.measure(metricConfig, System.currentTimeMillis());
  }

  /**
   * Record the rate value.
   */
  public void record() {
    sensor.record();
  }

  /**
   * Record the passed in double value.
   * @param value The passed in value to be recorded.
   */
  public void record(double value) {
    sensor.record(value);
  }

}
