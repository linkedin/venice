package com.linkedin.davinci.stats;

import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;


/**
 * This class serves as a latency sensor for write path that contains two types of stats: Avg and Max.
 * Note that this latency sensor should only be used in to write path as read path latency evaluation will need more
 * information than just max and average, e.g. 50/99 percentile numbers.
 */
public class WritePathLatencySensor {
  private final Lazy<Sensor> sensor;
  private final MetricConfig metricConfig;
  private final Avg avgStat = new Avg();
  private final Max maxStat = new Max();

  public WritePathLatencySensor(MetricsRepository metricsRepo, MetricConfig metricConfig, String sensorName) {
    this.metricConfig = metricConfig;
    this.sensor = Lazy.of(() -> {
      Sensor sensor = metricsRepo.sensor(sensorName);
      sensor.add(sensorName + Avg.class.getSimpleName(), avgStat);
      sensor.add(sensorName + Max.class.getSimpleName(), maxStat);
      return sensor;
    });
  }

  /**
   * @return Avg stats of this latency sensor in millisecond unit.
   */
  public double getAvg() {
    return avgStat.measure(metricConfig, System.currentTimeMillis());
  }

  /**
   * @return Max stats of this latency sensor in millisecond unit.
   */
  public double getMax() {
    return maxStat.measure(metricConfig, System.currentTimeMillis());
  }

  /**
   * Record the latency value.
   */
  public void record(double value, long currentTimeMs) {
    sensor.get().record(value, currentTimeMs);
  }
}
