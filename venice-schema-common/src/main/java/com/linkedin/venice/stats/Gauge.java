package com.linkedin.venice.stats;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;

/**
 * Gauge is a un-windowed MeasurableStat that has the maximum flexibility.
 * It takes a Lambda expression as parameter and calculates the real-time
 * value dynamically.
 * Gauge is introduced to report un-windowed metrics such like age of
 * store and ratio.
 */
public class Gauge implements MeasurableStat {
  private ParameteredTehutiOps ops;

  public Gauge(ParameteredTehutiOps ops) {
    this.ops = ops;
  }

  public Gauge(TehutiOps ops) {
    this.ops = ops;
  }

  @Override
  public void record(MetricConfig config, double value, long now) {}

  @Override
  public double measure(MetricConfig config, long now) {
    return ops.measure(config, now);
  }

  public interface TehutiOps extends ParameteredTehutiOps {
    default double measure(MetricConfig config, long now) {
      return measure();
    }
    double measure();
  }

  public interface ParameteredTehutiOps {
    double measure(MetricConfig config, long now);
  }
}
