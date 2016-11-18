package com.linkedin.venice.stats;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricConfig;
/**
 * LambdaStat is a un-windowed MeasurableStat that has the maximum flexibility.
 * It takes a Lambda expression as parameter and calculates the real-time
 * value dynamically.
 * LambdaStat is introduced to report un-windowed metrics such like age of
 * store and ratio.
 */
public class LambdaStat implements MeasurableStat{
  private TehutiOps ops;

  public LambdaStat(TehutiOps ops) {
    this.ops = ops;
  }

  @Override
  public void record(MetricConfig config, double value, long now) {}

  @Override
  public double measure(MetricConfig config, long now) {
    return ops.measure();
  }

  public interface TehutiOps {
    double measure();
  }
}
