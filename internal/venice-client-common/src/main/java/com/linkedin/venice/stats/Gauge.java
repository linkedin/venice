package com.linkedin.venice.stats;

import io.tehuti.metrics.Measurable;
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
  public interface SimpleMeasurable extends Measurable {
    default double measure(MetricConfig config, long now) {
      return measure();
    }

    double measure();
  }

  private double value;
  private final Measurable measurable;

  public Gauge() {
    this(Double.NaN);
  }

  public Gauge(double value) {
    this.value = value;
    this.measurable = (SimpleMeasurable) () -> this.value;
  }

  public Gauge(Measurable measurable) {
    this.measurable = measurable;
  }

  public Gauge(SimpleMeasurable measurable) {
    this.measurable = measurable;
  }

  @Override
  public void record(double value, long now) {
    this.value = value;
  }

  @Override
  public double measure(MetricConfig config, long now) {
    return measurable.measure(config, now);
  }
}
