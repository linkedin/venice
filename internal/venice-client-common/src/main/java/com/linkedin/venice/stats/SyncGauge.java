package com.linkedin.venice.stats;

import io.tehuti.metrics.Measurable;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.NamedMeasurableStat;


/**
 * A synchronous gauge that computes its value on-demand via a {@link Measurable} lambda.
 *
 * Unlike {@link LambdaStat} (which extends {@link io.tehuti.metrics.stats.AsyncGauge} and dispatches
 * measurement to a shared thread pool), this class calls the lambda directly in the calling thread.
 * Use this for cheap O(1) measurements (e.g. {@link java.util.concurrent.ThreadPoolExecutor#getMaximumPoolSize()})
 * that should never be measured asynchronously.
 *
 * The async executor in {@link io.tehuti.metrics.stats.AsyncGauge} has a small fixed thread pool (3 threads)
 * shared across all {@link LambdaStat} instances in the JVM. Under load (many registered metrics),
 * the executor can become saturated, causing {@code measure()} to time out and return a stale
 * cached value of 0.0 instead of the real value. {@code SyncGauge} avoids this by measuring inline.
 */
public class SyncGauge implements NamedMeasurableStat {
  private final Measurable measurable;
  private final String metricName;

  public SyncGauge(Measurable measurable, String metricName) {
    this.measurable = measurable;
    this.metricName = metricName;
  }

  @Override
  public String getStatName() {
    return metricName;
  }

  @Override
  public void record(double value, long now) {
    // no-op: value is computed on-demand via the measurable lambda
  }

  @Override
  public double measure(MetricConfig config, long now) {
    return measurable.measure(config, now);
  }
}
