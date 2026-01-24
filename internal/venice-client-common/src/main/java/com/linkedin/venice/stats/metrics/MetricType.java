package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;


/**
 * Metric type enum to define the type of metrics Venice supports via OpenTelemetry
 */
public enum MetricType {
  /**
   * Use Histogram to get percentiles/min/max/count/sum and other aggregates: can be configured to
   * be exponential or explicit bucket <br>
   * check {@link VeniceMetricsConfig.Builder#extractAndSetOtelConfigs} for more details
   */
  HISTOGRAM(false),

  /**
   * To get min/max/count/sum aggregation without the memory overhead to calculate percentiles, use
   * Otel Explicit bucket Histogram but without buckets .
   * check {@link VeniceOpenTelemetryMetricsRepository#createDoubleHistogram} and
   * {@link VeniceOpenTelemetryMetricsRepository#setExponentialHistogramAggregation} for more details
   */
  MIN_MAX_COUNT_SUM_AGGREGATIONS(false),

  /**
   * For Counter: A simple counter that can be added to.
   */
  COUNTER(false),

  /**
   * Use this instead of {@link #COUNTER} when recording happens at very high frequency
   * in hot path. <p>
   * Uses {@link java.util.concurrent.atomic.LongAdder} internally for fast recording,
   * and OpenTelemetry's {@link io.opentelemetry.api.metrics.ObservableLongCounter}
   * reads the accumulated values during metrics collection.
   * <p>
   */
  ASYNC_COUNTER_FOR_HIGH_PERF_CASES(true),

  /**
   * For UpDownCounter: A counter that supports positive and negative increments.
   * Useful when counts can increase or decrease over time.
   */
  UP_DOWN_COUNTER(false),

  /**
   * {@link io.opentelemetry.api.metrics.LongGauge}: Emits the absolute value of the metric value.
   */
  GAUGE(false),

  /**
   * For Async Gauge: Emits the absolute value of the metric value asynchronously.
   * Refer {@link io.opentelemetry.api.metrics.ObservableLongGauge}
   */
  ASYNC_GAUGE(true);

  private final boolean isAsyncMetric;

  MetricType(boolean isAsyncMetric) {
    this.isAsyncMetric = isAsyncMetric;
  }

  public boolean isAsyncMetric() {
    return isAsyncMetric;
  }
}
