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
  HISTOGRAM,

  /**
   * To get min/max/count/sum aggregation without the memory overhead to calculate percentiles, use
   * Otel Explicit bucket Histogram but without buckets .
   * check {@link VeniceOpenTelemetryMetricsRepository#createHistogram} and
   * {@link VeniceOpenTelemetryMetricsRepository#setExponentialHistogramAggregation} for more details
   */
  MIN_MAX_COUNT_SUM_AGGREGATIONS,

  /**
   * For Counter: A simple counter that can be added to.
   */
  COUNTER;
}
