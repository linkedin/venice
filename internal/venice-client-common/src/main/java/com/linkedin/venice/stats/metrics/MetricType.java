package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;


/**
 * Metric type enum to define the type of metrics Venice supports via OpenTelemetry
 */
public enum MetricType {
  /**
   * For Histogram with percentiles: can be configured to be exponential or explicit bucket
   * check {@link VeniceMetricsConfig.Builder#extractAndSetOtelConfigs} for more details
   */
  HISTOGRAM,
  /**
   * For Histogram without percentiles: Explicit bucket histogram.
   * Provides multiple aggregations like min, max, count and sum without the memory overhead of percentiles.
   * check {@link VeniceOpenTelemetryMetricsRepository#createHistogram} and
   * {@link VeniceOpenTelemetryMetricsRepository#setExponentialHistogramAggregation} for more details
   */
  HISTOGRAM_WITHOUT_BUCKETS,
  /**
   * For Counter: A simple counter that can be added to.
   */
  COUNTER;
}
