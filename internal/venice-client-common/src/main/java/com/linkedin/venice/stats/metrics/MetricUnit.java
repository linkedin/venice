package com.linkedin.venice.stats.metrics;

/**
 * Metric Unit enum to define list of Units supported for metrics
 */
public enum MetricUnit {
  NUMBER, MILLISECOND, SECOND, BYTES,
  /** Ratio value in [0.0, 1.0] range. Use with {@link MetricType#ASYNC_DOUBLE_GAUGE} or double-valued histograms. */
  RATIO
}
