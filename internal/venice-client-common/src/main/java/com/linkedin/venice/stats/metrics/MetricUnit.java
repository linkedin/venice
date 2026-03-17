package com.linkedin.venice.stats.metrics;

/**
 * Metric Unit enum to define list of Units supported for metrics
 */
public enum MetricUnit {
  NUMBER, MILLISECOND, SECOND, BYTES,
  /**
   * Ratio value where 1.0 = 100%. Typically in [0.0, 1.0] but may exceed 1.0 for over-quota
   * or over-capacity scenarios. Use with {@link MetricType#ASYNC_DOUBLE_GAUGE} or double-valued histograms.
   */
  RATIO
}
