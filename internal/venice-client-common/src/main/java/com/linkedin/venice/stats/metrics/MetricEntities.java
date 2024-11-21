package com.linkedin.venice.stats.metrics;

/**
 * Interface to get {@link MetricEntity}
 * All modules metric enum class should implement this interface.
 */
public interface MetricEntities {
  MetricEntity getMetricEntity();
}
