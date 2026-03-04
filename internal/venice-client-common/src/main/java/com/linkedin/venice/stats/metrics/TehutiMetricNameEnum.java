package com.linkedin.venice.stats.metrics;

/**
 * Interface for creating metric names enum for tehuti metrics
 */
public interface TehutiMetricNameEnum {
  default String getMetricName() {
    return ((Enum<?>) this).name().toLowerCase();
  }
}
