package com.linkedin.venice.client.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;


/**
 * Metric names for tehuti metrics used in this class.
 */
public enum ClientTehutiMetricName implements TehutiMetricNameEnum {
  REQUEST_RETRY_COUNT;

  private final String metricName;

  ClientTehutiMetricName() {
    this.metricName = name().toLowerCase();
  }

  @Override
  public String getMetricName() {
    return this.metricName;
  }
}
