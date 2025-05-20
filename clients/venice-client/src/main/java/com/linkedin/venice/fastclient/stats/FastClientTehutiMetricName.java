package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;


/**
 * Metric names for tehuti metrics used in this class.
 */
public enum FastClientTehutiMetricName implements TehutiMetricNameEnum {
  LONG_TAIL_RETRY_REQUEST, ERROR_RETRY_REQUEST;

  private final String metricName;

  FastClientTehutiMetricName() {
    this.metricName = name().toLowerCase();
  }

  @Override
  public String getMetricName() {
    return this.metricName;
  }
}
