package com.linkedin.venice.read;

public enum RequestType {
  SINGLE_GET(""),
  MULTI_GET("multiget_");

  private String metricPrefix;

  RequestType(String metricPrefix) {
    this.metricPrefix = metricPrefix;
  }

  public String getMetricPrefix() {
    return this.metricPrefix;
  }
}
