package com.linkedin.venice.read;

public enum RequestType {
  SINGLE_GET("", "single_get"), MULTI_GET("multiget_", "multi_get"),
  MULTI_GET_STREAMING("multiget_streaming_", "multi_get_streaming"), COMPUTE("compute_", "compute"),
  COMPUTE_STREAMING("compute_streaming_", "compute_streaming");

  private String metricPrefix;
  private String requestTypeName;

  RequestType(String metricPrefix, String requestTypeName) {
    this.metricPrefix = metricPrefix;
    this.requestTypeName = requestTypeName;
  }

  public String getMetricPrefix() {
    return this.metricPrefix;
  }

  public String getRequestTypeName() {
    return this.requestTypeName;
  }
}
