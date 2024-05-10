package com.linkedin.venice.read;

public enum RequestType {
  SINGLE_GET(""), MULTI_GET("multiget_"), MULTI_GET_STREAMING("multiget_streaming_"), COMPUTE("compute_"),
  COMPUTE_STREAMING("compute_streaming_"), BLOB_DISCOVERY_REQUEST("blob_request_");

  private String metricPrefix;

  RequestType(String metricPrefix) {
    this.metricPrefix = metricPrefix;
  }

  public String getMetricPrefix() {
    return this.metricPrefix;
  }
}
