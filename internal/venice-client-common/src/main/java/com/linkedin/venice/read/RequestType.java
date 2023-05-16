package com.linkedin.venice.read;

import com.linkedin.venice.exceptions.VeniceException;


public enum RequestType {
  SINGLE_GET(""), MULTI_GET("multiget_"), MULTI_GET_STREAMING("multiget_streaming_"), COMPUTE("compute_"),
  COMPUTE_STREAMING("compute_streaming_");

  private String metricPrefix;

  RequestType(String metricPrefix) {
    this.metricPrefix = metricPrefix;
  }

  public String getMetricPrefix() {
    return this.metricPrefix;
  }

  public static RequestType valueOf(int ordinal) {
    try {
      return values()[ordinal];
    } catch (IndexOutOfBoundsException e) {
      throw new VeniceException("Invalid request type: " + ordinal);
    }
  }
}
