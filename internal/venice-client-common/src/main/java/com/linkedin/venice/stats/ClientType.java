package com.linkedin.venice.stats;

public enum ClientType {
  THIN_CLIENT("thin-client", "thin_client"), FAST_CLIENT("fast-client", "fast_client"),
  DAVINCI_CLIENT("davinci-client", "davinci_client");

  private final String name;
  private final String otelMetricsPrefix;

  ClientType(String name, String otelMetricsPrefix) {
    this.name = name;
    this.otelMetricsPrefix = otelMetricsPrefix;
  }

  public String getName() {
    return name;
  }

  public String getMetricsPrefix() {
    return otelMetricsPrefix;
  }
}
