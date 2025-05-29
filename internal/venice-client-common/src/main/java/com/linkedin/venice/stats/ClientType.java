package com.linkedin.venice.stats;

public enum ClientType {
  THIN_CLIENT("thin-client"), FAST_CLIENT("fast-client"), DAVINCI_CLIENT("davinci-client");

  private final String name;
  private final String otelMetricsPrefix;

  ClientType(String clientName) {
    this.name = clientName;
    this.otelMetricsPrefix = this.name().toLowerCase();
  }

  public String getName() {
    return name;
  }

  public String getMetricsPrefix() {
    return otelMetricsPrefix;
  }

  public static boolean isDavinciClient(ClientType clientType) {
    return clientType == DAVINCI_CLIENT;
  }
}
