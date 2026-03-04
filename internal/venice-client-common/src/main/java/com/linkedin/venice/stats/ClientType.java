package com.linkedin.venice.stats;

public enum ClientType {
  THIN_CLIENT("thin-client"), FAST_CLIENT("fast-client"), DAVINCI_CLIENT("davinci-client"),
  CHANGE_DATA_CAPTURE_CLIENT("change-data-capture-client");

  private final String name;

  ClientType(String clientName) {
    this.name = clientName;
  }

  public String getName() {
    return name;
  }

  public String getMetricsPrefix() {
    return name().toLowerCase();
  }

  public static boolean isDavinciClient(ClientType clientType) {
    return clientType == DAVINCI_CLIENT;
  }
}
