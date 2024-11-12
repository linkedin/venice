package com.linkedin.venice.stats;

public enum VeniceResponseStatusCategory {
  HEALTHY("healthy"), UNHEALTHY("unhealthy"), TARDY("tardy"), THROTTLED("throttled"), BAD_REQUEST("bad_request");

  private final String category;

  VeniceResponseStatusCategory(String category) {
    this.category = category;
  }

  public String getCategory() {
    return this.category;
  }
}
