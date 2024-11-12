package com.linkedin.venice.stats;

public enum VeniceRequestRetryType {
  ERROR_RETRY("error_retry"), LONG_TAIL_RETRY("long_tail_retry");

  private final String retryType;

  VeniceRequestRetryType(String retryType) {
    this.retryType = retryType;
  }

  public String getRetryType() {
    return this.retryType;
  }
}
