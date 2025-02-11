package com.linkedin.venice.stats.dimensions;

public enum RequestRetryType {
  ERROR_RETRY, LONG_TAIL_RETRY;

  private final String retryType;

  RequestRetryType() {
    this.retryType = name().toLowerCase();
  }

  public String getRetryType() {
    return this.retryType;
  }
}
