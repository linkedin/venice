package com.linkedin.venice.stats.dimensions;

public enum RequestRetryType {
  ERROR_RETRY("error_retry"), LONG_TAIL_RETRY("long_tail_retry");

  private final String retryType;

  RequestRetryType(String retryType) {
    this.retryType = retryType;
  }

  public String getRetryType() {
    return this.retryType;
  }
}
