package com.linkedin.venice.stats.dimensions;

public enum RequestRetryAbortReason {
  SLOW_ROUTE, DELAY_CONSTRAINT, MAX_RETRY_ROUTE_LIMIT, NO_AVAILABLE_REPLICA;

  private final String abortReason;

  RequestRetryAbortReason() {
    this.abortReason = name().toLowerCase();
  }

  public String getAbortReason() {
    return this.abortReason;
  }
}
