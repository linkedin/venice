package com.linkedin.venice.stats.dimensions;

public enum RequestRetryAbortReason {
  SLOW_ROUTE("slow_route"), DELAY_CONSTRAINT("delay_constraint"), MAX_RETRY_ROUTE_LIMIT("max_retry_router_limit"),
  NO_AVAILABLE_REPLICA("no_available_replica");

  private final String abortReason;

  RequestRetryAbortReason(String abortReason) {
    this.abortReason = abortReason;
  }

  public String getAbortReason() {
    return this.abortReason;
  }
}
