package com.linkedin.venice.stats;

public enum VeniceRequestRetryAbortReason {
  RETRY_ABORTED_BY_SLOW_ROUTE("slow_route"), RETRY_ABORTED_BY_DELAY_CONSTRAINT("delay_constraint"),
  RETRY_ABORTED_BY_MAX_RETRY_ROUTE_LIMIT("max_retry_router_limit"),
  RETRY_ABORTED_BY_NO_AVAILABLE_REPLICA("no_available_replica");

  private final String abortReason;

  VeniceRequestRetryAbortReason(String abortReason) {
    this.abortReason = abortReason;
  }

  public String getAbortReason() {
    return this.abortReason;
  }
}
