package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class VeniceRequestRetryAbortReasonTest {
  @Test
  public void testRetryRequestAbortReason() {
    for (VeniceRequestRetryAbortReason reason: VeniceRequestRetryAbortReason.values()) {
      switch (reason) {
        case RETRY_ABORTED_BY_SLOW_ROUTE:
          assertEquals(reason.getAbortReason(), "slow_route");
          break;
        case RETRY_ABORTED_BY_DELAY_CONSTRAINT:
          assertEquals(reason.getAbortReason(), "delay_constraint");
          break;
        case RETRY_ABORTED_BY_MAX_RETRY_ROUTE_LIMIT:
          assertEquals(reason.getAbortReason(), "max_retry_router_limit");
          break;
        case RETRY_ABORTED_BY_NO_AVAILABLE_REPLICA:
          assertEquals(reason.getAbortReason(), "no_available_replica");
          break;
        default:
          throw new IllegalArgumentException("Unknown reason: " + reason);
      }
    }
  }
}
