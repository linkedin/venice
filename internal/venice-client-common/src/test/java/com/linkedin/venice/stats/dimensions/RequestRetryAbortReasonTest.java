package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class RequestRetryAbortReasonTest {
  @Test
  public void testRetryRequestAbortReason() {
    for (RequestRetryAbortReason reason: RequestRetryAbortReason.values()) {
      switch (reason) {
        case SLOW_ROUTE:
          assertEquals(reason.getAbortReason(), "slow_route");
          break;
        case DELAY_CONSTRAINT:
          assertEquals(reason.getAbortReason(), "delay_constraint");
          break;
        case MAX_RETRY_ROUTE_LIMIT:
          assertEquals(reason.getAbortReason(), "max_retry_route_limit");
          break;
        case NO_AVAILABLE_REPLICA:
          assertEquals(reason.getAbortReason(), "no_available_replica");
          break;
        default:
          throw new IllegalArgumentException("Unknown reason: " + reason);
      }
    }
  }
}
