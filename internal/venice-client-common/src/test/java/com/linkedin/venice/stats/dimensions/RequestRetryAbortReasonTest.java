package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class RequestRetryAbortReasonTest {
  @Test
  public void testDimensionInterface() {
    Map<RequestRetryAbortReason, String> expectedValues = CollectionUtils.<RequestRetryAbortReason, String>mapBuilder()
        .put(RequestRetryAbortReason.SLOW_ROUTE, "slow_route")
        .put(RequestRetryAbortReason.DELAY_CONSTRAINT, "delay_constraint")
        .put(RequestRetryAbortReason.MAX_RETRY_ROUTE_LIMIT, "max_retry_route_limit")
        .put(RequestRetryAbortReason.NO_AVAILABLE_REPLICA, "no_available_replica")
        .build();
    new VeniceDimensionTestFixture<>(
        RequestRetryAbortReason.class,
        VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON,
        expectedValues).assertAll();
  }
}
