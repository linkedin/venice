package com.linkedin.venice.router.throttle;

import org.testng.Assert;
import org.testng.annotations.Test;


public class PendingRequestThrottlerTest {
  @Test
  public void testThrottlePut() {
    PendingRequestThrottler throttler = new PendingRequestThrottler(2);
    Assert.assertTrue(throttler.put(), "1st request shouldn't be throttled");
    Assert.assertTrue(throttler.put(), "2nd request shouldn't be throttled");
    Assert.assertFalse(throttler.put(), "3rd request should be throttled");
    throttler.take();
    Assert.assertTrue(throttler.put(), "3rd request shouldn't be throttled after taking out one request");
    Assert.assertEquals(2, throttler.getCurrentPendingRequestCount());
  }
}
