package com.linkedin.venice.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import io.tehuti.utils.Time;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EventThrottlerTest {
  private TestTime testTime = new TestTime();

  @Test
  public void testRejectUnexpectedRequests() {
    int quota = 10;
    long timeWindowMS = 1000l;
    EventThrottler throttler = new EventThrottler(testTime, quota, timeWindowMS, "testRejectUnexpectedRequests", true,
        EventThrottler.REJECT_STRATEGY);

    // Send requests that number of them equals to the given quota.
    sendRequests(quota, throttler);
    // Quota exceeds.
    try {
      sendRequests(1, throttler);
      Assert.fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected.
    }

    // Time goes 1.5 second so throttler starts second time window. Now we have more capacity granted.
    testTime.sleep((long) (1500));
    sendRequests(1, throttler);
    // Quota exceeds, case we only have half quota for the in flight time window.
    try {
      sendRequests(quota / 2, throttler);
      Assert.fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected
    }
  }

  @Test
  public void testRejectExpansiveRequest() {
    long quota = 10;
    long timeWindowMS = 1000l;
    EventThrottler throttler = new EventThrottler(testTime, quota, timeWindowMS, "testRejectExpansiveRequest", true,
        EventThrottler.REJECT_STRATEGY);
    // Send the single request that usage exceeds the quota.
    try {
      throttler.maybeThrottle((int) quota * 10);
      Assert.fail("Usage exceeds the quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected
    }
  }

  private void sendRequests(int number, EventThrottler throttler) {
    for (int i = 0; i < number; i++) {
      throttler.maybeThrottle(1);
    }
  }

  private static class TestTime implements Time {
    long milliseconds = 0;

    @Override
    public long milliseconds() {
      return milliseconds;
    }

    @Override
    public long nanoseconds() {
      return TimeUnit.MILLISECONDS.toNanos(milliseconds);
    }

    @Override
    public void sleep(long l) {
      milliseconds += l;
    }
  }
}
