package com.linkedin.venice.throttle;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.QuotaExceededException;
import io.tehuti.utils.Time;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;


public class EventThrottlerTest {
  private TestTime testTime = new TestTime();

  @Test
  public void testRejectUnexpectedRequests() {
    int quota = 10;
    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        quota,
        timeWindowMS,
        "testRejectUnexpectedRequests",
        true,
        EventThrottler.REJECT_STRATEGY);

    // Send requests that number of them equals to the given quota.
    sendRequests(quota, throttler);
    // Quota exceeds.
    try {
      sendRequests(1, throttler);
      fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected.
    }

    // Time goes 1.5 second so throttler starts second time window. Now we have more capacity granted.
    testTime.sleep((long) (1500));
    sendRequests(1, throttler);
    // Quota exceeds, case we only have half quota for the in flight time window.
    try {
      sendRequests(quota / 2, throttler);
      fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected
    }
  }

  @Test
  public void testRejectExpansiveRequest() {
    long quota = 10;
    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        quota,
        timeWindowMS,
        "testRejectExpansiveRequest",
        true,
        EventThrottler.REJECT_STRATEGY);
    // Send the single request that usage exceeds the quota.
    try {
      throttler.maybeThrottle((int) quota * 10);
      fail("Usage exceeds the quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected
    }
    try {
      throttler.maybeThrottle(1);
    } catch (QuotaExceededException e) {
      fail("The previous usage should not be recorded, so we have enough quota to accept this request.");
    }
  }

  @Test
  public void testThrottlerWithSpike() {
    long quota = 10;
    long timeWindowMS = 30000l; // 30sec
    EventThrottler throttler = new EventThrottler(
        testTime,
        quota,
        timeWindowMS,
        "testRejectExpansiveRequest",
        true,
        EventThrottler.REJECT_STRATEGY);
    // we could use the quota for 30 sec.
    throttler.maybeThrottle((int) quota * 30);
    // Sleep 1.5 time window
    testTime.sleep((long) (timeWindowMS * 1.5));
    // Now we could use the quota for 15 sec
    throttler.maybeThrottle(quota * 30 / (double) 2);
    try {
      throttler.maybeThrottle(1);
      fail("Usage exceeds the quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
    }
  }

  @Test
  public void testThrottlerWithCheckingQuotaBeforeRecording() {
    long quota = 10;
    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        quota,
        timeWindowMS,
        "testThrottlerWithCheckingQuotaBeforeRecording",
        true,
        EventThrottler.REJECT_STRATEGY);
    sendRequests((int) quota, throttler);
    // Keep sending request even it's rejected. The usage should not record because we enable checkQuotaBeforeRecording.
    for (int i = 0; i < 100; i++) {
      try {
        sendRequests(1, throttler);
      } catch (QuotaExceededException e) {
        // ignore.
      }
    }
    // Time goes 1.5 second, we have extra half quota right now.
    testTime.sleep((long) (1500));
    try {
      sendRequests((int) quota / 2 - 1, throttler);
    } catch (QuotaExceededException e) {
      fail("Request should be accepted, throttler check the quota before recording, so we have enough quota now.", e);
    }
  }

  @Test
  public void testQuotaCanBeModifiedAtRuntime() {
    int startQuotaValue = 10;
    int updatedQuotaValue = 20;

    int currentQuotaValue = startQuotaValue;
    AtomicLong currentQuota = new AtomicLong(currentQuotaValue);

    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        currentQuota::get,
        timeWindowMS,
        "testQuotaCanBeModifiedAtRuntime",
        true,
        EventThrottler.REJECT_STRATEGY);

    // Send requests that number of them equals to the given quota.
    // Quota exceeds.
    try {
      sendRequests(updatedQuotaValue, throttler);
      fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected.
    }

    // Time goes 1.5 second so throttler starts second time window. Now we have more capacity granted.
    testTime.sleep((long) (1500));
    currentQuotaValue = updatedQuotaValue;
    currentQuota.set(currentQuotaValue);

    sendRequests(updatedQuotaValue, throttler);
    // Quota exceeds, case we only have half quota for the in flight time window.
    try {
      sendRequests(1, throttler);
      fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected
    }
  }

  @Test
  public void testZeroQuotaWillRejectRequests() {
    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        0,
        timeWindowMS,
        "testRejectUnexpectedRequests",
        true,
        EventThrottler.REJECT_STRATEGY);

    // Quota exceeds.
    try {
      sendRequests(1, throttler);
      fail("Number of request exceed quota, throttler should reject them.");
    } catch (QuotaExceededException e) {
      // expected.
    }
  }

  @Test
  public void testNegativeQuotaWillNotRejectRequests() {
    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        -1,
        timeWindowMS,
        "testRejectUnexpectedRequests",
        true,
        EventThrottler.REJECT_STRATEGY);

    // Quota shouldn't exceed.
    sendRequests(1000, throttler);
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

  @Test
  public void testTryAcquirePermit() {
    int quota = 10;
    long timeWindowMS = 1000L;
    EventThrottler throttler = new EventThrottler(
        testTime,
        quota,
        timeWindowMS,
        "testSilentRejectThrottler",
        true,
        EventThrottler.REJECT_STRATEGY);

    for (int i = 0; i < quota; i++) {
      assertTrue(throttler.tryAcquirePermit(1));
    }

    // Quota exceeds.
    assertFalse(throttler.tryAcquirePermit(5));

    // Time goes 1.5 second so throttler starts second time window. Now we have more capacity granted.
    testTime.sleep((long) (1500));
    assertTrue(throttler.tryAcquirePermit(1));
    assertFalse(throttler.tryAcquirePermit(quota));
  }
}
