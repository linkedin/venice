package com.linkedin.venice.throttle;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TokenBucketTest {
  @Test
  public static void testConsume() {
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();

    long capacity = 100;
    long refillAmount = 10;

    TokenBucket tokenBucket = new TokenBucket(capacity, refillAmount, 1, TimeUnit.SECONDS, mockClock);
    assertEquals(tokenBucket.getStaleTokenCount(), capacity, "TokenBucket must start with full capacity");
    assertTrue(tokenBucket.tryConsume(80), "TokenBucket must allow consuming available tokens");
    assertEquals(
        tokenBucket.getStaleTokenCount(),
        20,
        "After consuming tokens, the remaining tokens in the bucket must be reduced");
    assertFalse(tokenBucket.tryConsume(40), "TokenBucket must not allow consuming more tokens than available");
    assertEquals(
        tokenBucket.getStaleTokenCount(),
        20,
        "After failing to consume tokens, the remaining tokens in the bucket must be unchanged");
    assertEquals(tokenBucket.getStaleUsageRatio(), 0d, "Stale usage ratio should be zero since time hasn't moved");
    doReturn(start + 3500).when(mockClock).millis(); // 3 refills of 10 each puts bucket at 50.
    assertEquals(
        tokenBucket.getStaleUsageRatio(),
        (120 / (double) TimeUnit.MILLISECONDS.toSeconds(3500)) / tokenBucket.getAmortizedRefillPerSecond());

    assertTrue(tokenBucket.tryConsume(40), "After refill, bucket must support consumption");
    assertEquals(tokenBucket.getStaleTokenCount(), 10, "After refill and consumption, bucket must have correct tokens");
  }

  @Test
  public void testTokenBucketFromRcuPerSecond() {
    long totalRcuPerSecond = 1000;
    double bucketProportion = 0.5;
    long enforcementIntervalMs = 1000;
    int enforcementCapacityMultiple = 5;
    Clock testClock = Clock.systemUTC();

    TokenBucket bucket = TokenBucket.tokenBucketFromRcuPerSecond(
        totalRcuPerSecond,
        bucketProportion,
        enforcementIntervalMs,
        enforcementCapacityMultiple,
        testClock);

    long expectedRefill = 500; // (long) Math.ceil(1000 / 1000.0 * 1000 * 0.5);
    long expectedCapacity = 2500; // 500 * 5

    assertEquals(bucket.getCapacity(), expectedCapacity);
    assertEquals(bucket.getRefillAmount(), expectedRefill);
    assertEquals(bucket.getEnforcementInterval(), enforcementIntervalMs);

    Exception exception = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> TokenBucket.tokenBucketFromRcuPerSecond(0, 0.5, 1000, 1, testClock));
    assertTrue(exception.getMessage().contains("TokenBucket capacity 0 is not valid.  Must be greater than 0"));

    // 100% bucket proportion should match the total RCU
    totalRcuPerSecond = 2000;
    bucketProportion = 1.0;
    enforcementIntervalMs = 500;
    enforcementCapacityMultiple = 3;

    bucket = TokenBucket.tokenBucketFromRcuPerSecond(
        totalRcuPerSecond,
        bucketProportion,
        enforcementIntervalMs,
        enforcementCapacityMultiple,
        testClock);

    expectedRefill = 1000; // (long) Math.ceil(2000 / 1000.0 * 500 * 1.0);
    expectedCapacity = expectedRefill * enforcementCapacityMultiple;
    assertEquals(bucket.getCapacity(), expectedCapacity);
    assertEquals(bucket.getRefillAmount(), expectedRefill);
  }
}
