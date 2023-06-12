package com.linkedin.venice.throttle;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
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
    assertEquals(
        tokenBucket.getStaleUsageRatio(),
        (double) 80 / (double) 100,
        "No new consumption or refill since last consumption of 80, usage ratio should be 80/100");
    assertFalse(tokenBucket.tryConsume(40), "TokenBucket must not allow consuming more tokens than available");
    assertEquals(
        tokenBucket.getStaleTokenCount(),
        20,
        "After failing to consume tokens, the remaining tokens in the bucket must be unchanged");
    assertEquals(
        tokenBucket.getStaleUsageRatio(),
        (double) 80 / (double) 100,
        "After failing to consume tokens, the usage ratio should remain to be 80/100");
    doReturn(start + 3500).when(mockClock).millis(); // 3 refills of 10 each puts bucket at 50.

    assertTrue(tokenBucket.tryConsume(40), "After refill, bucket must support consumption");
    assertEquals(tokenBucket.getStaleTokenCount(), 10, "After refill and consumption, bucket must have correct tokens");
    assertEquals(
        tokenBucket.getStaleUsageRatio(),
        (double) 40 / (double) 50,
        "After 3 refills and the most recent consumption of 40 the usage ratio should be 40/50");
  }
}
