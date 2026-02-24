package com.linkedin.venice.throttle;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class GuavaRateLimiterTest {
  @Test
  public void testAcquirePermitBlocks() {
    // Create a rate limiter at 10 permits/sec (1 permit every 100ms)
    GuavaRateLimiter limiter = new GuavaRateLimiter(10);

    // First acquire is free (Guava pre-pays the first permit)
    limiter.acquirePermit(1);

    // Second acquire should block for ~100ms
    long startNanos = System.nanoTime();
    limiter.acquirePermit(1);
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertTrue(elapsedMs >= 50, "acquirePermit should block when rate is exceeded, elapsed: " + elapsedMs + "ms");
  }

  @Test
  public void testAcquirePermitDoesNotBlockWhenPermitsAvailable() {
    // High rate so permits are always available
    GuavaRateLimiter limiter = new GuavaRateLimiter(100_000);

    long startNanos = System.nanoTime();
    limiter.acquirePermit(1);
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertTrue(
        elapsedMs < 50,
        "acquirePermit should not block when permits are available, elapsed: " + elapsedMs + "ms");
  }
}
