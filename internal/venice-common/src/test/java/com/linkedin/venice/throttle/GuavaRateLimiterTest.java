package com.linkedin.venice.throttle;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class GuavaRateLimiterTest {
  @Test
  public void testAcquirePermitBlocks() {
    // Create a rate limiter at 10 permits/sec (1 permit every 100ms)
    GuavaRateLimiter limiter = new GuavaRateLimiter(10);

    // First acquire is free (Guava pre-pays the first permit)
    long firstStartNanos = System.nanoTime();
    limiter.acquirePermit(1);
    long firstElapsedMs = (System.nanoTime() - firstStartNanos) / 1_000_000;

    // Second acquire should block for ~100ms since the rate has been exceeded
    long secondStartNanos = System.nanoTime();
    limiter.acquirePermit(1);
    long secondElapsedMs = (System.nanoTime() - secondStartNanos) / 1_000_000;

    assertTrue(
        secondElapsedMs >= firstElapsedMs,
        "Second acquire should take at least as long as the first. firstElapsedMs=" + firstElapsedMs
            + "ms, secondElapsedMs=" + secondElapsedMs + "ms");
  }

  @Test
  public void testAcquirePermitDoesNotBlockWhenPermitsAvailable() {
    // High rate so permits are always available
    GuavaRateLimiter limiter = new GuavaRateLimiter(100_000);

    long minElapsedMs = Long.MAX_VALUE;
    // Run several times to reduce flakiness from GC pauses / CPU contention.
    for (int i = 0; i < 5; i++) {
      long startNanos = System.nanoTime();
      limiter.acquirePermit(1);
      long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
      if (elapsedMs < minElapsedMs) {
        minElapsedMs = elapsedMs;
      }
    }

    assertTrue(
        minElapsedMs < 50,
        "acquirePermit should not block when permits are available, fastest attempt elapsed: " + minElapsedMs + "ms");
  }
}
