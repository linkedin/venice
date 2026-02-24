package com.linkedin.venice.throttle;

import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


public class VeniceRateLimiterTest {
  @Test
  public void testDefaultAcquirePermitPollsUntilPermitAvailable() {
    AtomicInteger attempts = new AtomicInteger(0);

    // Custom implementation that rejects the first 2 attempts, then allows
    VeniceRateLimiter limiter = new VeniceRateLimiter() {
      @Override
      public boolean tryAcquirePermit(int units) {
        return attempts.incrementAndGet() > 2;
      }

      @Override
      public void setQuota(long quota) {
      }

      @Override
      public long getQuota() {
        return 0;
      }
    };

    limiter.acquirePermit(1);

    assertTrue(
        attempts.get() == 3,
        "Default acquirePermit should poll tryAcquirePermit until it succeeds, attempts: " + attempts.get());
  }

  @Test
  public void testDefaultAcquirePermitReturnsImmediatelyWhenPermitAvailable() {
    VeniceRateLimiter limiter = new VeniceRateLimiter() {
      @Override
      public boolean tryAcquirePermit(int units) {
        return true; // Always available
      }

      @Override
      public void setQuota(long quota) {
      }

      @Override
      public long getQuota() {
        return 0;
      }
    };

    long startNanos = System.nanoTime();
    limiter.acquirePermit(1);
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertTrue(elapsedMs < 50, "Should return immediately when permit is available, elapsed: " + elapsedMs + "ms");
  }

  @Test
  public void testDefaultAcquirePermitHandlesInterruption() throws InterruptedException {
    VeniceRateLimiter limiter = new VeniceRateLimiter() {
      @Override
      public boolean tryAcquirePermit(int units) {
        return false; // Never available
      }

      @Override
      public void setQuota(long quota) {
      }

      @Override
      public long getQuota() {
        return 0;
      }
    };

    Thread testThread = new Thread(() -> limiter.acquirePermit(1));
    testThread.start();

    // Give the thread time to enter the polling loop
    Thread.sleep(50);
    testThread.interrupt();
    testThread.join(1000);

    assertTrue(!testThread.isAlive(), "Thread should exit after interruption");
  }
}
