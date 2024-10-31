package com.linkedin.venice.throttle;

public interface VeniceRateLimiter {
  enum RateLimiterType {
    EVENT_THROTTLER_WITH_SILENT_REJECTION, GUAVA_RATE_LIMITER, TOKEN_BUCKET_INCREMENTAL_REFILL,
    TOKEN_BUCKET_GREEDY_REFILL,
  }

  /**
   * Try to acquire permit for the given rcu. Will not block if permit is not available.
   * @param units Number of units to acquire.
   * @return true if permit is acquired, false otherwise.
   */
  boolean tryAcquirePermit(int units);

  /**
   * The following methods are used only when checking if the new quota requests are
   * different from the existing quota.
   */
  void setQuota(long quota);

  long getQuota();
}
