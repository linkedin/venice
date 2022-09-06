package com.linkedin.venice.throttle;

import io.tehuti.utils.Time;


/**
 * This interface is used to abstract the strategy to handle the quota exceeding case.
 */
public interface EventThrottlingStrategy {
  /**
   * This method will be executed by event throttler once the usage exceeded the quota.
   */
  void onExceedQuota(Time time, String throttlerName, long currentRate, long quota, long timeWindowMS);
}
