package com.linkedin.venice.router.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;


public interface RouterThrottler {
  /**
   * Returns if the request should be allowed, throws a com.linkedin.venice.exceptions.QuotaExceededException if the
   * request is out of quota.
   *  @param storeName
   * @param readCapacityUnit
   * @param storageNodeId
   */
  void mayThrottleRead(String storeName, double readCapacityUnit, String storageNodeId) throws QuotaExceededException;

  int getReadCapacity();

  void setIsNoopThrottlerEnabled(boolean isNoopThrottlerEnabled);
}
