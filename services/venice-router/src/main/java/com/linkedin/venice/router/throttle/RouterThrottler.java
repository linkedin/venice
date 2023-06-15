package com.linkedin.venice.router.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import org.apache.logging.log4j.Logger;


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

  Logger getLogger();

  default long calculateStoreQuota(
      long storeQuota,
      int routerCount,
      long idealTotalQuotaPerRouter,
      long maxRouterReadCapacity,
      double perStoreRouterQuotaBuffer) {
    long idealStoreQuotaPerRouter = routerCount > 0
        ? Math.max(storeQuota / routerCount, 5) // Do not make quota to be 0 when storeQuota < routerCount
        : 0;

    if (idealTotalQuotaPerRouter <= maxRouterReadCapacity) {
      // Current router's capacity is big enough to be allocated to each store's quota.
      return idealStoreQuotaPerRouter * (1 + (long) perStoreRouterQuotaBuffer);
    } else {
      // If we allocate ideal quota value to each store, the total quota would exceed the router's capacity.
      // The reason is the cluster does not have enough number of routers.(Might be caused by to manny router failures)
      // So each store's quota must be adjusted accordingly to make sure total quota would not exceed router's capacity.
      // Compare to the solution that use a single throttler per router to protect usage exceeding router's capacity,
      // this logic could reduce the quota for each store in proportion which could prevent the usage of a few stores
      // eat all quota.
      getLogger().warn(
          "The ideal total quota per router: {} has exceeded the router's max capacity: {}, will reduce quotas for all store in proportion.",
          idealTotalQuotaPerRouter,
          maxRouterReadCapacity);
      return idealStoreQuotaPerRouter * maxRouterReadCapacity / idealTotalQuotaPerRouter;
    }
  }

  default long calculateIdealTotalQuotaPerRouter(
      AggRouterHttpRequestStats stats,
      int routerCount,
      long totalStoreReadQuota,
      long maxRouterReadCapacity) {
    long totalQuota = 0;

    if (routerCount != 0) {
      totalQuota = totalStoreReadQuota / routerCount;
    }

    stats.recordTotalQuota(Math.min(totalQuota, maxRouterReadCapacity));
    return totalQuota;
  }
}
