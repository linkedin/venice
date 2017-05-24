package com.linkedin.venice.router.throttle;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.router.RoutersClusterManager;
import com.linkedin.venice.router.ZkRoutersClusterManager;
import com.linkedin.venice.throttle.EventThrottler;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 * This class define the throttler on reads request. Basically it will calculate the local quota based on the total
 * quota and number of
 * routers. And for each read request throttler will check both store level quota and storage level quota then accept
 * or reject it.
 * // TODO Because right now we could not delete a store from the cluster, we should handle the store deletion here to
 * remove the related reads quota.
 */
public class ReadsRequestThrottler implements  RoutersClusterManager.RouterCountChangedListener {
  public static final long DEFAULT_STORE_READ_QUOTA = 1000;
  private static final Logger logger = Logger.getLogger(ReadsRequestThrottler.class);
  private final ZkRoutersClusterManager zkRoutersManager;
  private final ReadOnlyStoreRepository storeRepository;

  private final ConcurrentMap<String, EventThrottler> storesThrottlersMap;

  public ReadsRequestThrottler(ZkRoutersClusterManager zkRoutersManager, ReadOnlyStoreRepository storeRepository) {
    this.zkRoutersManager = zkRoutersManager;
    this.storeRepository = storeRepository;
    this.storesThrottlersMap = new ConcurrentHashMap<>();
    zkRoutersManager.subscribeRouterCountChangedEvent(this);
  }

  /**
   * Check the quota and reject the request if needed.
   *
   * @param storeName        name of the store that request is trying to visit.
   * @param readCapacityUnit usage of this read request.
   *
   * @throws QuotaExceededException if the usage exceeded the quota throw this exception to reject the request.
   */
  public void mayThrottleRead(String storeName, int readCapacityUnit)
      throws QuotaExceededException {
    mayThrottleReadOnStore(storeName, readCapacityUnit);
    // TODO Per storage node quota checking.
  }

  protected void mayThrottleReadOnStore(String storeName, int readCapacityUnit) {
    EventThrottler throttler = storesThrottlersMap.get(storeName);
    // Create throttler if it does not exist.
    if (throttler == null) {
      long quota = getQuotaForStore(storeName);
      throttler = new EventThrottler(quota, true, EventThrottler.REJECT_STRATEGY);
      EventThrottler existingThrottler = storesThrottlersMap.putIfAbsent(storeName, throttler);
      // In case that throttler has been created by other threads, use the existing one.
      throttler = existingThrottler == null ? throttler : existingThrottler;
      logger.info("Updated throttler for store:" + storeName + ", quota: " + quota);
    }
    throttler.maybeThrottle(readCapacityUnit);
  }

  protected long getQuotaForStore(String storeName) {
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }
    long totalQuota = store.getReadQuotaInCU();
    if (totalQuota <= 0) {
      // Have not set quota for this store. Give a default value.
      // If the store was created before we releasing quota feature, JSON framework wil give 0 as the default value
      // while deserializing the store from ZK.
      totalQuota = DEFAULT_STORE_READ_QUOTA;
    }
    int liveRouterCount = zkRoutersManager.getRoutersCount();
    // At least the current router is live. The number might have some delay due to zk Notification.
    if (liveRouterCount <= 0) {
      liveRouterCount = 1;
    }

    long quotaPerRouter = totalQuota / liveRouterCount;
    // TODO handle the case that there are too many routers failures, the quota per router exceed the capacity of
    // TODO single router.
    return quotaPerRouter;
  }

  // TODO will update once we complete some experiments to finalize the correlation between size and read capacity unit.
  // TODO right now read capacity unit is just QPS;
  public int getReadCapacity() {
    return 1;
  }

  @Override
  public void handleRouterCountChanged(int newRouterCount) {
    //Clean all existing throttlers. We will create them again with the latest router count once getting new requests.
    storesThrottlersMap.clear();
  }
}
