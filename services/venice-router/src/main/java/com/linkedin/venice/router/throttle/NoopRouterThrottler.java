package com.linkedin.venice.router.throttle;

import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutersClusterManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NoopRouterThrottler
    implements RouterThrottler, RoutersClusterManager.RouterCountChangedListener, StoreDataChangedListener {
  private final ZkRoutersClusterManager zkRoutersManager;
  private final ReadOnlyStoreRepository storeRepository;
  private final AggRouterHttpRequestStats stats;
  private final double perStoreRouterQuotaBuffer;

  private long idealTotalQuotaPerRouter;

  private final long maxRouterReadCapacity;

  private static final Logger LOGGER = LogManager.getLogger(NoopRouterThrottler.class);

  public NoopRouterThrottler(
      ZkRoutersClusterManager zkRoutersManager,
      ReadOnlyStoreRepository storeRepository,
      AggRouterHttpRequestStats stats,
      VeniceRouterConfig routerConfig) {
    this.zkRoutersManager = zkRoutersManager;
    this.storeRepository = storeRepository;
    this.stats = stats;
    this.zkRoutersManager.subscribeRouterCountChangedEvent(this);
    this.perStoreRouterQuotaBuffer = routerConfig.getPerStoreRouterQuotaBuffer();
    this.storeRepository.registerStoreDataChangedListener(this);
    this.maxRouterReadCapacity = routerConfig.getMaxRouterReadCapacityCu();
    this.idealTotalQuotaPerRouter = calculateIdealTotalQuotaPerRouter(
        stats,
        zkRoutersManager.getLiveRoutersCount(),
        storeRepository.getTotalStoreReadQuota(),
        maxRouterReadCapacity);
    buildAllStoreReadQuotaStats();
  }

  @Override
  public void mayThrottleRead(String storeName, double readCapacityUnit, String storageNodeId) {
    // noop
  }

  @Override
  public int getReadCapacity() {
    return 1;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  private void buildAllStoreReadQuotaStats() {
    List<Store> allStores = storeRepository.getAllStores();
    for (Store store: allStores) {
      buildStoreReadQuotaStats(store.getName(), store.getReadQuotaInCU());
    }
  }

  private void buildStoreReadQuotaStats(String storeName, long storeReadQuota) {
    int routerCount = zkRoutersManager.getLiveRoutersCount();
    long storeQuotaPerRouter = calculateStoreQuota(
        storeReadQuota,
        routerCount,
        idealTotalQuotaPerRouter,
        maxRouterReadCapacity,
        perStoreRouterQuotaBuffer);
    stats.recordQuota(storeName, storeQuotaPerRouter);
  }

  @Override
  public void handleStoreCreated(Store store) {
    buildStoreReadQuotaStats(store.getName(), store.getReadQuotaInCU());
  }

  @Override
  public void handleStoreChanged(Store store) {
    buildStoreReadQuotaStats(store.getName(), store.getReadQuotaInCU());
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    stats.recordQuota(storeName, 0);
  }

  @Override
  public void handleRouterCountChanged(int newRouterCount) {
    buildAllStoreReadQuotaStats();
  }
}
