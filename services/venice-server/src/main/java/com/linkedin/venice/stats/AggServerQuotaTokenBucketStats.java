package com.linkedin.venice.stats;

import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerQuotaTokenBucketStats} is the aggregate statistics for {@code ServerQuotaTokenBucketStats}.
 * It recomputes the aggregate metrics when store change event happens.
 */
public class AggServerQuotaTokenBucketStats extends AbstractVeniceAggStats<ServerQuotaTokenBucketStats>
    implements StoreDataChangedListener {
  public AggServerQuotaTokenBucketStats(
      MetricsRepository metricsRepository,
      ReadQuotaEnforcementHandler quotaEnforcer) {
    super(
        metricsRepository,
        (metrics, storeName) -> new ServerQuotaTokenBucketStats(
            metrics,
            storeName,
            () -> quotaEnforcer.getBucketForStore(storeName)));
  }

  @Override
  public void handleStoreCreated(Store store) {
    handleStoreChanged(store);
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    // no-op
  }

  @Override
  public void handleStoreChanged(Store store) {
    initializeStatsForStore(store.getName());
  }

  public void initializeStatsForStore(String storeName) {
    // The side-effect of this call is to create the stat if it does not
    // already exist. That side-effect is idempotent.
    getStoreStats(storeName);
  }
}
