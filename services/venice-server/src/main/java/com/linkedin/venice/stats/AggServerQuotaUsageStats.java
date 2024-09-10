package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerQuotaUsageStats} is the aggregate statistics for {@code ServerQuotaUsageStats} which
 * measures requests and quota rejections of each store.
 */
public class AggServerQuotaUsageStats extends AbstractVeniceAggStats<ServerQuotaUsageStats> {
  public AggServerQuotaUsageStats(MetricsRepository metricsRepository) {
    super(metricsRepository, (metrics, storeName) -> new ServerQuotaUsageStats(metrics, storeName));
  }

  public void recordAllowed(String storeName, long rcu) {
    totalStats.recordAllowed(rcu);
    getStoreStats(storeName).recordAllowed(rcu);
  }

  public void recordRejected(String storeName, long rcu) {
    totalStats.recordRejected(rcu);
    getStoreStats(storeName).recordRejected(rcu);
  }

  public void recordAllowedUnintentionally(String storeName, long rcu) {
    totalStats.recordAllowedUnintentionally(rcu);
    getStoreStats(storeName).recordAllowedUnintentionally(rcu);
  }

  public void setNodeQuotaResponsibility(String storeName, long nodeQpsResponsibility) {
    getStoreStats(storeName).setNodeQuotaResponsibility(nodeQpsResponsibility);
  }
}
