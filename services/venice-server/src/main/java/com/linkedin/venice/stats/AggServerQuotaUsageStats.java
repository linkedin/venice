package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerQuotaUsageStats} is the aggregate statistics for {@code ServerQuotaUsageStats} which
 * measures requests and quota rejections of each store.
 */
public class AggServerQuotaUsageStats extends AbstractVeniceAggStats<ServerReadQuotaUsageStats> {
  private static final int SINGLE_VERSION_FOR_TOTAL_STATS = 1;

  public AggServerQuotaUsageStats(String cluster, MetricsRepository metricsRepository) {
    super(
        cluster,
        metricsRepository,
        (metrics, storeName, clusterName) -> new ServerReadQuotaUsageStats(metrics, storeName),
        false);
    totalStats.setCurrentVersion(SINGLE_VERSION_FOR_TOTAL_STATS);
  }

  public void recordAllowed(String storeName, int version, long rcu) {
    totalStats.recordAllowed(SINGLE_VERSION_FOR_TOTAL_STATS, rcu);
    getStoreStats(storeName).recordAllowed(version, rcu);
  }

  public void recordRejected(String storeName, int version, long rcu) {
    totalStats.recordRejected(SINGLE_VERSION_FOR_TOTAL_STATS, rcu);
    getStoreStats(storeName).recordRejected(version, rcu);
  }

  public void recordAllowedUnintentionally(String storeName, long rcu) {
    totalStats.recordAllowedUnintentionally(rcu);
    getStoreStats(storeName).recordAllowedUnintentionally(rcu);
  }

  public void setNodeQuotaResponsibility(String storeName, int version, long nodeKpsResponsibility) {
    getStoreStats(storeName).setNodeQuotaResponsibility(version, nodeKpsResponsibility);
  }

  public void setCurrentVersion(String storeName, int version) {
    getStoreStats(storeName).setCurrentVersion(version);
  }

  public void setBackupVersion(String storeName, int version) {
    getStoreStats(storeName).setBackupVersion(version);
  }
}
