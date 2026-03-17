package com.linkedin.venice.stats;

import com.linkedin.venice.utils.SystemTime;
import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerQuotaUsageStats} is the aggregate statistics for {@code ServerReadQuotaUsageStats} which
 * measures requests and quota rejections of each store.
 */
public class AggServerQuotaUsageStats extends AbstractVeniceAggStats<ServerReadQuotaUsageStats> {
  private static final int SINGLE_VERSION_FOR_TOTAL_STATS = 1;

  public AggServerQuotaUsageStats(String cluster, MetricsRepository metricsRepository) {
    super(
        cluster,
        metricsRepository,
        (
            metrics,
            storeName,
            clusterName) -> new ServerReadQuotaUsageStats(metrics, storeName, new SystemTime(), clusterName),
        false);
    totalStats.setCurrentVersion(SINGLE_VERSION_FOR_TOTAL_STATS);
  }

  // Recording methods: totalStats has OTel disabled (detected via "total" name prefix in
  // AbstractVeniceStats.isTotalStats()) to prevent double-counting. Only per-store stats
  // emit OTel metrics; OTel aggregation derives the total at query time.

  public void recordAllowed(String storeName, int version, long rcu) {
    totalStats.recordAllowed(SINGLE_VERSION_FOR_TOTAL_STATS, rcu);
    getStoreStats(storeName).recordAllowed(version, rcu);
  }

  public void recordRejected(String storeName, int version, long rcu) {
    totalStats.recordRejected(SINGLE_VERSION_FOR_TOTAL_STATS, rcu);
    getStoreStats(storeName).recordRejected(version, rcu);
  }

  public void recordAllowedUnintentionally(String storeName, int version, long rcu) {
    totalStats.recordAllowedUnintentionally(SINGLE_VERSION_FOR_TOTAL_STATS, rcu);
    getStoreStats(storeName).recordAllowedUnintentionally(version, rcu);
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
