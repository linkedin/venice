package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.stats.AbstractVeniceAggStats;
import io.tehuti.metrics.MetricsRepository;


public class AggPushStatusCleanUpStats extends AbstractVeniceAggStats<PushStatusCleanUpStats> {
  public AggPushStatusCleanUpStats(String clusterName, MetricsRepository metricsRepository) {
    super(clusterName, metricsRepository, PushStatusCleanUpStats::new);
  }

  public void recordSuccessfulLeakedPushStatusCleanUpCount(String storeName, int count) {
    totalStats.recordSuccessfulLeakedPushStatusCleanUpCount(count);
    getStoreStats(storeName).recordSuccessfulLeakedPushStatusCleanUpCount(count);
  }

  public void recordFailedLeakedPushStatusCleanUpCount(String storeName, int count) {
    totalStats.recordFailedLeakedPushStatusCleanUpCount(count);
    getStoreStats(storeName).recordFailedLeakedPushStatusCleanUpCount(count);
  }

  public void recordLeakedPushStatusCleanUpServiceState(PushStatusCleanUpServiceState state) {
    totalStats.recordLeakedPushStatusCleanUpServiceState(state);
  }
}
