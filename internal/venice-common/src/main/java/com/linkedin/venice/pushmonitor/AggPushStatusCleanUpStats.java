package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import io.tehuti.metrics.MetricsRepository;


public class AggPushStatusCleanUpStats extends AbstractVeniceAggStoreStats<PushStatusCleanUpStats> {
  public AggPushStatusCleanUpStats(
      String clusterName,
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(
        clusterName,
        metricsRepository,
        PushStatusCleanUpStats::new,
        metadataRepository,
        isUnregisterMetricForDeletedStoreEnabled);
  }

  public void recordLeakedPushStatusCount(int count) {
    totalStats.recordLeakedPushStatusCount(count);
  }

  public void recordSuccessfulLeakedPushStatusCleanUpCount(int count) {
    totalStats.recordSuccessfulLeakedPushStatusCleanUpCount(count);
  }

  public void recordFailedLeakedPushStatusCleanUpCount(int count) {
    totalStats.recordFailedLeakedPushStatusCleanUpCount(count);
  }

  public void recordLeakedPushStatusCleanUpServiceState(PushStatusCleanUpServiceState state) {
    totalStats.recordLeakedPushStatusCleanUpServiceState(state);
  }
}
