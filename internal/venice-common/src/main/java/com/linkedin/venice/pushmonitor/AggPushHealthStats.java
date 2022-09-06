package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.stats.AbstractVeniceAggStoreStats;
import io.tehuti.metrics.MetricsRepository;


public class AggPushHealthStats extends AbstractVeniceAggStoreStats<PushHealthStats> {
  public AggPushHealthStats(
      String clusterName,
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(
        clusterName,
        metricsRepository,
        PushHealthStats::new,
        metadataRepository,
        isUnregisterMetricForDeletedStoreEnabled);
  }

  public void recordFailedPush(String storeName, long durationInSec) {
    totalStats.recordFailedPush(durationInSec);
    getStoreStats(storeName).recordFailedPush(durationInSec);
  }

  public void recordSuccessfulPush(String storeName, long durationInSec) {
    totalStats.recordSuccessfulPush(durationInSec);
    getStoreStats(storeName).recordSuccessfulPush(durationInSec);
  }

  public void recordSuccessfulPushGauge(String storeName, long durationInSec) {
    getStoreStats(storeName).recordSuccessfulPushGauge(durationInSec);
  }

  public void recordPushPrepartionDuration(String storeName, long durationInSec) {
    totalStats.recordPushPreparationDuration(durationInSec);
    getStoreStats(storeName).recordPushPreparationDuration(durationInSec);
  }
}
