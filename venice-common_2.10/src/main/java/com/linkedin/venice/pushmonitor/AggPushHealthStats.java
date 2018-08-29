package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.stats.AbstractVeniceAggStats;
import io.tehuti.metrics.MetricsRepository;


public class AggPushHealthStats extends AbstractVeniceAggStats<PushHealthStats> {
  public AggPushHealthStats(String clusterName, MetricsRepository metricsRepository) {
    super(clusterName, metricsRepository, (metricsRepo, storeName) -> new PushHealthStats(metricsRepo, storeName));
  }

  public void recordFailedPush(String storeName, long durationInSec) {
    totalStats.recordFailedPush(durationInSec);
    getStoreStats(storeName).recordFailedPush(durationInSec);
  }

  public void recordSuccessfulPush(String storeName, long durationInSec) {
    totalStats.recordSuccessfulPush(durationInSec);
    getStoreStats(storeName).recordSuccessfulPush(durationInSec);
  }

  public void recordPushPrepartionDuration(String storeName, long durationInSec) {
    totalStats.recordPushPreparationDuration(durationInSec);
    getStoreStats(storeName).recordPushPreparationDuration(durationInSec);
  }
}
