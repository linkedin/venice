package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceAggStats;
import io.tehuti.metrics.MetricsRepository;

public class AggRouterHttpRequestStats extends AbstractVeniceAggStats<RouterHttpRequestStats>{
  public AggRouterHttpRequestStats(MetricsRepository metricsRepository) {
    super(metricsRepository,
          (metricsRepo, storeName) -> new RouterHttpRequestStats(metricsRepo, storeName));
  }

    public void recordRequest(String storeName) {
      totalStats.recordRequest();
      getStoreStats(storeName).recordRequest();
    }

    public void recordHealthyRequest(String storeName) {
      totalStats.recordHealthyRequest();
      getStoreStats(storeName).recordHealthyRequest();
    }

    public void recordUnhealthyRequest(String storeName) {
      totalStats.recordUnhealthyRequest();
      getStoreStats(storeName).recordUnhealthyRequest();
    }

    public void recordLatency(String storeName, double latency) {
      totalStats.recordLatency(latency);
      getStoreStats(storeName).recordLatency(latency);
    }

    public void recordKeySize(String storeName, double keySize) {
      totalStats.recordKeySize(keySize);
      getStoreStats(storeName).recordKeySize(keySize);
    }

    public void recordValueSize(String storeName, double valueSize) {
      totalStats.recordValueSize(valueSize);
      getStoreStats(storeName).recordValueSize(valueSize);
    }
}
