package com.linkedin.venice.router.stats;

import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RouterAggStats {
  private static RouterAggStats instance;
  private static MetricsRepository metricsRepository;

    //per store metrics
    final Map<String, RouterStats> storeMetrics;
    final RouterStats totalStats;

    public static synchronized void init(MetricsRepository metricsRepository) {
      if(metricsRepository == null) {
        throw new IllegalArgumentException("metricsRepository is null");
      }
      if (instance == null) {
          RouterAggStats.metricsRepository = metricsRepository;
          instance = new RouterAggStats();
      }
    }

    public static RouterAggStats getInstance() {
      if (instance == null) {
          throw new VeniceException("RouterAggStats has not been initialized yet");
      }
      return instance;
    }

    private RouterStats getStoreStats(String storeName) {
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStats(metricsRepository, storeName));
      return storeStats;
    }

    private RouterAggStats() {
      this.storeMetrics = new ConcurrentHashMap<>();
      this.totalStats = new RouterStats(metricsRepository, "total");
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

    public void close() {
      for (RouterStats sotreStats : storeMetrics.values()) {
          sotreStats.close();
      }

      totalStats.close();
    }
}
