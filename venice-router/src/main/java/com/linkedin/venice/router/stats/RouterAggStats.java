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

    public void addRequest(String storeName) {
      totalStats.addRequest();
      getStoreStats(storeName).addRequest();
    }

    public void addHealthyRequest(String storeName) {
      totalStats.addHealthyRequest();
      getStoreStats(storeName).addHealthyRequest();
    }

    public void addUnhealthyRequest(String storeName) {
      totalStats.addUnhealthyRequest();
      getStoreStats(storeName).addUnhealthyRequest();
    }

    public void addLatency(String storeName, double latency) {
      totalStats.addLatency(latency);
      getStoreStats(storeName).addLatency(latency);
    }

    public void addKeySize(String storeName, double keySize) {
      totalStats.addLatency(keySize);
      getStoreStats(storeName).addKeySize(keySize);
    }

    public void addValueSize(String storeName, double valueSize) {
      totalStats.addValueSize(valueSize);
      getStoreStats(storeName).addValueSize(valueSize);
    }

    public void close() {
      for (RouterStats sotreStats : storeMetrics.values()) {
          sotreStats.close();
      }

      totalStats.close();
    }
}
