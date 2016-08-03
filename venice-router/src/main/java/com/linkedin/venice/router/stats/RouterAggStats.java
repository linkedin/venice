package com.linkedin.venice.router.stats;

import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RouterAggStats extends AbstractRouterStats {
  private static Logger logger = Logger.getLogger(RouterAggStats.class);

  private static RouterAggStats stats;

    //per store metrics
    final Map<String, RouterStats> storeMetrics;

    public static synchronized void init(MetricsRepository metricsRepository) {
      if (stats == null) {
          stats = new RouterAggStats(metricsRepository, "total");
      }
    }

    public static RouterAggStats getStats() {
      if (stats == null) {
          throw new VeniceException("RouterAggStats has not been initialized yet");
      }
      return stats;
    }

    private RouterAggStats(MetricsRepository metricsRepository, String name) {
      super(metricsRepository, name);
      storeMetrics = new ConcurrentHashMap<>();
    }

    public void addRequest(String storeName) {
      this.addRequest();
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStoreStats(metricsRepository, storeName));
      storeStats.addRequest();
    }

    public void addHealthyRequest(String storeName) {
      this.addHealthyRequest();
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStoreStats(metricsRepository, storeName));
      storeStats.addHealthyRequest();
    }

    public void addUnhealthyRequest(String storeName) {
      this.addUnhealthyRequest();
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStoreStats(metricsRepository, storeName));
      storeStats.addUnhealthyRequest();
    }

    public void addLatency(String storeName, double latency) {
      this.addLatency(latency);
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStoreStats(metricsRepository, storeName));
      storeStats.addLatency(latency);
    }

    public void addKeySize(String storeName, double keySize) {
      this.addLatency(keySize);
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStoreStats(metricsRepository, storeName));
      storeStats.addKeySize(keySize);
    }

    public void addValueSize(String storeName, double valueSize) {
      this.addValueSize(valueSize);
      RouterStats storeStats = storeMetrics.computeIfAbsent(storeName,
              k -> new RouterStoreStats(metricsRepository, storeName));
      storeStats.addValueSize(valueSize);
    }

    @Override
    public void close() {
      for (RouterStats sotreStats : storeMetrics.values()) {
          sotreStats.close();
      }

      super.close();
    }
}
