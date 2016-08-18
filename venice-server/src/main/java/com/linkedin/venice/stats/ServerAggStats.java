package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.server.VeniceServer;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ServerAggStats {
  private static ServerAggStats instance;
  private static MetricsRepository metricsRepository;

  //per store metrics
  final Map<String, ServerStats> storeMetrics;
  final ServerStats totalStats;

  public static synchronized void init(MetricsRepository metricsRepository) {
    if (metricsRepository == null) {
      throw new IllegalArgumentException("metricsRepository is null");
    }
    if (instance == null) {
      ServerAggStats.metricsRepository = metricsRepository;
      instance = new ServerAggStats();
    }
  }

  public static ServerAggStats getInstance() {
    if (instance == null) {
      init(TehutiUtils.getMetricsRepository(VeniceServer.SERVER_SERVICE_NAME));
    }
    return instance;
  }

  private ServerStats getStoreStats(String storeName) {
    ServerStats storeStats =
        storeMetrics.computeIfAbsent(storeName, k -> new ServerStats(metricsRepository, storeName));
    return storeStats;
  }

  private ServerAggStats() {
    this.storeMetrics = new ConcurrentHashMap<>();
    this.totalStats = new ServerStats(metricsRepository, "total");
  }

  public void addBytesConsumed(String storeName, long bytes) {
    totalStats.addBytesConsumed(bytes);
    getStoreStats(storeName).addBytesConsumed(bytes);
  }

  public void addRecordsConsumed(String storeName, int count) {
    totalStats.addRecordsConsumed(count);
    getStoreStats(storeName).addRecordsConsumed(count);
  }
}
