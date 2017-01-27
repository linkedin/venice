package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AggServerHttpRequestStats {
  private final ServerHttpRequestStats totalStats;
  private final Map<String, ServerHttpRequestStats> storeStats;

  private final MetricsRepository metricsRepository;

  public AggServerHttpRequestStats(MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;

    totalStats = new ServerHttpRequestStats(metricsRepository, "total");
    storeStats = new ConcurrentHashMap<>();
  }

  public void recordSuccessRequest(String storeName) {
    totalStats.recordSuccessRequest();
    getStoreStats(storeName).recordSuccessRequest();
  }

  public void recordErrorRequest() {
    totalStats.recordErrorRequest();
  }

  public void recordErrorRequest(String storeName) {
    totalStats.recordErrorRequest();
    getStoreStats(storeName).recordErrorRequest();
  }

  public void recordSuccessRequestLatency(String storeName, double latency) {
    totalStats.recordSuccessRequestLatency(latency);
    getStoreStats(storeName).recordSuccessRequestLatency(latency);
  }

  public void recordErrorRequestLatency (double latency) {
    totalStats.recordErrorRequestLatency(latency);
  }

  public void recordErrorRequestLatency(String storeName, double latency) {
    totalStats.recordErrorRequestLatency(latency);
    getStoreStats(storeName).recordErrorRequestLatency(latency);
  }

  public void recordBdbQueryLatency(String storeName, double latency) {
    totalStats.recordBdbQueryLatency(latency);
    getStoreStats(storeName).recordBdbQueryLatency(latency);
  }

  private ServerHttpRequestStats getStoreStats(String storeName) {
    return storeStats.computeIfAbsent(storeName,
        k -> new ServerHttpRequestStats(metricsRepository, storeName));
  }
}
