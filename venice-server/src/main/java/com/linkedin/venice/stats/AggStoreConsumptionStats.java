package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import io.tehuti.metrics.MetricsRepository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AggStoreConsumptionStats {
  private final StoreConsumptionStats totalStats;
  private final Map<String, StoreConsumptionStats> storeStats;

  private final MetricsRepository metricsRepository;

  public AggStoreConsumptionStats(MetricsRepository  metricsRepository) {
    this.metricsRepository = metricsRepository;

    totalStats = new StoreConsumptionStats(metricsRepository, "total");
    storeStats = new ConcurrentHashMap<>();
  }

  public void recordBytesConsumed(String storeName, long bytes) {
    totalStats.recordBytesConsumed(bytes);
    getStoreStats(storeName).recordBytesConsumed(bytes);
  }

  public void recordRecordsConsumed(String storeName, int count) {
    totalStats.recordRecordsConsumed(count);
    getStoreStats(storeName).recordRecordsConsumed(count);
  }

  public void recordPollRequest(String storeName) {
    totalStats.recordPollRequest();
    getStoreStats(storeName).recordPollRequest();
  }

  public void recordPollRequestLatency(String storeName, double latency) {
    totalStats.recordPollRequestLatency(latency);
    getStoreStats(storeName).recordPollRequestLatency(latency);
  }

  public void recordProcessPollResultLatency(String storeName, double latency) {
    totalStats.recordProcessPollResultLatency(latency);
    getStoreStats(storeName).recordProcessPollResultLatency(latency);
  }

  public void recordPollResultNum(String storeName, int count) {
    totalStats.recordPollResultNum(count);
    getStoreStats(storeName).recordPollResultNum(count);
  }

  public void updateStoreConsumptionTask(String storeName, StoreConsumptionTask task) {
    getStoreStats(storeName).updateStoreConsumptionTask(task );
  }

  private StoreConsumptionStats getStoreStats(String storeName) {
    return storeStats.computeIfAbsent(storeName,
        k -> new StoreConsumptionStats(metricsRepository, storeName));
  }
}
