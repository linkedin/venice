package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import io.tehuti.metrics.MetricsRepository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AggStoreConsumptionStats extends AbstractVeniceAggStats<StoreConsumptionStats> {
  public AggStoreConsumptionStats(MetricsRepository  metricsRepository) {
    super(metricsRepository,
          (metricsRepo, storeName) -> new StoreConsumptionStats(metricsRepo, storeName));
  }

  public void recordBytesConsumed(String storeName, long bytes) {
    totalStats.recordBytesConsumed(bytes);
    getStoreStats(storeName).recordBytesConsumed(bytes);
  }

  public void recordRecordsConsumed(String storeName, int count) {
    totalStats.recordRecordsConsumed(count);
    getStoreStats(storeName).recordRecordsConsumed(count);
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

  public void recordConsumerRecordsQueuePutLatency(String storeName, double latency) {
    totalStats.recordConsumerRecordsQueuePutLatency(latency);
    getStoreStats(storeName).recordConsumerRecordsQueuePutLatency(latency);
  }

  public void recordConsumerRecordsQueuePollLatency(String storeName, double latency) {
    totalStats.recordConsumerRecordsQueuePollLatency(latency);
    getStoreStats(storeName).recordConsumerRecordsQueuePollLatency(latency);
  }

  public void updateStoreConsumptionTask(String storeName, StoreConsumptionTask task) {
    getStoreStats(storeName).updateStoreConsumptionTask(task);
  }
}
