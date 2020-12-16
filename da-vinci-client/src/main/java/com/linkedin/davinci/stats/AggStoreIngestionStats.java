package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceAggStats;
import io.tehuti.metrics.MetricsRepository;

//TODO: once we've migrated this stats to multi-version. We might want to consider merge it with DIVStats
public class AggStoreIngestionStats extends AbstractVeniceAggStats<StoreIngestionStats> {
  public AggStoreIngestionStats(MetricsRepository  metricsRepository) {
    super(metricsRepository,
          (metricsRepo, storeName) -> new StoreIngestionStats(metricsRepo, storeName));
  }

  public void recordBytesConsumed(String storeName, long bytes) {
    totalStats.recordBytesConsumed(bytes);
    getStoreStats(storeName).recordBytesConsumed(bytes);
  }

  public void recordRecordsConsumed(String storeName, int count) {
    totalStats.recordRecordsConsumed(count);
    getStoreStats(storeName).recordRecordsConsumed(count);
  }

  public void recordStorageQuotaUsed(String storeName, double quota) {
    getStoreStats(storeName).recordStorageQuotaUsed(quota);
  }

  public void recordDiskQuotaAllowed(String storeName, long quotaAllowed) {
    getStoreStats(storeName).recordDiskQuotaAllowed(quotaAllowed);
  }

  public void recordPollRequestLatency(String storeName, double latency) {
    totalStats.recordPollRequestLatency(latency);
    getStoreStats(storeName).recordPollRequestLatency(latency);
  }

  public void recordPollResultNum(String storeName, int count) {
    totalStats.recordPollResultNum(count);
    getStoreStats(storeName).recordPollResultNum(count);
  }

  public void recordConsumerRecordsQueuePutLatency(String storeName, double latency) {
    totalStats.recordConsumerRecordsQueuePutLatency(latency);
    getStoreStats(storeName).recordConsumerRecordsQueuePutLatency(latency);
  }

  public void recordUnexpectedMessage(String storeName, int count) {
    totalStats.recordUnexpectedMessage(count);
    getStoreStats(storeName).recordUnexpectedMessage(count);
  }

  public void recordInconsistentStoreMetadata(String storeName, int count) {
    totalStats.recordInconsistentStoreMetadata(count);
    getStoreStats(storeName).recordInconsistentStoreMetadata(count);
  }

  public void recordKeySize(String storeName, long bytes) {
    //keySize aggregation among multiple stores is not necessary
    getStoreStats(storeName).recordKeySize(bytes);
  }

  public void recordValueSize(String storeName, long bytes) {
    //valueSize aggregation among multiple stores is not necessary
    getStoreStats(storeName).recordValueSize(bytes);
  }

  public void recordIngestionFailure(String storeName) {
    totalStats.recordIngestionFailure();
    getStoreStats(storeName).recordIngestionFailure();
  }

  public void recordLeaderProducerSynchronizeLatency(String storeName, double latency) {
    totalStats.recordLeaderProducerSynchronizeLatency(latency);
    getStoreStats(storeName).recordLeaderProducerSynchronizeLatency(latency);
  }

  public void recordWriteComputeLookUpLatency(String storeName, double latency) {
    totalStats.recordWriteComputeLookUpLatency(latency);
    getStoreStats(storeName).recordWriteComputeLookUpLatency(latency);
  }

  public void recordWriteComputeUpdateLatency(String storeName, double latency) {
    totalStats.recordWriteComputeUpdateLatency(latency);
    getStoreStats(storeName).recordWriteComputeUpdateLatency(latency);
  }

  public void recordTotalBytesReadFromKafkaAsUncompressedSize(long bytes) {
    totalStats.recordBytesReadFromKafkaAsUncompressedSize(bytes);
  }

  public void recordProcessConsumerActionLatency(String storeName, double latency) {
    totalStats.recordProcessConsumerActionLatency(latency);
    getStoreStats(storeName).recordProcessConsumerActionLatency(latency);
  }

  public void recordCheckLongRunningTasksLatency(String storeName, double latency) {
    totalStats.recordCheckLongRunningTasksLatency(latency);
    getStoreStats(storeName).recordCheckLongRunningTasksLatency(latency);
  }

  public void recordQuotaEnforcementLatency(String storeName, double latency) {
    totalStats.recordQuotaEnforcementLatency(latency);
    getStoreStats(storeName).recordQuotaEnforcementLatency(latency);
  }

  public void recordConsumerToQueueLatency(String storeName, double latency) {
    totalStats.recordConsumerToQueueLatency(latency);
    getStoreStats(storeName).recordConsumerToQueueLatency(latency);
  }

  public void recordStorageEnginePutLatency(String storeName, double latency) {
    totalStats.recordStorageEnginePutLatency(latency);
    getStoreStats(storeName).recordStorageEnginePutLatency(latency);
  }

  public void recordProduceToDrainQueueRecordNum(String storeName, int recordNum) {
    totalStats.recordProduceToDrainQueueRecordNum(recordNum);
    getStoreStats(storeName).recordProduceToDrainQueueRecordNum(recordNum);
  }

  public void recordProduceToKafkaRecordNum(String storeName, int recordNum) {
    totalStats.recordProduceToKafkaRecordNum(recordNum);
    getStoreStats(storeName).recordProduceToKafkaRecordNum(recordNum);
  }

  public void recordProduceToKafkaLatency(String storeName, double latency) {
    totalStats.recordProduceToKafkaLatency(latency);
    getStoreStats(storeName).recordProduceToKafkaLatency(latency);
  }

  public void recordWriteComputeCacheHitCount(String storeName) {
    totalStats.recordWriteComputeCacheHitCount();
    getStoreStats(storeName).recordWriteComputeCacheHitCount();
  }

}
