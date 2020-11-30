package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.offsets.OffsetRecord;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;


/**
 * TODO: create a abstract class to hold similar logic between {@link RocksDBMemorryEnforcement}
 * and {@link HybridStoreQuotaEnforcement}
 */
public class RocksDBMemorryEnforcement implements StoreDataChangedListener {
  private StoreIngestionTask task;
  private boolean ingestionPaused = false;

  public RocksDBMemorryEnforcement(
      StoreIngestionTask storeIngestionTask) {
    this.task = storeIngestionTask;
  }

  public void enforceMemory(long totalBytes) {
    if (task.rocksDBMemoryStats.get().isMemoryFull(task.storeName, totalBytes)) {
      pauseIngestion();
    } else if (ingestionPaused) {
      resumeIngestion();
    }
  }

  private String getConsumingTopic(int partition) {
    String consumingTopic = task.kafkaVersionTopic;
    if (task.partitionConsumptionStateMap.containsKey(partition)) {
      PartitionConsumptionState partitionConsumptionState = task.partitionConsumptionStateMap.get(partition);
      if (partitionConsumptionState.getLeaderState().equals(LEADER)) {
        OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
        if (offsetRecord.getLeaderTopic() != null) {
          consumingTopic = offsetRecord.getLeaderTopic();
        }
      }
    }
    return consumingTopic;
  }

  public boolean isIngestionPaused() {
    return ingestionPaused;
  }

  private void pauseIngestion() {
    if (ingestionPaused) {
      return;
    }
    for (int partition : task.partitionConsumptionStateMap.keySet()) {
      task.getConsumer().forEach(consumer -> consumer.pause(getConsumingTopic(partition), partition));
    }
    ingestionPaused = true;
  }

  private void resumeIngestion() {
    if (!ingestionPaused) {
      return;
    }
    for (int partition : task.partitionConsumptionStateMap.keySet()) {
      task.getConsumer().forEach(consumer -> consumer.resume(getConsumingTopic(partition), partition));
    }
    ingestionPaused = false;
  }

}
