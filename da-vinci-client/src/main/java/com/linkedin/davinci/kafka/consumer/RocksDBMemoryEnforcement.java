package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.offsets.OffsetRecord;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;


/**
 * TODO: create a abstract class to hold similar logic between {@link RocksDBMemoryEnforcement}
 * and {@link StorageUtilizationManager}
 */
public class RocksDBMemoryEnforcement implements StoreDataChangedListener {
  private StoreIngestionTask task;
  private boolean ingestionPaused = false;

  public RocksDBMemoryEnforcement(
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
      if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
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
      // Making calls on the consumer like this is probably unsafe in dedicated consumer mode. TODO: Fix thread-safety
      task.pauseConsumption(getConsumingTopic(partition), partition);
    }
    ingestionPaused = true;
  }

  private void resumeIngestion() {
    if (!ingestionPaused) {
      return;
    }
    for (int partition : task.partitionConsumptionStateMap.keySet()) {
      // Making calls on the consumer like this is probably unsafe in dedicated consumer mode. TODO: Fix thread-safety
      task.resumeConsumption(getConsumingTopic(partition), partition);
    }
    ingestionPaused = false;
  }

}
