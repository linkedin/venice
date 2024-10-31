package com.linkedin.davinci.ingestion.main;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;


/**
 * This class manages the ingestion status for each partition for a given topic when isolated ingestion is enabled.
 * Each partition that exists in the status map will be either be hosted in main process or forked process.
 */
public class MainTopicIngestionStatus {
  private final String topicName;
  private final Map<Integer, MainPartitionIngestionStatus> ingestionStatusMap = new VeniceConcurrentHashMap<>();

  public MainTopicIngestionStatus(String topicName) {
    this.topicName = topicName;
  }

  public void setPartitionIngestionStatusToLocalIngestion(int partitionId) {
    ingestionStatusMap.put(partitionId, MainPartitionIngestionStatus.MAIN);
  }

  public void setPartitionIngestionStatusToIsolatedIngestion(int partitionId) {
    ingestionStatusMap.put(partitionId, MainPartitionIngestionStatus.ISOLATED);
  }

  public void removePartitionIngestionStatus(int partitionId) {
    ingestionStatusMap.remove(partitionId);
  }

  public MainPartitionIngestionStatus getPartitionIngestionStatus(int partitionId) {
    return ingestionStatusMap.getOrDefault(partitionId, MainPartitionIngestionStatus.NOT_EXIST);
  }

  public Map<Integer, MainPartitionIngestionStatus> getPartitionIngestionStatusSet() {
    return ingestionStatusMap;
  }

  public long getIngestingPartitionCount() {
    return ingestionStatusMap.size();
  }

  public String getTopicName() {
    return topicName;
  }

  public boolean hasPartitionIngestingInIsolatedProcess() {
    for (Map.Entry<Integer, MainPartitionIngestionStatus> entry: ingestionStatusMap.entrySet()) {
      if (entry.getValue().equals(MainPartitionIngestionStatus.ISOLATED)) {
        return true;
      }
    }
    return false;
  }
}
