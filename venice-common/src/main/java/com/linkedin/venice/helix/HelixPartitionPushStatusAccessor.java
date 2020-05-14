package com.linkedin.venice.helix;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import org.apache.helix.HelixManager;

import java.util.HashMap;
import java.util.Map;


/**
 * A class for accessing partition offline push status in Helix customized state (per Helix
 * instance) on Zookeeper
 */

public class HelixPartitionPushStatusAccessor extends HelixPartitionStateAccessor {
  static final String PARTITION_DELIMITER = "_";

  public HelixPartitionPushStatusAccessor(HelixManager helixManager, String instanceId) {
    super(helixManager, instanceId);
  }

  public void updateReplicaStatus(String topic, int partitionId, ExecutionStatus status) {
    super.updateReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic,
        getPartitionNameFromId(topic, partitionId), status.name());
  }

  public ExecutionStatus getReplicaStatus(String topic, int partitionId) {
    return ExecutionStatus.valueOf(super.getReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic,
        getPartitionNameFromId(topic, partitionId)));
  }

  public Map<Integer, ExecutionStatus> getAllReplicaStatus(String topic) {
    Map<String, String> customizedStateMap =
        super.getAllReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic);
    Map<Integer, ExecutionStatus> results = new HashMap<>();
    for (Map.Entry entry : customizedStateMap.entrySet()) {
      String partitionName = entry.getKey().toString();
      int partitionId = getPartitionIdFromName(partitionName);
      results.put(partitionId, ExecutionStatus.valueOf(entry.getValue().toString()));
    }
    return results;
  }

  private String getPartitionNameFromId(String topic, int partitionId) {
    return topic + PARTITION_DELIMITER + partitionId;
  }

  private int getPartitionIdFromName(String partitionName) {
    return Integer.valueOf(partitionName.substring(partitionName.indexOf(PARTITION_DELIMITER) + 1));
  }
}
