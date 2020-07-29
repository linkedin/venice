package com.linkedin.venice.helix;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import org.apache.helix.HelixManager;

import java.util.HashMap;
import java.util.Map;


/**
 * A class for accessing partition offline push and hybrid quota status in Helix customized state (per Helix
 * instance) on Zookeeper
 */

public class HelixPartitionStatusAccessor extends HelixPartitionStateAccessor {
  static final String PARTITION_DELIMITER = "_";

  public HelixPartitionStatusAccessor(HelixManager helixManager, String instanceId) {
    super(helixManager, instanceId);
  }

  public void updateReplicaStatus(String topic, int partitionId, ExecutionStatus status) {
    super.updateReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic,
        getPartitionNameFromId(topic, partitionId), status.name());
  }

  public void updateHybridQuotaReplicaStatus(String topic, int partitionId, HybridStoreQuotaStatus status) {
    super.updateReplicaStatus(HelixPartitionState.HYBRID_STORE_QUOTA, topic,
        getPartitionNameFromId(topic, partitionId), status.name());
  }

  /**
   * When a replica is gone from an instance due to partition movement or resource drop, we need to call this delete
   * function to explicitly delete the customized state for that replica. Otherwise, customized state will still stay there.
   * Usually this should happen during state transition.
   */
  public void deleteReplicaStatus(String topic, int partitionId) {
    super.deleteReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic,
        getPartitionNameFromId(topic, partitionId));
    super.deleteReplicaStatus(HelixPartitionState.HYBRID_STORE_QUOTA, topic,
        getPartitionNameFromId(topic, partitionId));
  }

  public ExecutionStatus getReplicaStatus(String topic, int partitionId) {
    return ExecutionStatus.valueOf(super.getReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic,
        getPartitionNameFromId(topic, partitionId)));
  }

  public HybridStoreQuotaStatus getHybridQuotaReplicaStatus(String topic, int partitionId) {
    return HybridStoreQuotaStatus.valueOf(super.getReplicaStatus(HelixPartitionState.HYBRID_STORE_QUOTA, topic,
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
