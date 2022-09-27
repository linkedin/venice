package com.linkedin.venice.helix;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A class for accessing partition offline push and hybrid quota status in Helix customized state (per Helix
 * instance) on Zookeeper
 */

public class HelixPartitionStatusAccessor extends HelixPartitionStateAccessor {
  static final String PARTITION_DELIMITER = "_";
  private final boolean helixHybridStoreQuotaEnabled;
  private static final Logger LOGGER = LogManager.getLogger(HelixPartitionStatusAccessor.class);

  public HelixPartitionStatusAccessor(
      HelixManager helixManager,
      String instanceId,
      boolean isHelixHybridStoreQuotaEnabled) {
    super(helixManager, instanceId);
    this.helixHybridStoreQuotaEnabled = isHelixHybridStoreQuotaEnabled;
  }

  public void updateReplicaStatus(String topic, int partitionId, ExecutionStatus status) {
    super.updateReplicaStatus(
        HelixPartitionState.OFFLINE_PUSH,
        topic,
        getPartitionNameFromId(topic, partitionId),
        status.name());
  }

  public void updateHybridQuotaReplicaStatus(String topic, int partitionId, HybridStoreQuotaStatus status) {
    if (helixHybridStoreQuotaEnabled) {
      super.updateReplicaStatus(
          HelixPartitionState.HYBRID_STORE_QUOTA,
          topic,
          getPartitionNameFromId(topic, partitionId),
          status.name());
    }
  }

  /**
   * When a replica is gone from an instance due to partition movement or resource drop, we need to call this delete
   * function to explicitly delete the customized state for that replica. Otherwise, customized state will still stay there.
   * Usually this should happen during state transition.
   *
   * If the partition state is the last partition state in the resource znode, the znode will also be deleted.
   */
  public void deleteReplicaStatus(String topic, int partitionId) {
    /**
     * We don't want to do the two delete operations atomically; Even if one delete fails,
     * the other delete operation should still continue.
     */
    try {
      super.deleteReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic, getPartitionNameFromId(topic, partitionId));
    } catch (NullPointerException e) {
      LOGGER.warn(
          "The partition {} doesn't exist in resource {}, cannot delete a non-existent partition state for {}.",
          partitionId,
          topic,
          HelixPartitionState.OFFLINE_PUSH.name());
    }
    if (helixHybridStoreQuotaEnabled) {
      try {
        super.deleteReplicaStatus(
            HelixPartitionState.HYBRID_STORE_QUOTA,
            topic,
            getPartitionNameFromId(topic, partitionId));
      } catch (NullPointerException e) {
        LOGGER.warn(
            "The partition {} doesn't exist in resource {}, cannot delete a non-existent partition state for {}.",
            partitionId,
            topic,
            HelixPartitionState.HYBRID_STORE_QUOTA.name());
      }
    }
  }

  public ExecutionStatus getReplicaStatus(String topic, int partitionId) {
    return ExecutionStatus.valueOf(
        super.getReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic, getPartitionNameFromId(topic, partitionId)));
  }

  public HybridStoreQuotaStatus getHybridQuotaReplicaStatus(String topic, int partitionId) {
    if (helixHybridStoreQuotaEnabled) {
      return HybridStoreQuotaStatus.valueOf(
          super.getReplicaStatus(
              HelixPartitionState.HYBRID_STORE_QUOTA,
              topic,
              getPartitionNameFromId(topic, partitionId)));
    }
    return HybridStoreQuotaStatus.UNKNOWN;
  }

  public Map<Integer, ExecutionStatus> getAllReplicaStatus(String topic) {
    Map<String, String> customizedStateMap = super.getAllReplicaStatus(HelixPartitionState.OFFLINE_PUSH, topic);
    Map<Integer, ExecutionStatus> results = new HashMap<>();
    for (Map.Entry entry: customizedStateMap.entrySet()) {
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
    return Integer.parseInt(partitionName.substring(partitionName.indexOf(PARTITION_DELIMITER) + 1));
  }
}
