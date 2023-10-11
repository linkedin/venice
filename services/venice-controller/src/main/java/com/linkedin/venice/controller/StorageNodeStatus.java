package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixState;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Class used to represent the status of storage node. It contains the status value of all replicas in one storage node.
 */
public class StorageNodeStatus {
  private static Log logger = LogFactory.getLog(StorageNodeStatus.class);

  private final Map<String, HelixState> replicaToStatusMap;

  public StorageNodeStatus() {
    replicaToStatusMap = new HashMap<>();
  }

  void addStatusForReplica(String partitionName, HelixState state) {
    replicaToStatusMap.put(partitionName, state);
  }

  int getStatusValueForReplica(String partitionName) {
    // By default, the status value is 0. So when the new server status does not contain the given replica, the
    // status of this replica is 0.
    return replicaToStatusMap.getOrDefault(partitionName, HelixState.UNKNOWN).getStateValue();
  }

  /**
   * Only compare the partition assigned to the server now. If partition has been moved out to other server, just ignore
   * them.
   * <p>
   * Even the server is just restarted, as long as the partition is not moved out we could find the replica in our
   * routing data repository(actually from external view), the replica is OFFLINE. If the resource is deleted
   * during the server upgrading, the replica should be dropped, and we could not find the replica in the routing data
   * repository. So we don't need to filter the status by resources in IdealState to avoid the wrong result, because
   * we only compare the resource/replica in the current storage node status.
   */
  boolean isNewerOrEqual(StorageNodeStatus oldStatus) {
    for (String partitionName: replicaToStatusMap.keySet()) {
      int oldReplicaStatus = oldStatus.getStatusValueForReplica(partitionName);
      int replicaStatus = getStatusValueForReplica(partitionName);

      if (replicaStatus < oldReplicaStatus) {
        // the old status is larger than the current status.
        // Return directly, we don't need to compare the rest of status. Because, in the old server status, even if
        // there is only one replica's status was larger than the current one. The whole current storage node status
        // would not be newer or equal than the old one.
        logger.info(
            "Current storage node status is NOT newer than the old one. Replica +" + partitionName + " status, "
                + "old:" + oldReplicaStatus + " src:" + replicaStatus);
        return false;
      }
    }
    logger.info("Current storage node status is newer or equal than the old storage node status.");
    return true;
  }
}
