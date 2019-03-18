package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


public class StoreStatusDecider {
  private final static Logger logger = Logger.getLogger(StoreStatusDecider.class);

  /**
   * Get the statuses of given stores based on the replicas statues in the given assignment.
   *
   * @return a map which's key is store name and value is store's status.
   */
  public static Map<String, String> getStoreStatues(List<Store> storeList, ResourceAssignment resourceAssignment,
      VeniceControllerClusterConfig controllerClusterConfig) {
    Map<String, String> storeStatusMap = new HashMap<>();
    for (Store store : storeList) {
      int replicationFactor = controllerClusterConfig.getReplicaFactor();
      String resourceName = Version.composeKafkaTopic(store.getName(), store.getCurrentVersion());
      if (!resourceAssignment.containsResource(resourceName)) {
        // TODO: Determine if it makes sense to mark stores with no versions in them as UNAVAILABLE...? That seems ambiguous.
        logger.warn("Store:" + store.getName() + " is unavailable because current version: " + store.getCurrentVersion()
            + " does not exist ");
        storeStatusMap.put(store.getName(), StoreStatus.UNAVAILABLE.toString());
        continue;
      }
      PartitionAssignment currentVersionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
      if (currentVersionAssignment.getAssignedNumberOfPartitions()
          < currentVersionAssignment.getExpectedNumberOfPartitions()) {
        // One or more partition is unavailable.
        logger.warn("Store: " + store.getName() + " is unavailable because missing one or more partitions.");
        storeStatusMap.put(store.getName(), StoreStatus.DEGRADED.toString());
        continue;
      }

      StoreStatus status = StoreStatus.FULLLY_REPLICATED;
      for (Partition partition : currentVersionAssignment.getAllPartitions()) {
        int onlineReplicasCount = partition.getReadyToServeInstances().size();
        if (onlineReplicasCount == 0) {
          // Once one partition is unavailable we say this store is unavailable, do not need to continue.
          status = StoreStatus.DEGRADED;
          logger.warn("Store: " + store.getName() + " is unavailable because partition: " + partition.getId()
              + " has 0 ONLINE replicas.");
          break;
        } else if (onlineReplicasCount < replicationFactor) {
          // One partition is under replicated, degrade store status from fully replicated to under replicated.
          status = StoreStatus.UNDER_REPLICATED;
        }
      }
      storeStatusMap.put(store.getName(), status.toString());
    }
    return storeStatusMap;
  }
}
