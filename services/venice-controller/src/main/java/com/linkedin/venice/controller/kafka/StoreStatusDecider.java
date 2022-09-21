package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.PushMonitor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreStatusDecider {
  private static final Logger LOGGER = LogManager.getLogger(StoreStatusDecider.class);

  /**
   * Get the statuses of given stores based on the replicas statues in the given assignment.
   *
   * @return a map in which the key is store name and the value is store's status.
   */
  public static Map<String, String> getStoreStatues(
      List<Store> storeList,
      ResourceAssignment resourceAssignment,
      PushMonitor pushMonitor) {
    Map<String, String> storeStatusMap = new HashMap<>();
    for (Store store: storeList) {
      String resourceName = Version.composeKafkaTopic(store.getName(), store.getCurrentVersion());
      int replicationFactor = store.getReplicationFactor();
      if (!resourceAssignment.containsResource(resourceName)) {
        // TODO: Determine if it makes sense to mark stores with no versions in them as UNAVAILABLE...? That seems
        // ambiguous.
        LOGGER.warn(
            "Store: {} is unavailable because current version: {} does not exist.",
            store.getName(),
            store.getCurrentVersion());
        storeStatusMap.put(store.getName(), StoreStatus.UNAVAILABLE.toString());
        continue;
      }
      PartitionAssignment currentVersionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
      if (currentVersionAssignment.getAssignedNumberOfPartitions() < currentVersionAssignment
          .getExpectedNumberOfPartitions()) {
        // One or more partition is unavailable.
        LOGGER.warn("Store: {} is unavailable because missing one or more partitions.", store.getName());
        storeStatusMap.put(store.getName(), StoreStatus.DEGRADED.toString());
        continue;
      }

      StoreStatus status = StoreStatus.FULLLY_REPLICATED;
      for (Partition partition: currentVersionAssignment.getAllPartitions()) {
        int onlineReplicasCount =
            pushMonitor.getReadyToServeInstances(currentVersionAssignment, partition.getId()).size();
        if (onlineReplicasCount == 0) {
          // Once one partition is unavailable we say this store is unavailable, do not need to continue.
          status = StoreStatus.DEGRADED;
          LOGGER.warn(
              "Store: {} is unavailable because partition: {} has 0 ONLINE replicas.",
              store.getName(),
              partition.getId());
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
