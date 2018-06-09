package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * This class is not Thread-safe.
 */
public class InstanceStatusDecider {
  private static final Logger logger = Logger.getLogger(InstanceStatusDecider.class);

  protected static List<Replica> getReplicasForInstance(VeniceHelixResources resources, String instanceId) {
    RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();
    ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
    List<Replica> replicas = new ArrayList<>();
    // lock resource assignment to avoid it's updated by routing data repository during the searching.
    synchronized (resourceAssignment) {
      Set<String> resourceNames = resourceAssignment.getAssignedResources();

      for (String resourceName : resourceNames) {
        PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
        for (Partition partition : partitionAssignment.getAllPartitions()) {
          String status = partition.getInstanceStatusById(instanceId);
          if (status != null) {
            Replica replica = new Replica(Instance.fromNodeId(instanceId), partition.getId(), resourceName);
            replica.setStatus(status);
            replicas.add(replica);
          }
        }
      }
    }
    return replicas;
  }

  protected static NodeRemovableResult isRemovable(VeniceHelixResources resources, String clusterName,
      String instanceId, int minActiveReplicas) {

    return isRemovable(resources, clusterName, instanceId, minActiveReplicas, false);
  }

  /**
   * Decide whether the given instance could be moved out from the cluster. An instance is removable if:
   * 1. It's not a live instance any more.
   * 2. For each online version exists in this instance:
   *  2.1 No data loss after removing this node from the cluster.
   *  2.2 No Helix load re-balance after removing this node from cluster
   * 3. For each ongoing version(during the push) exists in this instance:
   *  3.1 Push will not fail after removing this node from the cluster.
   */
  protected static NodeRemovableResult isRemovable(VeniceHelixResources resources, String clusterName, String instanceId,
      int minActiveReplicas, boolean isInstanceView) {
    try {
      // If instance is not alive, it's removable.
      if (!HelixUtils.isLiveInstance(clusterName, instanceId, resources.getController())) {
        return NodeRemovableResult.removableResult();
      }

      RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();
      OfflinePushMonitor monitor = resources.getOfflinePushMonitor();

      // Get all of replicas hold by given instance
      List<Replica> replicas = getReplicasForInstance(resources, instanceId);
      // Get and lock the resource assignment to avoid it's changed during the checking.
      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      synchronized (resourceAssignment) {
        // Get resource names from replicas hold by this instance.
        Set<String> resourceNameSet = replicas.stream().map(Replica::getResource).collect(Collectors.toSet());

        for (String resourceName : resourceNameSet) {
          // Get partition assignments that if we removed the given instance from cluster.
          PartitionAssignment partitionAssignmentAfterRemoving =
              getPartitionAssignmentAfterRemoving(instanceId, resourceAssignment, resourceName, isInstanceView);

          VersionStatus versionStatus = getVersionStatus(resources.getMetadataRepository(), resourceName);
          if (versionStatus.equals(VersionStatus.ONLINE) || versionStatus.equals(VersionStatus.PUSHED)) {
            // Push has been completed normally. The version of this push is ready to serve read requests. It might or
            // might NOT be the current version of a store.
            // Venice can not remove the given instance once:
            // 1. Venice would lose data. If server hold the last ONLINE replica, we can NOT remove it otherwise data
            // would be lost.
            // 2. And a re-balance would be triggered. If there is not enough active replicas(including ONLINE,
            // BOOTSTRAP and ERROR replicas), helix will do re-balance immediately even we enabled delayed re-balance.
            // In that case partition would be moved to other instances and might cause the consumption from the begin
            // of topic.
            Pair<Boolean, String> result = willLoseData(partitionAssignmentAfterRemoving);
            if (result.getFirst()) {
              logger.info("Instance:" + instanceId + " is not removable because Version:" + resourceName
                  + " would lose data if this instance was removed from cluster:" + clusterName + " details: "
                  + result.getSecond());
              return NodeRemovableResult.nonremoveableResult(resourceName,
                  NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA, result.getSecond());
            }
            result = willTriggerRebalance(partitionAssignmentAfterRemoving, minActiveReplicas);
            if (result.getFirst()) {
              logger.info("Instance:" + instanceId + " is not removable because Version:" + resourceName
                  + " would be re-balanced if this instance was removed from cluster:" + clusterName + " details: "
                  + result.getSecond());
              return NodeRemovableResult.nonremoveableResult(resourceName,
                  NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE, result.getSecond());
            }
          } else if (versionStatus.equals(VersionStatus.STARTED)) {
            // Push is still running
            // Venice can not remove the given instance once job would fail due to removing.
            if (monitor.wouldJobFail(resourceName, partitionAssignmentAfterRemoving)) {
              logger.info(
                  "Instance:" + instanceId + " is not removable because the offline push for topic:" + resourceName
                      + " would fail if this instance was removed from cluster:" + clusterName);
              return NodeRemovableResult.nonremoveableResult(resourceName,
                  NodeRemovableResult.BlockingRemoveReason.WILL_FAIL_PUSH, null);
            }
          } else {
            if (logger.isDebugEnabled()) {
              // Ignore the error version or not created version.
              logger.debug(
                  "Version status: " + versionStatus.toString() + ", ignore it while judging whether instance: "
                      + instanceId + " is removable.");
            }
            continue;
          }
        }
      }
      return NodeRemovableResult.removableResult();
    } catch (Exception e) {
      String errorMsg = "Can not get current states for instance:" + instanceId + " from Zookeeper";
      logger.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  private static Pair<Boolean, String> willLoseData(PartitionAssignment partitionAssignmentAfterRemoving) {
    for (Partition partitionAfterRemoving : partitionAssignmentAfterRemoving.getAllPartitions()) {
      int onlineReplicasCount = partitionAfterRemoving.getReadyToServeInstances().size();
      if (onlineReplicasCount < 1) {
        // After removing the instance, no online replica exists. Venice will lose data in this case.
        return new Pair<>(true,
            "Partition: " + partitionAfterRemoving.getId() + " will have no online replicas after removing the node.");
      }
    }
    return new Pair<>(false, null);
  }

  private static Pair<Boolean, String> willTriggerRebalance(PartitionAssignment partitionAssignmentAfterRemoving,
      int minActiveReplicas) {
    for (Partition partitionAfterRemoving : partitionAssignmentAfterRemoving.getAllPartitions()) {
      int activeReplicaCount = partitionAfterRemoving.getBootstrapAndReadyToServeInstances().size();
      activeReplicaCount += partitionAfterRemoving.getErrorInstances().size();
      if (activeReplicaCount < minActiveReplicas) {
        // After removing the instance, Venice would not have enough active replicas so a rebalance would be triggered..
        return new Pair<>(true, "Partition: " + partitionAfterRemoving.getId() + " will only have " + activeReplicaCount
            + " active replicas which is smaller than required minimum active replicas: " + minActiveReplicas);
      }
    }
    return new Pair<>(false, null);
  }

  private static PartitionAssignment getPartitionAssignmentAfterRemoving(String instanceId,
      ResourceAssignment resourceAssignment, String resourceName, boolean isInstanceView) {
    PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
    PartitionAssignment partitionAssignmentAfterRemoving =
        new PartitionAssignment(resourceName, partitionAssignment.getExpectedNumberOfPartitions());
    for (Partition partition : partitionAssignment.getAllPartitions()) {
      if(isInstanceView) {
        // If the instance does not hold any replica of this partition, skip it. As in the instance' view, we only care
        // about the partitions that has been assigned to this instance.
        if(partition.getInstanceStatusById(instanceId) == null){
          continue;
        }
      }
      Partition partitionAfterRemoving = partition.withRemovedInstance(instanceId);
      partitionAssignmentAfterRemoving.addPartition(partitionAfterRemoving);
    }
    return partitionAssignmentAfterRemoving;
  }

  private static VersionStatus getVersionStatus(ReadWriteStoreRepository storeRepository, String resourceName) {
    Store store = storeRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName));
    if (store == null) {
      logger.info("Can not find store for the resource:" + resourceName);
      return VersionStatus.NOT_CREATED;
    }
    return store.getVersionStatus(Version.parseVersionFromKafkaTopicName(resourceName));
  }
}
