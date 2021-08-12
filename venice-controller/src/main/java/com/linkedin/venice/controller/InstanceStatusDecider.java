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
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * This class is not Thread-safe.
 */
public class InstanceStatusDecider {
  private static final Logger logger = Logger.getLogger(InstanceStatusDecider.class);

  protected static List<Replica> getReplicasForInstance(HelixVeniceClusterResources resources, String instanceId) {
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

  protected static NodeRemovableResult isRemovable(HelixVeniceClusterResources resources, String clusterName,
      String instanceId) {

    return isRemovable(resources, clusterName, instanceId, false);
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
  protected static NodeRemovableResult isRemovable(HelixVeniceClusterResources resources, String clusterName, String instanceId,
      boolean isInstanceView) {
    try {
      // If instance is not alive, it's removable.
      if (!HelixUtils.isLiveInstance(clusterName, instanceId, resources.getHelixManager())) {
        return NodeRemovableResult.removableResult();
      }

      RoutingDataRepository routingDataRepository = resources.getRoutingDataRepository();

      // Get all of replicas hold by given instance
      List<Replica> replicas = getReplicasForInstance(resources, instanceId);
      // Get and lock the resource assignment to avoid it's changed during the checking.
      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      long startTimeForAcquiringLockInMs = System.currentTimeMillis();
      synchronized (resourceAssignment) {
        logger.info("Spent " + LatencyUtils.getElapsedTimeInMs(startTimeForAcquiringLockInMs) + "ms on acquiring ResourceAssignment lock.");
        // Get resource names from replicas hold by this instance.
        Set<String> resourceNameSet = replicas.stream().map(Replica::getResource).collect(Collectors.toSet());

        for (String resourceName : resourceNameSet) {
          if (isCurrentVersion(resourceName, resources.getMetadataRepository())) {
            // Get partition assignments that if we removed the given instance from cluster.
            PartitionAssignment partitionAssignmentAfterRemoving =
                getPartitionAssignmentAfterRemoving(instanceId, resourceAssignment, resourceName, isInstanceView);

              // Push has been completed normally. The version of this push is ready to serve read requests. It is the
              // current version of a store (at least when it was checked recently).
              // Venice can not remove the given instance once:
              // 1. Venice would lose data. If server hold the last ONLINE replica, we can NOT remove it otherwise data
              // would be lost.
              // 2. And a re-balance would be triggered. If there is not enough active replicas(including ONLINE,
              // BOOTSTRAP and ERROR replicas), helix will do re-balance immediately even we enabled delayed re-balance.
              // In that case partition would be moved to other instances and might cause the consumption from the begin
              // of topic.
              Pair<Boolean, String> result = willLoseData(resources.getPushMonitor(), partitionAssignmentAfterRemoving);
              if (result.getFirst()) {
                logger.info("Instance:" + instanceId + " is not removable because Version:" + resourceName + " would lose data if this instance was removed from cluster:" + clusterName + " details: "
                    + result.getSecond());
                return NodeRemovableResult.nonremoveableResult(resourceName, NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA, result.getSecond());
              }

              Optional<Version> version = resources.getMetadataRepository()
                  .getStore(Version.parseStoreFromKafkaTopicName(resourceName))
                  .getVersion(Version.parseVersionFromKafkaTopicName(resourceName));

              if (version.isPresent()) {
                result = willTriggerRebalance(partitionAssignmentAfterRemoving, version.get().getMinActiveReplicas());
              } else {
                result = new Pair<>(false, "Cannot find the version info. Ignore it since it's been deleted. "
                    + "Resource: " + resourceName);
              }

              if (result.getFirst()) {
                logger.info("Instance:" + instanceId + " is not removable because Version:" + resourceName + " would be re-balanced if this instance was removed from cluster:" + clusterName + " details: "
                    + result.getSecond());
                return NodeRemovableResult.nonremoveableResult(resourceName, NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE, result.getSecond());
              }
            }
          }
        }
      return NodeRemovableResult.removableResult();
    } catch (Exception e) {
      String errorMsg = "Can not verify whether instance " + instanceId + " is removable.";
      logger.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  private static Pair<Boolean, String> willLoseData(PushMonitor pushMonitor,
      PartitionAssignment partitionAssignmentAfterRemoving) {
    for (Partition partitionAfterRemoving : partitionAssignmentAfterRemoving.getAllPartitions()) {
      if (pushMonitor.getReadyToServeInstances(partitionAssignmentAfterRemoving, partitionAfterRemoving.getId()).size() < 1) {
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
      int activeReplicaCount = partitionAfterRemoving.getWorkingInstances().size();
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
    if (!store.isEnableReads()) {
      // Ignore store replicas that have read disabled.
      logger.info("Ignoring node removal checks for resource: " + resourceName + " because the store: "
          + store.getName() + " has reads disabled");
      return VersionStatus.NOT_CREATED;
    }
    return store.getVersionStatus(Version.parseVersionFromKafkaTopicName(resourceName));
  }

  private static boolean isCurrentVersion(String resourceName, ReadWriteStoreRepository metadataRepo) {
    try{
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromKafkaTopicName(resourceName);

      return metadataRepo.getStore(storeName).getCurrentVersion() == version;
    } catch (VeniceException e) {
      return false;
    }
  }
}
