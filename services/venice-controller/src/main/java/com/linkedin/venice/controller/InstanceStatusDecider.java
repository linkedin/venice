package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.IdealState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is not Thread-safe.
 */
public class InstanceStatusDecider {
  private static final Logger LOGGER = LogManager.getLogger(InstanceStatusDecider.class);

  static List<Replica> getReplicasForInstance(HelixVeniceClusterResources resources, String instanceId) {
    RoutingDataRepository routingDataRepository = resources.getCustomizedViewRepository();
    return Utils.getReplicasForInstance(routingDataRepository, instanceId);
  }

  static NodeRemovableResult isRemovable(HelixVeniceClusterResources resources, String clusterName, String instanceId) {
    return isRemovable(resources, clusterName, instanceId, Collections.emptyList());
  }

  private static PartitionAssignment removeLockedResources(
      PartitionAssignment partitionAssignment,
      List<String> lockedNodes,
      String resourceName) {
    PartitionAssignment assignment = partitionAssignment;
    for (String lockedNodeId: lockedNodes) {
      assignment = getPartitionAssignmentAfterRemoving(lockedNodeId, assignment, resourceName, true);
    }
    return assignment;
  }

  /**
   * Decide whether the given instance could be moved out from the cluster. An instance is removable if:
   * <p>
   * 1. It's not a live instance anymore.
   * 2. For each online version exists in this instance:
   *  2.1 No data loss after removing this node from the cluster.
   *  2.2 No Helix load re-balance after removing this node from cluster
   * 3. For each ongoing version(during the push) exists in this instance:
   *  3.1 Push will not fail after removing this node from the cluster.
   */
  static NodeRemovableResult isRemovable(
      HelixVeniceClusterResources resources,
      String clusterName,
      String instanceId,
      List<String> lockedNodes) {
    List<NodeRemovableResult> list =
        getInstanceStoppableStatuses(resources, clusterName, Collections.singletonList(instanceId), lockedNodes);
    if (list.isEmpty()) {
      throw new VeniceException("Error in finding isRemovable call for instance " + instanceId);
    }
    return list.get(0);
  }

  static List<NodeRemovableResult> getInstanceStoppableStatuses(
      HelixVeniceClusterResources resources,
      String clusterName,
      List<String> instanceIds,
      List<String> lockedNodes) {
    List<NodeRemovableResult> removableResults = new ArrayList<>();
    List<String> toBeStoppedNodes = new ArrayList<>(lockedNodes);
    RoutingDataRepository routingDataRepository = resources.getCustomizedViewRepository();
    for (String instanceId: instanceIds) {
      boolean instanceNonStoppable = false;
      try {
        // If instance is not alive, it's removable.
        if (!HelixUtils.isLiveInstance(clusterName, instanceId, resources.getHelixManager())) {
          toBeStoppedNodes.add(instanceId);
          removableResults.add(
              NodeRemovableResult.removableResult(
                  instanceId,
                  "Instance " + instanceId + " not found in liveinstance set of cluster " + clusterName));
          continue;
        }

        // Get all replicas held by given instance.
        List<Replica> replicas = getReplicasForInstance(resources, instanceId);
        // Get and lock the resource assignment to avoid it's changed during the checking.
        ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
        long startTimeForAcquiringLockInMs = System.currentTimeMillis();
        synchronized (resourceAssignment) {
          LOGGER.info(
              "Spent {}ms on acquiring ResourceAssignment lock.",
              LatencyUtils.getElapsedTimeFromMsToMs(startTimeForAcquiringLockInMs));
          // Get resource names from replicas hold by this instance.
          Set<String> resourceNameSet = replicas.stream().map(Replica::getResource).collect(Collectors.toSet());

          for (String resourceName: resourceNameSet) {
            if (Utils.isCurrentVersion(resourceName, resources.getStoreMetadataRepository())) {
              PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(resourceName);

              // Get partition assignments that if we removed the given instance from cluster.
              PartitionAssignment partitionAssignmentAfterRemoving =
                  getPartitionAssignmentAfterRemoving(instanceId, partitionAssignment, resourceName, false);

              partitionAssignmentAfterRemoving =
                  removeLockedResources(partitionAssignmentAfterRemoving, toBeStoppedNodes, resourceName);

              // Push has been completed normally. The version of this push is ready to serve read requests. It is the
              // current version of a store (at least when it was checked recently).
              // Venice can not remove the given instance once:
              // 1. Venice would lose data. If server hold the last ONLINE replica, we can NOT remove it otherwise data
              // would be lost.
              // 2. And a re-balance would be triggered. If there is not enough active replicas(including ONLINE,
              // BOOTSTRAP and ERROR replicas), helix will do re-balance immediately even we enabled delayed re-balance.
              // In that case partition would be moved to other instances and might cause the consumption from the begin
              // of topic.
              Pair<Boolean, String> result = willLoseData(resources, partitionAssignmentAfterRemoving);
              if (result.getFirst()) {
                LOGGER.info(
                    "Instance: {} is not removable because Version: {} would lose data "
                        + "if this instance was removed from cluster: {} details: {}",
                    instanceId,
                    resourceName,
                    clusterName,
                    result.getSecond());
                instanceNonStoppable = true;
                removableResults.add(
                    NodeRemovableResult.nonRemovableResult(
                        instanceId,
                        resourceName,
                        NodeRemovableResult.BlockingRemoveReason.WILL_LOSE_DATA,
                        result.getSecond()));
                break;
              }

              IdealState idealState = HelixUtils.getIdealState(clusterName, resourceName, resources.getHelixManager());
              if (idealState != null && idealState.isEnabled() && idealState.isValid()) {
                result = willTriggerRebalance(partitionAssignmentAfterRemoving, idealState.getMinActiveReplicas());
              } else {
                result = new Pair<>(
                    false,
                    "Cannot find the ideal state. Ignore it since it does not exist. Resource: " + resourceName);
              }

              if (result.getFirst()) {
                LOGGER.info(
                    "Instance: {} is not removable because Version: {} would be re-balanced "
                        + "if this instance was removed from cluster: {} details: {}",
                    instanceId,
                    resourceName,
                    clusterName,
                    result.getSecond());
                instanceNonStoppable = true;
                removableResults.add(
                    NodeRemovableResult.nonRemovableResult(
                        instanceId,
                        resourceName,
                        NodeRemovableResult.BlockingRemoveReason.WILL_TRIGGER_LOAD_REBALANCE,
                        result.getSecond()));
                break;
              }
            }
          }
        }
        if (!instanceNonStoppable) {
          removableResults
              .add(NodeRemovableResult.removableResult(instanceId, "Instance " + instanceId + " can be removed."));
          toBeStoppedNodes.add(instanceId);
        }
      } catch (Exception e) {
        String errorMsg = "Can not verify whether instance " + instanceId + " is removable.";
        LOGGER.error(errorMsg, e);
      }
    }
    return removableResults;
  }

  private static Pair<Boolean, String> willLoseData(
      HelixVeniceClusterResources resources,
      PartitionAssignment partitionAssignmentAfterRemoving) {
    RoutingDataRepository routingDataRepository = resources.getCustomizedViewRepository();
    for (Partition partitionAfterRemoving: partitionAssignmentAfterRemoving.getAllPartitions()) {
      if (routingDataRepository
          .getReadyToServeInstances(partitionAssignmentAfterRemoving, partitionAfterRemoving.getId())
          .isEmpty()) {
        // After removing the instance, no online replica exists. Venice will lose data in this case.
        return new Pair<>(
            true,
            "Partition: " + partitionAfterRemoving.getId() + " will have no online replicas after removing the node.");
      }
    }

    return new Pair<>(false, null);
  }

  private static Pair<Boolean, String> willTriggerRebalance(
      PartitionAssignment partitionAssignmentAfterRemoving,
      int minActiveReplicas) {
    for (Partition partitionAfterRemoving: partitionAssignmentAfterRemoving.getAllPartitions()) {
      int activeReplicaCount = partitionAfterRemoving.getReadyToServeInstances().size();
      activeReplicaCount += partitionAfterRemoving.getErrorInstances().size();
      if (activeReplicaCount < minActiveReplicas) {
        // After removing the instance, Venice would not have enough active replicas so a re-balance would be triggered.
        return new Pair<>(
            true,
            "Partition: " + partitionAfterRemoving.getId() + " will only have " + activeReplicaCount
                + " active replicas which is smaller than required minimum active replicas: " + minActiveReplicas);
      }
    }
    return new Pair<>(false, null);
  }

  private static PartitionAssignment getPartitionAssignmentAfterRemoving(
      String instanceId,
      PartitionAssignment partitionAssignment,
      String resourceName,
      boolean lockedInstance) {
    PartitionAssignment partitionAssignmentAfterRemoving =
        new PartitionAssignment(resourceName, partitionAssignment.getExpectedNumberOfPartitions());
    for (Partition partition: partitionAssignment.getAllPartitions()) {
      if (!lockedInstance) {
        /*
         * If an instance is locked, we need to assume that the instance will eventually be removed. So, we need to
         * remove all partitions hosted by that instance.
         *
         * For instances that are not locked, we only need to ensure that the removal of those instances does not lead
         * to unsafe conditions. Hence, if there are no replicas of a partition, skip it. We only care about the
         * partitions that have been assigned to this instance.
         */
        if (partition.getHelixStateByInstanceId(instanceId) == null
            && partition.getExecutionStatusByInstanceId(instanceId) == null) {
          continue;
        }
      }
      Partition partitionAfterRemoving = partition.withRemovedInstance(instanceId);
      partitionAssignmentAfterRemoving.addPartition(partitionAfterRemoving);
    }
    return partitionAssignmentAfterRemoving;
  }
}
