package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.ResourceAssignment.*;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.IdealState;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Extend {@link HelixBaseRoutingRepository} to leverage customized view data for offline push.
 */
public class HelixCustomizedViewOfflinePushRepository extends HelixBaseRoutingRepository {
  private static final Logger logger = LogManager.getLogger(HelixCustomizedViewOfflinePushRepository.class);
  private final ReentrantReadWriteLock resourceAssignmentRWLock = new ReentrantReadWriteLock();
  private static final String LEADER_FOLLOWER_VENICE_STATE_FILLER = "N/A";

  public HelixCustomizedViewOfflinePushRepository(SafeHelixManager manager) {
    super(manager);
    dataSource.put(PropertyType.CUSTOMIZEDVIEW, Collections.singletonList(HelixPartitionState.OFFLINE_PUSH.name()));
  }

  @Override
  public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
    Partition partition;
    try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.readLock())) {
      partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
    }
    if (partition == null) {
      return Collections.emptyList();
    }
    return partition.getAllInstances()
        .entrySet()
        .stream()
        .flatMap(
            e -> e.getValue()
                .stream()
                .map(
                    instance -> new ReplicaState(
                        partitionId,
                        instance.getNodeId(),
                        LEADER_FOLLOWER_VENICE_STATE_FILLER,
                        e.getKey(),
                        e.getKey().equals(ExecutionStatus.COMPLETED.name()))))
        .collect(Collectors.toList());
  }

  private int getNumberOfReplicasInCompletedState(String kafkaTopic, int partitionId) {
    Partition partition;
    try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.readLock())) {
      partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
    }
    return partition == null ? 0 : partition.getInstancesInState(COMPLETED.name()).size();
  }

  /* Returns map of partitionId and the number of completed replicas in that partition */
  public Map<Integer, Integer> getCompletedStatusReplicas(String kafkaTopic, int numberOfPartitions) {
    Map<Integer, Integer> replicaCount = new HashMap<>();
    for (int partitionId = 0; partitionId < numberOfPartitions; partitionId++) {
      replicaCount.put(partitionId, getNumberOfReplicasInCompletedState(kafkaTopic, partitionId));
    }
    return replicaCount;
  }

  @Override
  protected void onExternalViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
    throw new VeniceException(
        "This function should not be called because this class handles updates on CV instead of EV.");
  }

  @Override
  protected void onCustomizedViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
    Collection<CustomizedView> customizedViewCollection = routingTableSnapshot.getCustomizeViews();
    if (customizedViewCollection == null) {
      logger.warn("There is no existing customized view");
      return;
    }
    /**
     * onDataChange logic for offline push status
     */
    if (routingTableSnapshot.getCustomizedStateType().equals(HelixPartitionState.OFFLINE_PUSH.name())) {
      // Create a snapshot to prevent live instances map being changed during this method execution.
      Map<String, Instance> liveInstanceSnapshot = convertLiveInstances(routingTableSnapshot.getLiveInstances());
      // Get number of partitions from Ideal state category in ZK.
      Map<String, Integer> resourceToPartitionCountMapSnapshot = resourceToIdealPartitionCountMap;
      ResourceAssignment newResourceAssignment = new ResourceAssignment();
      Set<String> resourcesInCustomizedView =
          customizedViewCollection.stream().map(CustomizedView::getResourceName).collect(Collectors.toSet());

      if (!resourceToPartitionCountMapSnapshot.keySet().containsAll(resourcesInCustomizedView)) {
        logger.info(
            "Found the inconsistent data between customized view and ideal state of cluster: "
                + manager.getClusterName() + ". Reading the latest ideal state from zk.");

        List<PropertyKey> keys = customizedViewCollection.stream()
            .map(cv -> keyBuilder.idealStates(cv.getResourceName()))
            .collect(Collectors.toList());
        try {
          List<IdealState> idealStates = manager.getHelixDataAccessor().getProperty(keys);
          refreshResourceToIdealPartitionCountMap(idealStates);
          resourceToPartitionCountMapSnapshot = resourceToIdealPartitionCountMap;
          logger.info("Ideal state of cluster: " + manager.getClusterName() + " is updated from zk");
        } catch (HelixMetaDataAccessException e) {
          logger.error(
              "Failed to update the ideal state of cluster: " + manager.getClusterName()
                  + " because we could not access to zk.",
              e);
          return;
        }
      }

      for (CustomizedView customizedView: customizedViewCollection) {
        String resourceName = customizedView.getResourceName();
        if (!resourceToPartitionCountMapSnapshot.containsKey(resourceName)) {
          logger.warn(
              "Could not find resource: " + resourceName + " in ideal state. Ideal state is up to date,"
                  + " so the resource has been deleted from ideal state or could not read " + "from "
                  + "zk. Ignore its customized view update.");
          continue;
        }
        PartitionAssignment partitionAssignment =
            new PartitionAssignment(resourceName, resourceToPartitionCountMapSnapshot.get(resourceName));
        for (String partitionName: customizedView.getPartitionSet()) {
          // Get instance to customized state map for this partition from local memory.
          Map<String, String> instanceStateMap = customizedView.getStateMap(partitionName);
          Map<String, List<Instance>> stateToInstanceMap = new HashMap<>();
          // Populate customized state to instance set map
          for (Map.Entry<String, String> entry: instanceStateMap.entrySet()) {
            String instanceName = entry.getKey();
            String instanceState = entry.getValue();
            Instance instance = liveInstanceSnapshot.get(instanceName);
            if (instance != null) {
              ExecutionStatus status;
              try {
                status = ExecutionStatus.valueOf(instanceState);
              } catch (Exception e) {
                logger.warn("Instance:" + instanceName + " unrecognized status:" + instanceState);
                continue;
              }
              stateToInstanceMap.computeIfAbsent(status.toString(), s -> new ArrayList<>()).add(instance);
            } else {
              logger.warn("Cannot find instance '" + instanceName + "' in /LIVEINSTANCES");
            }
          }
          // Update partitionAssignment of customized state
          int partitionId = HelixUtils.getPartitionId(partitionName);
          partitionAssignment.addPartition(new Partition(partitionId, stateToInstanceMap));

          // Update partition status to trigger callback
          // Note we do not change the callback function which listens on PartitionStatus change, instead, we populate
          // partition status with partition assignment data of customized view
          PartitionStatus partitionStatus = new PartitionStatus(partitionId);
          stateToInstanceMap.forEach(
              (key, value) -> value.forEach(
                  instance -> partitionStatus.updateReplicaStatus(instance.getNodeId(), ExecutionStatus.valueOf(key))));
          listenerManager.trigger(
              resourceName,
              listener -> listener
                  .onPartitionStatusChange(resourceName, ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus)));
        }
        newResourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
      }
      final ResourceAssignmentChanges updates;
      try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.writeLock())) {
        try (AutoCloseableLock ignore = AutoCloseableLock.of(liveInstancesMapLock)) {
          // Update the live instances as well. Helix updates live instances in this routing data
          // changed event.
          this.liveInstancesMap = Collections.unmodifiableMap(liveInstanceSnapshot);
        }
        updates = resourceAssignment.updateResourceAssignment(newResourceAssignment);
        logger.info("Updated resource assignment and live instances for .");
      }
      logger.info(
          "Customized view is changed. The number of active resources is " + resourcesInCustomizedView.size()
              + ", and the deleted resources are " + updates.getDeletedResource());
      // Notify listeners that listen on customized view data change
      for (String kafkaTopic: updates.getUpdatedResources()) {
        PartitionAssignment partitionAssignment;
        try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.readLock())) {
          partitionAssignment = resourceAssignment.getPartitionAssignment(kafkaTopic);
        }
        listenerManager.trigger(kafkaTopic, listener -> listener.onCustomizedViewChange(partitionAssignment));
      }
      // Notify events to the listeners which listen on deleted resources.
      for (String kafkaTopic: updates.getDeletedResource()) {
        listenerManager.trigger(kafkaTopic, listener -> listener.onRoutingDataDeleted(kafkaTopic));
      }
    }
  }

  @Override
  public void refreshRoutingDataForResource(String kafkaTopic) {
    throw new VeniceException("The function of refreshRoutingDataForResource is not implemented");
  }
}
