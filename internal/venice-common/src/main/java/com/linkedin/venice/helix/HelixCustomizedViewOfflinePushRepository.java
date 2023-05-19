package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.ResourceAssignment.ResourceAssignmentChanges;
import static com.linkedin.venice.meta.Store.*;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.helix.PropertyType;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Extend {@link HelixBaseRoutingRepository} to leverage customized view data for offline push.
 */
public class HelixCustomizedViewOfflinePushRepository extends HelixBaseRoutingRepository {
  private static final Logger LOGGER = LogManager.getLogger(HelixCustomizedViewOfflinePushRepository.class);
  private final ReentrantReadWriteLock resourceAssignmentRWLock = new ReentrantReadWriteLock();
  private static final String LEADER_FOLLOWER_VENICE_STATE_FILLER = "N/A";

  private final Map<String, Integer> resourceToPartitionCountMap = new VeniceConcurrentHashMap<>();

  private final ReadOnlyStoreRepository storeRepository;

  public HelixCustomizedViewOfflinePushRepository(SafeHelixManager manager, ReadOnlyStoreRepository storeRepository) {
    super(manager);
    dataSource.put(PropertyType.CUSTOMIZEDVIEW, Collections.singletonList(HelixPartitionState.OFFLINE_PUSH.name()));
    this.storeRepository = storeRepository;
    this.storeRepository.registerStoreDataChangedListener(new StoreChangeListener());
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
  public void clear() {
    this.resourceToPartitionCountMap.clear();
  }

  @Override
  protected void onCustomizedViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
    Collection<CustomizedView> customizedViewCollection = routingTableSnapshot.getCustomizeViews();
    if (customizedViewCollection == null) {
      LOGGER.warn("There is no existing customized view");
      return;
    }
    /**
     * onDataChange logic for offline push status
     */
    if (routingTableSnapshot.getCustomizedStateType().equals(HelixPartitionState.OFFLINE_PUSH.name())) {
      // Create a snapshot to prevent live instances map being changed during this method execution.
      Map<String, Instance> liveInstanceSnapshot = convertLiveInstances(routingTableSnapshot.getLiveInstances());
      // Get number of partitions from Ideal state category in ZK.
      ResourceAssignment newResourceAssignment = new ResourceAssignment();
      Set<String> resourcesInCustomizedView =
          customizedViewCollection.stream().map(CustomizedView::getResourceName).collect(Collectors.toSet());

      for (CustomizedView customizedView: customizedViewCollection) {
        String resourceName = customizedView.getResourceName();
        int partitionCount = resourceToPartitionCountMap.getOrDefault(resourceName, -1);
        if (partitionCount == -1) {
          String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
          int version = Version.parseVersionFromVersionTopicName(resourceName);
          Store store = storeRepository.getStore(storeName);
          if (store == null) {
            LOGGER.warn("Cannot find store for resource: {}.", resourceName);
            continue;
          }

          if (!store.getVersion(version).isPresent()) {
            LOGGER.warn("Version not found in store for resource: {}.", resourceName);
            continue;
          }
          partitionCount = store.getVersion(version).get().getPartitionCount();
          resourceToPartitionCountMap.put(resourceName, partitionCount);
        }
        PartitionAssignment partitionAssignment = new PartitionAssignment(resourceName, partitionCount);
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
                LOGGER.warn("Instance: {} unrecognized status: {}.", instanceName, instanceState);
                continue;
              }
              stateToInstanceMap.computeIfAbsent(status.toString(), s -> new ArrayList<>()).add(instance);
            } else {
              LOGGER.warn("Cannot find instance '{}' in /LIVEINSTANCES", instanceName);
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
        LOGGER.info("Updated resource assignment and live instances for .");
      }
      LOGGER.info(
          "Customized view is changed. The number of active resources is {}, and the deleted resources are {}.",
          resourcesInCustomizedView.size(),
          updates.getDeletedResource());
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

  // test only
  Map<String, Integer> getResourceToPartitionCountMap() {
    return Collections.unmodifiableMap(this.resourceToPartitionCountMap);
  }

  public class StoreChangeListener implements StoreDataChangedListener {
    @Override
    public void handleStoreCreated(Store store) {
      int currentVersion = store.getCurrentVersion();
      if (currentVersion == NON_EXISTING_VERSION) {
        return;
      }

      String newResourceName = Version.composeKafkaTopic(store.getName(), currentVersion);
      int partitionCount = store.getVersion(currentVersion).get().getPartitionCount();
      resourceToPartitionCountMap.put(newResourceName, partitionCount);
    }

    @Override
    public void handleStoreChanged(Store store) {
      int currentVersion = store.getCurrentVersion();
      if (currentVersion == NON_EXISTING_VERSION) {
        return;
      }

      IntSet versionsSet = new IntLinkedOpenHashSet(store.getVersions().size());
      store.getVersions().forEach(version -> versionsSet.add(version.getNumber()));

      resourceToPartitionCountMap.entrySet().removeIf(entry -> {
        String storeName = Version.parseStoreFromKafkaTopicName(entry.getKey());
        int version = Version.parseVersionFromVersionTopicName(entry.getKey());
        return store.getName().equals(storeName) && !versionsSet.contains(version);
      });

      String newResourceName = Version.composeKafkaTopic(store.getName(), currentVersion);
      int partitionCount = store.getVersion(currentVersion).get().getPartitionCount();
      resourceToPartitionCountMap.put(newResourceName, partitionCount);
    }

    @Override
    public void handleStoreDeleted(String storeName) {
      resourceToPartitionCountMap.entrySet().removeIf(entry -> {
        String name = Version.parseStoreFromKafkaTopicName(entry.getKey());
        return storeName.equals(name);
      });
    }
  }
}
