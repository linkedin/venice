package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.ResourceAssignment.ResourceAssignmentChanges;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
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
  private static final int STORE_VERSION_PARTITION_COUNT_NOT_FOUND = -1;
  private final ReentrantReadWriteLock resourceAssignmentRWLock = new ReentrantReadWriteLock();

  private final Map<String, Integer> resourceToPartitionCountMap = new VeniceConcurrentHashMap<>();

  private final ReadOnlyStoreRepository storeRepository;

  private final boolean enableReplicaStatusHistory;

  public HelixCustomizedViewOfflinePushRepository(
      SafeHelixManager manager,
      ReadOnlyStoreRepository storeRepository,
      boolean enableReplicaStatusHistory) {
    super(manager);
    dataSource.put(PropertyType.CUSTOMIZEDVIEW, Collections.singletonList(HelixPartitionState.OFFLINE_PUSH.name()));
    this.storeRepository = storeRepository;
    this.storeRepository.registerStoreDataChangedListener(new StoreChangeListener());
    this.enableReplicaStatusHistory = enableReplicaStatusHistory;
  }

  public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
    Partition partition;
    try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.readLock())) {
      partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
    }
    if (partition == null) {
      return Collections.emptyList();
    }
    return partition.getAllInstancesByExecutionStatus()
        .entrySet()
        .stream()
        .flatMap(
            e -> e.getValue().stream().map(instance -> new ReplicaState(partitionId, instance.getNodeId(), e.getKey())))
        .collect(Collectors.toList());
  }

  private int getNumberOfReplicasInCompletedState(String kafkaTopic, int partitionId) {
    Partition partition;
    try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.readLock())) {
      partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
    }
    return partition == null ? 0 : partition.getInstancesInState(COMPLETED).size();
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

      Set<String> instancesSeenInCustomizedViewButMissingFromLiveInstances = new HashSet<>();
      for (CustomizedView customizedView: customizedViewCollection) {
        String resourceName = customizedView.getResourceName();
        int partitionCount = getPartitionCount(resourceName);
        if (partitionCount == STORE_VERSION_PARTITION_COUNT_NOT_FOUND) {
          continue;
        }
        PartitionAssignment partitionAssignment = new PartitionAssignment(resourceName, partitionCount);
        for (String partitionName: customizedView.getPartitionSet()) {
          // Get instance to customized state map for this partition from local memory.
          Map<String, String> instanceStateMap = customizedView.getStateMap(partitionName);
          EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstanceMap = new EnumMap<>(ExecutionStatus.class);
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
              executionStatusToInstanceMap.computeIfAbsent(status, s -> new ArrayList<>()).add(instance);
            } else {
              instancesSeenInCustomizedViewButMissingFromLiveInstances.add(instanceName);
            }
          }
          // Update partitionAssignment of customized state
          int partitionId = HelixUtils.getPartitionId(partitionName);
          partitionAssignment
              .addPartition(new Partition(partitionId, new EnumMap<>(HelixState.class), executionStatusToInstanceMap));

          // Update partition status to trigger callback
          // Note we do not change the callback function which listens on PartitionStatus change, instead, we populate
          // partition status with partition assignment data of customized view
          PartitionStatus partitionStatus = new PartitionStatus(partitionId);
          for (Map.Entry<ExecutionStatus, List<Instance>> entry: executionStatusToInstanceMap.entrySet()) {
            for (Instance instance: entry.getValue()) {
              partitionStatus.updateReplicaStatus(instance.getNodeId(), entry.getKey(), enableReplicaStatusHistory);
            }
          }
          listenerManager.trigger(
              resourceName,
              listener -> listener
                  .onPartitionStatusChange(resourceName, ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus)));
        }
        newResourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
      }
      if (!instancesSeenInCustomizedViewButMissingFromLiveInstances.isEmpty()) {
        LOGGER.warn(
            "The following instances were found in the CV, but missing from Live Instances: {}",
            instancesSeenInCustomizedViewButMissingFromLiveInstances);
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
      for (String kafkaTopic: updates.getNewResources()) {
        PartitionAssignment partitionAssignment;
        try (AutoCloseableLock ignored = AutoCloseableLock.of(resourceAssignmentRWLock.readLock())) {
          partitionAssignment = resourceAssignment.getPartitionAssignment(kafkaTopic);
        }
        listenerManager.trigger(kafkaTopic, listener -> listener.onCustomizedViewAdded(partitionAssignment));
      }
    }
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
      updateResourceToPartitionCountMap(store, newResourceName, currentVersion);
    }

    @Override
    public void handleStoreChanged(Store store) {
      int currentVersion = store.getCurrentVersion();
      if (currentVersion == NON_EXISTING_VERSION) {
        return;
      }

      IntSet versionsSet = new IntLinkedOpenHashSet(store.getVersions().size());
      store.getVersions().forEach(version -> versionsSet.add(version.getNumber()));

      purgeResourceToPartitionCountMap(
          store.getName(),
          resourceName -> !versionsSet.contains(Version.parseVersionFromVersionTopicName(resourceName)));

      String newResourceName = Version.composeKafkaTopic(store.getName(), currentVersion);
      updateResourceToPartitionCountMap(store, newResourceName, currentVersion);
    }

    @Override
    public void handleStoreDeleted(String storeName) {
      purgeResourceToPartitionCountMap(storeName, versionNumberIsIgnored -> true);
    }
  }

  /**
   * This should be the only function which reads the {@link #resourceToPartitionCountMap}.
   *
   * @param resourceName for which to get the partition count.
   * @return the partition count, or {@link #STORE_VERSION_PARTITION_COUNT_NOT_FOUND}
   */
  private int getPartitionCount(String resourceName) {
    int partitionCount =
        this.resourceToPartitionCountMap.getOrDefault(resourceName, STORE_VERSION_PARTITION_COUNT_NOT_FOUND);
    if (partitionCount == STORE_VERSION_PARTITION_COUNT_NOT_FOUND) {
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromVersionTopicName(resourceName);
      Store store = this.storeRepository.getStore(storeName);
      partitionCount = updateResourceToPartitionCountMap(store, resourceName, version);
    }
    return partitionCount;
  }

  /**
   * This should be the only function which puts a new entry into the {@link #resourceToPartitionCountMap}.
   */
  private int updateResourceToPartitionCountMap(Store store, String resourceName, int versionNumber) {
    int partitionCount = STORE_VERSION_PARTITION_COUNT_NOT_FOUND;
    if (store == null) {
      LOGGER.warn("Cannot find store for resource: {}.", resourceName);
    } else {
      Version storeVersion = store.getVersion(versionNumber);
      if (storeVersion == null) {
        LOGGER.warn("Version not found in store for resource: {}.", resourceName);
      } else {
        partitionCount = storeVersion.getPartitionCount();
        this.resourceToPartitionCountMap.put(resourceName, partitionCount);
      }
    }
    return partitionCount;
  }

  private interface ResourceDeleter {
    boolean shouldDelete(String resourceName);
  }

  /**
   * This should be the only function which deletes entries from the {@link #resourceToPartitionCountMap}.
   */
  private void purgeResourceToPartitionCountMap(String storeName, ResourceDeleter resourceDeleter) {
    resourceToPartitionCountMap.entrySet()
        .removeIf(
            entry -> storeName.equals(Version.parseStoreFromKafkaTopicName(entry.getKey()))
                && resourceDeleter.shouldDelete(entry.getKey()));
  }
}
