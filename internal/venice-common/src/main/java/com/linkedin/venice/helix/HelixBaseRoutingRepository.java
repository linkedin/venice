package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ListenerManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Get routing data from Helix and convert it to our Venice partition and replica objects.
 * <p>
 * Although Helix RoutingTableProvider already cached routing data in local memory. But it only gets data from
 * /$cluster/EXTERNALVIEW, /$cluster/CONFIGS/PARTICIPANTS, /$cluster/CUSTOMIZEDVIEW.
 * Two parts of data are missed: Additional data in /$cluster/LIVEINSTANCES and
 * partition number in /$cluster/IDEALSTATES. So we cached Venice partitions and instances
 * here to include all of them and also convert them from Helix data structure to Venice data structure.
 * <p>
 * As this repository is used by Router, so here only cached the online instance at first. If Venice needs some more
 * instances in other state, could add them in the further.
 */
@BatchMode
public abstract class HelixBaseRoutingRepository
    implements RoutingDataRepository, ControllerChangeListener, RoutingTableChangeListener {
  private static final Logger LOGGER = LogManager.getLogger(HelixBaseRoutingRepository.class);

  /**
   * Manager used to communicate with Helix.
   */
  protected final SafeHelixManager manager;
  /**
   * Builder used to build the data path to access Helix internal data.
   */
  protected final PropertyKey.Builder keyBuilder;

  protected ResourceAssignment resourceAssignment = new ResourceAssignment();
  /**
   * leader controller of cluster.
   */
  private volatile Instance leaderController = null;

  protected final ListenerManager<RoutingDataChangedListener> listenerManager;

  protected final Lock liveInstancesMapLock = new ReentrantLock();
  protected Map<String, Instance> liveInstancesMap = new HashMap<>();

  private long leaderControllerChangeTimeMs = -1;

  private RoutingTableProvider routingTableProvider;

  protected final Map<PropertyType, List<String>> dataSource;

  public HelixBaseRoutingRepository(SafeHelixManager manager) {
    this.manager = manager;
    listenerManager = new ListenerManager<>(); // TODO make thread count configurable
    keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    dataSource = new HashMap<>();
  }

  /**
   * This method is used to add listener after HelixManager being connected. Otherwise, it will met error because adding
   * listener before connecting.
   */
  public void refresh() {
    try {
      LOGGER.info("Refresh started for cluster {}'s {}.", manager.getClusterName(), getClass().getSimpleName());
      // After adding the listener, helix will initialize the callback which will get the entire external view
      // and trigger the external view change event. In other words, venice will read the newest external view
      // immediately.
      manager.addControllerListener(this);
      // Use routing table provider to get the notification of the external view change, customized view change,
      // and live instances change.
      routingTableProvider = new RoutingTableProvider(manager.getOriginalManager(), dataSource);
      routingTableProvider.addRoutingTableChangeListener(this, null);
      // Get the current external view and customized views, process at first. As the new helix API will not init a
      // event after you add the listener.
      for (Map.Entry<PropertyType, List<String>> entry: dataSource.entrySet()) {
        PropertyType propertyType = entry.getKey();
        if (entry.getValue().isEmpty()) {
          LOGGER.info("Will call onRoutingTableChange for propertyType: {}" + propertyType);
          onRoutingTableChange(routingTableProvider.getRoutingTableSnapshot(propertyType), null);
        } else {
          for (String customizedStateType: entry.getValue()) {
            RoutingTableSnapshot routingTableSnapshot =
                routingTableProvider.getRoutingTableSnapshot(propertyType, customizedStateType);
            if (routingTableSnapshot.getCustomizeViews() != null
                && !routingTableSnapshot.getCustomizeViews().isEmpty()) {
              LOGGER.info(
                  "Will call onRoutingTableChange for propertyType: {}, customizedStateType: {}",
                  propertyType,
                  customizedStateType);
              onRoutingTableChange(routingTableSnapshot, null);
            } else {
              LOGGER.info(
                  "Will NOT call onRoutingTableChange for propertyType: {}, customizedStateType: {} since its CV is null or empty.",
                  propertyType,
                  customizedStateType);
            }
          }
        }
      }
      // TODO subscribe zk state change event after we can get zk client from HelixManager
      // (Should be fixed by Helix team soon)
      LOGGER.info("Refresh finished for cluster {}'s {}.", manager.getClusterName(), getClass().getSimpleName());
    } catch (Exception e) {
      String errorMessage = "Cannot refresh routing table from Helix for cluster " + manager.getClusterName();
      LOGGER.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
  }

  public void clear() {
    // removeListener method is a thread safe method, we don't need to lock here again.
    manager.removeListener(keyBuilder.controller(), this);
    if (routingTableProvider != null) {
      routingTableProvider.removeRoutingTableChangeListener(this);
      try {
        routingTableProvider.shutdown();
      } catch (Exception e) {
        LOGGER.error("Exception thrown during shutdown of routingTableProvider. " + e);
      }
    }
  }

  /**
   * Get instances from local memory. All instances are in {@link HelixState#LEADER} or {@link HelixState#STANDBY} state.
   */
  public List<Instance> getWorkingInstances(String kafkaTopic, int partitionId) {
    Partition partition = resourceAssignment.getPartitionAssignment(kafkaTopic).getPartition(partitionId);
    if (partition == null) {
      return Collections.emptyList();
    } else {
      return partition.getWorkingInstances();
    }
  }

  /**
   * Get instances from local memory. All instances are in {@link ExecutionStatus#COMPLETED} state.
   */
  public List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId) {
    return getReadyToServeInstances(resourceAssignment.getPartitionAssignment(kafkaTopic), partitionId);
  }

  /**
   * Get ready to serve instances from local memory. All instances are in {@link ExecutionStatus#COMPLETED} state.
   */
  @Override
  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    Partition partition = partitionAssignment.getPartition(partitionId);
    if (partition == null) {
      return Collections.emptyList();
    } else {
      return partition.getReadyToServeInstances();
    }
  }

  /**
   * This function is mainly used in VeniceVersionFinder#anyOfflinePartitions() when there is no online replica for
   * a specific partition, and it calls this function to get the partition assignment info for error msg. It's valid
   * case that there is no partition assignment for a specific partition and we return EMPTY_MAP.
   */
  public Map<ExecutionStatus, List<Instance>> getAllInstances(String kafkaTopic, int partitionId) {
    Partition partition = getPartitionAssignments(kafkaTopic).getPartition(partitionId);
    return partition != null ? partition.getAllInstancesByExecutionStatus() : Collections.emptyMap();
  }

  /**
   * @param resourceName Name of the resource.
   * @return {@link PartitionAssignment} of the resource from local memory.
   */
  public PartitionAssignment getPartitionAssignments(@Nonnull String resourceName) {
    return resourceAssignment.getPartitionAssignment(resourceName);
  }

  /**
   * @param resourceName Name of the resource.
   * @return The number of partition of the resource from local memory cache.
   */
  public int getNumberOfPartitions(@Nonnull String resourceName) {
    return resourceAssignment.getPartitionAssignment(resourceName).getExpectedNumberOfPartitions();
  }

  @Override
  public boolean containsKafkaTopic(String kafkaTopic) {
    return resourceAssignment.containsResource(kafkaTopic);
  }

  @Override
  public Instance getLeaderController() {
    if (leaderController == null) {
      throw new VeniceException(
          "There is no leader controller for this controller or we have not received leader changed event from helix.");
    }
    return leaderController;
  }

  @Override
  public void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
    listenerManager.subscribe(kafkaTopic, listener);
  }

  @Override
  public void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
    listenerManager.unsubscribe(kafkaTopic, listener);
  }

  @Override
  public boolean isLiveInstance(String instanceId) {
    try (AutoCloseableLock ignored = AutoCloseableLock.of(this.liveInstancesMapLock)) {
      return liveInstancesMap.containsKey(instanceId);
    }
  }

  @Override
  public long getLeaderControllerChangeTimeMs() {
    return this.leaderControllerChangeTimeMs;
  }

  @Override
  public void onControllerChange(NotificationContext changeContext) {
    if (changeContext.getType().equals(NotificationContext.Type.FINALIZE)) {
      // Finalized notification, listener will be removed.
      return;
    }
    LOGGER.info("Got notification type: {}. Leader controller is changed.", changeContext.getType());
    LiveInstance leader = manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
    this.leaderControllerChangeTimeMs = System.currentTimeMillis();
    if (leader == null) {
      this.leaderController = null;
      LOGGER.error("Cluster do not have leader controller now!");
    } else {
      this.leaderController = createInstanceFromLiveInstance(leader);
      LOGGER.info("New leader controller is: {}:{} ", leaderController.getHost(), leaderController.getPort());
    }
  }

  public ResourceAssignment getResourceAssignment() {
    return resourceAssignment;
  }

  @Override
  public boolean doesResourcesExistInIdealState(String resource) {
    PropertyKey key = keyBuilder.idealStates(resource);
    // Try to get the helix property for the given resource, if result is null means the resource does not exist in
    // ideal states.
    return manager.getHelixDataAccessor().getProperty(key) != null;
  }

  protected Map<String, Instance> convertLiveInstances(Collection<LiveInstance> helixLiveInstances) {
    HashMap<String, Instance> instancesMap = new HashMap<>();
    for (LiveInstance helixLiveInstance: helixLiveInstances) {
      Instance instance = createInstanceFromLiveInstance(helixLiveInstance);
      instancesMap.put(instance.getNodeId(), instance);
    }
    return instancesMap;
  }

  private static Instance createInstanceFromLiveInstance(LiveInstance liveInstance) {
    return new Instance(
        liveInstance.getId(),
        Utils.parseHostFromHelixNodeIdentifier(liveInstance.getId()),
        Utils.parsePortFromHelixNodeIdentifier(liveInstance.getId()));
  }

  @Override
  public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
    if (routingTableSnapshot == null) {
      LOGGER.warn("Routing table snapshot should not be null");
      return;
    }
    PropertyType helixPropertyType = routingTableSnapshot.getPropertyType();
    switch (helixPropertyType) {
      case EXTERNALVIEW:
        LOGGER.debug("Received Helix routing table change on External View");
        onExternalViewDataChange(routingTableSnapshot);
        break;
      case CUSTOMIZEDVIEW:
        LOGGER.debug("Received Helix routing table change on Customized View");
        onCustomizedViewDataChange(routingTableSnapshot);
        break;
      default:
        LOGGER.warn("Received Helix routing table change on invalid type: {}.", helixPropertyType);
    }
  }

  protected abstract void onExternalViewDataChange(RoutingTableSnapshot routingTableSnapshot);

  protected abstract void onCustomizedViewDataChange(RoutingTableSnapshot routingTableSnapshot);

  /**
   * Used by tests only. Evaluate carefully if there is an intent to start using this in the main code.
   *
   * @return the leader {@link Instance} or null if there isn't one
   */
  @Override
  public Instance getLeaderInstance(String resourceName, int partition) {
    Partition p = resourceAssignment.getPartition(resourceName, partition);
    if (p == null) {
      return null;
    }
    return p.getLeaderInstance();
  }
}
