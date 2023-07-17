package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.ResourceAssignment.ResourceAssignmentChanges;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Extend HelixBaseRoutingRepository to leverage external view data.
 */
@BatchMode
public class HelixExternalViewRepository extends HelixBaseRoutingRepository implements IdealStateChangeListener {
  private static final Logger LOGGER = LogManager.getLogger(HelixExternalViewRepository.class);

  private volatile Map<String, Integer> resourceToIdealPartitionCountMap;

  public HelixExternalViewRepository(SafeHelixManager manager) {
    super(manager);
    dataSource.put(PropertyType.EXTERNALVIEW, Collections.emptyList());
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext) {
    refreshResourceToIdealPartitionCountMap(idealStates);
  }

  public void refresh() {
    try {
      manager.addIdealStateChangeListener(this);
      super.refresh();
    } catch (Exception e) {
      String errorMessage = "Cannot refresh routing table from Helix for cluster " + manager.getClusterName();
      LOGGER.error(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
  }

  public void clear() {
    manager.removeListener(keyBuilder.idealStates(), this);
    super.clear();
  }

  @Override
  protected void onExternalViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
    if (routingTableSnapshot.getExternalViews() == null || routingTableSnapshot.getExternalViews().size() <= 0) {
      LOGGER.info("Ignore the empty external view.");
      // Update live instances even if there is nothing in the external view.
      try (AutoCloseableLock ignored = AutoCloseableLock.of(this.liveInstancesMapLock)) {
        liveInstancesMap = convertLiveInstances(routingTableSnapshot.getLiveInstances());
      }
      LOGGER.info("Updated live instances.");
      return;
    }
    Collection<ExternalView> externalViewCollection = routingTableSnapshot.getExternalViews();

    // Create a snapshot to prevent live instances map being changed during this method execution.
    Map<String, Instance> liveInstanceSnapshot = convertLiveInstances(routingTableSnapshot.getLiveInstances());
    // Get number of partitions from Ideal state category in ZK.
    Map<String, Integer> resourceToPartitionCountMapSnapshot = resourceToIdealPartitionCountMap;
    ResourceAssignment newResourceAssignment = new ResourceAssignment();
    Set<String> resourcesInExternalView =
        externalViewCollection.stream().map(ExternalView::getResourceName).collect(Collectors.toSet());
    if (!resourceToPartitionCountMapSnapshot.keySet().containsAll(resourcesInExternalView)) {
      LOGGER.info(
          "Found the inconsistent data between the external view and ideal state of cluster: {}."
              + " Reading the latest ideal state from zk.",
          manager.getClusterName());

      List<PropertyKey> keys = externalViewCollection.stream()
          .map(ev -> keyBuilder.idealStates(ev.getResourceName()))
          .collect(Collectors.toList());
      try {
        List<IdealState> idealStates = manager.getHelixDataAccessor().getProperty(keys);
        refreshResourceToIdealPartitionCountMap(idealStates);
        resourceToPartitionCountMapSnapshot = resourceToIdealPartitionCountMap;
        LOGGER.info("Ideal state of cluster: {} is updated from zk.", manager.getClusterName());
      } catch (HelixMetaDataAccessException e) {
        LOGGER.error(
            "Failed to update the ideal state of cluster: {}, because we could not access to zk.",
            manager.getClusterName(),
            e);
        return;
      }
    }

    for (ExternalView externalView: externalViewCollection) {
      String resourceName = externalView.getResourceName();
      if (!resourceToPartitionCountMapSnapshot.containsKey(resourceName)) {
        LOGGER.warn(
            "Could not find resource: {} in ideal state. Ideal state is up to date, so the resource has been "
                + "deleted from ideal state or could not read from zk. Ignore its external view update.",
            resourceName);
        continue;
      }
      PartitionAssignment partitionAssignment =
          new PartitionAssignment(resourceName, resourceToPartitionCountMapSnapshot.get(resourceName));
      for (String partitionName: externalView.getPartitionSet()) {
        // Get instance to state map for this partition from local memory.
        Map<String, String> instanceStateMap = externalView.getStateMap(partitionName);
        EnumMap<HelixState, List<Instance>> stateToInstanceMap = new EnumMap<>(HelixState.class);
        for (Map.Entry<String, String> entry: instanceStateMap.entrySet()) {
          String instanceName = entry.getKey();
          String instanceState = entry.getValue();
          Instance instance = liveInstanceSnapshot.get(instanceName);
          if (instance != null) {
            HelixState state;
            try {
              state = HelixState.valueOf(instanceState);
            } catch (Exception e) {
              LOGGER.warn("Instance: {} unrecognized state: {}.", instanceName, instanceState);
              continue;
            }
            stateToInstanceMap.computeIfAbsent(state, k -> new ArrayList<>()).add(instance);
          } else {
            LOGGER.warn("Cannot find instance '{}' in /LIVEINSTANCES", instanceName);
          }
        }
        int partitionId = HelixUtils.getPartitionId(partitionName);
        partitionAssignment
            .addPartition(new Partition(partitionId, stateToInstanceMap, new EnumMap<>(ExecutionStatus.class)));
      }
      newResourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
    }
    ResourceAssignmentChanges updates;
    synchronized (resourceAssignment) {
      // Update the live instances as well. Helix updates live instances in this routing data changed event.
      try (AutoCloseableLock ignored = AutoCloseableLock.of(this.liveInstancesMapLock)) {
        this.liveInstancesMap = Collections.unmodifiableMap(liveInstanceSnapshot);
      }
      updates = resourceAssignment.updateResourceAssignment(newResourceAssignment);
      LOGGER.info("Updated resource assignment and live instances.");
    }
    LOGGER.info("External view is changed.");
    // Start sending notification to listeners. As we can not get the changed data only from Helix, so we just notify
    // all listeners.
    // And assume that the listener would compare and decide how to handle this event.
    for (String kafkaTopic: updates.getUpdatedResources()) {
      PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(kafkaTopic);
      listenerManager.trigger(kafkaTopic, listener -> listener.onExternalViewChange(partitionAssignment));
    }

    // Notify events to the listeners which listen on deleted resources.
    for (String kafkaTopic: updates.getDeletedResource()) {
      listenerManager.trigger(kafkaTopic, listener -> listener.onRoutingDataDeleted(kafkaTopic));
    }
  }

  private void refreshResourceToIdealPartitionCountMap(List<IdealState> idealStates) {
    HashMap<String, Integer> partitionCountMap = new HashMap<>();
    for (IdealState idealState: idealStates) {
      // Ideal state could be null, if a resource has already been deleted.
      if (idealState != null) {
        partitionCountMap.put(idealState.getResourceName(), idealState.getNumPartitions());
      }
    }
    this.resourceToIdealPartitionCountMap = Collections.unmodifiableMap(partitionCountMap);
  }

  protected void onCustomizedViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
    throw new VeniceException("The function of onCustomizedViewDataChange is not implemented");
  }
}
