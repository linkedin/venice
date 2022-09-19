package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class defines the partition in Venice.
 * <p>
 * Partition is a logic unit to distribute the data in Venice cluster. Each resource(Store Version) will be assigned to
 * a set of partition so that data in this resource will be distributed averagely in ideal. Each partition contains 1 or
 * multiple replica which hold the same data in ideal.
 * <p>
 * In Helix Full-auto model, Helix manager is responsible to assign partitions to nodes. So here partition is read-only.
 * In the future, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public class Partition {
  private static final Logger LOGGER = LogManager.getLogger(Partition.class);
  /**
   * ID of partition. One of the number between [0 ~ total number of partition)
   */
  private final int id;

  private final Map<String, List<Instance>> stateToInstancesMap;

  public Partition(int id, Map<String, List<Instance>> stateToInstancesMap) {
    this.id = id;
    this.stateToInstancesMap = stateToInstancesMap;
  }

  public List<Instance> getInstancesInState(String state) {
    List<Instance> instances = stateToInstancesMap.get(state);
    return instances == null ? Collections.emptyList() : Collections.unmodifiableList(instances);
  }

  /**
   * Checking if a instance ready to serve via its Helix state is unsafe after L/F model is introduced.
   *
   * Avoid using this API outside of {@link com.linkedin.venice.pushmonitor.PushStatusDecider#checkPushStatusAndDetails}
   * and {@link HelixExternalViewRepository#getReadyToServeInstances(String, int)}
   *
   * TODO: remove this API once we've fully migrate to L/F model.
   */
  @Deprecated
  public List<Instance> getReadyToServeInstances() {
    return getInstancesInState(ExecutionStatus.COMPLETED.name()).size() > getInstancesInState(HelixState.ONLINE_STATE)
        .size() ? getInstancesInState(ExecutionStatus.COMPLETED.name()) : getReadyInstances();
  }

  private List<Instance> getReadyInstances() {
    List<Instance> instances = new ArrayList<>();
    instances.addAll(getInstancesInState(HelixState.ONLINE_STATE));
    instances.addAll(getInstancesInState(HelixState.STANDBY_STATE));
    instances.addAll(getInstancesInState(HelixState.LEADER_STATE));
    return Collections.unmodifiableList(instances);
  }

  public List<Instance> getWorkingInstances() {
    List<Instance> instances = new ArrayList<>();
    instances.addAll(getInstancesInState(HelixState.ONLINE_STATE));
    instances.addAll(getInstancesInState(HelixState.BOOTSTRAP_STATE));

    instances.addAll(getInstancesInState(HelixState.STANDBY_STATE));
    instances.addAll(getInstancesInState(HelixState.LEADER_STATE));
    return Collections.unmodifiableList(instances);
  }

  public List<Instance> getErrorInstances() {
    return getInstancesInState(HelixState.ERROR_STATE);
  }

  public List<Instance> getBootstrapInstances() {
    return getInstancesInState(HelixState.BOOTSTRAP_STATE);
  }

  public List<Instance> getOfflineInstances() {
    return getInstancesInState(HelixState.OFFLINE_STATE);
  }

  public Instance getLeaderInstance() {
    List<Instance> instances = getInstancesInState(HelixState.LEADER_STATE);

    if (instances.isEmpty()) {
      return null;
    }

    if (instances.size() > 1) {
      LOGGER.error(String.format("Detect multiple leaders. Partition: %d", id));
    }

    return instances.get(0);
  }

  public Map<String, List<Instance>> getAllInstances() {
    return Collections.unmodifiableMap(stateToInstancesMap);
  }

  public Map<Instance, String> getInstanceToStateMap() {
    Map<Instance, String> instanceToStateMap = new HashMap<>();
    stateToInstancesMap.forEach(
        (helixState, instanceList) -> instanceList.forEach(instance -> instanceToStateMap.put(instance, helixState)));

    return instanceToStateMap;
  }

  /**
   * Find the status of given instance in this partition.
   */
  public String getInstanceStatusById(String instanceId) {
    for (Map.Entry<String, List<Instance>> entry: stateToInstancesMap.entrySet()) {
      String status = entry.getKey();
      List<Instance> instances = entry.getValue();
      for (Instance instance: instances) {
        if (instance.getNodeId().equals(instanceId)) {
          return status;
        }
      }
    }
    return null;
  }

  public int getNumOfTotalInstances() {
    return stateToInstancesMap.values().stream().mapToInt(List::size).sum();
  }

  /**
   * Remove the given instance from this partition. As partition is an immutable object, so we return a new partition after removing.
   */
  public Partition withRemovedInstance(String instanceId) {
    HashMap<String, List<Instance>> newStateToInstancesMap = new HashMap<>();
    for (Map.Entry<String, List<Instance>> entry: stateToInstancesMap.entrySet()) {

      List<Instance> newInstances = new ArrayList<>(entry.getValue());
      newInstances.removeIf((Instance instance) -> instance.getNodeId().equals(instanceId));
      if (!newInstances.isEmpty()) {
        newStateToInstancesMap.put(entry.getKey(), newInstances);
      }
    }
    return new Partition(id, newStateToInstancesMap);
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " {id: " + id + ", stateToInstancesMap: " + stateToInstancesMap + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Partition) {
      return stateToInstancesMap.equals(((Partition) obj).stateToInstancesMap);
    }

    return false;
  }
}
