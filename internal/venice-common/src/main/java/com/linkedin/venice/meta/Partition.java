package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
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
  private final Map<HelixState, List<Instance>> helixStateToInstancesMap;
  private final Map<ExecutionStatus, List<Instance>> executionStatusToInstancesMap;
  private final List<Instance> readyInstances;
  private final List<Instance> workingInstances;

  public Partition(int id, Map<String, List<Instance>> stateToInstancesMap) {
    this.id = id;
    this.stateToInstancesMap = stateToInstancesMap;

    // Populate enum maps which are more efficient than the string-based map
    this.helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    this.executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    for (Map.Entry<String, List<Instance>> entry: stateToInstancesMap.entrySet()) {
      try {
        HelixState helixState = HelixState.valueOf(entry.getKey());
        this.helixStateToInstancesMap.put(helixState, Collections.unmodifiableList(entry.getValue()));
      } catch (IllegalArgumentException e) {
        // carry on
      }
      try {
        ExecutionStatus executionStatus = ExecutionStatus.valueOf(entry.getKey());
        this.executionStatusToInstancesMap.put(executionStatus, Collections.unmodifiableList(entry.getValue()));
      } catch (IllegalArgumentException e) {
        // carry on
      }
    }
    for (HelixState helixState: HelixState.values()) {
      this.helixStateToInstancesMap.putIfAbsent(helixState, Collections.emptyList());
    }
    for (ExecutionStatus executionStatus: ExecutionStatus.values()) {
      this.executionStatusToInstancesMap.putIfAbsent(executionStatus, Collections.emptyList());
    }

    // Populate lists of multiple related states
    List<Instance> instances = new ArrayList<>();
    instances.addAll(this.helixStateToInstancesMap.get(HelixState.ONLINE));
    instances.addAll(this.helixStateToInstancesMap.get(HelixState.STANDBY));
    instances.addAll(this.helixStateToInstancesMap.get(HelixState.LEADER));
    this.readyInstances = Collections.unmodifiableList(instances);

    List<Instance> instances2 = new ArrayList<>();
    instances2.addAll(this.helixStateToInstancesMap.get(HelixState.ONLINE));
    instances2.addAll(this.helixStateToInstancesMap.get(HelixState.BOOTSTRAP));
    instances2.addAll(this.helixStateToInstancesMap.get(HelixState.STANDBY));
    instances2.addAll(this.helixStateToInstancesMap.get(HelixState.LEADER));
    this.workingInstances = Collections.unmodifiableList(instances2);
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
  public List<Instance> getReadyToServeInstances() {
    List<Instance> completed = this.executionStatusToInstancesMap.get(ExecutionStatus.COMPLETED);
    List<Instance> online = this.helixStateToInstancesMap.get(HelixState.ONLINE);
    return completed.size() > online.size() ? completed : this.readyInstances;
  }

  public List<Instance> getWorkingInstances() {
    return this.workingInstances;
  }

  public List<Instance> getErrorInstances() {
    return this.helixStateToInstancesMap.get(HelixState.ERROR);
  }

  public List<Instance> getBootstrapInstances() {
    return this.helixStateToInstancesMap.get(HelixState.BOOTSTRAP);
  }

  public Instance getLeaderInstance() {
    List<Instance> instances = this.helixStateToInstancesMap.get(HelixState.LEADER);

    if (instances.isEmpty()) {
      return null;
    }

    if (instances.size() > 1) {
      LOGGER.error("Detect multiple leaders. Partition: {}", id);
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

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * id;
    result = 31 * result + stateToInstancesMap.hashCode();
    return result;
  }
}
