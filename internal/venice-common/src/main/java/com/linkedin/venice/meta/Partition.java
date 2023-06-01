package com.linkedin.venice.meta;

import static com.linkedin.venice.helix.HelixState.BOOTSTRAP;
import static com.linkedin.venice.helix.HelixState.LEADER;
import static com.linkedin.venice.helix.HelixState.ONLINE;
import static com.linkedin.venice.helix.HelixState.STANDBY;

import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * ID of partition. One of the number between [0, total number of partition[
   */
  private final int id;

  private final EnumMap<HelixState, List<Instance>> helixStateToInstancesMap;
  private final EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap;

  // Lazy state
  private List<Instance> readyInstances = null;
  private List<Instance> workingInstances = null;
  private Set<Instance> allInstances = null;
  private int hashCode = -1;

  public Partition(
      int id,
      EnumMap<HelixState, List<Instance>> helixStateToInstancesMap,
      EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap) {
    this.id = id;
    this.helixStateToInstancesMap = helixStateToInstancesMap;
    this.executionStatusToInstancesMap = executionStatusToInstancesMap;
    populateEmptyKeysOfEnumMaps();
  }

  /** Legacy constructor, used only by tests... TODO: Move everything over to the new constructor */
  @Deprecated
  public Partition(int id, Map<String, List<Instance>> stateToInstancesMap) {
    this.id = id;

    // Populate enum maps which are more efficient than the string-based map
    this.helixStateToInstancesMap = new EnumMap<>(HelixState.class);
    this.executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    for (Map.Entry<String, List<Instance>> entry: stateToInstancesMap.entrySet()) {
      /**
       * N.B. The design of this class is that the {@param stateToInstancesMap} is keyed by strings which could
       *      correspond to values of either {@link HelixState} of {@link ExecutionStatus}. That is why in the
       *      code below we catch {@link IllegalArgumentException} since they would be thrown whenever the map
       *      contains a key which is absent from either enum. This is not an ideal design, but refactoring it
       *      would be a big undertaking. TODO: refactor it anyway
       */
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
    populateEmptyKeysOfEnumMaps();
  }

  private void populateEmptyKeysOfEnumMaps() {
    for (HelixState helixState: HelixState.values()) {
      this.helixStateToInstancesMap.putIfAbsent(helixState, Collections.emptyList());
    }
    for (ExecutionStatus executionStatus: ExecutionStatus.values()) {
      this.executionStatusToInstancesMap.putIfAbsent(executionStatus, Collections.emptyList());
    }
  }

  private List<Instance> generateInstancesList(HelixState... states) {
    int count = 0;
    for (HelixState state: states) {
      count += this.helixStateToInstancesMap.get(state).size();
    }
    List<Instance> instances = new ArrayList<>(count);
    for (HelixState state: states) {
      instances.addAll(this.helixStateToInstancesMap.get(state));
    }
    return Collections.unmodifiableList(instances);
  }

  public List<Instance> getInstancesInState(ExecutionStatus state) {
    return this.executionStatusToInstancesMap.get(state);
  }

  public List<Instance> getInstancesInState(HelixState state) {
    return this.helixStateToInstancesMap.get(state);
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
    return completed.size() > online.size() ? completed : getReadyInstances();
  }

  public List<Instance> getWorkingInstances() {
    if (this.workingInstances == null) {
      this.workingInstances = generateInstancesList(ONLINE, BOOTSTRAP, STANDBY, LEADER);
    }
    return this.workingInstances;
  }

  private List<Instance> getReadyInstances() {
    if (this.readyInstances == null) {
      this.readyInstances = generateInstancesList(ONLINE, STANDBY, LEADER);
    }
    return this.readyInstances;
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

  public Set<Instance> getAllInstancesSet() {
    if (allInstances == null) {
      int count = 0;
      for (List<Instance> instances: this.helixStateToInstancesMap.values()) {
        count += instances.size();
      }
      for (List<Instance> instances: this.executionStatusToInstancesMap.values()) {
        count += instances.size();
      }

      Set<Instance> tmpAllInstances = new HashSet<>(count);
      for (List<Instance> instances: this.helixStateToInstancesMap.values()) {
        tmpAllInstances.addAll(instances);
      }
      for (List<Instance> instances: this.executionStatusToInstancesMap.values()) {
        tmpAllInstances.addAll(instances);
      }
      this.allInstances = Collections.unmodifiableSet(tmpAllInstances);
    }
    return this.allInstances;
  }

  public Map<HelixState, List<Instance>> getAllInstancesByHelixState() {
    return this.helixStateToInstancesMap;
  }

  public EnumMap<ExecutionStatus, List<Instance>> getAllInstancesByExecutionStatus() {
    return this.executionStatusToInstancesMap;
  }

  public Map<Instance, HelixState> getInstanceToHelixStateMap() {
    Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
    this.helixStateToInstancesMap
        .forEach((state, instanceList) -> instanceList.forEach(instance -> instanceToStateMap.put(instance, state)));

    return instanceToStateMap;
  }

  /**
   * Find the status of given instance in this partition.
   */
  public HelixState getHelixStateByInstanceId(String instanceId) {
    for (Map.Entry<HelixState, List<Instance>> entry: this.helixStateToInstancesMap.entrySet()) {
      List<Instance> instances = entry.getValue();
      for (Instance instance: instances) {
        if (instance.getNodeId().equals(instanceId)) {
          return entry.getKey();
        }
      }
    }
    return null;
  }

  public ExecutionStatus getExecutionStatusByInstanceId(String instanceId) {
    for (Map.Entry<ExecutionStatus, List<Instance>> entry: this.executionStatusToInstancesMap.entrySet()) {
      List<Instance> instances = entry.getValue();
      for (Instance instance: instances) {
        if (instance.getNodeId().equals(instanceId)) {
          return entry.getKey();
        }
      }
    }
    return null;
  }

  public int getNumOfTotalInstances() {
    return getAllInstancesSet().size();
  }

  /**
   * Remove the given instance from this partition. As partition is an immutable object, so we return a new partition after removing.
   */
  public Partition withRemovedInstance(String instanceId) {
    EnumMap<HelixState, List<Instance>> newHelixStateToInstancesMap = new EnumMap<>(HelixState.class);
    for (Map.Entry<HelixState, List<Instance>> entry: this.helixStateToInstancesMap.entrySet()) {
      List<Instance> newInstances = new ArrayList<>(entry.getValue());
      newInstances.removeIf((Instance instance) -> instance.getNodeId().equals(instanceId));
      if (!newInstances.isEmpty()) {
        newHelixStateToInstancesMap.put(entry.getKey(), Collections.unmodifiableList(newInstances));
      }
    }
    EnumMap<ExecutionStatus, List<Instance>> newExecutionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
    for (Map.Entry<ExecutionStatus, List<Instance>> entry: this.executionStatusToInstancesMap.entrySet()) {
      List<Instance> newInstances = new ArrayList<>(entry.getValue());
      newInstances.removeIf((Instance instance) -> instance.getNodeId().equals(instanceId));
      if (!newInstances.isEmpty()) {
        newExecutionStatusToInstancesMap.put(entry.getKey(), Collections.unmodifiableList(newInstances));
      }
    }
    return new Partition(id, newHelixStateToInstancesMap, newExecutionStatusToInstancesMap);
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " {id: " + id + ", helixStateToInstancesMap: "
        + this.helixStateToInstancesMap + ", executionStatusToInstancesMap: " + this.executionStatusToInstancesMap
        + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Partition) {
      Partition otherPartition = (Partition) obj;
      return this.helixStateToInstancesMap.equals(otherPartition.helixStateToInstancesMap)
          && this.executionStatusToInstancesMap.equals(otherPartition.executionStatusToInstancesMap);
    }

    return false;
  }

  @Override
  public int hashCode() {
    if (this.hashCode == -1) {
      int result = 1;
      result = 31 * id;
      result = 31 * result + this.helixStateToInstancesMap.hashCode();
      result = 31 * result + this.executionStatusToInstancesMap.hashCode();
      this.hashCode = result;
    }
    return this.hashCode;
  }
}
