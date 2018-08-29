package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Class defines the partition in Venice.
 * <p>
 * Partition is a logic unit to distributed the data in Venice cluster. Each resource(Store+version) will be assigned to
 * a set of partition so that data in this resource will be distributed averagely in ideal. Each partition contains 1 or
 * multiple replica which hold the same data in ideal.
 * <p>
 * In Helix Full-auto model, Helix manager is responsible to assign partitions to nodes. So here partition is read-only.
 * In the future, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public class Partition {
  /**
   * Id of partition. One of the number between [0 ~ total number of partition)
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

  public List<Instance> getReadyToServeInstances() {
    return getInstancesInState(HelixState.ONLINE_STATE);
  }

  public List<Instance> getBootstrapAndReadyToServeInstances() {
    List<Instance> instances = new ArrayList<>();
    instances.addAll(getInstancesInState(HelixState.ONLINE_STATE));
    instances.addAll(getInstancesInState(HelixState.BOOTSTRAP_STATE));
    return Collections.unmodifiableList(instances);
  }

  public List<Instance> getErrorInstances() {
    return getInstancesInState(HelixState.ERROR_STATE);
  }

  public List<Instance> getBootstrapInstances() {
    return getInstancesInState(HelixState.BOOTSTRAP_STATE);
  }

  public List<Instance> getOfflineInstances(){
    return getInstancesInState(HelixState.OFFLINE_STATE);
  }
  public Map<String, List<Instance>> getAllInstances() {
    return Collections.unmodifiableMap(stateToInstancesMap);
  }

  /**
   * Find the status of given instance in this partition.
   */
  public String getInstanceStatusById(String instanceId){
    for(String status:stateToInstancesMap.keySet()){
      List<Instance> instances = stateToInstancesMap.get(status);
      for(Instance instance : instances){
        if(instance.getNodeId().equals(instanceId)){
          return status;
        }
      }
    }
    return null;
  }

  /**
   * Remove the given instance from this partition. As partition is an immutable object, so we return a new partition after removing.
   */
  public Partition withRemovedInstance(String instanceId) {
    HashMap<String, List<Instance>> newStateToInstancesMap = new HashMap<>();
    for (Map.Entry<String, List<Instance>> entry : stateToInstancesMap.entrySet()) {

      List<Instance> newInstances = new ArrayList<>(entry.getValue());
      newInstances.removeIf((Instance instance) -> instance.getNodeId().equals(instanceId));
      if (!newInstances.isEmpty()) {
        newStateToInstancesMap.put(entry.getKey(), newInstances);
      }
    }
    Partition newPartition = new Partition(id, newStateToInstancesMap);
    return newPartition;
  }

  public int getId() {
    return id;
  }
}
