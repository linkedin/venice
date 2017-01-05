package com.linkedin.venice.job;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class TestJobStatusDecider {
  protected String topic = "testTopic";
  protected int numberOfPartition = 4;
  protected int replicationFactor = 3;
  protected String nodeId = "localhost_1234";
  protected PartitionAssignment partitionAssignment;

  public void createPartitions(int numberOfPartition, int replicationFactor) {
    for (int i = 0; i < numberOfPartition; i++) {
      List<Instance> instances = createInstances(replicationFactor);
      Map<String, List<Instance>> stateToInstancesMap = new HashMap<>();
      stateToInstancesMap.put(HelixState.BOOTSTRAP_STATE, instances);
      Partition partition = new Partition(i, stateToInstancesMap);
      partitionAssignment.addPartition(partition);
    }
  }

  protected List<Instance> createInstances(int replicationFactor) {
    List<Instance> instances = new ArrayList<>();
    for (int j = 0; j < replicationFactor; j++) {
      Instance instance = new Instance(nodeId + j, "localhost", 1235);
      instances.add(instance);
    }
    return instances;
  }

  protected void createTasksAndUpdateJob(OfflineJob job, int numberOfPartition, int replicationFactor,
      ExecutionStatus status, int exceptPartition, int exceptReplica) {
    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicationFactor; j++) {
        Task t = new Task(job.generateTaskId(i, nodeId + j), i, String.valueOf(j), status);
        if (i == exceptPartition && j == exceptReplica) {
          continue;
        }
        job.updateTaskStatus(t);
      }
    }
  }

  protected Partition changeReplicaState(Partition partition, String instanceId, HelixState newState) {
    Map<String, List<Instance>> newStateToInstancesMap = new HashMap<>();
    Instance targetInstance = null;
    for (String state : partition.getAllInstances().keySet()) {
      List<Instance> oldInstances = partition.getAllInstances().get(state);
      List<Instance> newInstances = new ArrayList<>(oldInstances);
      Iterator<Instance> iterator = newInstances.iterator();
      while (iterator.hasNext()) {
        Instance instance = iterator.next();
        if (instance.getNodeId().equals(instanceId)) {
          targetInstance = instance;
          iterator.remove();
        }
      }
      if(!newInstances.isEmpty()) {
        newStateToInstancesMap.put(state, newInstances);
      }
    }
    if (targetInstance == null) {
      throw new IllegalStateException("Can not find instance:" + instanceId);
    }
    List<Instance> newInstances = newStateToInstancesMap.get(newState.name());
    if (newInstances == null) {
      newInstances = new ArrayList<>();
      newStateToInstancesMap.put(newState.name(), newInstances);
    }
    newInstances.add(targetInstance);
    return new Partition(partition.getId(), newStateToInstancesMap);
  }
}
