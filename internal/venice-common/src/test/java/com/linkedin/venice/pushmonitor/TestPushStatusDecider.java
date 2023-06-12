package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;


public class TestPushStatusDecider {
  protected String topic = "testTopic";
  protected int numberOfPartition = 2;
  protected int replicationFactor = 3;
  protected String nodeId = "localhost_1234";
  protected PartitionAssignment partitionAssignment;

  public void createPartitions(int numberOfPartition, int replicationFactor) {
    for (int i = 0; i < numberOfPartition; i++) {
      List<Instance> instances = createInstances(replicationFactor);

      EnumMap<HelixState, List<Instance>> helixStateToInstancesMap = new EnumMap<>(HelixState.class);
      EnumMap<ExecutionStatus, List<Instance>> executionStatusToInstancesMap = new EnumMap<>(ExecutionStatus.class);
      executionStatusToInstancesMap.put(ExecutionStatus.STARTED, instances);
      Partition partition = new Partition(i, helixStateToInstancesMap, executionStatusToInstancesMap);
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
}
