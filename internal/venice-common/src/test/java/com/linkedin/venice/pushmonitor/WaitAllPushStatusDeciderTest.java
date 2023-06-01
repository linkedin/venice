package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class WaitAllPushStatusDeciderTest extends TestPushStatusDecider {
  private WaitAllPushStatusDecider statusDecider = new WaitAllPushStatusDecider();
  private OfflinePushStatus pushStatus;

  @BeforeMethod
  public void setUp() {
    numberOfPartition = 2;
    replicationFactor = 3;
    topic = "WaitAllPushStatusDeciderTest";
    pushStatus = new OfflinePushStatus(topic, numberOfPartition, replicationFactor, statusDecider.getStrategy());
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
  }

  @Test
  public void testCheckPushStatus() {
    // All replica are in bootstrap status, so the push is still in STARTED status.
    Assert.assertEquals(statusDecider.checkPushStatus(pushStatus, partitionAssignment), ExecutionStatus.STARTED);

    int partitionId = 0;
    for (int j = 0; j < replicationFactor; j++) {
      Partition oldPartition = partitionAssignment.getPartition(partitionId);
      Partition newPartition = changeReplicaState(oldPartition, nodeId + j, HelixState.ONLINE);
      partitionAssignment.addPartition(newPartition);
    }
    // All replicas in partition 0 are completed, but replicas in partition 1 are still in started.
    Assert.assertEquals(statusDecider.checkPushStatus(pushStatus, partitionAssignment), ExecutionStatus.STARTED);

    partitionId = 1;
    for (int j = 0; j < replicationFactor; j++) {
      Partition oldPartition = partitionAssignment.getPartition(partitionId);
      Partition newPartition = changeReplicaState(oldPartition, nodeId + j, HelixState.ONLINE);
      partitionAssignment.addPartition(newPartition);
    }
    // All replicas are completed.
    Assert.assertEquals(statusDecider.checkPushStatus(pushStatus, partitionAssignment), ExecutionStatus.COMPLETED);

    for (partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      Partition oldPartition = partitionAssignment.getPartition(partitionId);
      Partition newPartition = oldPartition.withRemovedInstance(nodeId + 0);
      partitionAssignment.addPartition(newPartition);
    }
    // One of instance is disconnected.
    Assert.assertEquals(statusDecider.checkPushStatus(pushStatus, partitionAssignment), ExecutionStatus.STARTED);
  }

  @Test
  public void testCheckPushStatusReturnError() {
    int partitionId = 0;
    // One of replica is error
    Partition oldPartition = partitionAssignment.getPartition(partitionId);
    Partition newPartition = changeReplicaState(oldPartition, nodeId + 0, HelixState.ERROR);
    partitionAssignment.addPartition(newPartition);
    Assert.assertEquals(statusDecider.checkPushStatus(pushStatus, partitionAssignment), ExecutionStatus.ERROR);
  }

  @Test
  public void testGetPartitionStatus() {
    PartitionStatus partitionStatus = new PartitionStatus(0);

    Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
    instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);

    // Not enough replicas
    partitionStatus.updateReplicaStatus("instance0", COMPLETED);
    partitionStatus.updateReplicaStatus("instance1", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // have enough replicas, but one of them hasn't finished yet
    partitionStatus.updateReplicaStatus("instance2", STARTED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // all the replicas have finished
    partitionStatus.updateReplicaStatus("instance2", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        COMPLETED);

    // one of the replicas failed
    partitionStatus.updateReplicaStatus("instance1", ERROR);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        ERROR);

    // a new replica joined but yet registered in external view
    partitionStatus.updateReplicaStatus("instance3", STARTED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        ERROR);

    instanceToStateMap.put(new Instance("instance3", "host3", 1), HelixState.STANDBY);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // new replica has finished
    partitionStatus.updateReplicaStatus("instance3", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        COMPLETED);
  }
}
