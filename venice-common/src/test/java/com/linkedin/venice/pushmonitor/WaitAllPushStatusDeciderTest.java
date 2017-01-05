package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.TestJobStatusDecider;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class WaitAllPushStatusDeciderTest extends TestJobStatusDecider {
  private WaitAllPushStatusDecider statusDecider = new WaitAllPushStatusDecider();
  private OfflinePushStatus pushStatus;

  @BeforeMethod
  public void setup() {
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
}
