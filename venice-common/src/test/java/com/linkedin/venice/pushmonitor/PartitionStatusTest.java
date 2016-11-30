package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.job.ExecutionStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionStatusTest {
  private int partitionId = 0;

  @Test
  public void testUpdateReplicaStatus() {
    PartitionStatus partitionStatus = new PartitionStatus(partitionId);
    String instanceId = "testInstance";
    Assert.assertEquals(partitionStatus.getReplicaStatus(instanceId), ExecutionStatus.NOT_CREATED);
    partitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.PROGRESS);
    Assert.assertEquals(partitionStatus.getReplicaStatus(instanceId), ExecutionStatus.PROGRESS);
    partitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.COMPLETED);
    Assert.assertEquals(partitionStatus.getReplicaStatus(instanceId), ExecutionStatus.COMPLETED);
  }
}
