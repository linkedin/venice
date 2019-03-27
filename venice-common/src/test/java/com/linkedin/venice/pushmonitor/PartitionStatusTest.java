package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
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

  @Test
  public void testReadonlyPartitionStatus() {
    String instanceId = "testInstance";
    PartitionStatus partitionStatus = new PartitionStatus(partitionId);
    partitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.PROGRESS);
    PartitionStatus readonlyPartitionStatus = ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus);
    try {
      readonlyPartitionStatus.updateProgress(instanceId, 1);
      Assert.fail("Can no update a readonly partition status.");
    } catch (VeniceException e) {
    }
    try {
      readonlyPartitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.COMPLETED);
      Assert.fail("Can no update a readonly partition status.");
    } catch (VeniceException e) {
    }
  }
}
