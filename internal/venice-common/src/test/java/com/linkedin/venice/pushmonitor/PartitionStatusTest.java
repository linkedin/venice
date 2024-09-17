package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.utils.Utils.FATAL_DATA_VALIDATION_ERROR;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
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
    assertThrows(
        VeniceException.class,
        () -> readonlyPartitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.COMPLETED));
    assertThrows(
        VeniceException.class,
        () -> readonlyPartitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.COMPLETED, false));
    assertThrows(
        VeniceException.class,
        () -> readonlyPartitionStatus.updateReplicaStatus(instanceId, ExecutionStatus.COMPLETED, "inc push", 1));
  }

  @Test
  public void testHasFatalDataValidationError() {
    PartitionStatus partitionStatus = new PartitionStatus(partitionId);
    Assert.assertFalse(partitionStatus.hasFatalDataValidationError());

    partitionStatus.updateReplicaStatus("testInstance1", ExecutionStatus.COMPLETED);
    Assert.assertFalse(partitionStatus.hasFatalDataValidationError());

    partitionStatus.updateReplicaStatus("testInstance2", ExecutionStatus.ERROR);
    Assert.assertFalse(partitionStatus.hasFatalDataValidationError());

    partitionStatus.updateReplicaStatus("testInstance3", ExecutionStatus.ERROR, null, 10);
    Assert.assertFalse(partitionStatus.hasFatalDataValidationError());

    partitionStatus
        .updateReplicaStatus("testInstance4", ExecutionStatus.ERROR, FATAL_DATA_VALIDATION_ERROR + " for replica", 10);
    Assert.assertTrue(partitionStatus.hasFatalDataValidationError());
  }

  @Test
  public void testValidateBatchUpdateEOIP() {
    PartitionStatus partitionStatus = new PartitionStatus(partitionId);
    Assert.assertFalse(partitionStatus.hasFatalDataValidationError());

    partitionStatus.batchUpdateReplicaIncPushStatus("testInstance1", Arrays.asList("a", "b", "c"), 100L);
    Assert.assertFalse(partitionStatus.hasFatalDataValidationError());
    Assert.assertEquals(
        partitionStatus.getReplicaStatus("testInstance1"),
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
    ReplicaStatus replicaStatus = partitionStatus.getReplicaStatuses().iterator().next();
    Assert.assertEquals(replicaStatus.getCurrentProgress(), 100L);
    Assert.assertEquals(replicaStatus.getStatusHistory().get(0).getIncrementalPushVersion(), "a");
    Assert.assertEquals(
        replicaStatus.getStatusHistory().get(0).getStatus(),
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
    Assert.assertEquals(replicaStatus.getStatusHistory().get(1).getIncrementalPushVersion(), "b");
    Assert.assertEquals(
        replicaStatus.getStatusHistory().get(1).getStatus(),
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
    Assert.assertEquals(replicaStatus.getStatusHistory().get(2).getIncrementalPushVersion(), "c");
    Assert.assertEquals(
        replicaStatus.getStatusHistory().get(2).getStatus(),
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
  }
}
