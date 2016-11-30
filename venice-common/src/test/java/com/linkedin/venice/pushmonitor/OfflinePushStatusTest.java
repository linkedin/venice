package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.job.ExecutionStatus.*;


public class OfflinePushStatusTest {
  private String kafkaTopic = "testTopic";
  private int numberOfPartition = 3;
  private int replciationFactor = 2;
  private OfflinePushStrategy strategy = OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;

  @Test
  public void testCreateOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replciationFactor, strategy);
    Assert.assertEquals(offlinePushStatus.getKafkaTopic(), kafkaTopic);
    Assert.assertEquals(offlinePushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(offlinePushStatus.getReplicationFactor(), replciationFactor);
    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), STARTED,
        "Once offline push status is created, it should in STARTED status by default.");
    Assert.assertEquals(offlinePushStatus.getStatusHistory().get(0).getStatus(), STARTED,
        "Once offline push status is created, it's in STARTED status and this status should be added into status history.");
    Assert.assertEquals(offlinePushStatus.getPartitionStatuses().size(), numberOfPartition,
        "Once offline push status is created, partition statuses should also be created too.");
  }

  @Test
  public void testUpdatePartitionStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replciationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(offlinePushStatus.getPartitionStatuses().get(1), partitionStatus);

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      Assert.fail("Partition 1000 dose not exist.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testUpdateStatusFromSTARTED() {
    testValidTargetStatuses(STARTED, COMPLETED, ERROR);
    testInvalidTargetStatuses(STARTED, STARTED, ARCHIVED, STARTED);
  }

  @Test
  public void testUpdateStatusFromERROR() {
    testValidTargetStatuses(ERROR, ARCHIVED);
    testInvalidTargetStatuses(ERROR, STARTED, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromCOMPLETED() {
    testValidTargetStatuses(COMPLETED, ARCHIVED);
    testInvalidTargetStatuses(COMPLETED, ERROR, COMPLETED, STARTED);
  }

  @Test
  public void testUpdateStatusFromARCHIVED() {
    testInvalidTargetStatuses(ARCHIVED, ARCHIVED, STARTED, ERROR, COMPLETED);
  }

  @Test
  public void testCloneOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replciationFactor, strategy);
    OfflinePushStatus clonedPush = offlinePushStatus.clonePushStatus();
    Assert.assertEquals(clonedPush, offlinePushStatus);

    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("i1", COMPLETED);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertNotEquals(clonedPush, offlinePushStatus);
  }

  private void testValidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      OfflinePushStatus offlinePushStatus =
          new OfflinePushStatus(kafkaTopic, numberOfPartition, replciationFactor, strategy);
      offlinePushStatus.setCurrentStatus(from);
      offlinePushStatus.updateStatus(status);
      Assert.assertEquals(offlinePushStatus.getCurrentStatus(), status, status + " should be valid from:" + from);
    }
  }

  public void testInvalidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      OfflinePushStatus offlinePushStatus =
          new OfflinePushStatus(kafkaTopic, numberOfPartition, replciationFactor, strategy);
      offlinePushStatus.setCurrentStatus(from);
      try {
        offlinePushStatus.updateStatus(status);
        Assert.fail(status + " is invalid from:" + from);
      } catch (VeniceException e) {
        //expected.
      }
    }
  }
}
