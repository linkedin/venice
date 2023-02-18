package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OfflinePushStatusTest {
  private String kafkaTopic = "testTopic";
  private int numberOfPartition = 3;
  private int replicationFactor = 2;
  private OfflinePushStrategy strategy = OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;

  @Test
  public void testCreateOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    Assert.assertEquals(offlinePushStatus.getKafkaTopic(), kafkaTopic);
    Assert.assertEquals(offlinePushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(offlinePushStatus.getReplicationFactor(), replicationFactor);
    Assert.assertEquals(
        offlinePushStatus.getCurrentStatus(),
        STARTED,
        "Once offline push status is created, it should in STARTED status by default.");
    Assert.assertEquals(
        offlinePushStatus.getStatusHistory().get(0).getStatus(),
        STARTED,
        "Once offline push status is created, it's in STARTED status and this status should be added into status history.");
    Assert.assertEquals(
        offlinePushStatus.getPartitionStatuses().size(),
        numberOfPartition,
        "Once offline push status is created, partition statuses should also be created too.");
  }

  @Test
  public void testUpdatePartitionStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(
        offlinePushStatus.getPartitionStatus(1),
        ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      Assert.fail("Partition 1000 dose not exist.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testIsReadyToStartBufferReplay() {
    // Make sure buffer replay can be started in the case where current replica status is PROGRESS but
    // END_OF_PUSH_RECEIVED was already received
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, 1, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    List<ReplicaStatus> replicaStatuses = new ArrayList<>(replicationFactor);
    for (int i = 0; i < replicationFactor; i++) {
      replicaStatuses.add(new ReplicaStatus(Integer.toString(i)));
    }
    partitionStatus.setReplicaStatuses(replicaStatuses);
    for (int i = 0; i < replicationFactor; i++) {
      partitionStatus.updateReplicaStatus(Integer.toString(i), END_OF_PUSH_RECEIVED);
      partitionStatus.updateReplicaStatus(Integer.toString(i), PROGRESS);
    }
    offlinePushStatus.setPartitionStatuses(Collections.singletonList(partitionStatus));
    Assert.assertTrue(
        offlinePushStatus.isReadyToStartBufferReplay(false),
        "Buffer replay should be allowed to start since END_OF_PUSH_RECEIVED was already received");
  }

  @Test
  public void testSetPartitionStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(
        offlinePushStatus.getPartitionStatus(1),
        ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      Assert.fail("Partition 1000 dose not exist.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testOfflinePushStatusIsComparable() {
    final int partitionNum = 20;
    List<PartitionStatus> partitionStatusList = new ArrayList<>(partitionNum);
    // The initial list is not ordered by partitionId
    for (int i = partitionNum - 1; i >= 0; i--) {
      partitionStatusList.add(new PartitionStatus(i));
    }
    Collections.sort(partitionStatusList);
    for (int i = 0; i < partitionNum; i++) {
      Assert.assertEquals(partitionStatusList.get(i).getPartitionId(), i);
    }
  }

  @Test
  public void testUpdateStatusFromSTARTED() {
    testValidTargetStatuses(STARTED, STARTED, COMPLETED, ERROR, END_OF_PUSH_RECEIVED);
    testInvalidTargetStatuses(STARTED, ARCHIVED);
  }

  @Test
  public void testUpdateStatusFromEndOfPushReceived() {
    testValidTargetStatuses(END_OF_PUSH_RECEIVED, COMPLETED, ERROR);
    testInvalidTargetStatuses(END_OF_PUSH_RECEIVED, STARTED, ARCHIVED);
  }

  @Test
  public void testUpdateStatusFromERROR() {
    testValidTargetStatuses(ERROR, ARCHIVED);
    testInvalidTargetStatuses(ERROR, STARTED, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromCOMPLETED() {
    testValidTargetStatuses(COMPLETED, ARCHIVED);
    testInvalidTargetStatuses(COMPLETED, ERROR, STARTED);
  }

  @Test
  public void testUpdateStatusFromARCHIVED() {
    testInvalidTargetStatuses(ARCHIVED, STARTED, ERROR, COMPLETED);
  }

  @Test
  public void testRedundantStatusChange() {
    testValidTargetStatuses(END_OF_PUSH_RECEIVED, END_OF_PUSH_RECEIVED);
  }

  @Test
  public void testCloneOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    OfflinePushStatus clonedPush = offlinePushStatus.clonePushStatus();
    Assert.assertEquals(clonedPush, offlinePushStatus);

    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("i1", COMPLETED);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertNotEquals(clonedPush, offlinePushStatus);
  }

  private void testValidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status: statuses) {
      OfflinePushStatus offlinePushStatus =
          new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
      offlinePushStatus.setCurrentStatus(from);
      offlinePushStatus.updateStatus(status);
      Assert.assertEquals(offlinePushStatus.getCurrentStatus(), status, status + " should be valid from:" + from);
    }
  }

  private void testInvalidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status: statuses) {
      OfflinePushStatus offlinePushStatus =
          new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
      offlinePushStatus.setCurrentStatus(from);
      try {
        offlinePushStatus.updateStatus(status);
        Assert.fail(status + " is invalid from:" + from);
      } catch (VeniceException e) {
        // expected.
      }
    }
  }
}
