package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NEW;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.UNKNOWN;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.WARNING;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
      fail("Partition 1000 dose not exist.");
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
      fail("Partition 1000 dose not exist.");
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
  public void testUpdateStatusValidTransitions() {
    // Define a map to store valid transitions for each source status
    Map<ExecutionStatus, Set<ExecutionStatus>> validTransitions = new HashMap<>();
    validTransitions.put(
        NOT_CREATED,
        EnumSet.of(
            STARTED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_OTHER));
    validTransitions.put(
        STARTED,
        EnumSet.of(
            STARTED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_OTHER,
            COMPLETED,
            END_OF_PUSH_RECEIVED));
    validTransitions.put(ERROR, EnumSet.of(ARCHIVED));
    validTransitions.put(COMPLETED, EnumSet.of(ARCHIVED));
    validTransitions.put(
        END_OF_PUSH_RECEIVED,
        EnumSet.of(
            COMPLETED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_OTHER));

    // add other status to the map manually such that test will fail if a new status is added but not updated in
    // validatePushStatusTransition and here
    validTransitions.put(NEW, new HashSet<>());
    validTransitions.put(PROGRESS, new HashSet<>());
    validTransitions.put(START_OF_BUFFER_REPLAY_RECEIVED, new HashSet<>());
    validTransitions.put(TOPIC_SWITCH_RECEIVED, new HashSet<>());
    validTransitions.put(START_OF_INCREMENTAL_PUSH_RECEIVED, new HashSet<>());
    validTransitions.put(END_OF_INCREMENTAL_PUSH_RECEIVED, new HashSet<>());
    validTransitions.put(DROPPED, new HashSet<>());
    validTransitions.put(WARNING, new HashSet<>());
    validTransitions.put(CATCH_UP_BASE_TOPIC_OFFSET_LAG, new HashSet<>());
    validTransitions.put(ARCHIVED, new HashSet<>());
    validTransitions.put(UNKNOWN, new HashSet<>());
    validTransitions.put(NOT_STARTED, new HashSet<>());
    validTransitions.put(DATA_RECOVERY_COMPLETED, new HashSet<>());
    validTransitions.put(DVC_INGESTION_ERROR_DISK_FULL, new HashSet<>());
    validTransitions.put(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED, new HashSet<>());
    validTransitions.put(DVC_INGESTION_ERROR_OTHER, new HashSet<>());

    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (!validTransitions.containsKey(status)) {
        fail("New ExecutionStatus " + status.toString() + " should be added to the test");
      }
    }
    for (ExecutionStatus fromStatus: ExecutionStatus.values()) {
      Set<ExecutionStatus> validTransitionsFromAStatus = validTransitions.get(fromStatus);
      for (ExecutionStatus toStatus: ExecutionStatus.values()) {
        if (validTransitionsFromAStatus.contains(toStatus)) {
          testValidTargetStatus(fromStatus, toStatus);
        } else {
          if (fromStatus == toStatus) {
            // not throwing exception for this case as its redundant, so not testing
            continue;
          }
          testInvalidTargetStatus(fromStatus, toStatus);
        }
      }
    }
  }

  @Test
  public void testisDVCIngestionError() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == DVC_INGESTION_ERROR_DISK_FULL || status == DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED
          || status == DVC_INGESTION_ERROR_OTHER) {
        assertTrue(status.isDVCIngestionError(), status + " should not pass isDVCIngestionError()");
      } else {
        assertFalse(status.isDVCIngestionError(), status + " should pass isDVCIngestionError()");
      }
    }
  }

  @Test
  public void testisIngestionError() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == ERROR || status == DVC_INGESTION_ERROR_DISK_FULL
          || status == DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED || status == DVC_INGESTION_ERROR_OTHER) {
        assertTrue(status.isIngestionError(), status + " should not pass isIngestionError()");
      } else {
        assertFalse(status.isIngestionError(), status + " should pass isIngestionError()");
      }
    }
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

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Partition 0 not found in partition assignment")
  public void testNPECaughtWhenPollingIncPushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionAssignment partitionAssignment = new PartitionAssignment("test-topic", 10);
    offlinePushStatus.getIncrementalPushStatus(partitionAssignment, "ignore");
  }

  private void testValidTargetStatus(ExecutionStatus from, ExecutionStatus to) {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    offlinePushStatus.setCurrentStatus(from);
    offlinePushStatus.updateStatus(to);
    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), to, to + " should be valid from:" + from);
  }

  private void testInvalidTargetStatus(ExecutionStatus from, ExecutionStatus to) {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    offlinePushStatus.setCurrentStatus(from);
    try {
      offlinePushStatus.updateStatus(to);
      fail(to + " is not invalid from:" + from);
    } catch (VeniceException e) {
      // expected.
    }
  }
}
