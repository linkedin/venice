package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
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
import static com.linkedin.venice.utils.Utils.FATAL_DATA_VALIDATION_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.DataProviderUtils;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
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
            DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
            DVC_INGESTION_ERROR_OTHER));

    validTransitions.put(
        STARTED,
        EnumSet.of(
            STARTED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
            DVC_INGESTION_ERROR_OTHER,
            COMPLETED,
            END_OF_PUSH_RECEIVED));

    // ERROR Cases
    validTransitions.put(ERROR, EnumSet.of(ARCHIVED));
    validTransitions.put(DVC_INGESTION_ERROR_DISK_FULL, validTransitions.get(ERROR));
    validTransitions.put(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED, validTransitions.get(ERROR));
    validTransitions.put(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES, validTransitions.get(ERROR));
    validTransitions.put(DVC_INGESTION_ERROR_OTHER, validTransitions.get(ERROR));

    validTransitions.put(COMPLETED, EnumSet.of(ARCHIVED));

    validTransitions.put(
        END_OF_PUSH_RECEIVED,
        EnumSet.of(
            COMPLETED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
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

  @Test
  public void testGetStatusUpdateTimestamp() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    Long statusUpdateTimestamp = offlinePushStatus.getStatusUpdateTimestamp();
    assertNotNull(statusUpdateTimestamp);

    // N.B. There are no currently production code paths setting current status to null, so this is just to test the
    // defensive code...
    offlinePushStatus.setCurrentStatus(null);
    statusUpdateTimestamp = offlinePushStatus.getStatusUpdateTimestamp();
    assertNull(statusUpdateTimestamp);

    offlinePushStatus.setCurrentStatus(PROGRESS);
    List<StatusSnapshot> statusSnapshots = new ArrayList<>();
    statusSnapshots.add(new StatusSnapshot(STARTED, LocalDateTime.of(2012, 12, 21, 0, 0, 0).toString()));
    LocalDateTime startOfProgress = LocalDateTime.of(2012, 12, 21, 1, 0, 0);
    statusSnapshots.add(new StatusSnapshot(PROGRESS, startOfProgress.toString()));
    statusSnapshots.add(new StatusSnapshot(PROGRESS, LocalDateTime.of(2012, 12, 21, 2, 0, 0).toString()));
    offlinePushStatus.setStatusHistory(statusSnapshots);
    statusUpdateTimestamp = offlinePushStatus.getStatusUpdateTimestamp();
    assertNotNull(statusUpdateTimestamp);
    assertEquals(statusUpdateTimestamp, Long.valueOf(startOfProgress.toEpochSecond(ZoneOffset.UTC)));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testHasFatalDataValidationError(boolean hasFatalDataValidationError) {
    String kafkaTopic = "testTopic";
    int numberOfPartition = 3;
    int replicationFactor = 2;
    OfflinePushStrategy strategy = OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;

    // Create an OfflinePushStatus object
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);

    // Create a PartitionStatus
    PartitionStatus partitionStatus = new PartitionStatus(1);

    // Create a ReplicaStatus with an error ExecutionStatus and an incrementalPushVersion that contains "Fatal data
    // validation problem"
    ReplicaStatus replicaStatus = new ReplicaStatus("instance1", true);
    if (hasFatalDataValidationError) {
      replicaStatus.updateStatus(ExecutionStatus.ERROR);
      replicaStatus.setIncrementalPushVersion(
          FATAL_DATA_VALIDATION_ERROR + " with partition 1, offset 1096534. Consumption will be halted.");
    } else {
      replicaStatus.updateStatus(ExecutionStatus.COMPLETED);
    }

    // Add the ReplicaStatus to the replicaStatusMap of the PartitionStatus
    partitionStatus.setReplicaStatuses(Collections.singleton(replicaStatus));

    // Set the PartitionStatus in the OfflinePushStatus
    offlinePushStatus.setPartitionStatus(partitionStatus);

    if (hasFatalDataValidationError) {
      // Assert that hasFatalDataValidationError returns true
      Assert.assertTrue(offlinePushStatus.hasFatalDataValidationError());
    } else {
      // Assert that hasFatalDataValidationError returns false
      Assert.assertFalse(offlinePushStatus.hasFatalDataValidationError());
    }
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
