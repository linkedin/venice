package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;
import static org.mockito.ArgumentMatchers.*;


public class OfflinePushStatusTest {

  private String kafkaTopic = "testTopic";
  private int numberOfPartition = 3;
  private int replicationFactor = 2;
  private OfflinePushStrategy strategy = OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;
  private String incrementalPushVersion;
  private HelixCustomizedViewOfflinePushRepository customizedView;
  private OfflinePushStatus offlinePushStatus;
  private PartitionAssignment partitionAssignment;


  @Test
  public void testCreateOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    Assert.assertEquals(offlinePushStatus.getKafkaTopic(), kafkaTopic);
    Assert.assertEquals(offlinePushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(offlinePushStatus.getReplicationFactor(), replicationFactor);
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
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(offlinePushStatus.getPartitionStatus(1),
        ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      Assert.fail("Partition 1000 dose not exist.");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testIsReadyToStartBufferReplay() {
    // Make sure buffer replay can be started in the case where current replica status is PROGRESS but END_OF_PUSH_RECEIVED was already received
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, 1, replicationFactor, strategy);
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
    Assert.assertTrue(offlinePushStatus.isReadyToStartBufferReplay(false),
        "Buffer replay should be allowed to start since END_OF_PUSH_RECEIVED was already received");
  }

  @Test
  public void testSetPartitionStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(offlinePushStatus.getPartitionStatus(1),
        ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));
    List<PartitionStatus> partitionStatuses = new ArrayList<>();

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      Assert.fail("Partition 1000 dose not exist.");
    } catch (IllegalArgumentException e) {
      //expected
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
  public void testUpdateStatusFromEndOfPushReceived(){
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

  private PartitionAssignment getPartitionAssignment(String kafkaTopic, int numberOfPartition, int replicationFactor) {
    PartitionAssignment partitionAssignment = new PartitionAssignment(kafkaTopic, numberOfPartition);
    for (int i = 0; i < numberOfPartition; i++) {
      Map<String, List<Instance>> stateToInstancesMap = new HashMap<>();
      List<Instance> instancesList = new ArrayList<>(replicationFactor);
      for (int j = 0; j < replicationFactor; j++) {
        instancesList.add(new Instance(String.valueOf(j), "localhost", j));
      }
      stateToInstancesMap.put(HelixState.ONLINE_STATE, instancesList);
      partitionAssignment.addPartition(new Partition(i, stateToInstancesMap));
    }
    return partitionAssignment;
  }

  private Map<Integer, PartitionStatus> getPartitionStatuses(int numberOfPartition, int replicationFactor) {
    Map<Integer, PartitionStatus> partitionStatusMap = new HashMap<>();
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      PartitionStatus partitionStatus = new PartitionStatus(partitionId);
      List<ReplicaStatus> replicaStatusList = new ArrayList<>(replicationFactor);
      for (int j = 0; j < replicationFactor; j++) {
        replicaStatusList.add(new ReplicaStatus(Integer.toString(j)));
      }
      partitionStatus.setReplicaStatuses(replicaStatusList);
      partitionStatusMap.put(partitionId, partitionStatus);
    }
    return partitionStatusMap;
  }

  private void setupForCheckIncrementalPush() {
    this.numberOfPartition = 2;
    this.replicationFactor = 3;
    this.incrementalPushVersion = "1612393202889";
    this.customizedView = Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    this.offlinePushStatus = new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    this.offlinePushStatus.updateStatus(COMPLETED);
    this.partitionAssignment = getPartitionAssignment(kafkaTopic, numberOfPartition, replicationFactor);
  }

  @Test(description = "Expect EOIP status when numberOfReplicasInCompletedState == replicationFactor and all these replicas have seen EOIP")
  void testCheckIncrementalPushStatusWhenAllPartitionReplicasHaveSeenEoip() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(eq(kafkaTopic), anyInt())).thenReturn(replicationFactor);
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect EOIP status when for one partition numberOfReplicasInCompletedState == (replicationFactor - 1) and only one replica of that partition has not seen EOIP yet")
  void testCheckIncrementalPushStatusWhenAllCompletedStateReplicasOfPartitionHaveSeenEoip() {
    //setup
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        // skip EOIP status update for just one replica of the first partition
        if (partitionId == 0 && r == 0) {
          continue;
        }
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    //mock
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(kafkaTopic, 0)).thenReturn(replicationFactor - 1);
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(kafkaTopic, 1)).thenReturn(replicationFactor);
    //verify
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect EOIP status when for one partition numberOfReplicasInCompletedState == (replicationFactor - 2) and only one replica of that partition has not seen EOIP yet")
  void testCheckIncrementalPushStatusWhenNMinusOneReplicasOfPartitionHaveSeenEoip() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        // skip EOIP status update for just one replica of first partition
        if (partitionId == 0 && r == 0) {
          continue;
        }
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(kafkaTopic, 0)).thenReturn(replicationFactor - 2);
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(kafkaTopic, 1)).thenReturn(replicationFactor);
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect SOIP status when numberOfReplicasInCompletedState == replicationFactor and just one replica of one partition has not seen EOIP yet")
  void testCheckIncrementalPushStatusWhenOneCompletedStateReplicaOfPartitionHasNotSeenEoip() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        // skip EOIP status update for just one replica of the first partition
        if (partitionId == 0 && r == 0) {
          continue;
        }
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(eq(kafkaTopic), anyInt())).thenReturn(replicationFactor);
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test(description = "Expect SOIP status when for one partition numberOfReplicasInCompletedState == (replicationFactor - 2) and two replicas of that partition have not seen EOIP yet")
  void testCheckIncrementalPushStatusWhenNMinusTwoReplicasOfPartitionHaveSeenEoip() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        // skip EOIP status update for two replicas of first partition
        if (partitionId == 0 && r < 2) {
          continue;
        }
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(kafkaTopic, 0)).thenReturn(replicationFactor - 2);
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(kafkaTopic, 1)).thenReturn(replicationFactor);
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test
  public void testCheckIncrementalPushStatusWhenNumberOfReplicasWithEoipAreMoreThanReplicationFactor() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor + 1);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      // 4 replicas received EOIP
      for (int r = 0; r < replicationFactor + 1; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Mockito.when(customizedView.getNumberOfReplicasInCompletedState(eq(kafkaTopic), anyInt())).thenReturn(replicationFactor);
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Test (description = "Expect ERROR status when any one replica belonging to any partition has WARNING state")
  void testCheckIncrementalPushStatusWhenAnyOfTheReplicaHasErrorStatus() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        if (partitionId == 1 && r == 0) {
          partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), WARNING, incrementalPushVersion);
        } else {
          partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, incrementalPushVersion);
        }
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), ERROR);
  }

  @Test (description = "Expect NOT_CREATED status when there is no replica history available for a given incremental push version")
  void testCheckIncrementalPushStatusWhenReplicaHistoryIsEmptyForGivenIncrementalPushVersion() {
    setupForCheckIncrementalPush();
    Map<Integer, PartitionStatus> partitionStatusMap = getPartitionStatuses(numberOfPartition, replicationFactor);
    for (int partitionId = 0; partitionId < numberOfPartition; partitionId++) {
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), START_OF_INCREMENTAL_PUSH_RECEIVED, "diffIncPushThanCurrent");
        partitionStatusMap.get(partitionId).updateReplicaStatus(Integer.toString(r), END_OF_INCREMENTAL_PUSH_RECEIVED, "diffIncPushThanCurrent");
      }
    }
    offlinePushStatus.setPartitionStatuses(new ArrayList<>(partitionStatusMap.values()));
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), NOT_CREATED);
  }

  @Test (description = "Expect NOT_CREATED when there are no replicas in replicaHistory for a given incremental push version")
  void testCheckIncrementalPushStatusWhenReplicaHistoryIsEmpty() {
    int numberOfPartition = 2;
    int replicationFactor = 3;
    String incrementalPushVersion = "1612393202889";
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionAssignment partitionAssignment = getPartitionAssignment(kafkaTopic, numberOfPartition, replicationFactor);
    HelixCustomizedViewOfflinePushRepository customizedView = Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    Assert.assertEquals(offlinePushStatus.checkIncrementalPushStatus(incrementalPushVersion, partitionAssignment, customizedView), NOT_CREATED);
  }

  private void testValidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      OfflinePushStatus offlinePushStatus =
          new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
      offlinePushStatus.setCurrentStatus(from);
      offlinePushStatus.updateStatus(status);
      Assert.assertEquals(offlinePushStatus.getCurrentStatus(), status, status + " should be valid from:" + from);
    }
  }

  private void testInvalidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      OfflinePushStatus offlinePushStatus =
          new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
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
