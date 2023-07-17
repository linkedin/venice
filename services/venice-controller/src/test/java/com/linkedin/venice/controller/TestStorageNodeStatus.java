package com.linkedin.venice.controller;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.utils.HelixUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStorageNodeStatus {
  @Test
  public void testBuildStorageNodeStatus() {
    String resourceName = "testBuildStorageNodeStatus";
    int partitionCount = 3;
    HelixState[] statusArray = new HelixState[] { HelixState.STANDBY, HelixState.LEADER, HelixState.OFFLINE };
    StorageNodeStatus status = new StorageNodeStatus();
    for (int i = 0; i < partitionCount; i++) {
      status.addStatusForReplica(HelixUtils.getPartitionName(resourceName, i), statusArray[i]);
    }
    Assert.assertEquals(
        status.getStatusValueForReplica(HelixUtils.getPartitionName(resourceName, 0)),
        HelixState.STANDBY.getStateValue());
    Assert.assertEquals(
        status.getStatusValueForReplica(HelixUtils.getPartitionName(resourceName, 1)),
        HelixState.LEADER.getStateValue());
    Assert.assertEquals(
        status.getStatusValueForReplica(HelixUtils.getPartitionName(resourceName, 2)),
        HelixState.OFFLINE.getStateValue());
  }

  @Test
  public void testIsNewerOrEqual() {
    String resourceName1 = "testIsNewerOrEqual1";
    String resourceName2 = "testIsNewerOrEqual2";
    // Build a status with 2 resources.
    StorageNodeStatus oldStatus = new StorageNodeStatus();
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 1), HelixState.STANDBY);
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 3), HelixState.LEADER);
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName2, 7), HelixState.STANDBY);

    Assert.assertTrue(oldStatus.isNewerOrEqual(oldStatus), "Status should be equal to itself.");
    StorageNodeStatus newStatus = new StorageNodeStatus();
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 1), HelixState.STANDBY);
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 3), HelixState.LEADER);
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName2, 7), HelixState.LEADER);
    Assert
        .assertTrue(newStatus.isNewerOrEqual(oldStatus), "new server is newer because resource2 partition7 is ONLINE.");

    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 1), HelixState.OFFLINE);
    Assert.assertFalse(
        newStatus.isNewerOrEqual(oldStatus),
        "new server is not newer because resource1 partition1 is OFFLINE.");
  }

  @Test
  public void testIsNewerOrEqualWithPartitionMovement() {
    String resourceName1 = "testIsNewerOrEqualWithPartitionMovement1";
    String resourceName2 = "testIsNewerOrEqualWithPartitionMovement2";
    // Build a status with 2 resources.
    StorageNodeStatus oldStatus = new StorageNodeStatus();
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 1), HelixState.STANDBY);
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 3), HelixState.LEADER);
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName2, 4), HelixState.STANDBY);
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName2, 7), HelixState.STANDBY);
    oldStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName2, 7), HelixState.STANDBY);

    // Build a status that some of partitions were moved out.
    StorageNodeStatus newStatus = new StorageNodeStatus();
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 1), HelixState.STANDBY);
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName2, 7), HelixState.LEADER);
    Assert.assertTrue(
        newStatus.isNewerOrEqual(oldStatus),
        "new server is equal to old one. Because the status partitions stay in the server are same.");

    // new partition was moved in.
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 2), HelixState.OFFLINE);
    Assert.assertTrue(
        newStatus.isNewerOrEqual(oldStatus),
        "new server is equal to old one. Because the status partitions stay in the server are same.");
    // New resource was assigned.
    String resourceName3 = "testIsNewerOrEqualWithPartitionMovement3";
    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName3, 0), HelixState.OFFLINE);
    Assert.assertTrue(
        newStatus.isNewerOrEqual(oldStatus),
        "new server is equal to old one. Because the status partitions stay in the server are same.");

    newStatus.addStatusForReplica(HelixUtils.getPartitionName(resourceName1, 1), HelixState.OFFLINE);
    Assert.assertFalse(
        newStatus.isNewerOrEqual(oldStatus),
        "new server is not newer because resource1 partition1 is OFFLINE.");
  }
}
