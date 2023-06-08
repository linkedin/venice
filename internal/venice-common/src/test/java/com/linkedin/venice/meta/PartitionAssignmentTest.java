package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.EnumMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionAssignmentTest {
  @Test
  public void testAddAndGetPartition() {
    int partitionCount = 3;
    PartitionAssignment partitionAssignment = new PartitionAssignment("test", partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      partitionAssignment
          .addPartition(new Partition(i, new EnumMap<>(HelixState.class), new EnumMap<>(ExecutionStatus.class)));
    }
    Assert.assertEquals(
        partitionAssignment.getAssignedNumberOfPartitions(),
        partitionCount,
        partitionCount + "Partitions have been added in to partition assignment object.");
    for (int i = 0; i < partitionCount; i++) {
      Assert.assertEquals(
          partitionAssignment.getPartition(i).getId(),
          i,
          "Partition:" + i + " has been added into partition assignment object.");
    }

    try {
      partitionAssignment
          .addPartition(new Partition(-1, new EnumMap<>(HelixState.class), new EnumMap<>(ExecutionStatus.class)));
      Assert.fail("-1 is not a valid partition id.");
    } catch (VeniceException e) {

    }

    try {
      partitionAssignment.addPartition(
          new Partition(partitionCount + 10, new EnumMap<>(HelixState.class), new EnumMap<>(ExecutionStatus.class)));
      Assert.fail(partitionCount + 10 + " is not a valid partition id.");
    } catch (VeniceException e) {

    }
  }

  @Test
  public void testInvalidAssignment() {
    try {
      new PartitionAssignment("test", 0);
      Assert.fail("Expected number of partitions should be larger than 0.");
    } catch (VeniceException e) {

    }
  }
}
