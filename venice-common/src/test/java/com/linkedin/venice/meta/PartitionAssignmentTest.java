package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionAssignmentTest {
  private List<Instance> emptyList = Collections.emptyList();
  @Test
  public void testAddAndGetPartition() {
    int partitionCount = 3;

    PartitionAssignment partitionAssignment = new PartitionAssignment("test", partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      partitionAssignment.addPartition(new Partition(i, emptyList, emptyList, emptyList));
    }
    Assert.assertEquals(partitionAssignment.getAssignedNumberOfPartitions(), partitionCount,
        partitionCount + "Partitions have been added in to partition assignment object.");
    for (int i = 0; i < partitionCount; i++) {
      Assert.assertEquals(partitionAssignment.getPartition(i).getId(), i,
          "Partition:" + i + " has been added into partition assignment object.");
    }

    try {
      partitionAssignment.addPartition(new Partition(-1, emptyList, emptyList, emptyList));
      Assert.fail("-1 is not a valid partition id.");
    } catch (VeniceException e) {

    }

    try {
      partitionAssignment.addPartition(new Partition(partitionCount + 10, emptyList, emptyList, emptyList));
      Assert.fail(partitionCount + 10 + " is not a valid partition id.");
    } catch (VeniceException e) {

    }
  }
  @Test
  public void testInvalidAssignment(){
    try {
      new PartitionAssignment("test", 0);
      Assert.fail("Expected number of partitions should be larger than 0.");
    }catch(VeniceException e){

    }
  }

  @Test
  public void testRemovePartition() {
    PartitionAssignment partitionAssignment = new PartitionAssignment("test", 2);
    partitionAssignment.addPartition(new Partition(1, emptyList, emptyList, emptyList));
    partitionAssignment.removePartition(1);
    Assert.assertEquals(partitionAssignment.getAssignedNumberOfPartitions(), 0,
        "Partition 1 is deleted, there is no partition assigned.");
  }
}
