package com.linkedin.davinci.utils;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class StoragePartitionDiskUsageTest {
  private final static int smallRecordSizeToBeAdded = 10;

  private StoragePartitionDiskUsage partitionDiskUsage;

  @BeforeMethod
  public void setUp() {
    partitionDiskUsage = new StoragePartitionDiskUsage(() -> 0);
  }

  @Test
  public void testAddAndGetPartitionUsage() {
    partitionDiskUsage.add(smallRecordSizeToBeAdded);
    Assert.assertEquals(smallRecordSizeToBeAdded, partitionDiskUsage.getUsage());
    // negative record size shouldn't be appended to partition diskUsage
    partitionDiskUsage.add(-smallRecordSizeToBeAdded);
    Assert.assertEquals(smallRecordSizeToBeAdded, partitionDiskUsage.getUsage());
  }
}
