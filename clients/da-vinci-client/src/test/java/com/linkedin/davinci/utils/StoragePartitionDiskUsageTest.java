package com.linkedin.davinci.utils;

import static org.mockito.Mockito.mock;

import com.linkedin.davinci.store.AbstractStorageEngine;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class StoragePartitionDiskUsageTest {
  private final static int partitionNum = 1;
  private final static int smallRecordSizeToBeAdded = 10;

  private AbstractStorageEngine storageEngine;
  private StoragePartitionDiskUsage partitionDiskUsage;

  @BeforeMethod
  public void setUp() {
    storageEngine = mock(AbstractStorageEngine.class);
    partitionDiskUsage = new StoragePartitionDiskUsage(partitionNum, storageEngine);
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
