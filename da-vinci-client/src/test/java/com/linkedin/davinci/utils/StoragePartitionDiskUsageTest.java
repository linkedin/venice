package com.linkedin.davinci.utils;

import com.linkedin.davinci.utils.StoragePartitionDiskUsage;
import com.linkedin.davinci.store.AbstractStorageEngine;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@Test
public class StoragePartitionDiskUsageTest {
  private final int partitionNum = 1;
  private final int smallRecordSizeToBeAdded = 10;
  private final long persistedPartitionSize = 100l;
  /**
   * This is one byte larger than size trigger in {@link StoragePartitionDiskUsage}
   * so it will trigger syncing up with db
   */
  private final int largeRecordSizeToBeAdded = 32 * 1024 * 1024 + 1;
  /**
   * This time lag is longer than time trigger in {@link StoragePartitionDiskUsage} to trigger syncing up with db
   */
  private final long timeLagToTriggerSyncUp = 10;
  private AbstractStorageEngine storageEngine;
  private StoragePartitionDiskUsage partitionDiskUsage;

  @BeforeMethod
  public void setUp() {
    storageEngine = mock(AbstractStorageEngine.class);
    partitionDiskUsage = new StoragePartitionDiskUsage(partitionNum, storageEngine);
  }

  @Test
  public void testAddAndGetPartitionUsage() {
    boolean added = partitionDiskUsage.add(smallRecordSizeToBeAdded );
    Assert.assertTrue(added);
    Assert.assertEquals(smallRecordSizeToBeAdded, partitionDiskUsage.getUsage());
    // negative record size shouldn't be appended to partition diskUsage
    added = partitionDiskUsage.add(-smallRecordSizeToBeAdded);
    Assert.assertFalse(added);
    Assert.assertEquals(smallRecordSizeToBeAdded, partitionDiskUsage.getUsage());
  }

  @Test
  public void testTimeTriggerSyncUp() {
    when(storageEngine.getPartitionSizeInBytes(partitionNum)).thenReturn(persistedPartitionSize);
    long currentTs = System.currentTimeMillis();
    // this should set the prev sync up time before threshold for sync up
    this.partitionDiskUsage.setPrevSyncUpTs(currentTs - TimeUnit.MINUTES.toMillis(timeLagToTriggerSyncUp));

    Assert.assertEquals(persistedPartitionSize, this.partitionDiskUsage.getUsage());
    Assert.assertEquals(0, partitionDiskUsage.getInMemoryOnlyPartitionUsage());
    Assert.assertEquals(persistedPartitionSize, partitionDiskUsage.getPersistedOnlyPartitionUsage());
  }

  @Test
  public void testDiskSizeTriggerSyncUp() {
    // this size should trigger syncing with db
    partitionDiskUsage.add(largeRecordSizeToBeAdded);
    when(storageEngine.getPartitionSizeInBytes(partitionNum)).thenReturn(persistedPartitionSize);

    Assert.assertEquals(persistedPartitionSize, partitionDiskUsage.getUsage());
    Assert.assertEquals(0, partitionDiskUsage.getInMemoryOnlyPartitionUsage());
    Assert.assertEquals(persistedPartitionSize, partitionDiskUsage.getPersistedOnlyPartitionUsage());
  }
}
