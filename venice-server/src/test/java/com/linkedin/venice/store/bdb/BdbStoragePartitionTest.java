package com.linkedin.venice.store.bdb;

import com.linkedin.venice.store.bdb.BdbStoragePartition;
import com.linkedin.venice.store.exception.InvalidDatabaseNameException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BdbStoragePartitionTest {
  @Test
  public void testGetStoreNameFromPartitionNameWithValidPartitionName() {
    String partition1 = "store0-1";
    String partition2 = "store1-1-2";
    Assert.assertEquals(BdbStoragePartition.getStoreNameFromPartitionName(partition1), "store0");
    Assert.assertEquals(BdbStoragePartition.getStoreNameFromPartitionName(partition2), "store1-1");
  }

  @Test
  public void testGetStoreNameFromPartitionNameWithInvalidPartitionName() {
    String partition = "store0_1";
    try {
      BdbStoragePartition.getStoreNameFromPartitionName(partition);
      Assert.fail("InvalidDatabaseNameException should be thrown on invalid database name: " + partition);
    } catch (InvalidDatabaseNameException e) {
      // good here
    }
    partition = "-1";
    try {
      BdbStoragePartition.getStoreNameFromPartitionName(partition);
      Assert.fail("InvalidDatabaseNameException should be thrown on invalid database name: " + partition);
    } catch (InvalidDatabaseNameException e) {
      // good here
    }
  }

  @Test
  public void testGetPartitionIdFromPartitionNameWithValidPartitionName() {
    String partition1 = "store0-1";
    String partition2 = "store1-1-2";
    Assert.assertEquals(BdbStoragePartition.getPartitionIdFromPartitionName(partition1), 1);
    Assert.assertEquals(BdbStoragePartition.getPartitionIdFromPartitionName(partition2), 2);
  }

  @Test
  public void testGetPartitionIdFromPartitionNameWithInvalidPartitionName() {
    String partition = "store0_1";
    try {
      BdbStoragePartition.getPartitionIdFromPartitionName(partition);
      Assert.fail("InvalidDatabaseNameException should be thrown on invalid database name: " + partition);
    } catch (InvalidDatabaseNameException e) {
      // good here
    }
    partition = "store1-";
    try {
      BdbStoragePartition.getPartitionIdFromPartitionName(partition);
      Assert.fail("InvalidDatabaseNameException should be thrown on invalid database name: " + partition);
    } catch (InvalidDatabaseNameException e) {
      // good here
    }
  }
}
