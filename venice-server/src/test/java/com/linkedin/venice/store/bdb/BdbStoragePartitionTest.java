package com.linkedin.venice.store.bdb;

import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.store.exception.InvalidDatabaseNameException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.sleepycat.je.Environment;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

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

  @Test
  public void testVerifyConfig() {
    String storeName = TestUtils.getUniqueString("test_v1");
    int partitionId = 1;
    StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(storeName, partitionId);
    StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(storeName, partitionId);
    deferredWritePartitionConfig.setDeferredWrite(true);

    // Open database in transactional mode
    Environment env = mock(Environment.class);
    BdbServerConfig serverConfig = new BdbServerConfig(new VeniceProperties(new Properties()));
    BdbStoragePartition storagePartition = new BdbStoragePartition(transactionalPartitionConfig, env, serverConfig);
    Assert.assertTrue(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertFalse(storagePartition.verifyConfig(deferredWritePartitionConfig));

    // Open database in deferred-write mode
     storagePartition = new BdbStoragePartition(deferredWritePartitionConfig, env, serverConfig);
    Assert.assertFalse(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertTrue(storagePartition.verifyConfig(deferredWritePartitionConfig));
  }
}
