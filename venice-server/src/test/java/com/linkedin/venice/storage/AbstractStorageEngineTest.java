package com.linkedin.venice.storage;

import com.linkedin.venice.Common.TestUtils;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.Store;
import org.testng.Assert;
import org.testng.annotations.Test;


public abstract class AbstractStorageEngineTest extends AbstractStoreTest {

  protected AbstractStorageEngine testStoreEngine;
  protected int partitionId;

  // creates instance for testStoreEngine
  public abstract void createStorageEngineForTest();

  @Override
  public void createStoreForTest() {
    testStore = (Store) testStoreEngine;
  }

  public void doAddPartition(int partitionId) {
    testStoreEngine.addStoragePartition(partitionId);
  }

  public void doRemovePartition(int partitionId) {
    testStoreEngine.removePartition(partitionId);
  }

  public void init()
      throws Exception {
    // create a  unique partitionId for this test which is outside number of partitions
    partitionId = TestUtils.getRandomIntInRange(numOfPartitions, numOfPartitions + 10);

    //ensure it does not exist
    if (testStoreEngine.containsPartition(partitionId)) {
      try {
        testStoreEngine.removePartition(partitionId);
      } catch (Exception e) {
        throw new Exception("Removing an existing partition failed");
      }
    }
  }

  @Test
  public void testPartitioning()
      throws Exception {
    init();

    //add new storage partition
    doAddPartition(partitionId);
    Assert.assertEquals(testStoreEngine.containsPartition(partitionId), true,
        "Failed to add new partition: " + partitionId + "  to the storage engine!");

    // remove existing partition
    doRemovePartition(partitionId);
    Assert.assertEquals(testStoreEngine.containsPartition(partitionId), false,
        "Failed to remove partition: " + partitionId + " from the storage engine!");
  }

  @Test
  public void testAddingAPartitionTwice()  throws Exception {
    init();

    //add new storage partition
    doAddPartition(partitionId);
    Assert.assertEquals(testStoreEngine.containsPartition(partitionId), true,
        "Failed to add new partition: " + partitionId + "  to the storage engine!");

    // add it again
    try {
      doAddPartition(partitionId);
    } catch (Exception e) {
      //TODO this should be the expected behavior. Please add the appropriate exception in catch phrase after exception
      // handling is designed. Till then this test does not have any value.
      return;
    } finally {
      //do clean up
      if (testStoreEngine.containsPartition(partitionId)) {
        doRemovePartition(partitionId);
      }
    }
    Assert.fail("Adding the same partition:" + partitionId + " again did not throw any exception as expected.");
  }

  @Test
  public void testRemovingPartitionTwice()  throws Exception{

    init();

    //first add partition
    doAddPartition(partitionId);

    if (!testStoreEngine.containsPartition(partitionId)) {
      Assert.fail("Adding a new partition: " + partitionId + "failed!");
    }

    // remove existign partition
    doRemovePartition(partitionId);
    Assert.assertEquals(testStoreEngine.containsPartition(partitionId), false,
        "Failed to remove partition: " + partitionId + " from the storage engine!");

    //remove it again
    try {
      doRemovePartition(partitionId);
    } catch (Exception e) {
      //TODO this should be the expected behavior. Please add the appropriate exception in catch phrase after exception
      // handling is designed. Till then this test does not have any value.
      return;
    }
    Assert.fail("Removing the same partition:" + partitionId + " again did not throw any exception as expected.");
  }

  @Test
  public void testOperationsOnNonExistingPartition() throws Exception {
    init();

    byte[] key = TestUtils.getRandomBytes(keySize);
    byte[] value = TestUtils.getRandomBytes(valueSize);

    //test put
    try {
      testStoreEngine.put(partitionId, key, value);
    } catch (VeniceStorageException e) {
      //This is expected.
      //TODO recheck Exception type once exception handling is in place.
    }

    byte[] found = null;
    try {
      found = testStoreEngine.get(partitionId, key);
    } catch (VeniceStorageException e) {
      //This is expected
      //TODO recheck Exception type once exception handling is in place.
    }

    Assert.assertEquals((found == null), true,
        "PUT and GET on key: " + key.toString() + " in invalid partition: " + partitionId + " succeeded");

    //test delete
    try {
      testStoreEngine.delete(partitionId, key);
    } catch (VeniceStorageException e) {
      //This is expected
      //TODO recheck Exception type once exception handling is in place.
      return;
    }
    // If we reach here it means delete succeeded unfortunately.
    Assert.fail("DELETE on key: " + key.toString() + " in an invalid partition: " + partitionId + " succeeded");
  }
}
