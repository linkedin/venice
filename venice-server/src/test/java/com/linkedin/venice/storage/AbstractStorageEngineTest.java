package com.linkedin.venice.storage;

import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;

import static com.linkedin.venice.ConfigKeys.*;


public abstract class AbstractStorageEngineTest extends AbstractStoreTest {

  protected AbstractStorageEngine testStoreEngine;
  protected int partitionId;

  public static VeniceProperties getServerProperties(PersistenceType type) {
    return getServerProperties(type, 1000);
  }

  public static VeniceProperties getServerProperties(PersistenceType persistenceType, long flushIntervalMs) {
    return new PropertyBuilder()
        .put(CLUSTER_NAME, "test_offset_manager")
        .put(ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, "true")
        .put(OFFSET_MANAGER_TYPE, "bdb")
        .put(OFFSET_MANAGER_FLUSH_INTERVAL_MS, flushIntervalMs)
        .put(HELIX_ENABLED, "false")
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(PERSISTENCE_TYPE, persistenceType.toString())
        .put(KAFKA_BROKERS, "localhost")
        .put(KAFKA_BROKER_PORT, "9092")
        .put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092")
        .put(KAFKA_AUTO_COMMIT_INTERVAL_MS, "1000")
        .put(LISTENER_PORT , 7072)
        .put(ADMIN_PORT , 7073)
        .put(DATA_BASE_PATH, "/tmp/data")
        .build();
  }

  // creates instance for testStoreEngine
  public abstract void createStorageEngineForTest()
      throws Exception;

  @Override
  public void createStoreForTest() {
    testStore = testStoreEngine;
  }

  public void doAddPartition(int partitionId)
      throws Exception {
    testStoreEngine.addStoragePartition(partitionId);
  }

  public void doRemovePartition(int partitionId)
      throws Exception {
    testStoreEngine.dropPartition(partitionId);
  }

  public void init()
      throws Exception {
    // create a  unique partitionId for this test which is outside number of partitions
    partitionId = RandomGenUtils.getRandomIntInRange(numOfPartitions+100, numOfPartitions + 500);

    //ensure it does not exist
    if (testStoreEngine.containsPartition(partitionId)) {
      try {
        testStoreEngine.dropPartition(partitionId);
      } catch (Exception e) {
        throw new Exception("Removing an existing partition failed");
      }
    }
  }

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

  public void testAddingAPartitionTwice()
      throws Exception {
    init();

    //add new storage partition
    doAddPartition(partitionId);
    Assert.assertEquals(testStoreEngine.containsPartition(partitionId), true,
        "Failed to add new partition: " + partitionId + "  to the storage engine!");

    // add it again
    try {
      doAddPartition(partitionId);
    } catch (StorageInitializationException e) {
      //this should be the expected behavior.
      return;
    } finally {
      //do clean up
      if (testStoreEngine.containsPartition(partitionId)) {
        doRemovePartition(partitionId);
      }
    }
    Assert.fail("Adding the same partition:" + partitionId + " again did not throw any exception as expected.");
  }

  public void testRemovingPartitionTwice()
      throws Exception {

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

  public void testOperationsOnNonExistingPartition()
      throws Exception {
    init();

    byte[] key = RandomGenUtils.getRandomBytes(keySize);
    byte[] value = RandomGenUtils.getRandomBytes(valueSize);

    //test put
    try {
      testStoreEngine.put(partitionId, key, value);
    } catch (PersistenceFailureException e) {
      //This is expected.
    }

    byte[] found = null;
    try {
      found = testStoreEngine.get(partitionId, key);
    } catch (PersistenceFailureException e) {
      //This is expected
    }

    Assert.assertEquals((found == null), true,
        "PUT and GET on key: " + key.toString() + " in invalid partition: " + partitionId + " succeeded");

    //test delete
    try {
      testStoreEngine.delete(partitionId, key);
    } catch (PersistenceFailureException e) {
      //This is expected
      return;
    }
    // If we reach here it means delete succeeded unfortunately.
    Assert.fail("DELETE on key: " + key.toString() + " in an invalid partition: " + partitionId + " succeeded");
  }
}
