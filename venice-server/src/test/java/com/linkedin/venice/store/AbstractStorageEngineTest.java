package com.linkedin.venice.store;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;


public abstract class AbstractStorageEngineTest extends AbstractStoreTest {

  protected AbstractStorageEngine testStoreEngine;
  protected int partitionId;

  public static VeniceProperties getServerProperties(PersistenceType type) {
    return getServerProperties(type, 1000);
  }

  public static VeniceProperties getServerProperties(PersistenceType persistenceType, long flushIntervalMs) {
    File dataDirectory = TestUtils.getTempDataDirectory();
    return new PropertyBuilder()
        .put(CLUSTER_NAME, "test_offset_manager")
        .put(ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, "true")
        .put(OFFSET_MANAGER_FLUSH_INTERVAL_MS, flushIntervalMs)
        .put(OFFSET_DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(PERSISTENCE_TYPE, persistenceType.toString())
        .put(KAFKA_BROKERS, "localhost")
        .put(KAFKA_BROKER_PORT, "9092")
        .put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092")
        .put(KAFKA_ZK_ADDRESS, "localhost:2181")
        .put(LISTENER_PORT , 7072)
        .put(ADMIN_PORT , 7073)
        .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .build();
  }

  public static VeniceConfigLoader getVeniceConfigLoader(VeniceProperties serverProperties) {
    return new VeniceConfigLoader(new VeniceProperties(new Properties()), serverProperties);
  }

  // creates instance for testStoreEngine
  public abstract void createStorageEngineForTest()
      throws Exception;

  public AbstractStorageEngine getTestStoreEngine() {
    return testStoreEngine;
  }

  @Override
  public void createStoreForTest() {
    testStore = testStoreEngine;
  }

  public void doAddPartition(int partitionId) {
    testStoreEngine.addStoragePartition(partitionId);
  }

  public void doRemovePartition(int partitionId) {
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
      Assert.fail("Removing a non-exist partition should not throw an exception.", e);
    }
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

  @Test (expectedExceptions = VeniceException.class)
  public void testAdjustStoragePartitionWithDifferentStoreName() {
    String storeName = TestUtils.getUniqueString("dummy_store_name");
    int partitionId = 1;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    testStoreEngine.adjustStoragePartition(partitionConfig);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testAdjustStoragePartitionWithUnknownPartitionId() {
    String storeName = testStoreEngine.getName();
    int unknownPartitionId = partitionId + 10000;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, unknownPartitionId);
    testStoreEngine.adjustStoragePartition(partitionConfig);
  }

  @Test
  public void testAdjustStoragePartitionFromTransactionalToDeferredWrite() {
    String storeName = testStoreEngine.getName();
    int newPartitionId = partitionId + 1;
    StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(storeName, newPartitionId);
    StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(storeName, newPartitionId);
    deferredWritePartitionConfig.setDeferredWrite(true);
    testStoreEngine.addStoragePartition(transactionalPartitionConfig);
    // Current partition should be transactional
    AbstractStoragePartition storagePartition = testStoreEngine.getPartitionOrThrow(newPartitionId);
    Assert.assertNotNull(storagePartition);
    Assert.assertTrue(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertFalse(storagePartition.verifyConfig(deferredWritePartitionConfig));

    testStoreEngine.adjustStoragePartition(deferredWritePartitionConfig);

    storagePartition = testStoreEngine.getPartitionOrThrow(newPartitionId);
    Assert.assertNotNull(storagePartition);
    Assert.assertFalse(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertTrue(storagePartition.verifyConfig(deferredWritePartitionConfig));

    testStoreEngine.dropPartition(newPartitionId);
  }

  @Test
  public void testAdjustStoragePartitionFromDeferredWriteToTransactional() {
    String storeName = testStoreEngine.getName();
    int newPartitionId = partitionId + 1;
    StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(storeName, newPartitionId);
    StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(storeName, newPartitionId);
    deferredWritePartitionConfig.setDeferredWrite(true);
    testStoreEngine.addStoragePartition(deferredWritePartitionConfig);
    // Current partition should be deferred-write
    AbstractStoragePartition storagePartition = testStoreEngine.getPartitionOrThrow(newPartitionId);
    Assert.assertNotNull(storagePartition);
    Assert.assertFalse(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertTrue(storagePartition.verifyConfig(deferredWritePartitionConfig));

    testStoreEngine.adjustStoragePartition(transactionalPartitionConfig);

    storagePartition = testStoreEngine.getPartitionOrThrow(newPartitionId);
    Assert.assertNotNull(storagePartition);
    Assert.assertTrue(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertFalse(storagePartition.verifyConfig(deferredWritePartitionConfig));

    testStoreEngine.dropPartition(newPartitionId);
  }
}
