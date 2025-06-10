package com.linkedin.davinci.store;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_RMD_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.ADMIN_PORT;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Properties;
import org.apache.commons.codec.binary.Hex;
import org.testng.Assert;
import org.testng.annotations.Test;


public abstract class AbstractStorageEngineTest<ASE extends AbstractStorageEngine> extends AbstractStoreTest {
  protected ASE testStoreEngine;
  protected int partitionId;

  public static VeniceProperties getServerProperties(PersistenceType type) {
    return getServerProperties(type, new Properties());
  }

  public static VeniceProperties getServerProperties(PersistenceType persistenceType, Properties properties) {
    File dataDirectory = Utils.getTempDataDirectory();
    return new PropertyBuilder().put(CLUSTER_NAME, "test_offset_manager")
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(PERSISTENCE_TYPE, persistenceType.toString())
        .put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092")
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(ROCKSDB_RMD_BLOCK_CACHE_SIZE_IN_BYTES, 1 * 1024 * 1024L)
        .put(LISTENER_PORT, 7072)
        .put(ADMIN_PORT, 7073)
        .put(DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .put(properties)
        .build();
  }

  public static VeniceConfigLoader getVeniceConfigLoader(VeniceProperties serverProperties) {
    return new VeniceConfigLoader(VeniceProperties.empty(), serverProperties);
  }

  // creates instance for testStoreEngine
  public abstract void createStorageEngineForTest() throws Exception;

  public StorageEngine getTestStoreEngine() {
    return testStoreEngine;
  }

  @Override
  public void createStoreForTest() {
    testStore = testStoreEngine;
  }

  public void doAddPartition(int partitionId) {
    testStoreEngine.addStoragePartitionIfAbsent(partitionId);
  }

  public void doRemovePartition(int partitionId) {
    testStoreEngine.dropPartition(partitionId);
  }

  public void init() throws Exception {
    // create a unique partitionId for this test which is outside number of partitions
    partitionId = RandomGenUtils.getRandomIntInRange(numOfPartitions + 100, numOfPartitions + 500);

    // ensure it does not exist
    if (testStoreEngine.containsPartition(partitionId)) {
      try {
        testStoreEngine.dropPartition(partitionId);
      } catch (Exception e) {
        throw new Exception("Removing an existing partition failed");
      }
    }
  }

  public void testPartitioning() throws Exception {
    init();

    // add new storage partition
    doAddPartition(partitionId);
    Assert.assertEquals(
        testStoreEngine.containsPartition(partitionId),
        true,
        "Failed to add new partition: " + partitionId + "  to the storage engine!");

    // remove existing partition
    doRemovePartition(partitionId);
    Assert.assertEquals(
        testStoreEngine.containsPartition(partitionId),
        false,
        "Failed to remove partition: " + partitionId + " from the storage engine!");
  }

  public void testAddingAPartitionTwice() throws Exception {
    init();

    // add new storage partition
    doAddPartition(partitionId);
    Assert.assertEquals(
        testStoreEngine.containsPartition(partitionId),
        true,
        "Failed to add new partition: " + partitionId + "  to the storage engine!");

    // add it again
    try {
      doAddPartition(partitionId);
    } finally {
      // do clean up
      if (testStoreEngine.containsPartition(partitionId)) {
        doRemovePartition(partitionId);
      }
    }
  }

  public void testRemovingPartitionTwice() throws Exception {

    init();

    // first add partition
    doAddPartition(partitionId);

    if (!testStoreEngine.containsPartition(partitionId)) {
      Assert.fail("Adding a new partition: " + partitionId + "failed!");
    }

    // remove existing partition
    doRemovePartition(partitionId);
    Assert.assertEquals(
        testStoreEngine.containsPartition(partitionId),
        false,
        "Failed to remove partition: " + partitionId + " from the storage engine!");

    // remove it again
    try {
      doRemovePartition(partitionId);
    } catch (Exception e) {
      Assert.fail("Removing a non-exist partition should not throw an exception.", e);
    }
  }

  public void testOperationsOnNonExistingPartition() throws Exception {
    init();

    byte[] key = RandomGenUtils.getRandomBytes(keySize);
    byte[] value = RandomGenUtils.getRandomBytes(valueSize);

    // test put
    try {
      testStoreEngine.put(partitionId, key, value);
    } catch (VeniceException e) {
      // This is expected.
    }

    byte[] found = null;
    try {
      found = testStoreEngine.get(partitionId, key);
    } catch (VeniceException e) {
      // This is expected
    }

    Assert.assertEquals(
        (found == null),
        true,
        "PUT and GET on key: " + Hex.encodeHexString(key) + " in invalid partition: " + partitionId + " succeeded");

    Assert.assertThrows(
        VeniceException.class,
        () -> testStoreEngine.getByKeyPrefix(partitionId, key, new BytesStreamingCallback() {
          @Override
          public void onRecordReceived(byte[] key, byte[] value) {
            Assert.fail(
                "GetByKeyPrefix on key: " + Hex.encodeHexString(key) + " in invalid partition: " + partitionId
                    + " succeeded when it should have failed.");
          }

          @Override
          public void onCompletion() {
            Assert.fail(
                "GetByKeyPrefix on key: " + Hex.encodeHexString(key) + " in invalid partition: " + partitionId
                    + " succeeded when it should have failed.");
          }
        }));

    // test delete
    try {
      testStoreEngine.delete(partitionId, key);
    } catch (VeniceException e) {
      // This is expected
      return;
    }
    // If we reach here it means delete succeeded unfortunately.
    Assert
        .fail("DELETE on key: " + Hex.encodeHexString(key) + " in an invalid partition: " + partitionId + " succeeded");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testAdjustStoragePartitionWithDifferentStoreName() {
    String storeName = Utils.getUniqueString("dummy_store_name");
    int partitionId = 1;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, partitionId);
    testStoreEngine
        .adjustStoragePartition(partitionId, StoragePartitionAdjustmentTrigger.BEGIN_BATCH_PUSH, partitionConfig);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testAdjustStoragePartitionWithUnknownPartitionId() {
    String storeName = testStoreEngine.getStoreVersionName();
    int unknownPartitionId = partitionId + 10000;
    StoragePartitionConfig partitionConfig = new StoragePartitionConfig(storeName, unknownPartitionId);
    testStoreEngine.adjustStoragePartition(
        unknownPartitionId,
        StoragePartitionAdjustmentTrigger.BEGIN_BATCH_PUSH,
        partitionConfig);
  }

  @Test
  public void testAdjustStoragePartitionFromTransactionalToDeferredWrite() {
    String storeName = testStoreEngine.getStoreVersionName();
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

    testStoreEngine.adjustStoragePartition(
        newPartitionId,
        StoragePartitionAdjustmentTrigger.BEGIN_BATCH_PUSH,
        deferredWritePartitionConfig);

    storagePartition = testStoreEngine.getPartitionOrThrow(newPartitionId);
    Assert.assertNotNull(storagePartition);
    Assert.assertFalse(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertTrue(storagePartition.verifyConfig(deferredWritePartitionConfig));

    testStoreEngine.dropPartition(newPartitionId);
  }

  @Test
  public void testAdjustStoragePartitionFromDeferredWriteToTransactional() {
    String storeName = testStoreEngine.getStoreVersionName();
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

    testStoreEngine.adjustStoragePartition(
        newPartitionId,
        StoragePartitionAdjustmentTrigger.END_BATCH_PUSH,
        transactionalPartitionConfig);

    storagePartition = testStoreEngine.getPartitionOrThrow(newPartitionId);
    Assert.assertNotNull(storagePartition);
    Assert.assertTrue(storagePartition.verifyConfig(transactionalPartitionConfig));
    Assert.assertFalse(storagePartition.verifyConfig(deferredWritePartitionConfig));

    testStoreEngine.dropPartition(newPartitionId);
  }

  @Test
  public void testIsMetadataPartition() {
    Assert.assertTrue(StorageService.isMetadataPartition(AbstractStorageEngine.METADATA_PARTITION_ID));
    Assert.assertFalse(StorageService.isMetadataPartition(AbstractStorageEngine.METADATA_PARTITION_ID + 1));
  }
}
