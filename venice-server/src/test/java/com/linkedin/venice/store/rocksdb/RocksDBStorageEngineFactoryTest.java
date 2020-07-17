package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBStorageEngineFactoryTest {

  @Test
  public void testRocksDBCreation() {
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = TestUtils.getUniqueString("test_store");
    VeniceStoreConfig testStoreConfig = new VeniceStoreConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    AbstractStorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
    Assert.assertTrue(storeEngine instanceof RocksDBStorageEngine, "Database generated by"
        + " 'RocksDBStorageEngineFactory' must be 'RocksDBStorageEngine' instance");
  }

  @Test
  public void testGetPersistedStoreNames() {
    // Create two databases
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore1 = TestUtils.getUniqueString("test_store");
    VeniceStoreConfig testStoreConfig1 = new VeniceStoreConfig(testStore1, veniceServerProperties, PersistenceType.ROCKS_DB);
    factory.getStorageEngine(testStoreConfig1);
    final String testStore2 = TestUtils.getUniqueString("test_store");
    VeniceStoreConfig testStoreConfig2 = new VeniceStoreConfig(testStore2, veniceServerProperties, PersistenceType.ROCKS_DB);
    factory.getStorageEngine(testStoreConfig2);
    factory.close();
    // Retrieve all the persisted stores from disk
    Set<String> storeNameSet = factory.getPersistedStoreNames();
    Assert.assertEquals(storeNameSet.size(), 2);
    Assert.assertTrue(storeNameSet.contains(testStore1));
    Assert.assertTrue(storeNameSet.contains(testStore2));
  }

  @Test
  public void testRemoveStorageEngine() {
    // Create one databases
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);

    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = TestUtils.getUniqueString("test_store");
    VeniceStoreConfig testStoreConfig = new VeniceStoreConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    AbstractStorageEngine storageEngine = factory.getStorageEngine(testStoreConfig);
    // drop the database
    factory.removeStorageEngine(storageEngine);
    factory.close();
    // Retrieve all the persisted stores from disk
    Set<String> storeNameSet = factory.getPersistedStoreNames();
    Assert.assertEquals(storeNameSet.size(), 0);
  }

  @Test
  public void testAddNewPartitionAfterRemoving() {
    VeniceProperties veniceServerProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceServerProperties);
    RocksDBStorageEngineFactory factory = new RocksDBStorageEngineFactory(serverConfig);

    final String testStore = TestUtils.getUniqueString("test_store_");
    VeniceStoreConfig testStoreConfig = new VeniceStoreConfig(testStore, veniceServerProperties, PersistenceType.ROCKS_DB);
    AbstractStorageEngine storeEngine = factory.getStorageEngine(testStoreConfig);
    storeEngine.addStoragePartition(1);
    storeEngine.dropPartition(1);
    factory.removeStorageEngine(storeEngine);
    // This could happen when re-balance happens or the pre-created partitions get moved/removed.
    storeEngine = factory.getStorageEngine(testStoreConfig);
    storeEngine.addStoragePartition(1);

    factory.removeStorageEngine(storeEngine);
  }
}
