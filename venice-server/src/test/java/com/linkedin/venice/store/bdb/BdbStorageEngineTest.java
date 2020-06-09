package com.linkedin.venice.store.bdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.RandomGenUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.*;


public class BdbStorageEngineTest extends AbstractStorageEngineTest {
  public static final int randomRecordNum = 150000;

  BdbStorageEngineFactory factory;

  public BdbStorageEngineTest() {

  }

  StorageService service;
  VeniceStoreConfig storeConfig;
  final String STORE_NAME = "storage-engine-test-bdb";
  final int PARTITION_ID = 0;

  @BeforeClass
  public void setup() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void tearDown() {
    if(service != null && storeConfig != null) {
      service.dropStorePartition(storeConfig , PARTITION_ID);
    }
  }

  @Override
  public void createStorageEngineForTest() {
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);
    service = new StorageService(configLoader, s -> s.toString(), mock(AggVersionedBdbStorageEngineStats.class),
        mock(AggVersionedStorageEngineStats.class), null);
    storeConfig = new VeniceStoreConfig(STORE_NAME, serverProperties);

    testStoreEngine = service.openStoreForNewPartition(storeConfig , PARTITION_ID);
    createStoreForTest();
  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  private void putRandomData(int recordsNum, int partitionId) {
    while (recordsNum-- >= 0) {
      byte[] key = RandomGenUtils.getRandomBytes(50);
      byte[] value = RandomGenUtils.getRandomBytes(500);
      try {
        doPut(partitionId, key, value);
      } catch (VeniceException e) {
        Assert.fail("Exception was thrown: " + e.getMessage(), e);
      }
    }
  }

  @Test
  public void testDelete() {
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
  }

  @Test
  public void testGetInvalidKeys() {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPutNullKey() {
    super.testPutNullKey();
  }

  @Test
  public void testPartitioning()
    throws Exception {
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice()
    throws Exception {
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice()
    throws Exception {
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition()
    throws Exception {
    super.testOperationsOnNonExistingPartition();
  }

  private void testRestoreStoragePartitions(boolean sharedEnv) {
    Map<String, Integer> storePartitionMap = new HashMap<>();
    storePartitionMap.put("store1", 2);
    storePartitionMap.put("store2", 3);
    storePartitionMap.put("store3", 4);
    // Create several stores first
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB, 1000);
    if (sharedEnv) {
      Properties newProperties = serverProperties.toProperties();
      newProperties.setProperty(BdbServerConfig.BDB_ONE_ENV_PER_STORE, "false");
      serverProperties = new VeniceProperties(newProperties);
    }
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);
    VeniceServerConfig serverConfig = configLoader.getVeniceServerConfig();
    BdbStorageEngineFactory bdbFactory = new BdbStorageEngineFactory(serverConfig);
    List<AbstractStorageEngine> storageEngineList = new LinkedList<>();
    for (Map.Entry<String, Integer> entry : storePartitionMap.entrySet()) {
      String storeName = entry.getKey();
      int partitionNum = entry.getValue();
      VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName, PersistenceType.BDB);
      AbstractStorageEngine storageEngine = bdbFactory.getStore(storeConfig);
      storageEngineList.add(storageEngine);
      for (int i = 0; i < partitionNum; ++i) {
        storageEngine.addStoragePartition(i);
      }
    }
    // Shutdown storage engine factory
    storageEngineList.stream().forEach(AbstractStorageEngine::close);
    storageEngineList.clear();
    bdbFactory.close();
    bdbFactory = new BdbStorageEngineFactory(serverConfig);
    for (Map.Entry<String, Integer> entry : storePartitionMap.entrySet()) {
      String storeName = entry.getKey();
      int partitionNum = entry.getValue();
      VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName, PersistenceType.BDB);
      AbstractStorageEngine storageEngine = bdbFactory.getStore(storeConfig);
      Assert.assertEquals(storageEngine.getPartitionIds().size(), partitionNum);
      storageEngineList.add(storageEngine);
    }

    storageEngineList.stream().forEach(AbstractStorageEngine::close);
    storageEngineList.clear();
    bdbFactory.close();
  }

  @Test
  public void testRestoreStoragePartitionsWithSharedEnv() {
    testRestoreStoragePartitions(true);
  }

  @Test
  public void testRestoreStoragePartitionsWithNonSharedEnv() {
    testRestoreStoragePartitions(false);
  }
}
