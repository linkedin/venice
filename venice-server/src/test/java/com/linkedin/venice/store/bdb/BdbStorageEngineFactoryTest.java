package com.linkedin.venice.store.bdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class BdbStorageEngineFactoryTest {
  private Map<String, Integer> storePartitionMap;

  public BdbStorageEngineFactoryTest() {
    storePartitionMap = new HashMap<>();
    storePartitionMap.put("store1", 2);
    storePartitionMap.put("store2", 3);
    storePartitionMap.put("store3", 4);
  }

  private void restoreStoreListTest(boolean shared) {
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB, 1000);
    if (shared) {
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
      AbstractStorageEngine storageEngine = bdbFactory.getStore(configLoader.getStoreConfig(storeName));
      storageEngineList.add(storageEngine);
      for (int i = 0; i < partitionNum; ++i) {
        storageEngine.addStoragePartition(i);
      }
    }
    // Shutdown storage engine factory
    storageEngineList.stream().forEach(AbstractStorageEngine::close);
    bdbFactory.close();

    bdbFactory = new BdbStorageEngineFactory(serverConfig);
    Set<String> storeNames = bdbFactory.getPersistedStoreNames();
    Assert.assertEquals(storeNames.size(), storePartitionMap.size());
    Assert.assertTrue(storeNames.containsAll(storePartitionMap.keySet()));

    bdbFactory.close();
  }

  @Test
  public void restoreStoreListTestWithSharedEnv() {
    restoreStoreListTest(true);
  }

  @Test
  public void restoreStoreListTestWithNonSharedEnv() {
    restoreStoreListTest(false);
  }
}
