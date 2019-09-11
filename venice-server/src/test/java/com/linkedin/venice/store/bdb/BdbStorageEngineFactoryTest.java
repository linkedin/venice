package com.linkedin.venice.store.bdb;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
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
      AbstractStorageEngine storageEngine = bdbFactory.getStore(configLoader.getStoreConfig(storeName, PersistenceType.BDB));
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

  @Test
  public void testBdbEnvironmentConfigCloneCorrectness() {
    EnvironmentConfig environmentConfig1 = new EnvironmentConfig();

    // Check pre-conditions
    Assert.assertFalse(environmentConfig1.getAllowCreate(), "The default value for 'allow create' is expected to be false.");

    environmentConfig1.setAllowCreate(true);
    EnvironmentConfig environmentConfig2 = environmentConfig1.clone();

    Assert.assertEquals(environmentConfig1.getAllowCreate(), environmentConfig2.getAllowCreate(),
        "Pre-clone mutations should be reflected if cloning is implemented correctly.");

    environmentConfig1.setAllowCreate(false);

    Assert.assertNotEquals(environmentConfig1.getAllowCreate(), environmentConfig2.getAllowCreate(),
        "Post-clone mutations should not leak across instances if cloning is implemented correctly.");
  }

  @Test
  public void testBdbEnvironmentCloneCorrectness() {
    EnvironmentConfig environmentConfig1 = new EnvironmentConfig();

    // Check pre-conditions
    Assert.assertFalse(environmentConfig1.getAllowCreate(), "The default value for 'allow create' is expected to be false.");
    Assert.assertFalse(environmentConfig1.getReadOnly(), "The default value for 'read only' is expected to be false.");

    environmentConfig1.setReadOnly(true);
    environmentConfig1.setAllowCreate(true);
    File env1dir = new File(TestUtils.getUniqueTempPath());
    env1dir.deleteOnExit();
    env1dir.mkdir();
    Environment environment1 = new Environment(env1dir, environmentConfig1);

    environmentConfig1.setReadOnly(false);
    File env2dir = new File(TestUtils.getUniqueTempPath());
    env2dir.mkdir();
    env2dir.deleteOnExit();
    Environment environment2 = new Environment(env2dir, environmentConfig1);

    Assert.assertEquals(environment1.getConfig().getAllowCreate(), environment2.getConfig().getAllowCreate(),
        "Pre-clone mutations should be reflected if cloning is implemented correctly.");
    Assert.assertNotEquals(environment1.getConfig().getReadOnly(), environment2.getConfig().getReadOnly(),
        "Post-clone mutations should not leak across instances if cloning is implemented correctly.");
  }
}
