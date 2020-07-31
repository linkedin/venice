package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Created by athirupa on 6/3/16.
 */
public class StorageServiceTest {

  @Test
  public void testRestoreAllStores() throws Exception {
    Map<String, Integer> storePartitionMap = new HashMap<>();
    String store1 = "store1", store2 = "store2", store3 = "store3";
    storePartitionMap.put(store1, 2);
    storePartitionMap.put(store2, 3);
    storePartitionMap.put(store3, 4);
    // Create several stores first
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, 1000);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);
    StorageService storageService = new StorageService(configLoader, s -> s.toString(), mock(AggVersionedBdbStorageEngineStats.class),
        mock(AggVersionedStorageEngineStats.class), null);

    for (Map.Entry<String, Integer> entry : storePartitionMap.entrySet()) {
      String storeName = entry.getKey();
      VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName);
      int partitionNum = entry.getValue();
      for (int i = 0; i < partitionNum; ++i) {
        storageService.openStoreForNewPartition(storeConfig, i);
      }
    }
    storageService.start();
    storageService.stop();
    int bigPartitionId = 100;
    int existingPartitionId = 0;
    storageService = new StorageService(configLoader, s -> s.toString(), mock(AggVersionedBdbStorageEngineStats.class),
        mock(AggVersionedStorageEngineStats.class), null);
    for (Map.Entry<String, Integer> entry : storePartitionMap.entrySet()) {
      String storeName = entry.getKey();
      int partitionNum = entry.getValue();
      VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName);
      // This operation won't add any new partition
      Assert.assertEquals(storageService.openStoreForNewPartition(storeConfig, existingPartitionId).getPartitionIds().size(), partitionNum);
      // this operation will add a new partition
      AbstractStorageEngine storageEngine = storageService.openStoreForNewPartition(storeConfig, bigPartitionId);
      // Expected partition set should be (# of existing partitions) + bigPartition.
      Assert.assertEquals(storageEngine.getPartitionIds().size(), partitionNum + 1);
    }
    // Shutdown storage service
    storageService.removeStorageEngine(store1);
    storageService.removeStorageEngine(store2);
    storageService.removeStorageEngine(store3);
    Assert.assertTrue(storageService.getStorageEngineRepository().getAllLocalStorageEngines().isEmpty());
  }
}