package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStorageEngineTest;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by athirupa on 6/3/16.
 */
public class StorageServiceTest {

  @Test
  public void testMultiThreadedBDB() throws Throwable {
    final int NUM_THREADS = 15;
    final int TOTAL_THREAD_JOIN_TIME = 30 * Time.MS_PER_SECOND;

    String storeName = "bdb-add-and-delete";
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProps);
    VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName);

    StorageService service = new StorageService(configLoader);

    BdbStorageEngineFactory factory = (BdbStorageEngineFactory) service.getInternalStorageEngineFactory(storeConfig);
    File directoryPath = factory.getStorePath(storeName);
    if(directoryPath.exists()) {
      FileUtils.deleteDirectory(directoryPath);
    }

    for(int times = 0; times < 3; times ++ ) {
      Assert.assertFalse(factory.getStorePath(storeName).exists(), "BDB should not exist when the test starts");
      Thread[] threads = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        BDBPerformOperation operation = new BDBPerformOperation(service, storeConfig, i);
        Thread thread = new Thread(operation, "thread" + i);
        threads[i] = thread;
      }

      for (Thread t : threads) {
        // Assertion raises error in other threads, if they are not
        // caught using exception handler, they are silently ignored.
        t.setUncaughtExceptionHandler(new UncaughtExceptionHandler());
        t.start();
      }

      long startTime = System.currentTimeMillis();
      for (Thread t : threads) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        long timeOut = Math.max(1, TOTAL_THREAD_JOIN_TIME - (elapsedTime));
        t.join(timeOut);
        if (t.isAlive()) {
          throw new RuntimeException("Thread did not complete in " + timeOut + " ms. Thread name: " + t.getName());
        }
      }

      if (!errors.isEmpty()) {
        Map.Entry<String, Throwable> entry = errors.poll();
        throw new RuntimeException("Error in BDB Worker thread" + entry.getKey(), entry.getValue());
      }

      Assert.assertFalse(factory.getStorePath(storeName).exists(), "BDB dir should be removed at the end");
    }
  }

  public static class BDBPerformOperation implements  Runnable {
    private final StorageService service;
    private final int partitionId;
    private final VeniceStoreConfig storeConfig;
    private final byte MAX_ENTRIES = 100;

    public BDBPerformOperation(StorageService service, VeniceStoreConfig storeConfig, int partitionId) {
      this.service = service;
      this.storeConfig = storeConfig;
      this.partitionId = partitionId;
    }

    @Override
    public void run() {
      //Write some values
      AbstractStorageEngine engine = service.openStoreForNewPartition(storeConfig, partitionId);


      String storeName = storeConfig.getStoreName();
      BdbStorageEngineFactory factory = (BdbStorageEngineFactory) service.getInternalStorageEngineFactory(storeConfig);
      Assert.assertTrue(factory.getStorePath(storeName).exists(), "BDB dir should exist");

      for(byte j = 0; j < MAX_ENTRIES; j ++) {
        engine.put(partitionId, new byte[]{j}, new byte[]{j});
      }

      // Read them back
      for( byte j = 0; j < MAX_ENTRIES ; j ++) {
        byte[] key = new byte[] {j};
        byte[] value = engine.get(partitionId , key);
        // Key and Value are the same as for put
        Assert.assertEquals(value, key, "bytes written is different from retrieved");
      }

      //Delete some at random
      for(byte j = 0 ; j < MAX_ENTRIES ; j += 2) {
        byte[] key = new byte[] {j};
        engine.delete(partitionId , key);

        byte[] value = engine.get(partitionId , key);
        Assert.assertNull(value, "Deleted value should return null on retrieval");
      }

      service.dropStorePartition(storeConfig, partitionId);
    }
  }

  Queue<Map.Entry<String,Throwable>> errors = new ConcurrentLinkedQueue<>();
  public class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      Map.Entry<String,Throwable> entry = new AbstractMap.SimpleEntry<>(t.getName(), e);
      errors.add(entry);
    }
  }

  @Test
  public void testRestoreAllStores() throws Exception {
    Map<String, Integer> storePartitionMap = new HashMap<>();
    storePartitionMap.put("store1", 2);
    storePartitionMap.put("store2", 3);
    storePartitionMap.put("store3", 4);
    // Create several stores first
    VeniceProperties serverProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB, 1000);
    VeniceConfigLoader configLoader = AbstractStorageEngineTest.getVeniceConfigLoader(serverProperties);
    StorageService storageService = new StorageService(configLoader);

    for (Map.Entry<String, Integer> entry : storePartitionMap.entrySet()) {
      String storeName = entry.getKey();
      VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName);
      int partitionNum = entry.getValue();
      for (int i = 0; i < partitionNum; ++i) {
        storageService.openStoreForNewPartition(storeConfig, i);
      }
    }
    // Shutdown storage service
    storageService.stop();

    int bigPartitionId = 100;
    int existingPartitionId = 0;
    storageService = new StorageService(configLoader);
    for (Map.Entry<String, Integer> entry : storePartitionMap.entrySet()) {
      String storeName = entry.getKey();
      int partitionNum = entry.getValue();
      VeniceStoreConfig storeConfig = configLoader.getStoreConfig(storeName);
      // This operation won't add any new partition
      storageService.openStoreForNewPartition(storeConfig, existingPartitionId);
      // this operation will add a new partition
      AbstractStorageEngine storageEngine = storageService.openStoreForNewPartition(storeConfig, bigPartitionId);
      Assert.assertEquals(storageEngine.getPartitionIds().size(), partitionNum + 1);
    }
    // Shutdown storage service
    storageService.stop();
  }
}