package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.AbstractMap;
import java.util.Map;
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


    String storeName = "bdb-add-and-delete";
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB);
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(storeName, serverProps);

    StorageService service = new StorageService(new VeniceServerConfig(serverProps));

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

      for (Thread t : threads) {
        long timeout = 10 * 1000;
        t.join(timeout);
        if (t.isAlive()) {
          throw new RuntimeException("Thread did not completed in millis" + timeout + " Name " + t.getName());
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

}