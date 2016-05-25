package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BdbStorageEngineTest extends AbstractStorageEngineTest {

  BdbStorageEngineFactory factory;

  public BdbStorageEngineTest()
    throws Exception {
    createStorageEngineForTest();
  }

  private void initializeBDBFactory(VeniceStoreConfig storeConfig, int totalPartitions) {
    PartitionAssignmentRepository partitionAssignmentRepository;

    partitionAssignmentRepository = new PartitionAssignmentRepository();
    for(int i = 0; i < totalPartitions ; i ++) {
      partitionAssignmentRepository.addPartition(storeConfig.getStoreName(), i);
    }
    factory = new BdbStorageEngineFactory(storeConfig, partitionAssignmentRepository);
  }

  @Override
  public void createStorageEngineForTest()
    throws Exception {
    String storeName = "testng-bdb";
    VeniceProperties storeProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB);
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(storeName, storeProps);

    initializeBDBFactory(storeConfig, 1);
    testStoreEngine = factory.getStore(storeConfig);

    createStoreForTest();
  }

  public static class BDBPerformOperation implements  Runnable {
    private final AbstractStorageEngine engine;
    private final int partitionId;
    private final byte MAX_ENTRIES = 100;

    public BDBPerformOperation(AbstractStorageEngine engine, int partitionId) {
      this.engine = engine;
      this.partitionId = partitionId;
    }

    @Override
    public void run() {
      //Write some values
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

      engine.dropPartition(partitionId);
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
  public void testMultiThreadedBDB() throws Throwable {
    final int NUM_THREADS = 15;


    String storeName = "bdb-add-and-delete";
    VeniceProperties storeProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB);
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(storeName, storeProps);

    initializeBDBFactory(storeConfig, NUM_THREADS);
    final AbstractStorageEngine engine = factory.getStore(storeConfig);

    Assert.assertTrue(factory.getStorePath(storeName).exists(), "BDB dir should exist");

    Thread[] threads = new Thread[NUM_THREADS];
    for(int i = 0; i < NUM_THREADS; i ++) {
      BDBPerformOperation operation = new BDBPerformOperation(engine, i);
      Thread thread = new Thread(operation , "thread" + i);
      threads[i] = thread;
    }

    for(Thread t: threads) {
      // Assertion raises error in other threads, if they are not
      // caught using exception handler, they are silently ignored.
      t.setUncaughtExceptionHandler( new UncaughtExceptionHandler());
      t.start();
    }

    for(Thread t: threads) {
      long timeout = 10 * 1000;
      t.join(timeout);
      if(t.isAlive()) {
        throw new RuntimeException("Thread did not completed in millis"+ timeout + " Name " + t.getName());
      }
    }

    if(!errors.isEmpty()) {
      Map.Entry<String,Throwable> entry = errors.poll();
      throw new RuntimeException("Error in BDB Worker thread" + entry.getKey() , entry.getValue());
    }

    Assert.assertFalse(factory.getStorePath(storeName).exists(), "BDB dir should be removed");
  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
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
}
