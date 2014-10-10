package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerException;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import com.linkedin.venice.metadata.NodeCache;
import com.linkedin.venice.storage.VeniceStorageManager;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.InMemoryStorageNode;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

/*
* NOTE: This class is currently Ignored, as VeniceStoreManager is being re-worked
* */

 /*
 * Tests the VeniceStorageManager class with In-Memory Storage
 * 1. Basic Put, Overwrite and Delete are covered
 * 2. Testing failure cases: Partition out of bounds, null message, null operation
 * 3. Testing that cache failure has no effect on correctness (cache can clear at any step)
 */

public class TestInMemoryManager {

  private static VeniceStorageManager storeManager;

  @BeforeClass
  public static void init() {

    try {

      GlobalConfiguration.initializeFromFile("./src/test/resources/test.properties");         // config file for testing
      KafkaConsumerPartitionManager.initialize("", Arrays.asList(""), 0);

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // initialize the storage engine, start n nodes and p partitions.
    storeManager = new VeniceStorageManager();

    try {

      // For testing, use 2 storage nodes
      for (int n = 0; n < GlobalConfiguration.getNumStorageNodes(); n++) {
        storeManager.registerNewNode(n);
      }

      // For testing, use 5 partitions
      for (int p = 0; p < GlobalConfiguration.getNumKafkaPartitions(); p++) {
        storeManager.registerNewPartition(p);
      }

    } catch (VeniceStorageException e) {
      Assert.fail(e.getMessage());
    } catch (VeniceKafkaConsumerException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test(enabled = false)
  public void testPutGetDelete() {

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());

    InMemoryStorageNode node;
    int partitionId = -1;
    int nodeId = -1;

    try {

      node = new InMemoryStorageNode(0);
      node.addStoragePartition(0);

      // Test 1
      partitionId = kp.partition("k1", GlobalConfiguration.getNumKafkaPartitions());
      node.put(partitionId, "k1", "payload");
      Assert.assertEquals("payload", storeManager.readValue("k1"));

      // Test 2
      partitionId = kp.partition("k1", GlobalConfiguration.getNumKafkaPartitions());
      node.put(partitionId, "k1", "payload2");
      Assert.assertEquals("payload2", storeManager.readValue("k1"));

      // Test 3
      partitionId = kp.partition("k3", GlobalConfiguration.getNumKafkaPartitions());
      node.put(partitionId, "k3", "payload3");
      Assert.assertEquals("payload3", storeManager.readValue("k3"));

      // Test 4 - delete the key
      node.put(partitionId, "k3", "");
      Assert.assertNull(storeManager.readValue("k3"));

    } catch (VeniceStorageException e) {

      Assert.fail(e.getMessage());

    }

  }

  @Test(enabled = false)
  public void testSuccessFails() {

    InMemoryStorageNode node = new InMemoryStorageNode(0);

    try {

      // fails to due partition out of bounds
      node.put(99, "key", "payload");
      Assert.fail("Exception not thrown");

    } catch (VeniceStorageException e) { } // Exception should be thrown

  }

  /* Tests cache clearing/failure at any step in the workflow */
  @Test(enabled = false)
  public void testCacheFailure() {

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());
    NodeCache nc = NodeCache.getInstance();

    int partitionId;

    try {

      InMemoryStorageNode node = new InMemoryStorageNode(0);
      node.addStoragePartition(0);

      // Test 1
      partitionId = kp.partition("k1", GlobalConfiguration.getNumKafkaPartitions());
      nc.clear();

      node.put(partitionId, "k1", "payload");
      Assert.assertEquals("payload", storeManager.readValue("k1"));

      // Test 2
      partitionId = kp.partition("k2", GlobalConfiguration.getNumKafkaPartitions());
      node.put(partitionId, "k2", "payload");
      nc.clear();
      Assert.assertEquals("payload", storeManager.readValue("k1"));

      // Test 3
      partitionId = kp.partition("k3", GlobalConfiguration.getNumKafkaPartitions());
      node.put(partitionId, "k3", "payload");
      Assert.assertEquals("payload", storeManager.readValue("k3"));
      nc.clear();
      Assert.assertEquals("payload", storeManager.readValue("k3"));

    }  catch (VeniceStorageException e) {

      Assert.fail(e.getMessage());

    }


  }

}
