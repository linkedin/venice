package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.metadata.NodeCache;
import com.linkedin.venice.storage.VeniceMessageException;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.VeniceStorageManager;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the VeniceStorageManager class with In-Memory Storage
 * 1. Basic Put, Overwrite and Delete are covered
 * 2. Testing failure cases: Partition out of bounds, null message, null operation
 * 3. Testing that cache failure has no effect on correctness (cache can clear at any step)
 */
public class TestInMemoryManager {

  private static VeniceStorageManager storeManager;

  @BeforeClass
  public static void init() {

    GlobalConfiguration.initialize("");         // config file for testing

    // initialize the storage engine, start n nodes and p partitions.
    storeManager = VeniceStorageManager.getInstance();

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
    }

  }

  @Test
  public void testPutGetDelete() {

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());
    int partitionId = 0;

    try {

      // Test 1
      partitionId = kp.partition("k1", GlobalConfiguration.getNumKafkaPartitions());
      storeManager.storeValue(partitionId, "k1", new VeniceMessage(OperationType.PUT, "payload"));
      Assert.assertEquals("payload", storeManager.readValue("k1"));

      // Test 2
      partitionId = kp.partition("k1", GlobalConfiguration.getNumKafkaPartitions());
      storeManager.storeValue(partitionId, "k1", new VeniceMessage(OperationType.PUT, "payload2"));
      Assert.assertEquals("payload2", storeManager.readValue("k1"));

      // Test 3
      partitionId = kp.partition("k3", GlobalConfiguration.getNumKafkaPartitions());
      storeManager.storeValue(partitionId, "k3", new VeniceMessage(OperationType.PUT, "payload3"));
      Assert.assertEquals("payload3", storeManager.readValue("k3"));

      // Test 4 - delete the key
      storeManager.storeValue(partitionId, "k3", new VeniceMessage(OperationType.DELETE, ""));
      Assert.assertNull(storeManager.readValue("k3"));

    } catch (VeniceMessageException e) {

      Assert.fail(e.getMessage());

    } catch (VeniceStorageException e) {

      Assert.fail(e.getMessage());

    }

  }

  @Test
  public void testSuccessFails() {

    try {

      // fails to due partition out of bounds
      storeManager.storeValue(99, "key", new VeniceMessage(OperationType.PUT, "payload"));
      Assert.fail("Exception not thrown");

    } catch (VeniceMessageException e) {

      Assert.fail(e.getMessage());

    } catch (VeniceStorageException e) { } // Exception should be thrown

    try {

      // fails to due null message
      storeManager.storeValue(5, "key", null);
      Assert.fail("Exception not thrown");

    } catch (VeniceStorageException e) {

      Assert.fail(e.getMessage());

    } catch (VeniceMessageException e) { } // Exception should be thrown

    try {

      // fails to due null operation type
      storeManager.storeValue(5, "key", new VeniceMessage(null, "payload"));
      Assert.fail("Exception not thrown");

    } catch (VeniceStorageException e) {

      Assert.fail(e.getMessage());

    } catch (VeniceMessageException e) { } // Exception should be thrown

  }

  /* Tests cache clearing/failure at any step in the workflow */
  @Test
  public void testCacheFailure() {

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());
    NodeCache nc = NodeCache.getInstance();

    int partitionId = 0;

    try {

      // Test 1
      partitionId = kp.partition("k1", GlobalConfiguration.getNumKafkaPartitions());
      nc.clear();
      storeManager.storeValue(partitionId, "k1", new VeniceMessage(OperationType.PUT, "payload"));
      Assert.assertEquals("payload", storeManager.readValue("k1"));

      // Test 2
      partitionId = kp.partition("k2", GlobalConfiguration.getNumKafkaPartitions());
      storeManager.storeValue(partitionId, "k2", new VeniceMessage(OperationType.PUT, "payload"));
      nc.clear();
      Assert.assertEquals("payload", storeManager.readValue("k1"));

      // Test 3
      partitionId = kp.partition("k3", GlobalConfiguration.getNumKafkaPartitions());
      storeManager.storeValue(partitionId, "k3", new VeniceMessage(OperationType.PUT, "payload"));
      Assert.assertEquals("payload", storeManager.readValue("k3"));
      nc.clear();
      Assert.assertEquals("payload", storeManager.readValue("k3"));

    } catch (VeniceMessageException e) {

      Assert.fail(e.getMessage());

    } catch (VeniceStorageException e) {

      Assert.fail(e.getMessage());

    }


  }

}
