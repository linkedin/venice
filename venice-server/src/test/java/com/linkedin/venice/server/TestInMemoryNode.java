package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerException;
import com.linkedin.venice.storage.InMemoryStorageNode;

import com.linkedin.venice.storage.InMemoryStoragePartition;
import com.linkedin.venice.storage.VeniceStorageException;
import junit.framework.Assert;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Tests the operations of the InMemoryStorageNode class
 * 1. Constructor assigns parameters successfully
 * 2. Put, Get, Delete and Overwriting Put on Node
 * 3. null is returned on non-existent key
 * 4. Partition addition and removal
 * 5. Partition failure operations (re-adding, removing twice)
 * 6. Operating on a non-existent partition returns null
 */
public class TestInMemoryNode {

  @BeforeClass
  public static void initConfig() {

    try {
      GlobalConfiguration.initializeFromFile("./src/test/resources/test.properties");         // config file for testing
      KafkaConsumerPartitionManager.initialize("", Arrays.asList(""), 0);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testNodeConstructor() {

    InMemoryStorageNode testNode = new InMemoryStorageNode(1);
    Assert.assertEquals(1, testNode.getNodeId());

    testNode = new InMemoryStorageNode(5);
    Assert.assertEquals(5, testNode.getNodeId());

  }

  @Test
  public void testNodeOperations() {

    InMemoryStorageNode testNode = new InMemoryStorageNode(1);

    try {

      testNode.addPartition(1);

      // test basic put and get
      testNode.put(1, "key", "value");
      testNode.put(1, "key2", "value2");
      Assert.assertEquals("value", testNode.get(1, "key"));
      Assert.assertEquals("value2", testNode.get(1, "key2"));

      // test overwrite
      testNode.put(1, "key2", "value3");
      Assert.assertEquals("value3", testNode.get(1, "key2"));

      // test delete
      testNode.delete(1, "key");
      Assert.assertNull(testNode.get(1, "key"));
      Assert.assertEquals("value3", testNode.get(1, "key2"));

      // test null key
      Assert.assertNull(testNode.get(1, "null_key"));

    } catch (VeniceStorageException e) {
      Assert.fail(e.getMessage());
    } catch (VeniceKafkaConsumerException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void testNodePartitioning() {

    InMemoryStorageNode testNode = new InMemoryStorageNode(1);    // nodeId = 1
    Assert.assertFalse(testNode.containsPartition(10));

    try {

      testNode.put(10, "dummy_key", "dummy_value");
      Assert.fail("Exception not thrown on null partition put.");

    } catch (VeniceStorageException e) { }

    // proceed with operations
    Assert.assertFalse(testNode.containsPartition(10));

    try {
      testNode.addPartition(10);

      Assert.assertTrue(testNode.containsPartition(10));

      InMemoryStoragePartition partition = testNode.removePartition(10);
      Assert.assertFalse(testNode.containsPartition(10));
      Assert.assertEquals(partition.getId(), 10);

    } catch (VeniceStorageException e) {
      Assert.fail(e.getMessage());
    } catch (VeniceKafkaConsumerException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void testPartitionFails() {

    InMemoryStorageNode testNode = new InMemoryStorageNode(1);    // nodeId = 1

    try {
      // successfully add a new partition
      testNode.addPartition(1);
    } catch (VeniceStorageException e) {
      Assert.fail(e.getMessage());
    } catch (VeniceKafkaConsumerException e) {
      Assert.fail(e.getMessage());
    }

    try {
      // attempting to re-add partition, should fail
      testNode.addPartition(1);
      Assert.fail("Exception not thrown on partition re-add");
    } catch (VeniceKafkaConsumerException e) {
      Assert.fail(e.getMessage());
    } catch (VeniceStorageException e) {
    }

    try {
      // successfully remove the partition
      testNode.removePartition(1);
    } catch (VeniceStorageException e) {
      Assert.fail(e.getMessage());
    }

    try {
      testNode.removePartition(1);
      Assert.fail("Exception not thrown on partition re-removal");
    } catch (VeniceStorageException e) {
    }

  }

  @Test
  public void testNodeNull() {

    InMemoryStorageNode testNode = new InMemoryStorageNode(1);    // nodeId = 1

    try {

      // should not work, due to lack of available partition
      testNode.put(1, "dummy_key", "dummy_value");
      Assert.fail("Exception not thrown");

    } catch (VeniceStorageException e) { }

    try {

      // should not work, due to lack of available partition
      testNode.get(1, "dummy_key");
      Assert.fail("Exception not thrown");

    } catch (VeniceStorageException e) { }

    try {

      // should not work, due to lack of available partition
      testNode.delete(1, "dummy_key");
      Assert.fail("Exception not thrown");

    } catch (VeniceStorageException e) { }

  }

}
