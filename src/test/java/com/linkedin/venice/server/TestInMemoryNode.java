package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.storage.InMemoryStoreNode;

import com.linkedin.venice.storage.InMemoryStorePartition;
import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by clfung on 9/25/14.
 */
public class TestInMemoryNode {

  @BeforeClass
  public static void initConfig() {
    GlobalConfiguration.initialize("");         // config file for testing
  }

  @Test
  public void testNodeConstructor() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1);
    Assert.assertEquals(1, testNode.getNodeId());

    testNode = new InMemoryStoreNode(5);
    Assert.assertEquals(5, testNode.getNodeId());

  }

  @Test
  public void testNodeOperations() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1);
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

  }

  @Test
  public void testNodePartitioning() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1);    // nodeId = 1

    Assert.assertFalse(testNode.containsPartition(10));
    testNode.put(10, "dummy_key", "dummy_value");

    Assert.assertFalse(testNode.containsPartition(10));
    testNode.addPartition(10);

    Assert.assertTrue(testNode.containsPartition(10));

    InMemoryStorePartition partition = testNode.removePartition(10);
    Assert.assertFalse(testNode.containsPartition(10));
    Assert.assertEquals(partition.getId(), 10);

  }

  @Test
  public void testPartitionFails() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1);    // nodeId = 1
    Assert.assertTrue(testNode.addPartition(1));

    // attempting to re-add partition, should return null
    Assert.assertFalse(testNode.addPartition(1));

    // should return the removed partition
    Assert.assertNotNull(testNode.removePartition(1));

    // attempting to remove non-existant partition, should return null
    Assert.assertNull(testNode.removePartition(1));


  }

  @Test
  public void testNodeNull() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1);    // nodeId = 1
    Assert.assertFalse(testNode.put(1, "dummy_key", "dummy_value")); // should not work, due to lack of available partition
    Assert.assertNull(testNode.get(1, "dummy_key"));
    Assert.assertFalse(testNode.delete(1, "dummy_key"));

  }

}
