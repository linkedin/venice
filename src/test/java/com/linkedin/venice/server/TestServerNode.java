package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.metadata.KeyAddress;
import com.linkedin.venice.storage.InMemoryStoreNode;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by clfung on 9/25/14.
 */
public class TestServerNode {

  @BeforeClass
  public static void initConfig() {
    GlobalConfiguration.initialize("");
  }

  @Test
  public void testNodeConstructor() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1, 1);
    Assert.assertEquals(1, testNode.getNodeId());

    testNode = new InMemoryStoreNode(5, 1);
    Assert.assertEquals(5, testNode.getNodeId());

  }

  @Test
  public void testNodeOperations() {

    InMemoryStoreNode testNode = new InMemoryStoreNode(1, 1);

    // test basic put and get
    testNode.put("key", "value");
    testNode.put("key2", "value2");

    Assert.assertEquals("value", testNode.get("key"));
    Assert.assertEquals("value2", testNode.get("key2"));

    testNode.put("key2", "value3");

    Assert.assertEquals("value3", testNode.get("key2"));

  }

  @Test
  public void testKeyAddress() {

    KeyAddress a1 = new KeyAddress(1, 1);
    KeyAddress a2 = new KeyAddress(2, 1);
    KeyAddress a3 = new KeyAddress(1, 2);
    KeyAddress a4 = new KeyAddress(1, 1);

    Assert.assertFalse(a1.equals(a2));
    Assert.assertFalse(a1.equals(a3));
    Assert.assertTrue(a1.equals(a4));

  }

}
