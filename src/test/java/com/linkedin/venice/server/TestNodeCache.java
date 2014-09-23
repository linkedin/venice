package com.linkedin.venice.server;

import com.linkedin.venice.metadata.NodeCache;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by clfung on 9/29/14.
 */
public class TestNodeCache {

  @Test
  public void testCaching() {

    NodeCache nc = NodeCache.getInstance();
    nc.clear(); //reset cache

    List<Integer> nodes1 = Arrays.asList(1,2,3);
    List<Integer> nodes2 = Arrays.asList(4,5,6);

    Assert.assertTrue(nc.registerNewMapping(1, nodes1));
    Assert.assertTrue(nc.registerNewMapping(1, nodes1)); // valid put-overwrite operation

    // verify cache data
    Assert.assertEquals(nc.getNodeIds(1), Arrays.asList(1,2,3));

    // attempt to overwrite cache with new data, verify original data persists
    Assert.assertFalse(nc.registerNewMapping(1, nodes2));
    Assert.assertEquals(nc.getNodeIds(1), Arrays.asList(1,2,3));

    // cache new instances
    Assert.assertTrue(nc.registerNewMapping(2, nodes2));
    Assert.assertEquals(nc.getNodeIds(2), Arrays.asList(4,5,6));

    Assert.assertEquals(1, nc.getMasterNodeId(1));
    Assert.assertEquals(4, nc.getMasterNodeId(2));

  }

}
