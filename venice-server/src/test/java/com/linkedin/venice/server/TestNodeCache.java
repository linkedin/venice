package com.linkedin.venice.server;

import com.linkedin.venice.metadata.NodeCache;
import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests the Functions of the Node Cache
 * 1. Registration of new mappings
 * 2. Overwriting mapping on same partitionId with same nodeIds
 * 3. Successfully rejects the mapping of a new new nodeId on the same partitionId
 * 4. Successfully return the master nodeId given a list of nodeIds
 */
public class TestNodeCache {

  @Test
  public void testCaching() {

    NodeCache nc = NodeCache.getInstance();
    nc.clear(); //reset cache

    List<Integer> nodes1 = Arrays.asList(1,2,3);
    List<Integer> nodes2 = Arrays.asList(4,5,6);

    nc.registerNewMapping(1, nodes1);
    nc.registerNewMapping(1, nodes1); // valid put-overwrite operation

    // verify cache data
    Assert.assertEquals(nc.getNodeIds(1), Arrays.asList(1,2,3));

    // attempt to overwrite cache with new data, verify original data persists
    nc.registerNewMapping(1, nodes2);
    Assert.assertEquals(nc.getNodeIds(1), Arrays.asList(1,2,3));

    // cache new instances
    nc.registerNewMapping(2, nodes2);
    Assert.assertEquals(nc.getNodeIds(2), Arrays.asList(4,5,6));

    Assert.assertEquals(1, nc.getMasterNodeId(1));
    Assert.assertEquals(4, nc.getMasterNodeId(2));

  }

}
