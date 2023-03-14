package com.linkedin.venice.fastclient.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import org.testng.annotations.Test;


public class PartitionerPairTest {
  @Test
  public void test() {
    VenicePartitioner venicePartitioner = new DefaultVenicePartitioner();
    PartitionerPair partitionerPair1 = new PartitionerPair(venicePartitioner, 0);
    PartitionerPair partitionerPair2 = new PartitionerPair(venicePartitioner, 0);
    PartitionerPair partitionerPair3 = new PartitionerPair(venicePartitioner, 1);

    assertEquals(partitionerPair1, partitionerPair2);
    assertNotEquals(partitionerPair1, partitionerPair3);

    assertEquals(partitionerPair1.hashCode(), partitionerPair2.hashCode());
    assertNotEquals(partitionerPair1.hashCode(), partitionerPair3.hashCode());
  }
}
