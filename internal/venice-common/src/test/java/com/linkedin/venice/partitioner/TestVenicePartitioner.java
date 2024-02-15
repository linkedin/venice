package com.linkedin.venice.partitioner;

import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the Venice Partitioner. It doesn't really matter what the results are, as long as they are consistent.
 * 1. Testing several strings and ensuring that they are partitioned consistently
 * - numeric
 * - special characters
 * - white space
 */
public class TestVenicePartitioner {
  @Test
  public void testConsistentPartitioning() {

    VenicePartitioner vp = new DefaultVenicePartitioner();

    byte[] keyBytes = "key1".getBytes();

    int numPartitions = 3;

    // Test 1
    int partition1 = vp.getPartitionId(keyBytes, numPartitions);
    int partition2 = vp.getPartitionId(keyBytes, numPartitions);
    Assert.assertEquals(partition1, partition2);

    // Test 2
    keyBytes = "    ".getBytes();
    partition1 = vp.getPartitionId(keyBytes, numPartitions);
    partition2 = vp.getPartitionId(keyBytes, numPartitions);
    Assert.assertEquals(partition1, partition2);

    // Test 3
    keyBytes = "00000".getBytes();
    partition1 = vp.getPartitionId(keyBytes, numPartitions);
    partition2 = vp.getPartitionId(keyBytes, numPartitions);
    Assert.assertEquals(partition1, partition2);

    // Test 4
    // Use sumPartitioner since DefaultVenicePartitioner overrides
    // getPartitionId(byte[] keyBytes, int offset, int length, int numPartitions)
    VenicePartitioner sumPartitioner = new VenicePartitioner() {
      @Override
      public int getPartitionId(byte[] keyBytes, int numPartitions) {
        int sum = 0;
        for (byte keyByte: keyBytes) {
          sum += keyByte;
        }
        return Math.abs(sum) % numPartitions;
      }

      @Override
      public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
        int sum = 0;
        for (int i = keyByteBuffer.position(); i < keyByteBuffer.remaining(); i++) {
          sum += keyByteBuffer.get(i);
        }
        return Math.abs(sum) % numPartitions;
      }
    };
    byte[] keyBytes1 = "123456suffix".getBytes();
    byte[] keyBytes2 = "123456".getBytes();

    partition1 = sumPartitioner.getPartitionId(keyBytes1, 0, 6, 16);
    partition2 = sumPartitioner.getPartitionId(keyBytes2, 16);
    Assert.assertEquals(partition1, partition2);
  }
}
