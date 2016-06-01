package com.linkedin.venice.kafka;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.message.KafkaKey;
import org.apache.kafka.common.PartitionInfo;
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
        KafkaKey key = new KafkaKey(MessageType.PUT, keyBytes);  // OperationType doesn't matter. We are just testing the partitioning.

        PartitionInfo [] partitionArray = {new PartitionInfo("", 0, null, null, null),
            new PartitionInfo("", 1, null, null, null), new PartitionInfo("", 2, null, null, null)};

        // Test 1
        int partition1 = vp.getPartitionId(keyBytes, partitionArray.length);
        int partition2 = vp.getPartitionId(keyBytes, partitionArray.length);
        Assert.assertEquals(partition1, partition2);

        // Test 2
      keyBytes = "    ".getBytes();
        partition1 = vp.getPartitionId(keyBytes, partitionArray.length);
        partition2 = vp.getPartitionId(keyBytes, partitionArray.length);
        Assert.assertEquals(partition1, partition2);

        // Test 3
        keyBytes = "00000".getBytes();
        partition1 = vp.getPartitionId(keyBytes, partitionArray.length);
        partition2 = vp.getPartitionId(keyBytes, partitionArray.length);
        Assert.assertEquals(partition1, partition2);

    }
}
