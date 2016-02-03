package com.linkedin.venice.kafka;

import com.linkedin.venice.kafka.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.kafka.partitioner.VenicePartitioner;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.OperationType;
import java.util.Arrays;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
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
        KafkaKey key = new KafkaKey(OperationType.WRITE, keyBytes);  // OperationType doesn't matter. We are just testing the partitioning.

        PartitionInfo [] partitionArray = {new PartitionInfo("", 0, null, null, null),
            new PartitionInfo("", 1, null, null, null), new PartitionInfo("", 2, null, null, null)};
        Node [] nodeArray = {new Node(0, "127.0.0.1",2000)};
        Cluster cluster = new Cluster(Arrays.asList(nodeArray), Arrays.asList(partitionArray));

        // Test 1
        int partition1 = vp.partition("", key, keyBytes, null, null, cluster);
        int partition2 = vp.partition("", key, keyBytes, null, null, cluster);
        Assert.assertEquals(partition1, partition2);

        // Test 2
        key = new KafkaKey(OperationType.WRITE, "    ".getBytes());
        partition1 = vp.partition("", key, "    ".getBytes(), null, null, cluster);
        partition2 = vp.partition("", key, "    ".getBytes(), null, null, cluster);
        Assert.assertEquals(partition1, partition2);

        // Test 3
        key = new KafkaKey(OperationType.WRITE, "00000".getBytes());
        partition1 = vp.partition("", key, "00000".getBytes(), null, null, cluster);
        partition2 = vp.partition("", key, "00000".getBytes(), null, null, cluster);
        Assert.assertEquals(partition1, partition2);

    }
}
