package com.linkedin.venice.kafka;

import com.linkedin.venice.kafka.partitioner.DefaultKafkaPartitioner;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import com.linkedin.venice.message.KafkaKey;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.testng.annotations.Test;


/**
 * Tests for the Kafka Partitioner. It doesn't really matter what the results are, as long as they are consistent.
 * 1. Testing several strings and ensuring that they are partitioned consistently
 *    - numeric
 *    - special characters
 *    - white space
 */
public class TestKafkaPartitioner {

  @Test
  public void testConsistentPartitioning() {

    KafkaPartitioner kp = new DefaultKafkaPartitioner(new VerifiableProperties());

    byte[] keyBytes =   "key1".getBytes();
    KafkaKey key = new KafkaKey(keyBytes);

    // Test 1
    int partition1 = kp.partition(key, 3);
    int partition2 = kp.partition(key, 3);
    Assert.assertEquals(partition1, partition2);

    // Test 2
    key = new KafkaKey("    ".getBytes());
    partition1 = kp.partition(key, 7);
    partition2 = kp.partition(key, 7);
    Assert.assertEquals(partition1, partition2);

    // Test 3
    key = new KafkaKey("00000".getBytes());
    partition1 = kp.partition(key, 4);
    partition2 = kp.partition(key, 4);
    Assert.assertEquals(partition1, partition2);

    // Test 4
    key = new KafkaKey("0_0_0".getBytes());
    partition1 = kp.partition(key, 6);
    partition2 = kp.partition(key, 6);
    Assert.assertEquals(partition1, partition2);

    // Test 5
    key = new KafkaKey("a!b@c$d%e&f".getBytes());
    partition1 = kp.partition(key, 5);
    partition2 = kp.partition(key, 5);
    Assert.assertEquals(partition1, partition2);
  }
}
