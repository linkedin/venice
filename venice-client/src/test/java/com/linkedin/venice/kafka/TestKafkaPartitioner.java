package com.linkedin.venice.kafka;

import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
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

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());

    byte[] key =   "key1".getBytes();

    // Test 1
    int partition1 = kp.partition(key, 3);
    int partition2 = kp.partition(key, 3);
    Assert.assertEquals(partition1, partition2);

    // Test 2
    key =  "    ".getBytes();
    partition1 = kp.partition(key, 7);
    partition2 = kp.partition(key, 7);
    Assert.assertEquals(partition1, partition2);

    // Test 3
    key = "00000".getBytes();
    partition1 = kp.partition(key, 4);
    partition2 = kp.partition(key, 4);
    Assert.assertEquals(partition1, partition2);

    // Test 4
    key = "0_0_0".getBytes();
    partition1 = kp.partition(key, 6);
    partition2 = kp.partition(key, 6);
    Assert.assertEquals(partition1, partition2);

    // Test 5
    key = "a!b@c$d%e&f".getBytes();
    partition1 = kp.partition(key, 5);
    partition2 = kp.partition(key, 5);
    Assert.assertEquals(partition1, partition2);
  }
}
