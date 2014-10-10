package com.linkedin.venice.kafka;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the Kafka Partitioner. It doesn't really matter what the results are, as long as they are consistent.
 * 1. Testing several strings and ensuring that they are partitioned consistently
 *    - numeric
 *    - special characters
 *    - white space
 */
public class TestKafkaPartitioner {

  @BeforeClass
  public static void initConfig() {

    try {
      GlobalConfiguration.initializeFromFile("./src/test/resources/test.properties");         // config file for testing
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void testConsistentPartitioning() {

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());

    // Test 1
    int partition1 = kp.partition("key1", 3);
    int partition2 = kp.partition("key1", 3);

    Assert.assertEquals(partition1, partition2);

    // Test 2
    partition1 = kp.partition("    ", 7);
    partition2 = kp.partition("    ", 7);

    Assert.assertEquals(partition1, partition2);

    // Test 3
    partition1 = kp.partition("00000", 4);
    partition2 = kp.partition("00000", 4);

    Assert.assertEquals(partition1, partition2);

    // Test 4
    partition1 = kp.partition("0_0_0", 6);
    partition2 = kp.partition("0_0_0", 6);

    Assert.assertEquals(partition1, partition2);

    // Test 5
    partition1 = kp.partition("a!b@c$d%e&f", 5);
    partition2 = kp.partition("a!b@c$d%e&f", 5);

    Assert.assertEquals(partition1, partition2);

  }

}
