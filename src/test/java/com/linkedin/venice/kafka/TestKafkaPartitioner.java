package com.linkedin.venice.kafka;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import junit.framework.Assert;
import kafka.utils.VerifiableProperties;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by clfung on 9/29/14.
 */
public class TestKafkaPartitioner {

  @BeforeClass
  public static void initConfig() {
    GlobalConfiguration.initialize("");         // config file for testing
  }

  @Test
  public void testConsistentPartitioning() {

    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());

    // Test 1
    int partition1 = kp.partition("key1", 3);
    int partition2 = kp.partition("key1", 3);

    Assert.assertEquals(partition1, partition2);

    // Test 2
    partition1 = kp.partition("00000", 7);
    partition2 = kp.partition("00000", 7);

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
