package com.linkedin.venice.hadoop.input.kafka.ttl;

import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestVeniceKafkaInputTTLFilter {
  VeniceKafkaInputTTLFilter filterWithSupportedPolicy;
  VeniceKafkaInputTTLFilter filterWithUnsupportedPolicy;

  @BeforeClass
  public void setUp() {
    Properties validProps = new Properties();
    validProps.put(VenicePushJob.REPUSH_TTL_IN_HOURS, 10L);
    validProps.put(VenicePushJob.REPUSH_TTL_POLICY, 0);
    VeniceProperties valid = new VeniceProperties(validProps);

    Properties invalidProps = new Properties();
    invalidProps.put(VenicePushJob.REPUSH_TTL_IN_HOURS, 10L);
    invalidProps.put(VenicePushJob.REPUSH_TTL_POLICY, 1);
    VeniceProperties invalid = new VeniceProperties(invalidProps);

    this.filterWithSupportedPolicy = new VeniceKafkaInputTTLFilter(valid);
    this.filterWithUnsupportedPolicy = new VeniceKafkaInputTTLFilter(invalid);
  }

  @Test
  public void testFilterChain() {
    filterWithSupportedPolicy.setNext(filterWithUnsupportedPolicy);
    Assert.assertTrue(filterWithSupportedPolicy.hasNext());
    Assert.assertEquals(filterWithSupportedPolicy.next(), filterWithUnsupportedPolicy);
    Assert.assertFalse(filterWithUnsupportedPolicy.hasNext());
    filterWithSupportedPolicy.setNext(null);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = ".*policy is not supported.*")
  public void testFilterWithUnsupportedPolicy() {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    filterWithUnsupportedPolicy.applyRecursively(value);
  }

  @Test
  public void testFilterWithRejectBatchWritePolicy() {
    KafkaInputMapperValue value = new KafkaInputMapperValue();
    // TODO change this once the implementation of REJECT_BATCH_WRITE is completed
    Assert.assertFalse(filterWithSupportedPolicy.applyRecursively(value));
  }
}
