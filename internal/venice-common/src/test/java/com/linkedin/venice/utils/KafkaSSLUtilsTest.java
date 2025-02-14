package com.linkedin.venice.utils;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import org.testng.annotations.Test;


public class KafkaSSLUtilsTest {
  @Test
  public void testIsKafkaProtocolValid() {
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("SSL"));
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("PLAINTEXT"));
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("SASL_SSL"));
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("SASL_PLAINTEXT"));
  }

  @Test
  public void testIsKafkaSSLProtocol() {
    assertTrue(ApacheKafkaUtils.isKafkaSSLProtocol("SSL"));
    assertFalse(ApacheKafkaUtils.isKafkaSSLProtocol("PLAINTEXT"));
    assertTrue(ApacheKafkaUtils.isKafkaSSLProtocol("SASL_SSL"));
    assertFalse(ApacheKafkaUtils.isKafkaSSLProtocol("SASL_PLAINTEXT"));
  }

  @Test
  public void testTestIsKafkaSSLProtocol() {
    assertTrue(ApacheKafkaUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.SSL));
    assertFalse(ApacheKafkaUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.PLAINTEXT));
    assertTrue(ApacheKafkaUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.SASL_SSL));
    assertFalse(ApacheKafkaUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.SASL_PLAINTEXT));
  }
}
