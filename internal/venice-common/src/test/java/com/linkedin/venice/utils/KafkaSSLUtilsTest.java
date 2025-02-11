package com.linkedin.venice.utils;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import org.testng.annotations.Test;


public class KafkaSSLUtilsTest {
  @Test
  public void testIsKafkaProtocolValid() {
    assertTrue(KafkaSSLUtils.isKafkaProtocolValid("SSL"));
    assertTrue(KafkaSSLUtils.isKafkaProtocolValid("PLAINTEXT"));
    assertTrue(KafkaSSLUtils.isKafkaProtocolValid("SASL_SSL"));
    assertTrue(KafkaSSLUtils.isKafkaProtocolValid("SASL_PLAINTEXT"));
  }

  @Test
  public void testIsKafkaSSLProtocol() {
    assertTrue(KafkaSSLUtils.isKafkaSSLProtocol("SSL"));
    assertFalse(KafkaSSLUtils.isKafkaSSLProtocol("PLAINTEXT"));
    assertTrue(KafkaSSLUtils.isKafkaSSLProtocol("SASL_SSL"));
    assertFalse(KafkaSSLUtils.isKafkaSSLProtocol("SASL_PLAINTEXT"));
  }

  @Test
  public void testTestIsKafkaSSLProtocol() {
    assertTrue(KafkaSSLUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.SSL));
    assertFalse(KafkaSSLUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.PLAINTEXT));
    assertTrue(KafkaSSLUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.SASL_SSL));
    assertFalse(KafkaSSLUtils.isKafkaSSLProtocol(PubSubSecurityProtocol.SASL_PLAINTEXT));
  }
}
