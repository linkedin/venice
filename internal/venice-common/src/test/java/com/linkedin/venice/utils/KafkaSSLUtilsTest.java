package com.linkedin.venice.utils;

import static org.testng.Assert.*;

import org.apache.kafka.common.protocol.SecurityProtocol;
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
    assertTrue(KafkaSSLUtils.isKafkaSSLProtocol(SecurityProtocol.SSL));
    assertFalse(KafkaSSLUtils.isKafkaSSLProtocol(SecurityProtocol.PLAINTEXT));
    assertTrue(KafkaSSLUtils.isKafkaSSLProtocol(SecurityProtocol.SASL_SSL));
    assertFalse(KafkaSSLUtils.isKafkaSSLProtocol(SecurityProtocol.SASL_PLAINTEXT));
  }
}
