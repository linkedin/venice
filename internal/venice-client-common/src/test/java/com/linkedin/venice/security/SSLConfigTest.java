package com.linkedin.venice.security;

import static org.testng.Assert.*;

import com.linkedin.venice.CommonConfigKeys;
import java.util.Properties;
import org.testng.annotations.Test;


public class SSLConfigTest {
  @Test
  public void testBuildFromProperties() {
    final Properties properties = new Properties();
    properties.setProperty(CommonConfigKeys.SSL_ENABLED, "true");
    properties.setProperty(CommonConfigKeys.SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(CommonConfigKeys.SSL_KEYSTORE_LOCATION, "/local/path");
    properties.setProperty(CommonConfigKeys.SSL_TRUSTSTORE_LOCATION, "/local/pathts");
    properties.setProperty(CommonConfigKeys.SSL_KEYSTORE_PASSWORD, "keystorepwd");
    properties.setProperty(CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD, "truststorepwd");
    properties.setProperty(CommonConfigKeys.SSL_NEEDS_CLIENT_CERT, "false");
    final SSLConfig sslConfig = SSLConfig.buildConfig(properties);
    assertTrue(sslConfig.getSslEnabled());
    assertEquals(sslConfig.getKeyStoreFilePath(), "/local/path");
    assertEquals(sslConfig.getKeyStorePassword(), "keystorepwd");
    assertEquals(sslConfig.getKeyStoreType(), "JKS");
    assertEquals(sslConfig.getTrustStoreFilePassword(), "truststorepwd");
    assertEquals(sslConfig.getTrustStoreFilePath(), "/local/pathts");
    assertFalse(sslConfig.doesSslRequireClientCerts());
  }
}
