package com.linkedin.venice.utils;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.security.SSLFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.conscrypt.Conscrypt;
import org.testng.annotations.Test;


public class SslUtilsTest {
  private static Logger LOGGER = LogManager.getLogger(SslUtilsTest.class);

  @Test
  public void testIsConscryptAvailable() {
    String osName = System.getProperty("os.name");
    String osArch = System.getProperty("os.arch");
    if (osName.equalsIgnoreCase("MAC OS X") && osArch.equalsIgnoreCase("aarch64")) {
      assertFalse(SslUtils.isConscryptAvailable(), "Conscrypt shouldn't be available for MAC OS X with aarch64");
    }
    if (osName.equalsIgnoreCase("linux") && osArch.contains("64")) {
      assertTrue(SslUtils.isConscryptAvailable(), "Conscrypt shouldn be available for 64-bit Linux");
    }
  }

  @Test
  public void testSSLFactoryWithOpensslSupport() {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    if (SslUtils.isConscryptAvailable()) {
      SSLFactory adaptedSSLFactory = SslUtils.toSSLFactoryWithOpenSSLSupport(sslFactory);
      assertTrue(
          Conscrypt.isConscrypt(adaptedSSLFactory.getSSLContext()),
          "The adapted SSLContext should be backed by openssl");
    } else {
      LOGGER.info("testSSLFactoryWithOpensslSupport will be skipped since 'Conscrypt' is not available");
    }
  }
}
