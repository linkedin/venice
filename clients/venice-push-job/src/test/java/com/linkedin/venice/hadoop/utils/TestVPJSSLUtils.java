package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVPJSSLUtils {
  @Test(expectedExceptions = VeniceException.class)
  public void testValidateInvalidSslProperties() {
    VPJSSLUtils.validateSslProperties(VeniceProperties.empty());
  }

  @Test
  public void testValidateValidSslProperties() {
    Properties props = new Properties();
    props.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "TEST");
    props.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "TEST");
    props.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "TEST");
    props.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "TEST");
    try {
      VPJSSLUtils.validateSslProperties(new VeniceProperties(props));
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }
}
