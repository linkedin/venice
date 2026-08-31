package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestVPJSSLUtils {
  @BeforeMethod
  public void resetExecutorSslPropsCache() {
    // Ensure each test starts from a clean per-JVM SSL properties cache, since Gradle may reuse the
    // same test worker JVM across test methods/classes.
    VPJSSLUtils.resetExecutorSslPropsCacheForTests();
  }

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

  @Test
  public void testSetupSSLForExecutorIsNoOpWithoutSslConfigurator() {
    VeniceProperties props = new VeniceProperties(new Properties());
    VeniceProperties result = VPJSSLUtils.setupSSLForExecutor(props);
    Assert.assertSame(
        result,
        props,
        "setupSSLForExecutor should return the original properties unchanged when no SSL configurator is configured");
  }

  /**
   * Edge/failure case: when an SSL configurator is configured but the Hadoop token file location cannot be
   * resolved, executor-side SSL materialization must fail loudly with a VeniceException instead of silently
   * falling back to a consumer/filter without SSL properties (which previously caused missing
   * "ssl.keystore.type" failures deep inside PubSub consumer creation).
   */
  @Test
  public void testSetupSSLForExecutorThrowsWhenTokenFileIsUnresolvable() {
    if (System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION) != null) {
      throw new SkipException(
          "Skipping because " + UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION
              + " is set in the test environment and would make the failure scenario unreachable");
    }

    String previousTokenFileLocation = System.getProperty(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    System.clearProperty(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    try {
      Properties props = new Properties();
      props.setProperty(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName());
      VPJSSLUtils.setupSSLForExecutor(new VeniceProperties(props));
      Assert.fail("Expected a VeniceException when the Hadoop token file location cannot be resolved");
    } catch (VeniceException e) {
      Assert.assertTrue(
          e.getMessage().contains("Failed to setup SSL for executor-side PubSub client creation"),
          "Unexpected exception message: " + e.getMessage());
    } finally {
      if (previousTokenFileLocation != null) {
        System.setProperty(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, previousTokenFileLocation);
      }
    }
  }
}
