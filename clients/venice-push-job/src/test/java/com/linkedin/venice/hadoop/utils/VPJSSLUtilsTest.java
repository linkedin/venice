package com.linkedin.venice.hadoop.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.annotations.Test;


public class VPJSSLUtilsTest {
  @Test
  public void testSetupSSLForExecutorWithoutConfiguratorReturnsOriginalConfig() {
    VeniceProperties config = new VeniceProperties(new Properties());

    assertSame(VPJSSLUtils.setupSSLForExecutor(config), config);
  }

  @Test
  public void testSetupSSLForExecutorMergesGeneratedProperties() throws Exception {
    withTokenFile(() -> {
      Properties properties = new Properties();
      properties.setProperty(SSL_CONFIGURATOR_CLASS_CONFIG, TestSSLConfigurator.class.getName());
      properties.setProperty("existing.property", "existing-value");

      VeniceProperties result = VPJSSLUtils.setupSSLForExecutor(new VeniceProperties(properties));

      assertEquals(result.getString(SSL_KEYSTORE_TYPE), "PKCS12");
      assertEquals(result.getString("existing.property"), "existing-value");
      assertEquals(result.getString(SSL_CONFIGURATOR_CLASS_CONFIG), TestSSLConfigurator.class.getName());
    });
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Failed to setup SSL for executor-side PubSub client creation.*")
  public void testSetupSSLForExecutorSurfacesConfiguratorFailure() throws Exception {
    withTokenFile(() -> {
      Properties properties = new Properties();
      properties.setProperty(SSL_CONFIGURATOR_CLASS_CONFIG, FailingSSLConfigurator.class.getName());
      VPJSSLUtils.setupSSLForExecutor(new VeniceProperties(properties));
    });
  }

  @Test
  public void testSetupSSLForExecutorFailsWhenTokenFileIsMissing() {
    String tokenFileProperty = UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
    String previousTokenFile = System.getProperty(tokenFileProperty);
    try {
      System.clearProperty(tokenFileProperty);
      VeniceException exception =
          expectThrows(VeniceException.class, () -> VPJSSLUtils.setupSSLForExecutor(configWithTestConfigurator()));
      assertEquals(exception.getMessage().contains("Hadoop token file is accessible"), true);
    } finally {
      restoreSystemProperty(tokenFileProperty, previousTokenFile);
    }
  }

  @Test
  public void testSetupSSLForExecutorFailsWhenTokenFileIsInvalid() throws Exception {
    File tokenFile = Files.createTempFile("vpj-ssl-utils-invalid", ".tokens").toFile();
    String tokenFileProperty = UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
    String previousTokenFile = System.getProperty(tokenFileProperty);
    try {
      Files.write(tokenFile.toPath(), "not-a-token-file".getBytes(StandardCharsets.UTF_8));
      System.setProperty(tokenFileProperty, tokenFile.getAbsolutePath());
      expectThrows(VeniceException.class, () -> VPJSSLUtils.setupSSLForExecutor(configWithTestConfigurator()));
    } finally {
      restoreSystemProperty(tokenFileProperty, previousTokenFile);
      Files.deleteIfExists(tokenFile.toPath());
    }
  }

  private VeniceProperties configWithTestConfigurator() {
    Properties properties = new Properties();
    properties.setProperty(SSL_CONFIGURATOR_CLASS_CONFIG, TestSSLConfigurator.class.getName());
    return new VeniceProperties(properties);
  }

  private void withTokenFile(ThrowingRunnable runnable) throws Exception {
    File tokenFile = Files.createTempFile("vpj-ssl-utils", ".tokens").toFile();
    String tokenFileProperty = UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
    String previousTokenFile = System.getProperty(tokenFileProperty);
    try {
      Credentials credentials = new Credentials();
      for (int i = 0; i < UserCredentialsFactory.REQUIRED_SECRET_KEY_COUNT; i++) {
        credentials.addSecretKey(new Text("secret-" + i), ("value-" + i).getBytes(StandardCharsets.UTF_8));
      }
      credentials.writeTokenStorageFile(new Path(tokenFile.toURI()), new Configuration());
      System.setProperty(tokenFileProperty, tokenFile.getAbsolutePath());
      runnable.run();
    } finally {
      if (previousTokenFile == null) {
        System.clearProperty(tokenFileProperty);
      } else {
        System.setProperty(tokenFileProperty, previousTokenFile);
      }
      Files.deleteIfExists(tokenFile.toPath());
    }
  }

  private void restoreSystemProperty(String propertyName, String previousValue) {
    if (previousValue == null) {
      System.clearProperty(propertyName);
    } else {
      System.setProperty(propertyName, previousValue);
    }
  }

  public static class TestSSLConfigurator implements SSLConfigurator {
    @Override
    public Properties setupSSLConfig(Properties properties, Credentials userCredentials) {
      Properties sslProperties = new Properties();
      sslProperties.setProperty(SSL_KEYSTORE_TYPE, "PKCS12");
      return sslProperties;
    }
  }

  public static class FailingSSLConfigurator implements SSLConfigurator {
    @Override
    public Properties setupSSLConfig(Properties properties, Credentials userCredentials) {
      throw new VeniceException("Test SSL setup failure");
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
