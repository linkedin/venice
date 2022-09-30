package com.linkedin.venice.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;

import com.linkedin.alpini.base.ssl.SslFactory;
import com.linkedin.alpini.netty4.ssl.SSLEngineFactoryImpl;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SslUtils {
  private static final Logger LOGGER = LogManager.getLogger(SslUtils.class);

  /**
   * Self-signed cert. Use keystore as truststore since self-signed. Cert has CN=localhost
   *
   * IMPORTANT NOTE: the "localhost.jks", "localhost.cert", "localhost.key" and "localhost.p12" files only exist
   *                 in the code base; do not try to load this files in actual hosts
   */
  public static final String LOCAL_PASSWORD = "dev_pass";
  public static final String LOCAL_KEYSTORE_JKS = "localhost.jks";

  /**
   * This function should be used in test cases only.
   *
   * @return an instance of {@link SSLConfig} with local SSL config.
   */
  public static SSLConfig getLocalSslConfig() {
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setKeyStoreFilePath(keyStorePath);
    sslConfig.setKeyStorePassword(LOCAL_PASSWORD);
    sslConfig.setKeyStoreType("JKS");
    sslConfig.setTrustStoreFilePath(keyStorePath);
    sslConfig.setTrustStoreFilePassword(LOCAL_PASSWORD);
    sslConfig.setSslEnabled(true);
    return sslConfig;
  }

  /**
   * This function should be used in test cases only.
   *
   * @return a local SSL factory that uses a self-signed development certificate.
   */
  public static SSLFactory getVeniceLocalSslFactory() {
    Properties sslProperties = getVeniceLocalSslProperties();
    try {
      return new DefaultSSLFactory(sslProperties);
    } catch (Exception e) {
      throw new VeniceException("Failed to build Venice local SSL factory.", e);
    }
  }

  /**
   * This function should be used in test cases only.
   *
   * @return an instance of {@link Properties} that contains local SSL configs.
   */
  public static Properties getVeniceLocalSslProperties() {
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    Properties sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED, "true");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_KEYSTORE_LOCATION, keyStorePath);
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD);
    sslProperties.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, keyStorePath);
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD);
    sslProperties.setProperty(SSL_KEY_PASSWORD, LOCAL_PASSWORD);
    sslProperties.setProperty(SSL_KEYMANAGER_ALGORITHM, "SunX509");
    sslProperties.setProperty(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
    sslProperties.setProperty(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    return sslProperties;
  }

  /**
   * This function should be used in test cases only.
   *
   * @param resource -- System resource name
   * @return the path to the local key store location
   */
  protected static String getPathForResource(String resource) {
    String systemTempDir = System.getProperty("java.io.tmpdir");
    String subDir = "venice-keys-" + UUID.randomUUID();
    File tempDir = new File(systemTempDir, subDir);
    tempDir.mkdir();
    tempDir.deleteOnExit();
    File file = new File(tempDir.getAbsolutePath(), resource);
    if (!file.exists()) {
      try (InputStream is = (ClassLoader.getSystemResourceAsStream(resource))) {
        if (is == null) {
          throw new IllegalStateException(
              "ClassLoader.getSystemResourceAsStream returned null resource for: " + resource);
        }
        Files.copy(is, file.getAbsoluteFile().toPath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy resource: " + resource + " to tmp dir", e);
      } finally {
        file.deleteOnExit();
      }
    }
    return file.getAbsolutePath();
  }

  public static SslFactory toAlpiniSSLFactory(SSLFactory sslFactory) {
    try {
      return new SSLEngineFactoryImpl(toAlpiniSSLConfig(sslFactory.getSSLConfig()));
    } catch (Exception e) {
      throw new VeniceException("Unable to create SSL factory", e);
    }
  }

  public static SSLEngineFactoryImpl.Config toAlpiniSSLConfig(SSLConfig sslConfig) {
    SSLEngineFactoryImpl.Config config = new SSLEngineFactoryImpl.Config();
    config.setSslEnabled(sslConfig.getSslEnabled());
    config.setKeyStoreType(sslConfig.getKeyStoreType());
    config.setKeyStoreData(sslConfig.getKeyStoreData());
    config.setKeyStorePassword(sslConfig.getKeyStorePassword());
    config.setKeyStoreFilePath(sslConfig.getKeyStoreFilePath());
    config.setTrustStoreFilePath(sslConfig.getTrustStoreFilePath());
    config.setTrustStoreFilePassword(sslConfig.getTrustStoreFilePassword());
    return config;
  }

  /**
   * A helper function that return an instance of {@link SSLFactory} with ssl Properties.
   * @param sslProperties
   * @param factoryClassName Different products can plug-in different factory classes.
   */
  public static SSLFactory getSSLFactory(Properties sslProperties, String factoryClassName) {
    Class<SSLFactory> factoryClass = ReflectUtils.loadClass(factoryClassName);
    Class<Properties> propertiesClass = ReflectUtils.loadClass(Properties.class.getName());
    SSLFactory sslFactory =
        ReflectUtils.callConstructor(factoryClass, new Class[] { propertiesClass }, new Object[] { sslProperties });
    return sslFactory;
  }

  /**
   * Build an instance of {@link Properties} with a file path to SSL config file.
   *
   * An example of SSL config file:
   * ssl.enabled=true
   * keystore.type=PKCS12
   * keystore.password=local_password
   * keystore.path=./identity.p12
   * truststore.password=local_password
   * truststore.path=/etc/riddler/cacerts
   */
  public static Properties loadSSLConfig(String configFilePath) throws IOException {
    Properties props = new Properties();
    try (FileInputStream inputStream = new FileInputStream(configFilePath)) {
      props.load(inputStream);
    } catch (IOException e) {
      LOGGER.error("Could not load ssl config file from path: {}", configFilePath, e);
      throw e;
    }
    return props;
  }

  public static X509Certificate getX509Certificate(Certificate certificate) {
    if (!(certificate instanceof X509Certificate)) {
      String exceptionMessage = new StringBuilder().append("Only certificates of type ")
          .append(X509Certificate.class.getName())
          .append(" are supported. Received certificate of type ")
          .append(certificate.getClass().getName())
          .toString();
      throw new IllegalArgumentException(exceptionMessage);
    }

    return (X509Certificate) certificate;
  }
}
