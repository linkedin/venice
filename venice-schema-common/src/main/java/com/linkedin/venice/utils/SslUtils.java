package com.linkedin.venice.utils;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.CommonConfigKeys.*;


public class SslUtils {
  private static final Logger logger = LogManager.getLogger(SslUtils.class);

  /**
   * Self-signed cert, expires 2027, use keystore as truststore since self-signed.
   * cert has CN=localhost
   *
   * IMPORTANT NOTE: the "localhost.jks", "localhost.cert", "localhost.key" and "localhost.p12" files only exist
   *                 in the code base; do not try to load this files in actual hosts
   */
  public static final String LOCAL_PASSWORD = "dev_pass";
  private static final String LOCAL_KEYSTORE_P12 = "localhost.p12";
  private static final String LOCAL_KEYSTORE_JKS = "localhost.jks";
  private static final String LOCAL_CERT = "localhost.cert";
  private static final String LOCAL_KEY = "localhost.key";

  /**
   * This function should be used in test cases only.
   *
   * TODO: after Router and Server migrate to {@link SSLFactory} and get rid of {@link SSLEngineComponentFactory} in
   * Venice project, we should remove this helper function.
   *
   * @return factory that corresponds to self-signed development certificate
   */
  public static SSLEngineComponentFactory getLocalSslFactory() {
    SSLEngineComponentFactoryImpl.Config sslConfig = getLocalSslConfig();
    try {
      return new SSLEngineComponentFactoryImpl(sslConfig);
    } catch (Exception e) {
      throw new VeniceException("Failed to create local ssl factory with a self-signed cert", e);
    }
  }

  /**
   * This function should be used in test cases only.
   *
   * TODO: after Router and Server migrate to {@link SSLFactory} and get rid of {@link SSLEngineComponentFactory} in
   * Venice project, we should remove this helper function.
   *
   * @return an instance of {@link SSLEngineComponentFactoryImpl.Config} with local SSL config.
   */
  public static SSLEngineComponentFactoryImpl.Config getLocalSslConfig() {
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    SSLEngineComponentFactoryImpl.Config sslConfig = new SSLEngineComponentFactoryImpl.Config();
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
        Files.copy(is, file.getAbsoluteFile().toPath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy resource: " + resource + " to tmp dir", e);
      } finally {
        file.deleteOnExit();
      }
    }
    return file.getAbsolutePath();
  }

  /**
   * A helper function that returns an instance of {@link SSLEngineComponentFactory} with ssl properties.
   *
   * TODO: This function should be removed after Router and Server migrate to {@link SSLFactory}
   */
  public static SSLEngineComponentFactory getSSLEngineComponentFactory(Properties sslProperties)
      throws Exception {
    SSLEngineComponentFactoryImpl.Config config = new SSLEngineComponentFactoryImpl.Config();
    config.setSslEnabled(Boolean.valueOf(sslProperties.getProperty(SSL_ENABLED)));
    config.setKeyStoreType(sslProperties.getProperty(SSL_KEYSTORE_TYPE));
    /**
     * There is no "setTrustStoreType" api in {@link SSLEngineComponentFactoryImpl.Config}
     */
    config.setKeyStoreFilePath(sslProperties.getProperty(SSL_KEYSTORE_LOCATION));
    config.setTrustStoreFilePath(sslProperties.getProperty(SSL_TRUSTSTORE_LOCATION));
    config.setKeyStorePassword(sslProperties.getProperty(SSL_KEYSTORE_PASSWORD));
    config.setTrustStoreFilePassword(sslProperties.getProperty(SSL_TRUSTSTORE_PASSWORD));

    try {
      return new SSLEngineComponentFactoryImpl(config);
    } catch (Exception e) {
      logger.error("Failed to build ssl engine component factory by config.", e);
      throw e;
    }
  }

  /**
   * A helper function that return an instance of {@link SSLFactory} with ssl Properties.
   * @param sslProperties
   * @param factoryClassName Different products can plug-in different factory classes.
   */
  public static SSLFactory getSSLFactory(Properties sslProperties, String factoryClassName) {
    Class<SSLFactory> factoryClass = ReflectUtils.loadClass(factoryClassName);
    Class<Properties> propertiesClass = ReflectUtils.loadClass(Properties.class.getName());
    SSLFactory sslFactory = ReflectUtils.callConstructor(factoryClass,
        new Class[]{propertiesClass},
        new Object[]{sslProperties});
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
  public static Properties loadSSLConfig(String configFilePath)
      throws IOException {
    Properties props = new Properties();
    try (FileInputStream inputStream = new FileInputStream(configFilePath)) {
      props.load(inputStream);
    } catch (IOException e) {
      logger.error("Could not load ssl config file from path: " + configFilePath, e);
      throw e;
    }
    return props;
  }
}
