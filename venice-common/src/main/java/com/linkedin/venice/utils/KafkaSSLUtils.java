package com.linkedin.venice.utils;

import com.linkedin.venice.guid.GuidUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;


public class KafkaSSLUtils {
  // Self-signed cert, expires 2027, use keystore as truststore since self-signed.
  // cert has CN=localhost
  private static final String LOCAL_PASSWORD = "dev_pass";
  private static final String LOCAL_KEYSTORE_JKS = "localhost.jks";

  public static Properties getLocalKafkaBrokerSSlConfig(String host, int port, int sslPort) {
    Properties properties = new Properties();
    properties.put(KafkaConfig.SslProtocolProp(), "TLS");
    //Listen on two ports, one for ssl one for non-ssl
    properties.put(KafkaConfig.ListenersProp(), "PLAINTEXT://" + host + ":" + port + ",SSL://" + host + ":" + sslPort);
    properties.putAll(getLocalCommonKafkaSSLConfig());
    return properties;
  }

  public static Properties getLocalCommonKafkaSSLConfig() {
    Properties properties = new Properties();
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath);
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, LOCAL_PASSWORD);
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStorePath);
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, LOCAL_PASSWORD);
    properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, LOCAL_PASSWORD);
    properties.put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, "SHA1PRNG");
    properties.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "SunX509");
    properties.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, "SunX509");
    return properties;
  }

  public static Properties getLocalKafkaClientSSLConfig() {
    Properties properties = new Properties();
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    properties.putAll(getLocalCommonKafkaSSLConfig());
    return properties;
  }

  /**
   * Right now, Venice only supports two Kafka protocols:
   * {@link SecurityProtocol#PLAINTEXT}
   * {@link SecurityProtocol#SSL}
   *
   * @param kafkaProtocol
   * @return
   */
  public static boolean isKafkaProtocolValid(String kafkaProtocol) {
    return kafkaProtocol.equals(SecurityProtocol.PLAINTEXT.name()) || kafkaProtocol.equals(SecurityProtocol.SSL.name());
  }

  public static boolean isKafkaSSLProtocol(String kafkaProtocol) {
    return kafkaProtocol.equals(SecurityProtocol.SSL.name());
  }

  protected static String getPathForResource(String resource) {
    String systemTempDir = System.getProperty("java.io.tmpdir");
    String subDir = "venice-keys-" + GuidUtils.getGUIDString();
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
}
