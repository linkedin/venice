package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.SslUtils.VeniceTlsConfiguration;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;


/**
 * Utilities for Kafka broker and client configuration
 */
public class KafkaTestUtils {
  public static Properties getLocalKafkaBrokerSSlConfig(
      VeniceTlsConfiguration tlsConfiguration,
      String host,
      int port,
      int sslPort) {
    Properties properties = new Properties();
    properties.put(KafkaConfig.SslProtocolProp(), "TLS");
    // Listen on two ports, one for ssl one for non-ssl
    properties.put(KafkaConfig.ListenersProp(), "PLAINTEXT://" + host + ":" + port + ",SSL://" + host + ":" + sslPort);
    properties.putAll(getLocalCommonKafkaSSLConfig(tlsConfiguration));
    return properties;
  }

  public static Properties getLocalCommonKafkaSSLConfig(VeniceTlsConfiguration tlsConfiguration) {
    Properties properties = new Properties();
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, tlsConfiguration.getKeyStorePath());
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, tlsConfiguration.getKeyStorePassword());
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tlsConfiguration.getTrustStorePath());
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tlsConfiguration.getTrustStorePassword());
    properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, tlsConfiguration.getKeyStoreType());
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, tlsConfiguration.getTrustStoreType());
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, tlsConfiguration.getKeyPassphrase());
    properties.put(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, tlsConfiguration.getSecureRandomAlgorithm());
    properties.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, tlsConfiguration.getTrustManagerAlgorithm());
    properties.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, tlsConfiguration.getKeyManagerAlgorithm());
    return properties;
  }

  public static Properties getLocalKafkaClientSSLConfig() {
    Properties properties = new Properties();
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    properties.putAll(getLocalCommonKafkaSSLConfig(SslUtils.getTlsConfiguration()));
    return properties;
  }
}
