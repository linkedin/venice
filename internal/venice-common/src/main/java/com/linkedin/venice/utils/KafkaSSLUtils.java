package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;


public class KafkaSSLUtils {
  /**
   * Mandatory Kafka SSL configs when SSL is enabled.
   */
  private static final List<String> KAFKA_SSL_MANDATORY_CONFIGS = Arrays.asList(
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
      SslConfigs.SSL_KEY_PASSWORD_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
      SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
      SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
      SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);

  public static Properties getLocalKafkaBrokerSSlConfig(String host, int port, int sslPort) {
    Properties properties = new Properties();
    properties.put(KafkaConfig.SslProtocolProp(), "TLS");
    // Listen on two ports, one for ssl one for non-ssl
    properties.put(KafkaConfig.ListenersProp(), "PLAINTEXT://" + host + ":" + port + ",SSL://" + host + ":" + sslPort);
    properties.putAll(getLocalCommonKafkaSSLConfig());
    properties.put(SslConfigs.SSL_CONTEXT_PROVIDER_CLASS_CONFIG, DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME);
    return properties;
  }

  public static Properties getLocalCommonKafkaSSLConfig() {
    Properties properties = new Properties();
    String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
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

  public static boolean isKafkaSSLProtocol(SecurityProtocol kafkaProtocol) {
    return kafkaProtocol == SecurityProtocol.SSL;
  }

  /**
   * This function will extract SSL related config if Kafka SSL is enabled.
   *
   * @param veniceProperties
   * @param properties
   * @return whether Kafka SSL is enabled or not
   */
  public static boolean validateAndCopyKafkaSSLConfig(VeniceProperties veniceProperties, Properties properties) {
    if (!veniceProperties.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
      // No security protocol specified
      return false;
    }
    String kafkaProtocol = veniceProperties.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (!KafkaSSLUtils.isKafkaProtocolValid(kafkaProtocol)) {
      throw new VeniceException("Invalid Kafka protocol specified: " + kafkaProtocol);
    }
    if (!KafkaSSLUtils.isKafkaSSLProtocol(kafkaProtocol)) {
      // TLS/SSL is not enabled
      return false;
    }
    // Since SSL is enabled, the following configs are mandatory
    KAFKA_SSL_MANDATORY_CONFIGS.forEach(config -> {
      if (!veniceProperties.containsKey(config)) {
        throw new VeniceException(config + " is required when Kafka SSL is enabled");
      }
      properties.setProperty(config, veniceProperties.getString(config));
    });
    return true;
  }
}
