package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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

  /**
   * Right now, Venice only supports two Kafka protocols:
   * {@link SecurityProtocol#PLAINTEXT}
   * {@link SecurityProtocol#SSL}
   *
   * @param kafkaProtocol
   * @return
   */
  public static boolean isKafkaProtocolValid(String kafkaProtocol) {
    return kafkaProtocol.equals(SecurityProtocol.PLAINTEXT.name()) || kafkaProtocol.equals(SecurityProtocol.SSL.name())
        || kafkaProtocol.equals(SecurityProtocol.SASL_PLAINTEXT.name())
        || kafkaProtocol.equals(SecurityProtocol.SASL_SSL.name());
  }

  public static boolean isKafkaSSLProtocol(String kafkaProtocol) {
    return kafkaProtocol.equals(SecurityProtocol.SSL.name()) || kafkaProtocol.equals(SecurityProtocol.SASL_SSL.name());
  }

  public static boolean isKafkaSSLProtocol(SecurityProtocol kafkaProtocol) {
    return kafkaProtocol == SecurityProtocol.SSL || kafkaProtocol == SecurityProtocol.SASL_SSL;
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
