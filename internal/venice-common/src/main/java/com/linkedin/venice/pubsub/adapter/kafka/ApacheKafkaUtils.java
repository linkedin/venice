package com.linkedin.venice.pubsub.adapter.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_SECURITY_PROTOCOL_WITH_PREFIX;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.PUBSUB_KAFKA_CLIENT_CONFIG_PREFIX;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.internals.RecordHeaders;


public class ApacheKafkaUtils {
  public static final RecordHeaders EMPTY_RECORD_HEADERS = new RecordHeaders();

  public static final Set<String> KAFKA_CONFIG_PREFIXES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(KAFKA_CONFIG_PREFIX, PUBSUB_KAFKA_CLIENT_CONFIG_PREFIX)));

  static {
    EMPTY_RECORD_HEADERS.setReadOnly();
  }

  public static RecordHeaders convertToKafkaSpecificHeaders(PubSubMessageHeaders headers) {
    if (headers == null || headers.isEmpty()) {
      return EMPTY_RECORD_HEADERS;
    }
    RecordHeaders recordHeaders = new RecordHeaders();
    for (PubSubMessageHeader header: headers) {
      recordHeaders.add(header.key(), header.value());
    }
    return recordHeaders;
  }

  /**
   * Mandatory Kafka SSL configs when SSL is enabled.
   */
  protected static final Set<String> KAFKA_SSL_MANDATORY_CONFIGS = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(
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
              SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG)));

  /**
   * Extracts and returns only the valid Kafka client configuration properties from the provided
   * {@link VeniceProperties}.
   *
   * <p>
   * This method filters the provided properties against a supplied set of valid configuration
   * keys specific to a Kafka client type (e.g., {@code ProducerConfig.configNames()},
   * {@code ConsumerConfig.configNames()}, or {@code AdminClientConfig.configNames()}).
   * </p>
   *
   * <p>
   * In addition to the client-specific configuration keys, this method always retains common
   * SASL-related properties defined in {@code KAFKA_SASL_CONFIGS}. If the extracted configuration
   * specifies a Kafka security protocol that implies SSL (e.g., {@code SSL} or {@code SASL_SSL}),
   * it also validates that all required SSL configurations are present. These required keys are
   * defined in {@code KAFKA_SSL_MANDATORY_CONFIGS}. If any mandatory SSL property is missing or
   * an invalid security protocol is specified, a {@link VeniceException} is thrown.
   * </p>
   *
   * <p>
   * This utility is intended for safely extracting Kafka configuration subsets suitable for
   * initializing Kafka {@code Producer}, {@code Consumer}, or {@code AdminClient} instances.
   * </p>
   *
   * @param veniceProperties The source {@link VeniceProperties} containing client configuration.
   * @param validKafkaClientSpecificConfigKeys The set of config keys valid for the specific Kafka client type.
   * @return A {@link Properties} object containing only valid and required Kafka client configurations.
   * @throws VeniceException if required SSL configs are missing or an invalid protocol is specified.
   */
  public static Properties getValidKafkaClientProperties(
      final VeniceProperties veniceProperties,
      final Set<String> validKafkaClientSpecificConfigKeys) {
    Properties extractedValidProperties = new Properties();

    // Step 1: Extract properties with the specified prefixes
    Properties strippedProperties = veniceProperties.clipAndFilterNamespace(KAFKA_CONFIG_PREFIXES).toProperties();

    // Step 2: Retain only properties that are either valid Kafka client-specific configs
    strippedProperties.forEach((configKey, configVal) -> {
      if (validKafkaClientSpecificConfigKeys.contains((String) configKey)) {
        extractedValidProperties.put(configKey, configVal);
      }
    });

    // Step 3: Copy SSL-related properties. These properties are mandatory if SSL is enabled,
    // but they typically do not have prefixes.
    validateAndCopyKafkaSSLConfig(veniceProperties, extractedValidProperties);

    return extractedValidProperties;
  }

  /**
   * This function will extract SSL related config if Kafka SSL is enabled.
   *
   * @param veniceProperties
   * @param properties
   * @return whether Kafka SSL is enabled or not0
   */
  private static boolean validateAndCopyKafkaSSLConfig(VeniceProperties veniceProperties, Properties properties) {
    String kafkaProtocol =
        veniceProperties.getStringWithAlternative(KAFKA_SECURITY_PROTOCOL, KAFKA_SECURITY_PROTOCOL_WITH_PREFIX, null);

    if (kafkaProtocol == null) {
      // No security protocol specified
      return false;
    }
    if (!isKafkaProtocolValid(kafkaProtocol)) {
      throw new VeniceException("Invalid Kafka protocol specified: " + kafkaProtocol);
    }
    if (!isKafkaSSLProtocol(kafkaProtocol)) {
      // TLS/SSL is not enabled
      return false;
    }
    // Since SSL is enabled, the following configs are mandatory
    KAFKA_SSL_MANDATORY_CONFIGS.forEach(config -> {
      String configWithPrefix = KAFKA_CONFIG_PREFIX + config;
      String value = veniceProperties.getStringWithAlternative(configWithPrefix, config, null);
      if (value == null) {
        throw new VeniceException(
            "Missing required config: " + config + ". This configuration is mandatory when Kafka SSL is enabled "
                + "(security protocol: " + kafkaProtocol + ").");
      }
      properties.setProperty(config, value);
    });
    return true;
  }

  public static boolean isKafkaSSLProtocol(PubSubSecurityProtocol kafkaProtocol) {
    return kafkaProtocol == PubSubSecurityProtocol.SSL || kafkaProtocol == PubSubSecurityProtocol.SASL_SSL;
  }

  public static boolean isKafkaProtocolValid(String kafkaProtocol) {
    return kafkaProtocol.equals(PubSubSecurityProtocol.PLAINTEXT.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SSL.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SASL_PLAINTEXT.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SASL_SSL.name());
  }

  public static boolean isKafkaSSLProtocol(String kafkaProtocol) {
    return kafkaProtocol.equals(PubSubSecurityProtocol.SSL.name())
        || kafkaProtocol.equals(PubSubSecurityProtocol.SASL_SSL.name());
  }

  /**
   * Generates a standardized and unique client ID for Kafka clients.
   *
   * <p>
   * This ensures uniqueness in client IDs, preventing naming collisions that could cause
   * `InstanceAlreadyExistsException` during JMX metric registration. If multiple Kafka clients
   * share the same client ID, Kafka's internal JMX registration can fail, leading to errors.
   * By appending a timestamp, this method guarantees that each generated ID is unique.
   * </p>
   *
   * <p>
   * If the provided client name is null, it defaults to "kc".
   * If the broker address is null, it defaults to an empty string.
   * The generated client ID follows the format:
   * <pre>{@code clientName-brokerAddress-timestamp}</pre>
   * </p>
   *
   * @param clientName    The name of the client (can be null, defaults to "kc").
   * @param brokerAddress The broker address (can be null, defaults to an empty string).
   * @return A unique client ID in the format: {@code clientName-brokerAddress-timestamp}.
   */
  public static String generateClientId(String clientName, String brokerAddress) {
    String resolvedClientName = (clientName != null) ? clientName : "kc";
    String resolvedBrokerAddress = (brokerAddress != null) ? brokerAddress : "";

    return String.format("%s-%s-%d", resolvedClientName, resolvedBrokerAddress, System.currentTimeMillis());
  }
}
