package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CLIENT_CONFIG_PREFIX;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS;
import static com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils.generateClientId;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubMessageSerializer;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class holds all properties used to construct {@link ApacheKafkaProducerAdapter}
 * (This class could be refactored to hold consumer properties as well).
 *
 * Tune and adjust the configs in this class to control the behavior of Apache Kafka producer.
 */
public class ApacheKafkaProducerConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaProducerConfig.class);

  /**
   * Legacy Kafka configs are using only kafka prefix. But now we are using pubsub.kafka prefix for all Kafka configs.
   */
  public static final String KAFKA_CONFIG_PREFIX = "kafka.";
  public static final String PUBSUB_KAFKA_CLIENT_CONFIG_PREFIX = PUBSUB_CLIENT_CONFIG_PREFIX + KAFKA_CONFIG_PREFIX;

  public static final String KAFKA_SECURITY_PROTOCOL =
      KAFKA_CONFIG_PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
  public static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String KAFKA_PRODUCER_RETRIES_CONFIG = KAFKA_CONFIG_PREFIX + ProducerConfig.RETRIES_CONFIG;
  public static final String KAFKA_LINGER_MS = KAFKA_CONFIG_PREFIX + ProducerConfig.LINGER_MS_CONFIG;
  public static final String KAFKA_BUFFER_MEMORY = KAFKA_CONFIG_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG;
  public static final String KAFKA_CLIENT_ID = KAFKA_CONFIG_PREFIX + ProducerConfig.CLIENT_ID_CONFIG;
  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS =
      KAFKA_CONFIG_PREFIX + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS =
      KAFKA_CONFIG_PREFIX + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
  public static final String SSL_KAFKA_BOOTSTRAP_SERVERS = "ssl." + KAFKA_BOOTSTRAP_SERVERS;

  /**
   * N.B. do not attempt to change spelling, "kakfa", without carefully replacing all instances in use and some of them
   * may be external to this repo
   * @deprecated Use {@link #KAFKA_OVER_SSL} instead
   */
  @Deprecated
  public static final String SSL_TO_KAFKA_LEGACY = "ssl.to.kakfa";
  public static final String KAFKA_OVER_SSL = KAFKA_CONFIG_PREFIX + "over.ssl";

  /**
   * Default Kafka batch size and linger time for better producer performance during ingestion.
   */
  public static final String DEFAULT_KAFKA_BATCH_SIZE = "524288";
  public static final String DEFAULT_KAFKA_LINGER_MS = "1000";

  private final Properties producerProperties;
  private final PubSubMessageSerializer pubSubMessageSerializer;

  public ApacheKafkaProducerConfig(PubSubProducerAdapterContext context) {
    String brokerAddressToOverride = context.getBrokerAddress();
    String producerName = context.getProducerName();
    VeniceProperties allVeniceProperties = context.getVeniceProperties();
    boolean strictConfigs = context.shouldValidateProducerConfigStrictly();
    this.pubSubMessageSerializer = context.getPubSubMessageSerializer();
    String brokerAddress =
        brokerAddressToOverride != null ? brokerAddressToOverride : getPubsubBrokerAddress(allVeniceProperties);
    this.producerProperties =
        ApacheKafkaUtils.getValidKafkaClientProperties(allVeniceProperties, ProducerConfig.configNames());

    this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
    validateAndUpdateProperties(this.producerProperties, strictConfigs);
    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, generateClientId(producerName, brokerAddress));

    if (allVeniceProperties.getBoolean(PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS, false)) {
      addHighThroughputDefaults();
    }

    if (context.isProducerCompressionEnabled()) {
      this.producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, context.getCompressionType());
    }

    // Do not remove the following configurations unless you fully understand the implications.
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
  }

  /**
   * Setup default batch size and linger time for better producing performance during server new push ingestion.
   * These configs are set for large volume ingestion, not for integration test.
   */
  private void addHighThroughputDefaults() {
    if (!producerProperties.containsKey(ProducerConfig.BATCH_SIZE_CONFIG)) {
      producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, DEFAULT_KAFKA_BATCH_SIZE);
    }
    if (!producerProperties.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
      producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_KAFKA_LINGER_MS);
    }
  }

  public Properties getProducerProperties() {
    return producerProperties;
  }

  public static String getPubsubBrokerAddress(VeniceProperties properties) {
    if (Boolean.parseBoolean(properties.getStringWithAlternative(SSL_TO_KAFKA_LEGACY, KAFKA_OVER_SSL, "false"))) {
      checkProperty(properties, SSL_KAFKA_BOOTSTRAP_SERVERS);
      return properties.getString(SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
    checkProperty(properties, KAFKA_BOOTSTRAP_SERVERS);
    return properties.getString(KAFKA_BOOTSTRAP_SERVERS);
  }

  private static void checkProperty(VeniceProperties properties, String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Kafka producer factory. Required property: " + key + " is missing.");
    }
  }

  public String getBrokerAddress() {
    return producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
  }

  private void validateAndUpdateProperties(Properties kafkaProducerProperties, boolean strictConfigs) {
    // This is to guarantee ordering, even in the face of failures.
    validateOrPopulateProp(
        kafkaProducerProperties,
        strictConfigs,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        "1");
    // This will ensure the durability on Kafka broker side
    validateOrPopulateProp(kafkaProducerProperties, strictConfigs, ProducerConfig.ACKS_CONFIG, "all");

    if (!kafkaProducerProperties.containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
      kafkaProducerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "300000"); // 5min
    }

    if (!kafkaProducerProperties.containsKey(ProducerConfig.RETRIES_CONFIG)) {
      kafkaProducerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    }

    if (!kafkaProducerProperties.contains(ProducerConfig.RETRY_BACKOFF_MS_CONFIG)) {
      // Hard-coded backoff config to be 1 sec
      validateOrPopulateProp(kafkaProducerProperties, strictConfigs, ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
    }

    if (!kafkaProducerProperties.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG)) {
      // Block if buffer is full
      validateOrPopulateProp(
          kafkaProducerProperties,
          strictConfigs,
          ProducerConfig.MAX_BLOCK_MS_CONFIG,
          String.valueOf(Long.MAX_VALUE));
    }

    if (kafkaProducerProperties.containsKey(ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
      LOGGER.info(
          "Compression type explicitly specified by config: {}",
          kafkaProducerProperties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    } else {
      /**
       * In general, 'gzip' compression ratio is the best among all the available codecs:
       * 1. none
       * 2. lz4
       * 3. gzip
       * 4. snappy
       *
       * We want to minimize the cross-COLO bandwidth usage.
       */
      kafkaProducerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    }
  }

  /**
   * Function which sets some required defaults. Also bubbles up an exception in
   * order to fail fast if any calling class tries to override these defaults.
   */
  private void validateOrPopulateProp(
      Properties properties,
      boolean strictConfigs,
      String requiredConfigKey,
      String requiredConfigValue) {
    String actualConfigValue = properties.getProperty(requiredConfigKey);
    if (actualConfigValue == null) {
      properties.setProperty(requiredConfigKey, requiredConfigValue);
    } else if (!actualConfigValue.equals(requiredConfigValue) && strictConfigs) {
      // We fail fast rather than attempting to use non-standard serializers
      throw new VeniceException(
          "The Kafka Producer must use certain configuration settings in order to work properly. "
              + "requiredConfigKey: '" + requiredConfigKey + "', requiredConfigValue: '" + requiredConfigValue
              + "', actualConfigValue: '" + actualConfigValue + "'.");
    }
  }

  public static void copyKafkaSASLProperties(Properties configuration, Properties properties, boolean stripPrefix) {
    String saslConfiguration = configuration.getProperty("kafka.sasl.jaas.config", "");
    if (saslConfiguration != null && !saslConfiguration.isEmpty()) {
      if (stripPrefix) {
        properties.put("sasl.jaas.config", saslConfiguration);
      } else {
        properties.put("kafka.sasl.jaas.config", saslConfiguration);
      }
    }

    String saslMechanism = configuration.getProperty("kafka.sasl.mechanism", "");
    if (saslMechanism != null && !saslMechanism.isEmpty()) {
      if (stripPrefix) {
        properties.put("sasl.mechanism", saslMechanism);
      } else {
        properties.put("kafka.sasl.mechanism", saslMechanism);
      }
    }

    String securityProtocol = configuration.getProperty("kafka.security.protocol", "");
    if (securityProtocol != null && !securityProtocol.isEmpty()) {
      if (stripPrefix) {
        properties.put("security.protocol", securityProtocol);
      } else {
        properties.put("kafka.security.protocol", securityProtocol);
      }
    }
  }

  public PubSubMessageSerializer getPubSubMessageSerializer() {
    return pubSubMessageSerializer;
  }
}
