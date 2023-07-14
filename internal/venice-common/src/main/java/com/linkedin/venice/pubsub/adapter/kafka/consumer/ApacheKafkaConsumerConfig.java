package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.VeniceClusterConfig;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminConfig;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaConsumerConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaConsumerConfig.class);

  public static final String KAFKA_AUTO_OFFSET_RESET_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
  public static final String KAFKA_ENABLE_AUTO_COMMIT_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
  public static final String KAFKA_FETCH_MIN_BYTES_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
  public static final String KAFKA_FETCH_MAX_BYTES_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
  public static final String KAFKA_MAX_POLL_RECORDS_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
  public static final String KAFKA_FETCH_MAX_WAIT_MS_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
  public static final String KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
  public static final String KAFKA_CONSUMER_POLL_RETRY_TIMES_CONFIG =
      KAFKA_CONFIG_PREFIX + ApacheKafkaConsumerAdapter.CONSUMER_POLL_RETRY_TIMES_CONFIG;
  public static final String KAFKA_CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG =
      KAFKA_CONFIG_PREFIX + ApacheKafkaConsumerAdapter.CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG;
  public static final String KAFKA_CLIENT_ID_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.CLIENT_ID_CONFIG;
  public static final String KAFKA_GROUP_ID_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.GROUP_ID_CONFIG;

  private final Properties consumerProperties;

  public ApacheKafkaConsumerConfig(VeniceProperties veniceProperties, String consumerName) {
    Properties properties = veniceProperties.toProperties();
    if (veniceProperties.containsKey(ConfigKeys.PUB_SUB_COMPONENTS_USAGE)) {
      if (veniceProperties.getString(ConfigKeys.PUB_SUB_COMPONENTS_USAGE).equals("controller")) {
        properties = ApacheKafkaAdminConfig.preparePubSubSSLProperties(veniceProperties);
      } else if (veniceProperties.getString(ConfigKeys.PUB_SUB_COMPONENTS_USAGE).equals("server")) {
        properties = preparePubSubSSLPropertiesForServer(veniceProperties);
      }
    }
    VeniceProperties postVeniceProperties = new VeniceProperties(properties);

    this.consumerProperties = postVeniceProperties.clipAndFilterNamespace(KAFKA_CONFIG_PREFIX).toProperties();
    if (consumerName != null) {
      consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerName);
    }
    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(postVeniceProperties, this.consumerProperties)) {
      LOGGER.info("Will initialize an SSL Kafka consumer client");
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka consumer client");
    }

    // Copied from KafkaClientFactory
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
  }

  public static Properties preparePubSubSSLPropertiesForServer(VeniceProperties veniceProperties) {

    Properties properties = veniceProperties.toProperties();
    String kafkaBootstrapUrls = ApacheKafkaProducerConfig.getPubsubBrokerAddress(veniceProperties);
    if (veniceProperties.containsKey(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE)) {
      kafkaBootstrapUrls = veniceProperties.getString(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE);
    }

    try {
      Map<String, Map<String, String>> kafkaClusterMap =
          VeniceClusterConfig.getKafkaClusterMapFromStr(veniceProperties);
      VeniceClusterConfig clusterConfig = new VeniceClusterConfig(veniceProperties, kafkaClusterMap);
      if (!kafkaBootstrapUrls.equals(clusterConfig.getKafkaBootstrapServers())) {
        Properties clonedProperties = clusterConfig.getClusterProperties().toProperties();
        clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrls);
        clusterConfig =
            new VeniceClusterConfig(new VeniceProperties(clonedProperties), clusterConfig.getKafkaClusterMap());
      }
      VeniceProperties clusterProperties = clusterConfig.getClusterProperties();
      properties = clusterConfig.getClusterProperties().getPropertiesCopy();
      ApacheKafkaProducerConfig.copyKafkaSASLProperties(clusterProperties, properties, false);
      kafkaBootstrapUrls = clusterConfig.getKafkaBootstrapServers();
      String resolvedKafkaUrl = clusterConfig.getKafkaClusterUrlResolver().apply(kafkaBootstrapUrls);
      if (resolvedKafkaUrl != null) {
        kafkaBootstrapUrls = resolvedKafkaUrl;
      }
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrls);
      SecurityProtocol securityProtocol = clusterConfig.getKafkaSecurityProtocol(kafkaBootstrapUrls);
      if (KafkaSSLUtils.isKafkaSSLProtocol(securityProtocol)) {
        Optional<SSLConfig> sslConfig = clusterConfig.getSslConfig();
        if (!sslConfig.isPresent()) {
          throw new VeniceException("SSLConfig should be present when Kafka SSL is enabled");
        }
        properties.putAll(sslConfig.get().getKafkaSSLConfig());
      }
      properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get Kafka cluster map from Venice properties", e);
    }
    return properties;
  }

  public Properties getConsumerProperties() {
    return consumerProperties;
  }
}
