package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.VeniceClusterConfig;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
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
      String pubSubComponentsUsage = veniceProperties.getString(ConfigKeys.PUB_SUB_COMPONENTS_USAGE);
      if (pubSubComponentsUsage.equals(PubSubClientsFactory.PUB_SUB_CLIENT_USAGE_FOR_CONTROLLER)) {
        properties = preparePubSubSSLPropertiesForController(veniceProperties);
        properties.setProperty(ApacheKafkaConsumerConfig.KAFKA_AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * Reason to disable auto_commit
         * 1. {@link AdminConsumptionTask} is persisting {@link com.linkedin.venice.offsets.OffsetRecord} in Zookeeper.
         */
        properties.setProperty(ApacheKafkaConsumerConfig.KAFKA_ENABLE_AUTO_COMMIT_CONFIG, "false");
      } else if (pubSubComponentsUsage.equals(PubSubClientsFactory.PUB_SUB_CLIENT_USAGE_FOR_SERVER)) {
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

    Map<String, Map<String, String>> kafkaClusterMap = VeniceClusterConfig.getKafkaClusterMapFromStr(veniceProperties);
    VeniceClusterConfig clusterConfig = new VeniceClusterConfig(veniceProperties, kafkaClusterMap);
    if (!kafkaBootstrapUrls.equals(clusterConfig.getKafkaBootstrapServers())) {
      Properties clonedProperties = clusterConfig.getClusterProperties().toProperties();
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrls);
      clusterConfig =
          new VeniceClusterConfig(new VeniceProperties(clonedProperties), clusterConfig.getKafkaClusterMap());
    }

    if (veniceProperties.containsKey(PUB_SUB_CONSUMER_LOCAL_CONSUMPTION)) {
      properties.putAll(getCommonKafkaConsumerPropertiesForServer(clusterConfig));
      if (veniceProperties.getBoolean(PUB_SUB_CONSUMER_LOCAL_CONSUMPTION)) {
        if (!clusterConfig.getKafkaConsumerConfigsForLocalConsumption().isEmpty()) {
          properties.putAll(clusterConfig.getKafkaConsumerConfigsForLocalConsumption().toProperties());
        }
      } else {
        if (!clusterConfig.getKafkaConsumerConfigsForRemoteConsumption().isEmpty()) {
          properties.putAll(clusterConfig.getKafkaConsumerConfigsForRemoteConsumption().toProperties());
        }
      }
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
    return properties;
  }

  private static Properties getCommonKafkaConsumerPropertiesForServer(VeniceClusterConfig serverConfig) {
    Properties kafkaConsumerProperties = serverConfig.getClusterProperties().getPropertiesCopy();
    ApacheKafkaProducerConfig
        .copyKafkaSASLProperties(serverConfig.getClusterProperties(), kafkaConsumerProperties, false);
    kafkaConsumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig.getKafkaBootstrapServers());
    kafkaConsumerProperties.setProperty(KAFKA_AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Venice is persisting offset in local offset db.
    kafkaConsumerProperties.setProperty(KAFKA_ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaConsumerProperties
        .setProperty(KAFKA_FETCH_MIN_BYTES_CONFIG, String.valueOf(serverConfig.getKafkaFetchMinSizePerSecond()));
    kafkaConsumerProperties
        .setProperty(KAFKA_FETCH_MAX_BYTES_CONFIG, String.valueOf(serverConfig.getKafkaFetchMaxSizePerSecond()));
    /**
     * The following setting is used to control the maximum number of records to returned in one poll request.
     */
    kafkaConsumerProperties
        .setProperty(KAFKA_MAX_POLL_RECORDS_CONFIG, Integer.toString(serverConfig.getKafkaMaxPollRecords()));
    kafkaConsumerProperties
        .setProperty(KAFKA_FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(serverConfig.getKafkaFetchMaxTimeMS()));
    kafkaConsumerProperties.setProperty(
        KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG,
        String.valueOf(serverConfig.getKafkaFetchPartitionMaxSizePerSecond()));
    kafkaConsumerProperties
        .setProperty(KAFKA_CONSUMER_POLL_RETRY_TIMES_CONFIG, String.valueOf(serverConfig.getKafkaPollRetryTimes()));
    kafkaConsumerProperties.setProperty(
        KAFKA_CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG,
        String.valueOf(serverConfig.getKafkaPollRetryBackoffMs()));

    return kafkaConsumerProperties;
  }

  public static Properties preparePubSubSSLPropertiesForController(VeniceProperties veniceProperties) {
    Properties clonedProperties = veniceProperties.toProperties();
    String brokerAddress = ApacheKafkaProducerConfig.getPubsubBrokerAddress(veniceProperties);
    if (veniceProperties.containsKey(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE)) {
      brokerAddress = veniceProperties.getString(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE);
    }

    if (veniceProperties.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false)) {
      clonedProperties.setProperty(SSL_KAFKA_BOOTSTRAP_SERVERS, brokerAddress);
    } else {
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, brokerAddress);
    }

    Properties properties = new Properties();
    VeniceProperties clonedVeniceProperties = new VeniceProperties(clonedProperties);
    String kafkaSecurityProtocol =
        clonedVeniceProperties.getString(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name());
    ApacheKafkaProducerConfig.copyKafkaSASLProperties(clonedProperties, properties, false);
    if (!KafkaSSLUtils.isKafkaProtocolValid(kafkaSecurityProtocol)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocol);
    }

    Optional<SSLConfig> sslConfig;
    if (KafkaSSLUtils.isKafkaSSLProtocol(kafkaSecurityProtocol)) {
      sslConfig = Optional.of(new SSLConfig(clonedVeniceProperties));
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
      properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSecurityProtocol);
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, clonedVeniceProperties.getString(SSL_KAFKA_BOOTSTRAP_SERVERS));
    } else {
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, clonedProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS));
    }
    return properties;
  }

  public Properties getConsumerProperties() {
    return consumerProperties;
  }
}
