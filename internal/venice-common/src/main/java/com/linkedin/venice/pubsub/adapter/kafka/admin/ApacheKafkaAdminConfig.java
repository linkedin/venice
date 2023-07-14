package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.SSLConfig.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminConfig.class);

  private final Properties adminProperties;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties) {
    // this.brokerAddress = ApacheKafkaProducerConfig.getPubsubBrokerAddress(veniceProperties);
    Properties properties = veniceProperties.toProperties();

    if (veniceProperties.containsKey(ConfigKeys.PUB_SUB_COMPONENTS_USAGE)) {
      if (veniceProperties.getString(ConfigKeys.PUB_SUB_COMPONENTS_USAGE).equals("controller")) {
        LOGGER.info("Before preparePubSubSSLProperties: " + veniceProperties);
        properties = preparePubSubSSLProperties(veniceProperties);
      }
    }

    VeniceProperties postVeniceProperties = new VeniceProperties(properties);
    this.adminProperties =
        postVeniceProperties.clipAndFilterNamespace(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX).toProperties();
    // this.adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(postVeniceProperties, this.adminProperties)) {
      LOGGER.info("Will initialize an SSL Kafka admin client");
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka admin client");
    }
    adminProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    if (!adminProperties.contains(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC)) {
      adminProperties.put(
          ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
          DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS);
    }
  }

  public static Properties preparePubSubSSLProperties(VeniceProperties veniceProperties) {
    Properties clonedProperties = veniceProperties.toProperties();
    String brokerAddress = ApacheKafkaProducerConfig.getPubsubBrokerAddress(veniceProperties);
    ;
    if (veniceProperties.containsKey(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE)) {
      brokerAddress = veniceProperties.getString(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE);
    }
    if (veniceProperties.getBooleanWithAlternative(KAFKA_OVER_SSL, SSL_TO_KAFKA_LEGACY, false)) {
      LOGGER.info("Using SSL brokers: " + brokerAddress);
      clonedProperties.setProperty(SSL_KAFKA_BOOTSTRAP_SERVERS, brokerAddress);
    } else {
      LOGGER.info("Using non-SSL brokers: " + brokerAddress);
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, brokerAddress);
    }
    Properties properties = new Properties();

    VeniceProperties clonedVeniceProperties = new VeniceProperties(clonedProperties);
    String kafkaSecurityProtocol =
        clonedVeniceProperties.getString(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name());
    if (!KafkaSSLUtils.isKafkaProtocolValid(kafkaSecurityProtocol)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocol);
    }
    final boolean controllerSslEnabled =
        clonedVeniceProperties.getBoolean(CONTROLLER_SSL_ENABLED, DEFAULT_CONTROLLER_SSL_ENABLED);
    final boolean kafkaNeedsSsl = KafkaSSLUtils.isKafkaSSLProtocol(kafkaSecurityProtocol);
    Optional<SSLConfig> sslConfig;
    if (KafkaSSLUtils.isKafkaSSLProtocol(kafkaSecurityProtocol)) {
      sslConfig = Optional.of(new SSLConfig(clonedVeniceProperties));
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
      properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSecurityProtocol);
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, clonedVeniceProperties.getString(SSL_KAFKA_BOOTSTRAP_SERVERS));
      LOGGER.info("Going with SSL brokers: " + clonedVeniceProperties.getString(SSL_KAFKA_BOOTSTRAP_SERVERS));
    } else {
      properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, clonedProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS));
      LOGGER.info("Going with non-SSL brokers: " + clonedVeniceProperties.getString(KAFKA_BOOTSTRAP_SERVERS));
    }
    return properties;
  }

  public Properties getAdminProperties() {
    return adminProperties;
  }
}
