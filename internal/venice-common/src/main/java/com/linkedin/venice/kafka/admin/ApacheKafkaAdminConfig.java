package com.linkedin.venice.kafka.admin;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminConfig.class);

  private final Properties adminProperties;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties, String brokerAddressToOverride) {
    String brokerAddress = getPubsubBrokerAddress(veniceProperties);
    this.adminProperties = veniceProperties.clipAndFilterNamespace(KAFKA_CONFIG_PREFIX).toProperties();
    this.adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
    LOGGER.info(
        "Kafka admin client will connect to broker address: " + brokerAddress + " with properties: " + veniceProperties
            + " brokerAddressToOverride: " + brokerAddressToOverride);
    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(veniceProperties, this.adminProperties)) {
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
    LOGGER.info("Kafka admin client properties: " + adminProperties);
  }

  public Properties getAdminProperties() {
    return adminProperties;
  }

}
