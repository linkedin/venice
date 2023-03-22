package com.linkedin.venice.kafka.admin;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminConfig.class);

  private final Properties adminProperties;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties, String brokerAddressToOverride) {
    this.adminProperties = veniceProperties.toProperties();
    /*
    this.adminProperties = new Properties();
    
    
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(veniceProperties, this.adminProperties)) {
    LOGGER.info("Will initialize a SSL Kafka admin client.");
    if (veniceProperties.getBoolean("controllerOrNot")) {
    LOGGER.info("For controller.");
    adminProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, veniceProperties.getString(SSL_KAFKA_BOOTSTRAP_SERVERS));
    } else {
    LOGGER.info("For server.");
    adminProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, veniceProperties.getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }
    
    } else {
    LOGGER.info("Will initialize a non-SSL Kafka admin client. \n" + veniceProperties);
    if (veniceProperties.getBoolean("controllerOrNot")) {
    LOGGER.info("For controller.");
    adminProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, veniceProperties.getString(KAFKA_BOOTSTRAP_SERVERS));
    } else {
    LOGGER.info("For server.");
    adminProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, veniceProperties.getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }
    }
    
    // Setup ssl config if needed.
    if (brokerAddressToOverride != null) {
    adminProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerAddressToOverride);
    }
    */
    // Copied from KafkaClientFactory
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
