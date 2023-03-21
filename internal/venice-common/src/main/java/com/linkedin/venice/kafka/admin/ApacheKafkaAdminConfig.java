package com.linkedin.venice.kafka.admin;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminConfig.class);

  private final Properties adminProperties;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties, String brokerAddressToOverride) {
    this.adminProperties = veniceProperties.toProperties();
    if (brokerAddressToOverride != null) {
      adminProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerAddressToOverride);
    }
    // Copied from KafkaClientFactory
    adminProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    if (!adminProperties.contains(ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC)) {
      adminProperties.put(
          ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
          DEFAULT_KAFKA_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS);
    }
  }

  public Properties getAdminProperties() {
    return adminProperties;
  }

}
