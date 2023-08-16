package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminConfig.class);

  private final Properties adminProperties;
  private final String brokerAddress;
  private final long topicConfigMaxRetryInMs;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties) {
    this.brokerAddress = veniceProperties.getString(ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS);
    this.adminProperties = getValidAdminProperties(
        veniceProperties.clipAndFilterNamespace(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX).toProperties());
    this.adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(veniceProperties, this.adminProperties)) {
      LOGGER.info("Will initialize an SSL Kafka admin client - bootstrapServers: {}", brokerAddress);
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka admin client - bootstrapServers: {}", brokerAddress);
    }
    this.adminProperties.put(AdminClientConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    this.topicConfigMaxRetryInMs =
        Duration
            .ofSeconds(
                veniceProperties.getLong(
                    ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
                    PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE))
            .toMillis();
  }

  long getTopicConfigMaxRetryInMs() {
    return topicConfigMaxRetryInMs;
  }

  public Properties getAdminProperties() {
    return adminProperties;
  }

  public String getBrokerAddress() {
    return brokerAddress;
  }

  public static Properties getValidAdminProperties(Properties extractedProperties) {
    Properties validProperties = new Properties();
    extractedProperties.forEach((configKey, configVal) -> {
      if (AdminClientConfig.configNames().contains(configKey)) {
        validProperties.put(configKey, configVal);
      }
    });
    return validProperties;
  }
}
