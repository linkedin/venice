package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
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
  private final Duration defaultApiTimeout;

  public ApacheKafkaAdminConfig(VeniceProperties veniceProperties) {
    this.brokerAddress = veniceProperties.getString(ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS);
    this.adminProperties =
        ApacheKafkaUtils.getValidKafkaClientProperties(veniceProperties, AdminClientConfig.configNames());
    this.adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
    // Setup ssl config if needed.
    this.adminProperties.put(AdminClientConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    this.topicConfigMaxRetryInMs =
        Duration
            .ofSeconds(
                veniceProperties.getLong(
                    ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC,
                    PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE))
            .toMillis();

    int defaultApiTimeoutInMs = veniceProperties.getInt(
        PubSubConstants.PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS,
        PubSubConstants.PUBSUB_ADMIN_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE);
    defaultApiTimeout = Duration.ofMillis(defaultApiTimeoutInMs);
    this.adminProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, defaultApiTimeoutInMs);
    this.adminProperties
        .put(AdminClientConfig.CLIENT_ID_CONFIG, ApacheKafkaUtils.generateClientId("KcAdmin", brokerAddress));

    LOGGER.debug("Created ApacheKafkaAdminConfig: {} - adminProperties: {}", this, adminProperties);
  }

  @Override
  public String toString() {
    return "ApacheKafkaAdminConfig{brokerAddress=" + adminProperties.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
        + ", defaultApiTimeout=" + defaultApiTimeout + "}";
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
}
