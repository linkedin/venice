package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POSITION_RESET_STRATEGY;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;

import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration for Apache Kafka consumer.
 */
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
  public static final String KAFKA_CLIENT_ID_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.CLIENT_ID_CONFIG;
  public static final String KAFKA_GROUP_ID_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.GROUP_ID_CONFIG;
  public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 1024 * 1024;

  private final Properties consumerProperties;
  private final boolean isSslEnabled;
  private final int consumerPollRetryTimes;
  private final int consumerPollRetryBackoffMs;
  private final int topicQueryRetryTimes;
  private final int topicQueryRetryIntervalMs;
  private final Duration defaultApiTimeout;
  private final boolean shouldCheckTopicExistenceBeforeConsuming;

  ApacheKafkaConsumerConfig(VeniceProperties veniceProperties, String consumerName) {
    this.consumerProperties =
        getValidConsumerProperties(veniceProperties.clipAndFilterNamespace(KAFKA_CONFIG_PREFIX).toProperties());
    if (consumerName != null) {
      consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerName);
    }

    // Setup ssl config if needed.
    isSslEnabled = KafkaSSLUtils.validateAndCopyKafkaSSLConfig(veniceProperties, this.consumerProperties);

    if (!consumerProperties.containsKey(ConsumerConfig.RECEIVE_BUFFER_CONFIG)) {
      consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, DEFAULT_RECEIVE_BUFFER_SIZE);
    }

    if (!consumerProperties.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
        && veniceProperties.containsKey(PUBSUB_CONSUMER_POSITION_RESET_STRATEGY)) {
      consumerProperties.put(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          veniceProperties.getString(PUBSUB_CONSUMER_POSITION_RESET_STRATEGY));
    }

    // Do not change the default value of the following two configs unless you know what you are doing.
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    // Timeout for consumer APIs which do not have explicit timeout parameter AND have potential to get blocked;
    // When this is not specified, Kafka consumer will use default value of 1 minute.

    int defaultApiTimeoutInMs = veniceProperties.getInt(
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS,
        PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE);
    defaultApiTimeout = Duration.ofMillis(defaultApiTimeoutInMs);
    consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutInMs);

    // Number of times to retry poll() upon failure
    consumerPollRetryTimes = veniceProperties.getInt(
        PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_TIMES,
        PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_TIMES_DEFAULT_VALUE);

    // Backoff time in milliseconds between poll() retries
    consumerPollRetryBackoffMs = veniceProperties.getInt(
        PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS,
        PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT_VALUE);

    topicQueryRetryTimes = veniceProperties.getInt(
        PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES,
        PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES_DEFAULT_VALUE);

    topicQueryRetryIntervalMs = veniceProperties.getInt(
        PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS,
        PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS_DEFAULT_VALUE);

    shouldCheckTopicExistenceBeforeConsuming = veniceProperties.getBoolean(
        PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE,
        PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE_DEFAULT_VALUE);

    LOGGER.debug("Created ApacheKafkaConsumerConfig: {} - consumerProperties: {}", this, consumerProperties);
  }

  @Override
  public String toString() {
    return "ApacheKafkaConsumerConfig{brokerAddress=" + consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
        + ", isSslEnabled=" + isSslEnabled + ", consumerPollRetryTimes=" + consumerPollRetryTimes
        + ", consumerPollRetryBackoffMs=" + consumerPollRetryBackoffMs + "}";
  }

  Properties getConsumerProperties() {
    return consumerProperties;
  }

  boolean isSslEnabled() {
    return isSslEnabled;
  }

  int getConsumerPollRetryTimes() {
    return consumerPollRetryTimes;
  }

  int getConsumerPollRetryBackoffMs() {
    return consumerPollRetryBackoffMs;
  }

  Duration getDefaultApiTimeout() {
    return defaultApiTimeout;
  }

  int getTopicQueryRetryTimes() {
    return topicQueryRetryTimes;
  }

  int getTopicQueryRetryIntervalMs() {
    return topicQueryRetryIntervalMs;
  }

  boolean shouldCheckTopicExistenceBeforeConsuming() {
    return shouldCheckTopicExistenceBeforeConsuming;
  }

  public static Properties getValidConsumerProperties(Properties extractedProperties) {
    Properties validProperties = new Properties();
    extractedProperties.forEach((configKey, configVal) -> {
      if (ConsumerConfig.configNames().contains(configKey)) {
        validProperties.put(configKey, configVal);
      }
    });
    return validProperties;
  }
}
