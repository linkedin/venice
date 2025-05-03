package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_TIMES;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_TIMES_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.PUBSUB_KAFKA_CLIENT_CONFIG_PREFIX;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.adapter.kafka.TopicPartitionsOffsetsTracker;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Configuration for Apache Kafka consumer.
 */
public class ApacheKafkaConsumerConfig {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaConsumerConfig.class);

  private static final String KAFKA_FETCH_MIN_BYTES_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
  private static final String KAFKA_FETCH_MAX_BYTES_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
  private static final String KAFKA_MAX_POLL_RECORDS_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
  private static final String KAFKA_FETCH_MAX_WAIT_MS_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
  private static final String KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG =
      KAFKA_CONFIG_PREFIX + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
  private static final String KAFKA_RECEIVE_BUFFER_CONFIG = KAFKA_CONFIG_PREFIX + ConsumerConfig.RECEIVE_BUFFER_CONFIG;
  protected static final int DEFAULT_RECEIVE_BUFFER_SIZE = 1024 * 1024;

  /**
   * Use the following prefix to get the consumer properties from the {@link VeniceProperties} object.
   */
  private static final String PUBSUB_KAFKA_CONSUMER_CONFIG_PREFIX =
      PubSubUtil.getPubSubConsumerConfigPrefix(KAFKA_CONFIG_PREFIX);
  protected static final Set<String> KAFKA_CONSUMER_PREFIXES = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(KAFKA_CONFIG_PREFIX, PUBSUB_KAFKA_CLIENT_CONFIG_PREFIX, PUBSUB_KAFKA_CONSUMER_CONFIG_PREFIX)));

  private final Properties consumerProperties;
  private final int consumerPollRetryTimes;
  private final int consumerPollRetryBackoffMs;
  private final int topicQueryRetryTimes;
  private final int topicQueryRetryIntervalMs;
  private final Duration defaultApiTimeout;
  private final boolean shouldCheckTopicExistenceBeforeConsuming;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private TopicPartitionsOffsetsTracker offsetsTracker;

  ApacheKafkaConsumerConfig(PubSubConsumerAdapterContext context) {
    VeniceProperties veniceProperties = context.getVeniceProperties();
    this.consumerProperties = ApacheKafkaUtils.getValidKafkaClientProperties(
        veniceProperties,
        context.getPubSubSecurityProtocol(),
        ConsumerConfig.configNames(),
        KAFKA_CONSUMER_PREFIXES);

    // Override the broker address after filtering the properties to ensure that we use the correct broker address.
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getPubSubBrokerAddress());
    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, context.getConsumerName());
    pubSubMessageDeserializer = context.getPubSubMessageDeserializer();
    offsetsTracker = context.isOffsetCollectionEnabled() ? new TopicPartitionsOffsetsTracker() : null;

    // Timeout for consumer APIs which do not have explicit timeout parameter AND have potential to get blocked;
    // When this is not specified, Kafka consumer will use default value of 1 minute.
    int defaultApiTimeoutInMs = veniceProperties
        .getInt(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS, PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE);
    defaultApiTimeout = Duration.ofMillis(defaultApiTimeoutInMs);
    consumerProperties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutInMs);

    // Number of times to retry poll() upon failure
    consumerPollRetryTimes =
        veniceProperties.getInt(PUBSUB_CONSUMER_POLL_RETRY_TIMES, PUBSUB_CONSUMER_POLL_RETRY_TIMES_DEFAULT_VALUE);

    // Backoff time in milliseconds between poll() retries
    consumerPollRetryBackoffMs = veniceProperties
        .getInt(PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS, PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT_VALUE);

    topicQueryRetryTimes = veniceProperties
        .getInt(PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES, PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_TIMES_DEFAULT_VALUE);

    topicQueryRetryIntervalMs = veniceProperties.getInt(
        PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS,
        PUBSUB_CONSUMER_TOPIC_QUERY_RETRY_INTERVAL_MS_DEFAULT_VALUE);

    shouldCheckTopicExistenceBeforeConsuming = veniceProperties
        .getBoolean(PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE, PUBSUB_CONSUMER_CHECK_TOPIC_EXISTENCE_DEFAULT_VALUE);

    // Do not change the default value of the following configs unless you know what you are doing.
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    LOGGER.debug("Created ApacheKafkaConsumerConfig: {} - consumerProperties: {}", this, consumerProperties);
  }

  @Override
  public String toString() {
    return "ApacheKafkaConsumerConfig{brokerAddress=" + consumerProperties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
        + ", securityProtocol=" + consumerProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) + ", clientId="
        + consumerProperties.get(ConsumerConfig.CLIENT_ID_CONFIG) + ", consumerPollRetryTimes=" + consumerPollRetryTimes
        + ", consumerPollRetryBackoffMs=" + consumerPollRetryBackoffMs + "}";
  }

  Properties getConsumerProperties() {
    return consumerProperties;
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

  PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  TopicPartitionsOffsetsTracker getOffsetsTracker() {
    return offsetsTracker;
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

  @VisibleForTesting
  void setTopicPartitionsOffsetsTracker(TopicPartitionsOffsetsTracker offsetsTracker) {
    this.offsetsTracker = offsetsTracker;
  }

  private static Properties getDefaultKafkaConsumerPropertiesForServers() {
    Properties properties = new Properties();

    /**
     * The maximum number of records returned in a single call to poll().
     */
    properties.put(KAFKA_MAX_POLL_RECORDS_CONFIG, 100);

    /**
     * Minimum data the server should return for a fetch request. Higher values can improve throughput
     * but may add latency if data is not immediately available.
     */
    properties.put(KAFKA_FETCH_MIN_BYTES_CONFIG, 1);

    /**
     * Maximum data the server should return for a fetch request. Not a strict limit—if the first record
     * batch exceeds this size, it will still be returned to allow consumer progress. Multiple fetches
     * may occur in parallel.
     */
    properties.put(KAFKA_FETCH_MAX_BYTES_CONFIG, ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);

    /**
     * Maximum time the server will wait to respond to a fetch request if {@link KAFKA_FETCH_MIN_BYTES_CONFIG}
     * is not yet satisfied.
     */
    properties.put(KAFKA_FETCH_MAX_WAIT_MS_CONFIG, 500);

    /**
     * Maximum data the server will return per partition in a fetch request. Not a strict limit—if the first
     * record batch exceeds it, the batch is still returned to ensure consumer progress.
     * See {@link KAFKA_FETCH_MAX_BYTES_CONFIG} to limit total fetch size.
     */
    properties.put(KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG, ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

    /**
     * Size of the TCP receive buffer (SO_RCVBUF) used for reading data. A value of -1 uses the OS default.
     */
    properties.put(KAFKA_RECEIVE_BUFFER_CONFIG, DEFAULT_RECEIVE_BUFFER_SIZE);

    return properties;
  }
}
