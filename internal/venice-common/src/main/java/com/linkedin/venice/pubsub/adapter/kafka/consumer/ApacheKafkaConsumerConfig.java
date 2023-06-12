package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;

import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    this.consumerProperties = veniceProperties.clipAndFilterNamespace(KAFKA_CONFIG_PREFIX).toProperties();
    if (consumerName != null) {
      consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerName);
    }
    // Setup ssl config if needed.
    if (KafkaSSLUtils.validateAndCopyKafkaSSLConfig(veniceProperties, this.consumerProperties)) {
      LOGGER.info("Will initialize an SSL Kafka consumer client");
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka consumer client");
    }

    // Copied from KafkaClientFactory
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
  }

  public Properties getConsumerProperties() {
    return consumerProperties;
  }
}
