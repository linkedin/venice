package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.StoreIngestionTask.REDUNDANT_LOGGING_FILTER;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to throttle records consumed per Kafka cluster
 */
public class KafkaClusterBasedRecordThrottler {
  private static final Logger LOGGER = LogManager.getLogger(KafkaClusterBasedRecordThrottler.class);
  // Kafka URL to records throttler
  private final Map<String, EventThrottler> kafkaUrlToRecordsThrottler;
  // Kafka URL to throttled records
  protected Map<String, Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> kafkaUrlToThrottledRecords;

  public KafkaClusterBasedRecordThrottler(Map<String, EventThrottler> kafkaUrlToRecordsThrottler) {
    this.kafkaUrlToRecordsThrottler = kafkaUrlToRecordsThrottler;
    this.kafkaUrlToThrottledRecords = new VeniceConcurrentHashMap<>();
  }

  public Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(
      PubSubConsumerAdapter consumer,
      String kafkaUrl,
      long pollTimeoutMs) {
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumerRecords =
        kafkaUrlToThrottledRecords.get(kafkaUrl);
    if (consumerRecords == null) {
      consumerRecords = consumer.poll(pollTimeoutMs);
      if (consumerRecords == null) {
        consumerRecords = Collections.emptyMap();
      }
    }

    if (kafkaUrlToRecordsThrottler != null) {
      try {
        EventThrottler eventThrottler = kafkaUrlToRecordsThrottler.get(kafkaUrl);
        if (eventThrottler != null) {
          eventThrottler.maybeThrottle(consumerRecords.values().stream().mapToInt(List::size).sum());
          // if code reaches here, then the consumer records should be ingested to storage engine
          kafkaUrlToThrottledRecords.remove(kafkaUrl);
        }
      } catch (QuotaExceededException quotaExceededException) {
        String msgIdentifier = kafkaUrl + "_records_quota_exceeded";
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
          LOGGER.info("Ingestion quota exceeded for Kafka URL {}", kafkaUrl);
        }

        kafkaUrlToThrottledRecords.put(kafkaUrl, consumerRecords);

        try {
          Thread.sleep(pollTimeoutMs);
        } catch (InterruptedException interruptedException) {
          throw new VeniceException(
              "Kafka Cluster based throttled records poll sleep got interrupted",
              interruptedException);
        }

        // Return early so we don't ingest these records
        return Collections.emptyMap();
      }
    }

    return consumerRecords;
  }
}
