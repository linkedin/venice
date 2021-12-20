package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.davinci.kafka.consumer.StoreIngestionTask.*;


/**
 * This class is used to throttle records consumed per Kafka cluster
 */
public class KafkaClusterBasedRecordThrottler {
  private static final Logger logger = LogManager.getLogger(KafkaClusterBasedRecordThrottler.class);
  // Kafka URL to records throttler
  private final Map<String, EventThrottler> kafkaUrlToRecordsThrottler;
  // Kafka URL to throttled records
  protected Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> kafkaUrlToThrottledRecords;

  public KafkaClusterBasedRecordThrottler(Map<String, EventThrottler> kafkaUrlToRecordsThrottler) {
    this.kafkaUrlToRecordsThrottler = kafkaUrlToRecordsThrottler;
    this.kafkaUrlToThrottledRecords = new VeniceConcurrentHashMap<>();
  }

  public ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(KafkaConsumerWrapper consumer, String kafkaUrl, long pollTimeoutMs) {
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords;
    if (kafkaUrlToThrottledRecords.containsKey(kafkaUrl)) {
      consumerRecords = kafkaUrlToThrottledRecords.get(kafkaUrl);
    } else {
      consumerRecords = consumer.poll(pollTimeoutMs);
      if (consumerRecords == null) {
        consumerRecords = ConsumerRecords.empty();
      }
    }

    if (kafkaUrlToRecordsThrottler != null && kafkaUrlToRecordsThrottler.containsKey(kafkaUrl)) {
      try {
        kafkaUrlToRecordsThrottler.get(kafkaUrl).maybeThrottle(consumerRecords.count());
        // if code reaches here, then the consumer records should be ingested to storage engine
        kafkaUrlToThrottledRecords.remove(kafkaUrl);
      } catch (QuotaExceededException quotaExceededException) {
        String msgIdentifier = kafkaUrl + "_records_quota_exceeded";
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
          logger.info("Ingestion quota exceeded for Kafka URL " + kafkaUrl);
        }

        kafkaUrlToThrottledRecords.put(kafkaUrl, consumerRecords);

        try {
          Thread.sleep(pollTimeoutMs);
        } catch (InterruptedException interruptedException) {
          throw new VeniceException("Kafka Cluster based throttled records poll sleep got interrupted", interruptedException);
        }

        // Return early so we don't ingest these records
        return ConsumerRecords.empty();
      }
    }

    return consumerRecords;
  }
}
