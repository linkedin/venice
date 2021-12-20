package com.linkedin.venice.kafka.validation;

import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * This class is a library that can validate the Kafka message during consumption, which can be used in
 * Venice-Server/Da-Vinci and ETL. In high level, it keeps track of messages produced by different producers,
 * and validates data integrity in 4 perspectives:
 * 1. Whether a segment starts from a non-zero sequence number (UNREGISTERED_PRODUCER);
 * 2. Whether there is a gap between segments or within a segment (MISSING);
 * 3. Whether data within a segment is corrupted (CORRUPT);
 * 4. Whether producers have produced duplicate messages, which is fine and expected due to producer retries (DUPLICATE).
 */
public class KafkaDataIntegrityValidator {
  private final String kafkaVersionTopic;
  private final long kafkaLogCompactionDelayInMs;

  /** Keeps track of every upstream producer this consumer task has seen so far. */
  protected final Map<GUID, ProducerTracker> producerTrackerMap;
  protected final Function<GUID, ProducerTracker> producerTrackerCreator;

  public KafkaDataIntegrityValidator(String kafkaVersionTopic) {
    this(kafkaVersionTopic, -1);
  }

  public KafkaDataIntegrityValidator(String kafkaVersionTopic, long kafkaLogCompactionDelayInMs) {
    this.kafkaVersionTopic = kafkaVersionTopic;
    this.kafkaLogCompactionDelayInMs = kafkaLogCompactionDelayInMs;
    this.producerTrackerMap = new VeniceConcurrentHashMap<>();
    this.producerTrackerCreator = guid -> new ProducerTracker(guid, kafkaVersionTopic);
  }

  public ProducerTracker registerProducer(GUID producerGuid) {
    return producerTrackerMap.computeIfAbsent(producerGuid, producerTrackerCreator);
  }

  public void clearPartition(int partition) {
    producerTrackerMap.values().forEach(
        producerTracker -> producerTracker.clearPartition(partition)
    );
  }

  /**
   * Run a thorough DIV check on the message, including UNREGISTERED_PRODUCER, MISSING, CORRUPT and DUPLICATE;
   * also return a closure to transform {@link com.linkedin.venice.offsets.OffsetRecord} in ingestion services lazily.
   *
   * This API is mainly used in Venice-Server/Da-Vinci.
   */
  public OffsetRecordTransformer validateMessageAndGetOffsetRecordTransformer(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      boolean endOfPushReceived,
      boolean tolerateMissingMsgs) throws DataValidationException {
    final GUID producerGUID = consumerRecord.value().producerMetadata.producerGUID;
    ProducerTracker producerTracker = registerProducer(producerGUID);
    return producerTracker.validateMessageAndGetOffsetRecordTransformer(consumerRecord, endOfPushReceived, tolerateMissingMsgs);
  }

  /**
   * Run a thorough DIV check on the message, including UNREGISTERED_PRODUCER, MISSING, CORRUPT and DUPLICATE.
   *
   * This API can be used for ETL.
   */
  public Segment validateMessage(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      boolean endOfPushReceived, boolean tolerateMissingMsgs) throws DataValidationException {
    final GUID producerGUID = consumerRecord.value().producerMetadata.producerGUID;
    ProducerTracker producerTracker = registerProducer(producerGUID);
    return producerTracker.validateMessage(consumerRecord, endOfPushReceived, tolerateMissingMsgs);
  }

  /**
   * Only check for missing sequence number; segment starting from a positive sequence number is acceptable considering
   * real-time buffer replay would start in the middle of a segment; checksum is also ignored for the same reason.
   *
   * If missing message happens for an old message that is older than the Kafka log compaction lag threshold, MISSING
   * exception will not be thrown because it's expected that log compaction would compact old messages. However, if data
   * are fresh and missing message is detected, MISSING exception will be thrown.
   *
   * This API can be used for ETL.
   */
  public void checkMissingMessage(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      Optional<ProducerTracker.DIVErrorMetricCallback> errorMetricCallback) throws DataValidationException {
    final GUID producerGUID = consumerRecord.value().producerMetadata.producerGUID;
    ProducerTracker producerTracker = registerProducer(producerGUID);
    producerTracker.checkMissingMessage(consumerRecord, errorMetricCallback, this.kafkaLogCompactionDelayInMs);
  }

}
