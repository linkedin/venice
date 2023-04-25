package com.linkedin.davinci.validation;

import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;


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
  private final long kafkaLogCompactionDelayInMs;

  /** Keeps track of every upstream producer this consumer task has seen so far. */
  protected final Map<GUID, ProducerTracker> producerTrackerMap;
  protected final Function<GUID, ProducerTracker> producerTrackerCreator;

  public KafkaDataIntegrityValidator(String kafkaVersionTopic) {
    this(kafkaVersionTopic, -1);
  }

  public KafkaDataIntegrityValidator(String kafkaVersionTopic, long kafkaLogCompactionDelayInMs) {
    this.kafkaLogCompactionDelayInMs = kafkaLogCompactionDelayInMs;
    this.producerTrackerMap = new VeniceConcurrentHashMap<>();
    this.producerTrackerCreator = guid -> new ProducerTracker(guid, kafkaVersionTopic);
  }

  public ProducerTracker registerProducer(GUID producerGuid) {
    return producerTrackerMap.computeIfAbsent(producerGuid, producerTrackerCreator);
  }

  public void clearPartition(int partition) {
    producerTrackerMap.values().forEach(producerTracker -> producerTracker.clearPartition(partition));
  }

  /**
   * Run a thorough DIV check on the message, including UNREGISTERED_PRODUCER, MISSING, CORRUPT and DUPLICATE.
   */
  public void validateMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      Lazy<Boolean> tolerateMissingMsgs) throws DataValidationException {
    final GUID producerGUID = consumerRecord.getValue().producerMetadata.producerGUID;
    ProducerTracker producerTracker = registerProducer(producerGUID);
    producerTracker.validateMessage(consumerRecord, endOfPushReceived, tolerateMissingMsgs);
  }

  /**
   * For a given partition, find all the producers that has written to this partition and update the offsetRecord using
   * segment information.
   * @param partition
   * @param offsetRecord
   */
  public void updateOffsetRecordForPartition(int partition, OffsetRecord offsetRecord) {
    producerTrackerMap.values().forEach(producerTracker -> producerTracker.updateOffsetRecord(partition, offsetRecord));
  }

  public void cloneProducerStates(int partition, KafkaDataIntegrityValidator newValidator) {
    for (Map.Entry<GUID, ProducerTracker> entry: producerTrackerMap.entrySet()) {
      GUID producerGUID = entry.getKey();
      ProducerTracker sourceProducerTracker = entry.getValue();
      ProducerTracker destProducerTracker = newValidator.registerProducer(producerGUID);
      sourceProducerTracker.cloneProducerStates(partition, destProducerTracker);
    }
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
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      Optional<ProducerTracker.DIVErrorMetricCallback> errorMetricCallback) throws DataValidationException {
    final GUID producerGUID = consumerRecord.getValue().producerMetadata.producerGUID;
    ProducerTracker producerTracker = registerProducer(producerGUID);
    producerTracker.checkMissingMessage(consumerRecord, errorMetricCallback, this.kafkaLogCompactionDelayInMs);
  }

}
