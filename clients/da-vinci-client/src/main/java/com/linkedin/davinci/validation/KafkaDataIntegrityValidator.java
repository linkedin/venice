package com.linkedin.davinci.validation;

import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
  private static final Logger LOGGER = LogManager.getLogger(KafkaDataIntegrityValidator.class);
  public static final long DISABLED = -1;
  private final long kafkaLogCompactionDelayInMs;
  private final long maxAgeInMs;
  /** Keeps track of every upstream producer this consumer task has seen so far for each partition. */
  protected final SparseConcurrentList<PartitionTracker> partitionTrackers = new SparseConcurrentList<>();
  protected final IntFunction<PartitionTracker> partitionTrackerCreator;

  public KafkaDataIntegrityValidator(String topicName) {
    this(topicName, DISABLED, DISABLED);
  }

  /**
   * This constructor is used by a proprietary ETL project. Do not clean up (yet)!
   *
   * TODO: Open source the ETL or make it stop depending on an exotic open source API
   */
  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs) {
    this(topicName, kafkaLogCompactionDelayInMs, DISABLED);
  }

  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs, long maxAgeInMs) {
    this.kafkaLogCompactionDelayInMs = kafkaLogCompactionDelayInMs;
    this.maxAgeInMs = maxAgeInMs;
    this.partitionTrackerCreator = p -> new PartitionTracker(topicName, p);
    LOGGER.info(
        "Initialized KafkaDataIntegrityValidator with topicName: {}, maxAgeInMs: {}, kafkaLogCompactionDelayInMs: {}",
        topicName,
        maxAgeInMs,
        kafkaLogCompactionDelayInMs);
  }

  /** N.B.: Package-private for test purposes */
  PartitionTracker registerPartition(int partition) {
    return partitionTrackers.computeIfAbsent(partition, partitionTrackerCreator);
  }

  /**
   * In some cases, such as when resetting offsets or unsubscribing from a partition,
   * the {@link PartitionTracker} should forget about the state that it accumulated
   * for a given partition.
   *
   * @param partition to clear state for
   */
  public void clearPartition(int partition) {
    partitionTrackers.remove(partition);
  }

  public void setPartitionState(PartitionTracker.TopicType type, int partition, OffsetRecord offsetRecord) {
    registerPartition(partition).setPartitionState(type, offsetRecord, this.maxAgeInMs);
  }

  /**
   * Run a thorough DIV check on the message, including UNREGISTERED_PRODUCER, MISSING, CORRUPT and DUPLICATE.
   */
  public void validateMessage(
      PartitionTracker.TopicType type,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      Lazy<Boolean> tolerateMissingMsgs) throws DataValidationException {
    PartitionTracker partitionTracker = registerPartition(consumerRecord.getPartition());
    partitionTracker.validateMessage(type, consumerRecord, endOfPushReceived, tolerateMissingMsgs);
  }

  /**
   * For a given partition, find all the producers that has written to this partition and update the offsetRecord using
   * segment information. Prior to this, the state which is expired according to {@link #maxAgeInMs} will be cleared.
   *
   * @param partition to extract info for
   * @param offsetRecord to modify
   */
  public void updateOffsetRecordForPartition(
      PartitionTracker.TopicType type,
      int partition,
      OffsetRecord offsetRecord) {
    PartitionTracker partitionTracker = registerPartition(partition);
    if (this.maxAgeInMs != DISABLED) {
      partitionTracker.clearExpiredStateAndUpdateOffsetRecord(type, offsetRecord, this.maxAgeInMs);
    } else {
      partitionTracker.updateOffsetRecord(type, offsetRecord);
    }
  }

  public void cloneProducerStates(int partition, KafkaDataIntegrityValidator newValidator) {
    PartitionTracker destPartitionTracker = newValidator.registerPartition(partition);
    this.partitionTrackers.get(partition).cloneProducerStates(destPartitionTracker);
  }

  /**
   * Only check for missing sequence number; segment starting from a positive sequence number is acceptable considering
   * real-time buffer replay would start in the middle of a segment; checksum is also ignored for the same reason.
   *
   * If missing message happens for an old message that is older than the Kafka log compaction lag threshold, MISSING
   * exception will not be thrown because it's expected that log compaction would compact old messages. However, if data
   * are fresh and missing message is detected, MISSING exception will be thrown.
   *
   * This API is used by a proprietary ETL project. Do not clean up (yet)!
   *
   * TODO: Open source the ETL or make it stop depending on an exotic open source API
   */
  public void checkMissingMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      Optional<PartitionTracker.DIVErrorMetricCallback> errorMetricCallback) throws DataValidationException {
    PartitionTracker partitionTracker = registerPartition(consumerRecord.getPartition());
    partitionTracker.checkMissingMessage(
        PartitionTracker.VERSION_TOPIC,
        consumerRecord,
        errorMetricCallback,
        this.kafkaLogCompactionDelayInMs);
  }

  /**
   * N.B. Intended for tests only
   *
   * @return the number of tracked producer GUIDs for the VERSION_TOPIC only.
   */
  int getNumberOfTrackedProducerGUIDs() {
    Set<GUID> guids = new HashSet<>();
    for (PartitionTracker partitionTracker: this.partitionTrackers.values()) {
      guids.addAll(partitionTracker.getTrackedGUIDs(PartitionTracker.VERSION_TOPIC));
    }
    return guids.size();
  }

  /** N.B. Intended for tests */
  int getNumberOfTrackedPartitions() {
    return this.partitionTrackers.values().size();
  }
}
