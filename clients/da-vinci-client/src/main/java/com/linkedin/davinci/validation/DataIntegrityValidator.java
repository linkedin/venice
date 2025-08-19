package com.linkedin.davinci.validation;

import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Colloquially known as DIV (Data Integrity Validator).
 * This class is a library that can validate the topic message during consumption, which can be used in
 * Venice-Server/Da-Vinci and ETL. In high level, it keeps track of messages produced by different producers,
 * and validates data integrity in 4 perspectives:
 * 1. Whether a segment starts from a non-zero sequence number (UNREGISTERED_PRODUCER);
 * 2. Whether there is a gap between segments or within a segment (MISSING);
 * 3. Whether data within a segment is corrupted (CORRUPT);
 * 4. Whether producers have produced duplicate messages, which is fine and expected due to producer retries (DUPLICATE).
 */
public class DataIntegrityValidator {
  private static final Logger LOGGER = LogManager.getLogger(DataIntegrityValidator.class);
  public static final long DISABLED = -1;
  private final long logCompactionDelayInMs;
  private final long maxAgeInMs;
  /** Keeps track of every upstream producer this consumer task has seen so far for each partition. */
  protected final SparseConcurrentList<PartitionTracker> partitionTrackers = new SparseConcurrentList<>();
  protected final IntFunction<PartitionTracker> partitionTrackerCreator;

  public DataIntegrityValidator(String topicName) {
    this(topicName, DISABLED, DISABLED);
  }

  /**
   * This constructor is used by a proprietary ETL project. Do not clean up (yet)!
   *
   * TODO: Open source the ETL or make it stop depending on an exotic open source API
   */
  public DataIntegrityValidator(String topicName, long logCompactionDelayInMs) {
    this(topicName, logCompactionDelayInMs, DISABLED);
  }

  public DataIntegrityValidator(String topicName, long logCompactionDelayInMs, long maxAgeInMs) {
    this.logCompactionDelayInMs = logCompactionDelayInMs;
    this.maxAgeInMs = maxAgeInMs;
    this.partitionTrackerCreator = p -> new PartitionTracker(topicName, p);
    LOGGER.info(
        "Initialized DataIntegrityValidator with topicName: {}, maxAgeInMs: {}, logCompactionDelayInMs: {}",
        topicName,
        maxAgeInMs,
        logCompactionDelayInMs);
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

  public void setPartitionState(
      PartitionTracker.TopicType type,
      int partition,
      Map<CharSequence, ProducerPartitionState> producerPartitionStateMap) {
    // TODO: can maxAgeInMs be used without offsetRecord.getMaxMessageTimeInMs()?
    registerPartition(partition).setPartitionState(type, producerPartitionStateMap, DISABLED);
  }

  /**
   * Run a thorough DIV check on the message, including UNREGISTERED_PRODUCER, MISSING, CORRUPT and DUPLICATE.
   */
  public void validateMessage(
      PartitionTracker.TopicType type,
      DefaultPubSubMessage consumerRecord,
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

  public void cloneVtProducerStates(int partition, DataIntegrityValidator newValidator) {
    PartitionTracker destPartitionTracker = newValidator.registerPartition(partition);
    registerPartition(partition).cloneVtProducerStates(destPartitionTracker);
  }

  /**
   * Returns the RT DIV state for a given partition and broker URL pair.
   */
  public PartitionTracker cloneRtProducerStates(int partition, String brokerUrl) {
    PartitionTracker clonedPartitionTracker = partitionTrackerCreator.apply(partition);
    final PartitionTracker existingPartitionTracker = registerPartition(partition);
    existingPartitionTracker.cloneRtProducerStates(clonedPartitionTracker, brokerUrl); // for a single broker
    return clonedPartitionTracker;
  }

  /**
   * Returns the VT DIV state and latest consumed vt offset for a given partition.
   */
  public PartitionTracker cloneVtProducerStates(int partition) {
    PartitionTracker clonedPartitionTracker = partitionTrackerCreator.apply(partition);
    final PartitionTracker existingPartitionTracker = registerPartition(partition);
    existingPartitionTracker.cloneVtProducerStates(clonedPartitionTracker); // for a single broker
    return clonedPartitionTracker;
  }

  /**
   * Only check for missing sequence number; segment starting from a positive sequence number is acceptable considering
   * real-time buffer replay would start in the middle of a segment; checksum is also ignored for the same reason.
   *
   * If missing message happens for an old message that is older than the log compaction lag threshold, MISSING
   * exception will not be thrown because it's expected that log compaction would compact old messages. However, if data
   * are fresh and missing message is detected, MISSING exception will be thrown.
   *
   * This API is used by a proprietary ETL project. Do not clean up (yet)!
   *
   * TODO: Open source the ETL or make it stop depending on an exotic open source API
   */
  public void checkMissingMessage(
      DefaultPubSubMessage consumerRecord,
      Optional<PartitionTracker.DIVErrorMetricCallback> errorMetricCallback) throws DataValidationException {
    PartitionTracker partitionTracker = registerPartition(consumerRecord.getPartition());
    partitionTracker.checkMissingMessage(
        PartitionTracker.VERSION_TOPIC,
        consumerRecord,
        errorMetricCallback,
        this.logCompactionDelayInMs);
  }

  public void updateLatestConsumedVtOffset(int partition, PubSubPosition offset) {
    PartitionTracker partitionTracker = registerPartition(partition);
    partitionTracker.updateLatestConsumedVtPosition(offset);
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
