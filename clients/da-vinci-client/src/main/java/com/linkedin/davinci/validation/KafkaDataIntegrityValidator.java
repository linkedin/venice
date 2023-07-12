package com.linkedin.davinci.validation;

import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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
  private final String topicName;
  private final long kafkaLogCompactionDelayInMs;
  private final long maxAgeInMs;
  /** Keeps track of every upstream producer this consumer task has seen so far. */
  protected final Map<GUID, ProducerTracker> producerTrackerMap;
  protected final Function<GUID, ProducerTracker> producerTrackerCreator;

  /**
   * This constructor is used by a proprietary ETL project. Do not clean up (yet)!
   *
   * TODO: Open source the ETL or make it stop depending on an exotic open source API
   */
  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs) {
    this(topicName, kafkaLogCompactionDelayInMs, DISABLED);
  }

  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs, long maxAgeInMs) {
    this.topicName = topicName;
    this.kafkaLogCompactionDelayInMs = kafkaLogCompactionDelayInMs;
    this.maxAgeInMs = maxAgeInMs;
    this.producerTrackerMap = new VeniceConcurrentHashMap<>();
    this.producerTrackerCreator = guid -> new ProducerTracker(guid, topicName);
  }

  /** N.B.: Package-private for test purposes */
  ProducerTracker registerProducer(GUID producerGuid) {
    return producerTrackerMap.computeIfAbsent(producerGuid, producerTrackerCreator);
  }

  public void clearPartition(int partition) {
    producerTrackerMap.values().forEach(producerTracker -> producerTracker.clearPartition(partition));
  }

  public void setPartitionState(int partition, OffsetRecord offsetRecord) {
    long minimumRequiredRecordProducerTimestamp =
        this.maxAgeInMs == DISABLED ? DISABLED : offsetRecord.getMaxMessageTimeInMs() - this.maxAgeInMs;
    Iterator<Map.Entry<CharSequence, ProducerPartitionState>> iterator =
        offsetRecord.getProducerPartitionStateMap().entrySet().iterator();
    Map.Entry<CharSequence, ProducerPartitionState> entry;
    GUID producerGuid;
    ProducerPartitionState producerPartitionState;
    while (iterator.hasNext()) {
      entry = iterator.next();
      producerGuid = GuidUtils.getGuidFromCharSequence(entry.getKey());
      producerPartitionState = entry.getValue();
      if (producerPartitionState.messageTimestamp >= minimumRequiredRecordProducerTimestamp) {
        /**
         * This {@link producerPartitionState} is eligible to be retained, so we'll set the state in the
         * {@link ProducerTracker}.
         */
        registerProducer(producerGuid).setPartitionState(partition, producerPartitionState);
      } else {
        // The state is eligible to be cleared.
        producerTrackerMap.compute(producerGuid, (k, v) -> {
          if (v == null) {
            /** If the {@link ProducerTracker} did not exist yet, we leave things as is. This is the expected case. */
            return null;
          }

          v.removeState(partition);
          if (v.isEmpty()) {
            /**
             * If the {@link ProducerTracker} carries no other state after removing that belonging to the
             * {@link partition} of interest, then we also clear it from the {@link producerTrackerMap}.
             */
            return null;
          }
          /** Finally, if the {@link ProducerTracker} is still carrying state for other partitions, we leave it in. */
          return v;
        });
        iterator.remove();
      }
    }
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
   * segment information. Prior to this, the state which is expired according to {@link #maxAgeInMs} will be cleared.
   *
   * @param partition to extract info for
   * @param offsetRecord to modify
   */
  public void updateOffsetRecordForPartition(int partition, OffsetRecord offsetRecord) {
    if (this.maxAgeInMs != DISABLED) {
      clearExpiredStateAndUpdateOffsetRecordForPartition(partition, offsetRecord);
    } else {
      Iterator<ProducerTracker> iterator = this.producerTrackerMap.values().iterator();
      while (iterator.hasNext()) {
        iterator.next().updateOffsetRecord(partition, offsetRecord);
      }
    }
  }

  void clearExpiredStateAndUpdateOffsetRecordForPartition(int partition, OffsetRecord offsetRecord) {
    long minimumRequiredRecordProducerTimestamp = offsetRecord.getMaxMessageTimeInMs() - this.maxAgeInMs;
    int numberOfClearedGUIDs = 0;
    Iterator<ProducerTracker> iterator = this.producerTrackerMap.values().iterator();
    ProducerTracker producerTracker;
    while (iterator.hasNext()) {
      producerTracker = iterator.next();
      if (producerTracker.clearExpiredState(partition, minimumRequiredRecordProducerTimestamp)) {
        if (producerTracker.isEmpty()) {
          /**
           * N.B.: There is technically a race condition right here. If we clear the last partition, and then
           * {@link ProducerTracker#isEmpty()} is true, but right after that, a new partition starts getting tracked,
           * we would still go ahead and remove the previously-empty {@link ProducerTracker} out of the
           * {@link producerTrackerMap}. Guarding against this race condition could be done by locking a bunch of other
           * functions in this class, but this seems impractical. So instead, we redo the {@link ProducerTracker#isEmpty()}
           * check one more time to check if the race happened, and if it did, we add it back into the map.
           */
          iterator.remove();
          if (producerTracker.isEmpty()) {
            offsetRecord.removeProducerPartitionState(producerTracker.getProducerGUID());
            numberOfClearedGUIDs++;
          } else {
            this.producerTrackerMap.put(producerTracker.getProducerGUID(), producerTracker);
          }
        }
      } else {
        producerTracker.updateOffsetRecord(partition, offsetRecord);
      }
    }
    if (numberOfClearedGUIDs > 0) {
      LOGGER.info("Cleared {} expired producer GUID(s) for '{}'", numberOfClearedGUIDs, this.topicName);
    }
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
   * This API is used by a proprietary ETL project. Do not clean up (yet)!
   *
   * TODO: Open source the ETL or make it stop depending on an exotic open source API
   */
  public void checkMissingMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      Optional<ProducerTracker.DIVErrorMetricCallback> errorMetricCallback) throws DataValidationException {
    final GUID producerGUID = consumerRecord.getValue().producerMetadata.producerGUID;
    ProducerTracker producerTracker = registerProducer(producerGUID);
    producerTracker.checkMissingMessage(consumerRecord, errorMetricCallback, this.kafkaLogCompactionDelayInMs);
  }

  /** N.B. Intended for tests */
  int getNumberOfTrackedProducerGUIDs() {
    return this.producerTrackerMap.size();
  }
}
