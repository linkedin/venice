package com.linkedin.davinci.validation;

import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
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
  /** The oldest producer timestamp we retained during the last run of {@link #clearExpiredState()} */
  private volatile long oldestRetainedProducerTS = 0;

  /** Keeps track of every upstream producer this consumer task has seen so far. */
  protected final Map<GUID, ProducerTracker> producerTrackerMap;
  protected final Function<GUID, ProducerTracker> producerTrackerCreator;
  private final Time time;

  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs) {
    this(topicName, kafkaLogCompactionDelayInMs, DISABLED);
  }

  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs, long maxAgeInMs) {
    this(topicName, kafkaLogCompactionDelayInMs, maxAgeInMs, SystemTime.INSTANCE);
  }

  public KafkaDataIntegrityValidator(String topicName, long kafkaLogCompactionDelayInMs, long maxAgeInMs, Time time) {
    this.topicName = topicName;
    this.kafkaLogCompactionDelayInMs = kafkaLogCompactionDelayInMs;
    this.maxAgeInMs = maxAgeInMs;
    this.producerTrackerMap = new VeniceConcurrentHashMap<>();
    this.producerTrackerCreator = guid -> new ProducerTracker(guid, topicName);
    this.time = time;
  }

  /** N.B.: Package-private for test purposes */
  ProducerTracker registerProducer(GUID producerGuid) {
    return producerTrackerMap.computeIfAbsent(producerGuid, producerTrackerCreator);
  }

  public void clearPartition(int partition) {
    producerTrackerMap.values().forEach(producerTracker -> producerTracker.clearPartition(partition));
  }

  public void setPartitionState(int partition, GUID producerGuid, ProducerPartitionState partitionState) {
    registerProducer(producerGuid).setPartitionState(partition, partitionState);
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
    clearExpiredState();
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

  void clearExpiredState() {
    if (this.maxAgeInMs != DISABLED) {
      long currentTime = time.getMilliseconds();
      long maxRecordProducerTimestamp = currentTime - this.maxAgeInMs;
      if (this.oldestRetainedProducerTS < maxRecordProducerTimestamp) {
        synchronized (this) {
          if (this.oldestRetainedProducerTS < maxRecordProducerTimestamp) {
            int numberOfClearedGUIDs = 0;
            long newOldestRetainedProducerTS = Long.MAX_VALUE;
            Iterator<ProducerTracker> iterator = this.producerTrackerMap.values().iterator();
            ProducerTracker producerTracker;
            while (iterator.hasNext()) {
              producerTracker = iterator.next();
              long currentProducerOldestRetainedTS = producerTracker.clearExpiredState(maxRecordProducerTimestamp);
              if (currentProducerOldestRetainedTS == Long.MAX_VALUE) {
                iterator.remove();
                numberOfClearedGUIDs++;
              } else {
                newOldestRetainedProducerTS = Math.min(newOldestRetainedProducerTS, currentProducerOldestRetainedTS);
              }
            }
            if (newOldestRetainedProducerTS == Long.MAX_VALUE) {
              /** We only update the {@link #oldestRetainedProducerTS} if there is any state we retained. */
              this.oldestRetainedProducerTS = newOldestRetainedProducerTS;
            }
            if (numberOfClearedGUIDs > 0) {
              LOGGER.info("Cleared {} expired producer GUID(s) for '{}'", numberOfClearedGUIDs, this.topicName);
            }
          }
        }
      }
    }
  }
}
