package com.linkedin.davinci.validation;

import static com.linkedin.davinci.validation.KafkaDataIntegrityValidator.DISABLED;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.IncomingDataAfterSegmentEndedException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class maintains state about all the upstream producers for a given partition.
 * It keeps track of the last segment, last sequence number and incrementally computed
 * checksum for each producer (identified by a producer GUID).
 *
 * This class is thread safe. Locking is at the granularity of producers. Multiple
 * threads can process records from the same partition concurrently.
 *
 * This class also encapsulates the capability to clear expired state, in the functions
 * which take in the maxAgeInMs parameter:
 *
 * - {@link #clearExpiredStateAndUpdateOffsetRecord(OffsetRecord, long)}
 * - {@link #setPartitionState(OffsetRecord, long)}
 */
@Threadsafe
public class PartitionTracker {
  /**
   * If an exception will be tolerated, there is no need to print a log for each single message;
   * we can log only once a minute. The error message identifier pattern for log throttling is:
   * topicName-partitionNum-exceptionType
   */
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final Logger logger;
  private final String topicName;
  private final int partition;
  private final Map<GUID, Segment> segments = new VeniceConcurrentHashMap<>();

  public PartitionTracker(String topicName, int partition) {
    this.topicName = topicName;
    this.partition = partition;
    this.logger = LogManager.getLogger(this.toString());
  }

  public int getPartition() {
    return partition;
  }

  public final String toString() {
    return PartitionTracker.class.getSimpleName() + "(topic: " + topicName + ", partition: " + partition + ")";
  }

  /** N.B. Intended for tests */
  Set<GUID> getTrackedGUIDs() {
    return Collections.unmodifiableSet(this.segments.keySet());
  }

  /**
   * @param guid for which to retrieve the lock and segment
   * @return a {@link Segment} or null if it's absent
   */
  Segment getSegment(GUID guid) {
    return this.segments.get(guid);
  }

  public void setPartitionState(OffsetRecord offsetRecord, long maxAgeInMs) {
    long minimumRequiredRecordProducerTimestamp =
        maxAgeInMs == DISABLED ? DISABLED : offsetRecord.getMaxMessageTimeInMs() - maxAgeInMs;
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
         * {@link PartitionTracker}.
         */
        setSegment(producerGuid, new Segment(partition, producerPartitionState));
      } else {
        // The state is eligible to be cleared.
        segments.remove(producerGuid);
        iterator.remove();
      }
    }
  }

  private void setSegment(GUID guid, Segment segment) {
    Segment previousSegment = this.segments.put(guid, segment);
    if (previousSegment == null) {
      logger.info(" set state for partition: {}, New state: {}", partition, segment);
    } else {
      logger.info(
          " will overwrite previous state for partition: {}, Previous state: {}, New state: {}",
          partition,
          previousSegment,
          segment);
    }
  }

  public void cloneProducerStates(PartitionTracker destProducerTracker) {
    for (Map.Entry<GUID, Segment> entry: this.segments.entrySet()) {
      destProducerTracker.setSegment(entry.getKey(), new Segment(entry.getValue()));
    }
  }

  private void updateOffsetRecord(GUID guid, Segment segment, OffsetRecord offsetRecord) {
    if (segment == null) {
      // This producer didn't write anything to this GUID
      return;
    }
    ProducerPartitionState state = offsetRecord.getProducerPartitionState(guid);
    if (state == null) {
      state = new ProducerPartitionState();

      /**
       * The aggregates and debugInfo being stored in the {@link ProducerPartitionState} will add a bit
       * of overhead when we checkpoint this metadata to disk, so we should be careful not to add a very
       * large number of elements to these arbitrary collections.
       *
       * In the case of the debugInfo, it is expected (at the time of writing this comment) that all
       * partitions produced by the same producer GUID would have the same debug values (though nothing
       * precludes us from having per-partition debug values in the future if there is a use case for
       * that). It is redundant that we store the same debug values once per partition. In the future,
       * if we want to eliminate this redundancy, we could move the per-producer debug info to another
       * data structure, though that would increase bookkeeping complexity. This is expected to be a
       * minor overhead, and therefore it appears to be a premature to optimize this now.
       */
      state.aggregates = segment.getAggregates();
      state.debugInfo = segment.getDebugInfo();
    }
    state.checksumType = segment.getCheckSumType().getValue();
    /**
     * {@link MD5Digest#getEncodedState()} is allocating a byte array to contain the intermediate state,
     * which is expensive. We should only invoke this closure when necessary.
     */
    state.checksumState = ByteBuffer.wrap(segment.getCheckSumState());
    state.segmentNumber = segment.getSegmentNumber();
    state.messageSequenceNumber = segment.getSequenceNumber();
    state.messageTimestamp = segment.getLastRecordProducerTimestamp();
    state.segmentStatus = segment.getStatus().getValue();
    state.isRegistered = segment.isRegistered();

    offsetRecord.setProducerPartitionState(guid, state);
  }

  public void updateOffsetRecord(OffsetRecord offsetRecord) {
    for (Map.Entry<GUID, Segment> entry: this.segments.entrySet()) {
      updateOffsetRecord(entry.getKey(), entry.getValue(), offsetRecord);
    }
  }

  /**
   * Ensures the integrity of the data by maintaining state about all of the data produced by a specific
   * upstream producer:
   *
   * 1. Segment, which should be equal or greater to the previous segment.
   * 2. Sequence number, which should be exactly one greater than the previous sequence number.
   * 3. Checksum, which is computed incrementally until the end of a segment.
   *
   *
   * @param consumerRecord
   * @throws DataValidationException
   */
  public void validateMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      Lazy<Boolean> tolerateMissingMsgs) throws DataValidationException {
    Segment segment = getSegment(consumerRecord.getValue().getProducerMetadata().getProducerGUID());
    boolean hasPreviousSegment = segment != null;
    segment = trackSegment(segment, consumerRecord, endOfPushReceived, tolerateMissingMsgs);
    trackSequenceNumber(segment, consumerRecord, endOfPushReceived, tolerateMissingMsgs, hasPreviousSegment);
    // This is the last step, because we want failures in the previous steps to short-circuit execution.
    trackCheckSum(segment, consumerRecord, endOfPushReceived, tolerateMissingMsgs);
    segment.setLastSuccessfulOffset(consumerRecord.getOffset());
    segment.setNewSegment(false);
  }

  /**
   * This function ensures that the segment number is either equal or greater than the previous segment
   * seen for this specific partition.
   *
   * This function has the side-effect of initializing a new {@link Segment} if:
   * 1. The previous segment does not exist, or
   * 2. The incoming segment is exactly one greater than the previous one, and the previous segment is ended.
   *
   * @see #initializeNewSegment(PubSubMessage, boolean, boolean)
   *
   * @param consumerRecord
   * @throws DuplicateDataException if the incoming segment is lower than the previously seen segment.
   */
  private Segment trackSegment(
      Segment previousSegment,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      Lazy<Boolean> tolerateMissingMsgs) throws DuplicateDataException {
    int incomingSegmentNumber = consumerRecord.getValue().producerMetadata.segmentNumber;
    if (previousSegment == null) {
      if (incomingSegmentNumber != 0) {
        handleUnregisteredProducer(
            "track new segment with non-zero incomingSegment=" + incomingSegmentNumber,
            consumerRecord,
            null,
            endOfPushReceived);
      }
      Segment newSegment = initializeNewSegment(consumerRecord, endOfPushReceived, true);
      return newSegment;
    } else {
      int previousSegmentNumber = previousSegment.getSegmentNumber();
      if (incomingSegmentNumber == previousSegmentNumber) {
        return previousSegment;
      } else if (incomingSegmentNumber == previousSegmentNumber + 1 && previousSegment.isEnded()) {
        /** tolerateAnyMessageType should always be false in this scenario, regardless of {@param endOfPushReceived} */
        return initializeNewSegment(consumerRecord, endOfPushReceived, false);
      } else if (incomingSegmentNumber > previousSegmentNumber) {
        if (tolerateMissingMsgs.get()) {
          return initializeNewSegment(consumerRecord, endOfPushReceived, true);
        } else {
          throw DataFaultType.MISSING.getNewException(previousSegment, consumerRecord);
        }
      } else if (incomingSegmentNumber < previousSegmentNumber) {
        throw DataFaultType.DUPLICATE.getNewException(previousSegment, consumerRecord);
      } else {
        // Defensive code.
        throw new IllegalStateException(
            "This condition should never happen. " + getClass().getSimpleName() + " may have a regression.");
      }
    }
  }

  /**
   * @param consumerRecord
   * @param tolerateAnyMessageType if true, we will tolerate initializing the Segment on any message
   *                               if false, we will only tolerate initializing on a {@link ControlMessageType#START_OF_SEGMENT}
   * @return the newly initialized {@link Segment}
   * @throws IllegalStateException if called for a message other than a {@link ControlMessageType#START_OF_SEGMENT}
   */
  private Segment initializeNewSegment(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      boolean tolerateAnyMessageType) {
    CheckSumType checkSumType = CheckSumType.NONE;
    boolean unregisteredProducer = true;
    Map<CharSequence, CharSequence> debugInfo = Collections.emptyMap();
    Map<CharSequence, Long> aggregates = Collections.emptyMap();

    if (MessageType.valueOf(consumerRecord.getValue()) == MessageType.CONTROL_MESSAGE) {
      ControlMessage controlMessage = (ControlMessage) consumerRecord.getValue().getPayloadUnion();
      if (ControlMessageType.valueOf(controlMessage) == ControlMessageType.START_OF_SEGMENT) {
        StartOfSegment startOfSegment = (StartOfSegment) controlMessage.getControlMessageUnion();
        checkSumType = CheckSumType.valueOf(startOfSegment.getChecksumType());
        debugInfo = CollectionUtils.substituteEmptyMap(controlMessage.getDebugInfo());
        if (startOfSegment.getUpcomingAggregates() != null && !startOfSegment.getUpcomingAggregates().isEmpty()) {
          aggregates = new HashMap<>(startOfSegment.getUpcomingAggregates().size());
          for (CharSequence name: startOfSegment.getUpcomingAggregates()) {
            aggregates.put(name, 0L);
          }
        }
        unregisteredProducer = false;
      }
    }

    Segment newSegment = new Segment(
        consumerRecord.getTopicPartition().getPartitionNumber(),
        consumerRecord.getValue().getProducerMetadata().getSegmentNumber(),
        consumerRecord.getValue().getProducerMetadata().getMessageSequenceNumber(),
        checkSumType,
        debugInfo,
        aggregates);
    newSegment.setLastRecordProducerTimestamp(consumerRecord.getValue().getProducerMetadata().getMessageTimestamp());
    this.segments.put(consumerRecord.getValue().getProducerMetadata().getProducerGUID(), newSegment);

    if (unregisteredProducer) {
      handleUnregisteredProducer(
          "initialize new segment with a non-" + ControlMessageType.START_OF_SEGMENT.name() + " message",
          consumerRecord,
          null,
          endOfPushReceived,
          tolerateAnyMessageType);
    } else {
      newSegment.registeredSegment();
    }

    return newSegment;
  }

  private void handleUnregisteredProducer(
      String scenario,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      Segment segment,
      boolean endOfPushReceived) {
    handleUnregisteredProducer(scenario, consumerRecord, segment, endOfPushReceived, true);
  }

  /**
   * Found an unregistered producer when creating a segment.
   * @param endOfPushReceived Whether end of push is received for this partition.
   * @param tolerateAnyMessageType If true, then a segment can be initialized without "START_OF_SEGMENT".
   */
  private void handleUnregisteredProducer(
      String scenario,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      Segment segment,
      boolean endOfPushReceived,
      boolean tolerateAnyMessageType) {
    if (endOfPushReceived && tolerateAnyMessageType) {
      String errorMsgIdentifier = consumerRecord.getTopicPartition().getPubSubTopic().getName() + "-"
          + consumerRecord.getTopicPartition().getPartitionNumber() + "-" + DataFaultType.UNREGISTERED_PRODUCER;
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMsgIdentifier)) {
        logger.warn("Will {}, endOfPushReceived=true, tolerateAnyMessageType=true", scenario);
      }
    } else {
      throw DataFaultType.UNREGISTERED_PRODUCER.getNewException(
          segment,
          consumerRecord,
          "Cannot " + scenario + ", endOfPushReceived=" + endOfPushReceived + ", tolerateAnyMessageType="
              + tolerateAnyMessageType);
    }
  }

  /**
   * This function ensures that the sequence number is strictly one greater than the previous incoming
   * message for this specific partition.
   *
   * This function has the side-effect of altering the sequence number stored in the {@link Segment}.
   *
   * @param segment for which the incoming message belongs to
   * @param consumerRecord
   * @param endOfPushReceived whether endOfPush is received
   * @param tolerateMissingMsgs whether log compaction could potentially happen to this record
   * @param hasPreviousSegment whether previous segment exists
   * @throws MissingDataException if the incoming sequence number is greater than the previous sequence number + 1
   * @throws DuplicateDataException if the incoming sequence number is equal to or smaller than the previous sequence number
   */
  private void trackSequenceNumber(
      Segment segment,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      Lazy<Boolean> tolerateMissingMsgs,
      boolean hasPreviousSegment) throws MissingDataException, DuplicateDataException {

    int previousSequenceNumber = segment.getSequenceNumber();
    int incomingSequenceNumber = consumerRecord.getValue().producerMetadata.messageSequenceNumber;

    if (!segment.isStarted()) {
      segment.start();
      segment.setLastRecordProducerTimestamp(consumerRecord.getValue().producerMetadata.messageTimestamp);
      return;
    } else if (segment.isNewSegment() && incomingSequenceNumber == previousSequenceNumber) {
      if (segment.getSequenceNumber() > 0 && !tolerateMissingMsgs.get()) {
        throw DataFaultType.MISSING.getNewException(segment, consumerRecord);
      }
      segment.setLastRecordProducerTimestamp(consumerRecord.getValue().producerMetadata.messageTimestamp);
      /**
       * In any other cases for newly constructed segment, don't check sequence number anymore,
       * because "previousSequenceNumber" is going to be equal to "incomingSequenceNumber", and thus this message
       * will be treated as DUPLICATE message and dropped.
       */
      return;
    }

    if (incomingSequenceNumber == previousSequenceNumber + 1) {
      // Expected case, in steady state
      segment.getAndIncrementSequenceNumber();
      segment.setLastRecordProducerTimestamp(consumerRecord.getValue().producerMetadata.messageTimestamp);
    } else if (incomingSequenceNumber <= previousSequenceNumber) {
      if (!hasPreviousSegment) {
        // When hasPrevSegment is false, SN meets a producer for the first time. For hybrid + L/F case, a follower may
        // never
        // see the record coming from samza producer before it is promoted to leader. This check prevents the first
        // message to be considered as "duplicated" and skipped.
        segment.setLastRecordProducerTimestamp(consumerRecord.getValue().producerMetadata.messageTimestamp);
        return;
      }
      // This is a duplicate message, which we can safely ignore.

      // Although data duplication is a benign fault, we need to bubble up for two reasons:
      // 1. We want to short-circuit data validation, because the running checksum depends on exactly-once guarantees.
      // 2. The upstream caller can choose to avoid writing duplicate data, as an optimization.
      // 3. We don't want to re-calculate checksum for duplicated msgs. It's an incorrect behavior.
      throw DataFaultType.DUPLICATE.getNewException(segment, consumerRecord);
    } else if (incomingSequenceNumber > previousSequenceNumber + 1) {
      // There is a gap in the sequence, so we are missing some data!

      DataValidationException dataMissingException = DataFaultType.MISSING.getNewException(segment, consumerRecord);
      /**
       * We will swallow {@link DataFaultType.MISSING} in either of the two scenarios:
       * 1. The segment was sent by unregistered producers after EOP
       * 2. The topic might have been compacted for the record so that tolerateMissingMsgs is true
       */
      if ((endOfPushReceived && !segment.isRegistered()) || tolerateMissingMsgs.get()) {
        /**
         * In this branch of the if, we need to adjust the sequence number, otherwise,
         * this will cause spurious missing data metrics on further events...
         * and the partition won't become 'ONLINE' if it is not 'ONLINE' yet.
          */
        segment.setSequenceNumber(incomingSequenceNumber);
        segment.setLastRecordProducerTimestamp(consumerRecord.getValue().producerMetadata.messageTimestamp);
      } else {
        throw dataMissingException;
      }
    } else {
      // Defensive coding, to prevent regressions in the above code from causing silent failures
      throw new IllegalStateException("Unreachable code!");
    }
  }

  /**
   * This function maintains a running checksum of the data seen so far for this specific partition.
   *
   * This function has the side-effect of marking the {@link Segment} as ended when it encounters a
   * {@link ControlMessageType#END_OF_SEGMENT} message and the incrementally computed checksum matches
   * the expected one.
   *
   * @param segment for which the incoming message belongs to
   * @param consumerRecord coming from the Kafka consumer
   * @throws CorruptDataException if the data is corrupt. Can only happen when processing control message of type:
   *                              {@link ControlMessageType#END_OF_SEGMENT}
   */
  private void trackCheckSum(
      Segment segment,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      Lazy<Boolean> tolerateMissingMsgs) throws CorruptDataException {
    /**
     * {@link Segment#addToCheckSum(KafkaKey, KafkaMessageEnvelope)} is an expensive operation because of the internal
     * memory allocation.
     * TODO: we could disable checksum validation if we think it is not necessary any more later on.
     */
    boolean update = true;
    try {
      /**
       * We update the checksum successfully if this returns true
       */
      update = segment.addToCheckSum(consumerRecord.getKey(), consumerRecord.getValue());
    } catch (IncomingDataAfterSegmentEndedException e) {
      /**
       * We received user messages after EOS in the same segment.
       */
      if (!tolerateMissingMsgs.get()) {
        throw e;
      }
    }
    if (!update) {
      /** We have consumed an {@link ControlMessageType#END_OF_SEGMENT}. Time to verify the checksum. */
      ControlMessage controlMessage = (ControlMessage) consumerRecord.getValue().payloadUnion;
      EndOfSegment incomingEndOfSegment = (EndOfSegment) controlMessage.controlMessageUnion;

      if (ByteBuffer.wrap(segment.getFinalCheckSum()).equals(incomingEndOfSegment.checksumValue)) {
        // We're good, the expected checksum matches the one we computed on the receiving end (:
        segment.end(incomingEndOfSegment.finalSegment);
      } else {
        DataValidationException dataCorruptException = DataFaultType.CORRUPT.getNewException(segment, consumerRecord);
        /**
         * We will swallow {@link DataFaultType.CORRUPT} in either of the two scenarios:
         * 1. The segment was sent by unregistered producers after EOP
         * 2. The topic might have been compacted for the record so that tolerateMissingMsgs is true
         */
        if ((endOfPushReceived && !segment.isRegistered()) || tolerateMissingMsgs.get()) {
          segment.end(incomingEndOfSegment.finalSegment);
        } else if (endOfPushReceived) {
          /**
           * If EOP is received, we will still end the segment and then throw exceptions.
           * Ending the segment so that next SOS message wouldn't get misleading
           * missing exceptions in {@link #trackSegment(ConsumerRecord, boolean)}
           */
          segment.end(incomingEndOfSegment.finalSegment);
          throw dataCorruptException;
        } else {
          throw dataCorruptException;
        }
      }
    }
  }

  /**
   * This API is used for stateless DIV; it takes Kafka log compaction into consideration:
   * i. If missing message happens for very old messages whose broker timestamps are older than the log compaction
   *    delay threshold, it indicates that log compaction might already take place, so missing message is expected
   *    and will be tolerated;
   * ii. if the data are fresh and missing message is detected, error will be thrown.
   *
   * If "logCompactionDelayInMs" is not a positive number, it indicates there is no delay for Kafka log compaction,
   * Kafka would compact message at any time for hybrid stores, so missing messages is expected; no error will be thrown.
   *
   * If "errorMetricCallback" is present, the callback will be triggered before throwing MISSING_MESSAGE exception;
   * users can register their own callback to emit metrics, produce Kafka events, etc.
   */
  private void validateSequenceNumber(
      Segment segment,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      long logCompactionDelayInMs,
      Optional<DIVErrorMetricCallback> errorMetricCallback) throws MissingDataException {
    int previousSequenceNumber = segment.getSequenceNumber();
    int incomingSequenceNumber = consumerRecord.getValue().producerMetadata.messageSequenceNumber;

    if (!segment.isStarted()) {
      segment.start();
      segment.setSequenceNumber(incomingSequenceNumber);
      segment.setLastRecordTimestamp(consumerRecord.getPubSubMessageTime());
    } else if (incomingSequenceNumber == previousSequenceNumber + 1) {
      // Expected case, in steady state
      segment.getAndIncrementSequenceNumber();
      segment.setLastRecordTimestamp(consumerRecord.getPubSubMessageTime());
    } else if (incomingSequenceNumber <= previousSequenceNumber) {
      /**
       * Duplicate message is acceptable, there is no data loss.
       */
      segment.setLastRecordTimestamp(consumerRecord.getPubSubMessageTime());
    } else if (incomingSequenceNumber > previousSequenceNumber + 1) {
      /**
       * A gap is detected in sequence number. If the data are fresh, data are within the Kafka log compaction
       * delay threshold, it indicates a clear data loss signal; if the broker timestamp of the data are older
       * than the log compaction point, log compaction might delete the data before this message, so missing
       * message is expected.
       */
      long lastRecordTimestamp = segment.getLastRecordTimestamp();
      if (logCompactionDelayInMs > 0 && LatencyUtils.getElapsedTimeInMs(lastRecordTimestamp) < logCompactionDelayInMs) {
        DataValidationException dataMissingException = DataFaultType.MISSING.getNewException(segment, consumerRecord);
        logger.error(
            "Encountered missing data message within the log compaction time window. Error msg: {}",
            dataMissingException.getMessage());
        if (errorMetricCallback.isPresent()) {
          errorMetricCallback.get().execute(dataMissingException);
        }
        throw dataMissingException;
      }
      segment.setSequenceNumber(incomingSequenceNumber);
      segment.setLastRecordTimestamp(consumerRecord.getPubSubMessageTime());
    } else {
      // Defensive coding, to prevent regressions in the above code from causing silent failures
      throw new IllegalStateException("Unreachable code!");
    }
  }

  public void checkMissingMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      Optional<PartitionTracker.DIVErrorMetricCallback> errorMetricCallback,
      long kafkaLogCompactionDelayInMs) throws DataValidationException {
    Segment segment = getSegment(consumerRecord.getValue().getProducerMetadata().getProducerGUID());

    try {
      /**
       * Explicitly suppress UNREGISTERED_PRODUCER DIV error.
       */
      segment = trackSegment(segment, consumerRecord, true, Lazy.FALSE);
    } catch (DuplicateDataException duplicate) {
      /**
       * Tolerate a segment rewind and not necessary to validate a previous segment;
       */
      return;
    } catch (MissingDataException missingSegment) {
      /**
       * Missing an entire segment is not acceptable, even though Kafka log compaction kicks in and all the data
       * messages within a segment are compacted; START_OF_SEGMENT and END_OF_SEGMENT messages should still be there.
       */
      logger.error(
          "Encountered a missing segment. This is unacceptable even if log compaction kicks in. Error msg: {}",
          missingSegment.getMessage());
      if (errorMetricCallback.isPresent()) {
        errorMetricCallback.get().execute(missingSegment);
      }
      throw missingSegment;
    }

    /**
     * Check missing sequence number.
     */
    validateSequenceNumber(segment, consumerRecord, kafkaLogCompactionDelayInMs, errorMetricCallback);
    segment.setLastRecordTimestamp(consumerRecord.getPubSubMessageTime());

    /**
     * End the segment without checking checksum if END_OF_SEGMENT received
     */
    KafkaMessageEnvelope messageEnvelope = consumerRecord.getValue();
    if (MessageType.valueOf(messageEnvelope) == MessageType.CONTROL_MESSAGE) {
      ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
      if (ControlMessageType.valueOf(controlMessage) == ControlMessageType.END_OF_SEGMENT) {
        segment.end(true);
      }
    }
  }

  void clearExpiredStateAndUpdateOffsetRecord(OffsetRecord offsetRecord, long maxAgeInMs) {
    long minimumRequiredRecordProducerTimestamp = offsetRecord.getMaxMessageTimeInMs() - maxAgeInMs;
    int numberOfClearedGUIDs = 0;
    Iterator<Map.Entry<GUID, Segment>> iterator = this.segments.entrySet().iterator();
    Map.Entry<GUID, Segment> entry;
    Segment segment;
    while (iterator.hasNext()) {
      entry = iterator.next();
      segment = entry.getValue();
      if (segment.getLastRecordProducerTimestamp() < minimumRequiredRecordProducerTimestamp) {
        iterator.remove();
        offsetRecord.removeProducerPartitionState(entry.getKey());
        numberOfClearedGUIDs++;
      } else {
        updateOffsetRecord(entry.getKey(), segment, offsetRecord);
      }
    }
    if (numberOfClearedGUIDs > 0) {
      logger.info("Cleared {} expired producer GUID(s).", numberOfClearedGUIDs);
    }
  }

  enum DataFaultType {
    /**
     * A given producer sent a message with a sequence number smaller or equal to the previously received
     * sequence number, rather than being exactly one greater than the previous.
     */
    DUPLICATE(msg -> new DuplicateDataException(msg)),

    /**
     * A given producer sent a message with a sequence number more than one greater than the previously
     * received sequence number, rather than being exactly one greater than the previous.
     *
     * N.B.: Out-of-order data can manifest as missing data, since the Venice Transport Protocol only
     *       keeps track of a high-water mark (the sequence number). Dealing gracefully with out-of-order
     *       data and reconstructing the proper order would require buffering arbitrarily large amounts
     *       of data would be more complex, hence why it is not supported at this time.
     */
    MISSING(msg -> new MissingDataException(msg)),

    /**
     * A given producer sent a {@link ControlMessageType#END_OF_SEGMENT} which included a checksum that
     * did not match to the data received by the same producer.
     */
    CORRUPT(msg -> new CorruptDataException(msg)),

    /**
     * Received a message from a given producer without first receiving a {@link
     * ControlMessageType#START_OF_SEGMENT}.
     *
     * N.B.: This used to show up as {@link DataFaultType#MISSING} data. This new fault type was
     *       introduced in order to disambiguate these two cases, because in some cases, the upstream
     *       code may want to be more lenient with this specific type of failure (such as when a {@link
     *       ControlMessageType#TOPIC_SWITCH} was received).
     */
    UNREGISTERED_PRODUCER(msg -> new ImproperlyStartedSegmentException(msg));

    final Function<String, DataValidationException> exceptionSupplier;

    DataFaultType(Function<String, DataValidationException> exceptionSupplier) {
      this.exceptionSupplier = exceptionSupplier;
    }

    DataValidationException getNewException(
        Segment segment,
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord) {
      return getNewException(segment, consumerRecord, null);
    }

    DataValidationException getNewException(
        Segment segment,
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
        String extraInfo) {
      ProducerMetadata producerMetadata = consumerRecord.getValue().producerMetadata;
      MessageType messageType = MessageType.valueOf(consumerRecord.getValue());
      String previousSegment, previousSequenceNumber;

      if (segment == null) {
        previousSegment = previousSequenceNumber = "N/A (null segment)";
      } else {
        previousSegment = String.valueOf(segment.getSegmentNumber());
        previousSequenceNumber = String.valueOf(segment.getSequenceNumber());
      }
      StringBuilder sb = new StringBuilder();
      // during parsing the logs, you can pipe these lines to
      // "tr ';' '\n' " and get the more readable presentation
      sb.append(name())
          .append(" data detected for producer GUID: ")
          .append(GuidUtils.getHexFromGuid(producerMetadata.producerGUID))
          .append("; message type: ")
          .append(messageType.name());
      if (MessageType.CONTROL_MESSAGE.equals(messageType)) {
        ControlMessage controlMessage = (ControlMessage) consumerRecord.getValue().payloadUnion;
        sb.append(" (").append(ControlMessageType.valueOf(controlMessage).name()).append(")");
      }

      sb.append("; partition: ").append(consumerRecord.getTopicPartition().getPartitionNumber());
      if (segment != null) {
        sb.append("; previous successful offset (in same segment): ").append(segment.getLastSuccessfulOffset());
      }
      sb.append("; incoming offset: ")
          .append(consumerRecord.getOffset())
          .append(";previous segment: ")
          .append(previousSegment)
          .append("; incoming segment: ")
          .append(producerMetadata.segmentNumber)
          .append("; previous sequence number: ")
          .append(previousSequenceNumber)
          .append("; incoming sequence number: ")
          .append(producerMetadata.messageSequenceNumber)
          .append("; consumer record timestamp: ")
          .append(consumerRecord.getPubSubMessageTime())
          .append(" (")
          .append(new Date(consumerRecord.getPubSubMessageTime()))
          .append("); producer timestamp: ")
          .append(producerMetadata.messageTimestamp)
          .append(" (")
          .append(new Date(producerMetadata.messageTimestamp))
          .append(")");
      if (consumerRecord.getValue().leaderMetadataFooter != null) {
        sb.append("; leader metadata's upstream offset: ")
            .append(consumerRecord.getValue().leaderMetadataFooter.upstreamOffset)
            .append("; leader metadata's host name: ")
            .append(consumerRecord.getValue().leaderMetadataFooter.hostName);
      }
      if (segment != null) {
        sb.append("; aggregates: ");
        printMap(segment.getAggregates(), sb);
        sb.append("; debugInfo: ");
        printMap(segment.getDebugInfo(), sb);
      }
      if (extraInfo != null) {
        sb.append("; extra info: ").append(extraInfo);
      }

      return exceptionSupplier.apply(sb.toString());
    }

    private <K, V> void printMap(Map<K, V> map, StringBuilder sb) {
      sb.append("{");
      boolean first = true;
      for (Map.Entry<K, V> entry: map.entrySet()) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(entry.getKey());
        sb.append(": ");
        sb.append(entry.getValue());
      }
      sb.append("}");
    }
  }

  public interface DIVErrorMetricCallback {
    void execute(DataValidationException exception);
  }
}
