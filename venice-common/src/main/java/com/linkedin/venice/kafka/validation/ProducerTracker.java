package com.linkedin.venice.kafka.validation;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.log4j.Logger;

/**
 * This class maintains state about what an upstream producer has written into Kafka.
 * It keeps track of the last segment, last sequence number and incrementally computed
 * checksum for any given partition.
 *
 * This class is not thread safe, or at least, not for concurrent calls to the same
 * partition. It is intended to be used in a single-threaded tight loop.
 */
@NotThreadsafe
public class ProducerTracker {
  private final Logger logger;

  private final GUID producerGUID;
  // This will allow to create segments for different partitions in parallel.
  private final ConcurrentMap<Integer, Segment> segments = new ConcurrentHashMap<>();

  public ProducerTracker(GUID producerGUID) {
    this.producerGUID = producerGUID;
    this.logger = Logger.getLogger(this.toString());
  }

  public String toString() {
    return ProducerTracker.class.getSimpleName() + "(GUID: " + ByteUtils.toHexString(producerGUID.bytes()) + ")";
  }

  /**
   * In some cases, such as when resetting offsets or unsubscribing from a partition,
   * the {@link ProducerTracker} should forget about the state that it accumulated
   * for a given partition.
   *
   * @param partition to clear state for
   */
  public void clearPartition(int partition) {
    segments.remove(partition);
  }

  public void setPartitionState(int partition, ProducerPartitionState state) {
    Segment segment = new Segment(partition, state);
    if (segments.containsKey(partition)) {
      logger.info(this.toString() + " will overwrite previous state for partition: " + partition +
          "\nPrevious state: " + segments.get(partition) +
          "\nNew state: " + segment);
    } else {
      logger.info(this.toString() + " will set state for partition: " + partition +
          "\nNew state: " + segment);
    }
    segments.put(partition, segment);
  }

  /**
   * Ensures the integrity of the data by maintaining state about all of the data produced by a specific
   * upstream producer:
   *
   * 1. Segment, which should be equal or greater to the previous segment.
   * 2. Sequence number, which should be exactly one greater than the previous sequence number.
   * 3. Checksum, which is computed incrementally until the end of a segment.
   *
   * @param partition from which the message was consumed
   * @param key of the consumed message
   * @param messageEnvelope which was consumed
   * @return A closure which will apply the appropriate state change to a {@link OffsetRecord}. This allows
   *         the caller to decide whether to apply the state change or not. Furthermore, it minimizes the
   *         chances that a state change may be partially applied, which could cause weird bugs if it happened.
   * @throws DataValidationException
   */
  public OffsetRecordTransformer addMessage(int partition, KafkaKey key, KafkaMessageEnvelope messageEnvelope, boolean endOfPushReceived)
      throws DataValidationException {

    Segment segment = trackSegment(partition, messageEnvelope, endOfPushReceived);
    trackSequenceNumber(segment, messageEnvelope, endOfPushReceived);

    // This is the last step, because we want failures in the previous steps to short-circuit execution.
    trackCheckSum(segment, key, messageEnvelope);

    // We return a closure, so that it is the caller's responsibility to decide whether to execute the state change or not.
    return offsetRecord -> {
      ProducerPartitionState state = offsetRecord.getProducerPartitionState(producerGUID);
      if (null == state) {
        state = new ProducerPartitionState();

        // TOOD: Set these maps properly from the beginning of segment record.
        state.aggregates = new HashMap<>();
        state.debugInfo = new HashMap<>();
        state.checksumType = segment.getCheckSumType().getValue();
      }
      state.checksumState = ByteBuffer.wrap(segment.getCheckSumState()); // TODO: Tune GC later
      state.segmentNumber = segment.getSegmentNumber();
      state.messageSequenceNumber = segment.getSequenceNumber();
      state.messageTimestamp = messageEnvelope.producerMetadata.messageTimestamp;
      state.segmentStatus = segment.getStatus().getValue();

      offsetRecord.setProducerPartitionState(producerGUID, state);

      logger.trace("ProducerPartitionState updated.");

      return offsetRecord;
    };
  }

  /**
   * This function ensures that the segment number is either equal or greater than the previous segment
   * seen for this specific partition.
   *
   * This function has the side-effect of initializing a new {@link Segment} if:
   * 1. The previous segment does not exist, or
   * 2. The incoming segment is exactly one greater than the previous one, and the previous segment is ended.
   *
   * @see #initializeNewSegment(int, KafkaMessageEnvelope, boolean, boolean)
   *
   * @param partition for which the incoming message belongs to
   * @param messageEnvelope of the incoming message
   * @throws DuplicateDataException if the incoming segment is lower than the previously seen segment.
   */
  private Segment trackSegment(int partition, KafkaMessageEnvelope messageEnvelope, boolean endOfPushReceived) throws DuplicateDataException {
    int incomingSegment = messageEnvelope.producerMetadata.segmentNumber;
    Segment previousSegment = segments.get(partition);
    if (previousSegment == null) {
      if (incomingSegment != 0) {
        handleUnregisteredProducer("track new segment with non-zero incomingSegment=" + incomingSegment,
            messageEnvelope, null, endOfPushReceived);
      }

      Segment newSegment = initializeNewSegment(partition, messageEnvelope, endOfPushReceived, true);
      return newSegment;
    } else {
      int previousSegmentNumber = previousSegment.getSegmentNumber();
      if (incomingSegment == previousSegmentNumber) {
        return previousSegment;
      } else if (incomingSegment == previousSegmentNumber + 1) {
        if (previousSegment.isEnded()) {
          /** tolerateAnyMessageType should always be false in this scenario, regardless of {@param endOfPushReceived} */
          return initializeNewSegment(partition, messageEnvelope, endOfPushReceived, false);
        } else {
          throw DataFaultType.MISSING.getNewException(previousSegment, messageEnvelope);
        }
      } else if (incomingSegment > previousSegmentNumber + 1) {
        throw DataFaultType.MISSING.getNewException(previousSegment, messageEnvelope);
      } else if (incomingSegment < previousSegmentNumber) {
        throw DataFaultType.DUPLICATE.getNewException(previousSegment, messageEnvelope);
      } else {
        // Defensive code.
        throw new IllegalStateException("This condition should never happen. " +
            getClass().getSimpleName() + " may have a regression.");
      }
    }
  }

  /**
   * @param partition for which we are initializing a new {@link Segment}
   * @param messageEnvelope which triggered this function call.
   * @param tolerateAnyMessageType if true, we will tolerate initializing the Segment on any message
   *                               if false, we will only tolerate initializing on a {@link ControlMessageType#START_OF_SEGMENT}
   * @return the newly initialized {@link Segment}
   * @throws IllegalStateException if called for a message other than a {@link ControlMessageType#START_OF_SEGMENT}
   */
  private Segment initializeNewSegment(int partition,
                                       KafkaMessageEnvelope messageEnvelope,
                                       boolean endOfPushReceived,
                                       boolean tolerateAnyMessageType) {
    CheckSumType checkSumType = CheckSumType.NONE;
    boolean unregisteredProducer = true;

    switch (MessageType.valueOf(messageEnvelope)) {
      case CONTROL_MESSAGE:
        ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
        switch (ControlMessageType.valueOf(controlMessage)) {
          case START_OF_SEGMENT:
            StartOfSegment startOfSegment = (StartOfSegment) controlMessage.controlMessageUnion;
            checkSumType = CheckSumType.valueOf(startOfSegment.checksumType);
            unregisteredProducer = false;
        }
    }

    Segment newSegment = new Segment(
        partition,
        messageEnvelope.producerMetadata.segmentNumber,
        messageEnvelope.producerMetadata.messageSequenceNumber,
        checkSumType);
    segments.put(partition, newSegment);

    if (unregisteredProducer) {
      handleUnregisteredProducer(
          "initialize new segment with a non-" + ControlMessageType.START_OF_SEGMENT.name() + " message",
          messageEnvelope, null, endOfPushReceived, Optional.of(tolerateAnyMessageType));
    }

    return newSegment;
  }

  private void handleUnregisteredProducer(String scenario,
                                          KafkaMessageEnvelope messageEnvelope,
                                          Segment segment,
                                          boolean endOfPushReceived) {
    handleUnregisteredProducer(scenario, messageEnvelope, segment, endOfPushReceived, Optional.empty());
  }

  private void handleUnregisteredProducer(String scenario,
                                          KafkaMessageEnvelope messageEnvelope,
                                          Segment segment,
                                          boolean endOfPushReceived,
                                          Optional<Boolean> tolerateAnyMessageType) {
    String extraInfoWithEndOfPush = scenario + ", endOfPushReceived=" + endOfPushReceived;
    if (tolerateAnyMessageType.isPresent()) {
      extraInfoWithEndOfPush += ", tolerateAnyMessageType=" + tolerateAnyMessageType;
    }
    if (endOfPushReceived && tolerateAnyMessageType.orElse(true)) {
      logger.info("Will " + extraInfoWithEndOfPush);
    } else {
      throw DataFaultType.UNREGISTERED_PRODUCER.getNewException(segment, messageEnvelope, Optional.of("Cannot " + extraInfoWithEndOfPush));
    }
  }

  /**
   * This function ensures that the sequence number is strictly one greater than the previous incoming
   * message for this specific partition.
   *
   * This function has the side-effect of altering the sequence number stored in the {@link Segment}.
   *
   * @param segment for which the incoming message belongs to
   * @param messageEnvelope of the incoming message
   * @throws MissingDataException if the incoming sequence number is greater than the previous sequence number + 1
   * @throws DuplicateDataException if the incoming sequence number is equal to or smaller than the previous sequence number
   */
  private void trackSequenceNumber(Segment segment, KafkaMessageEnvelope messageEnvelope, boolean endOfPushReceived)
      throws MissingDataException, DuplicateDataException {

    int previousSequenceNumber = segment.getSequenceNumber();
    int incomingSequenceNumber = messageEnvelope.producerMetadata.messageSequenceNumber;

    if (!segment.isStarted()) {
      if (previousSequenceNumber != 0) {
        handleUnregisteredProducer(
            "mark segment as started with non-zero previousSequenceNumber=" + previousSequenceNumber, messageEnvelope,
            segment, endOfPushReceived);
      }
      segment.start();
    } else if (incomingSequenceNumber == previousSequenceNumber + 1) {
      // Expected case, in steady state
      segment.getAndIncrementSequenceNumber();
    } else if (incomingSequenceNumber <= previousSequenceNumber) {
      // This is a duplicate message, which we can safely ignore.

      // Although data duplication is a benign fault, we need to bubble up for two reasons:
      // 1. We want to short-circuit data validation, because the running checksum depends on exactly-once guarantees.
      // 2. The upstream caller can choose to avoid writing duplicate data, as an optimization.
      throw DataFaultType.DUPLICATE.getNewException(segment, messageEnvelope);
    } else if (incomingSequenceNumber > previousSequenceNumber + 1) {
      // There is a gap in the sequence, so we are missing some data!

      if (endOfPushReceived) {
        // TODO: In this branch of the if, we need to adjust the sequence number, otherwise, this will cause spurious missing data metrics on further events...
      }

      throw DataFaultType.MISSING.getNewException(segment, messageEnvelope);
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
   * @param messageEnvelope of the incoming message
   * @throws CorruptDataException if the data is corrupt. Can only happen when processing control message of type:
   *                              {@link ControlMessageType#END_OF_SEGMENT}
   */
  private void trackCheckSum(Segment segment, KafkaKey key, KafkaMessageEnvelope messageEnvelope)
      throws CorruptDataException {

    if (segment.addToCheckSum(key, messageEnvelope)) {
      // The running checksum was updated successfully. Moving on.
      // TODO: Record metrics for messages added into checksum?
    } else {
      /** We have consumed an {@link ControlMessageType#END_OF_SEGMENT}. Time to verify the checksum. */
      ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
      EndOfSegment incomingEndOfSegment = (EndOfSegment) controlMessage.controlMessageUnion;
      if (Arrays.equals(segment.getFinalCheckSum(), incomingEndOfSegment.checksumValue.array())) {
        // We're good, the expected checksum matches the one we computed on the receiving end (:
        // TODO: Record metrics for successfully ended segments.
        segment.end(incomingEndOfSegment.finalSegment);
      } else {
        // Uh oh. Checksums don't match.
        // TODO: Record metrics for bad checksums.
        throw DataFaultType.CORRUPT.getNewException( segment, messageEnvelope);
      }
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
     *       ControlMessageType#START_OF_BUFFER_REPLAY} was received).
     */
    UNREGISTERED_PRODUCER(msg -> new ImproperlyStartedSegmentException(msg));

    final Function<String, DataValidationException> exceptionSupplier;

    DataFaultType(Function<String, DataValidationException> exceptionSupplier) {
      this.exceptionSupplier = exceptionSupplier;
    }

    DataValidationException getNewException(Segment segment, KafkaMessageEnvelope messageEnvelope) {
      return getNewException(segment, messageEnvelope, Optional.<String>empty());
    }


    DataValidationException getNewException(Segment segment, KafkaMessageEnvelope messageEnvelope, Optional<String> extraInfo) {
      ProducerMetadata producerMetadata = messageEnvelope.producerMetadata;
      MessageType messageType = MessageType.valueOf(messageEnvelope);
      String messageTypeString = messageType.name();
      if (MessageType.CONTROL_MESSAGE.equals(messageType)) {
        ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
        messageTypeString += " (" + ControlMessageType.valueOf(controlMessage).name() + ")";
      }

      String partition, previousSegment, previousSequenceNumber;

      if (null == segment) {
        partition = previousSegment = previousSequenceNumber = "N/A (null segment)";
      } else {
        partition = String.valueOf(segment.getPartition());
        previousSegment = String.valueOf(segment.getSegmentNumber());
        previousSequenceNumber = String.valueOf(segment.getSequenceNumber());
      }

      String exceptionMsg = name() + " data detected for producer GUID: "
          + producerMetadata.producerGUID + ",\n"
          + "message type: " + messageTypeString + ",\n"
          + "partition: " + partition + ",\n"
          + "previous segment: " + previousSegment + ",\n"
          + "incoming segment: " + producerMetadata.segmentNumber + ",\n"
          + "previous sequence number: " + previousSequenceNumber + ",\n"
          + "incoming sequence number: " + producerMetadata.messageSequenceNumber;

      if (extraInfo.isPresent()) {
        exceptionMsg += ",\n" + "extra info: " + extraInfo.get();
      }

      return exceptionSupplier.apply(exceptionMsg);
    }
  }
}
