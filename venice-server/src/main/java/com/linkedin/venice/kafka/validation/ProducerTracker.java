package com.linkedin.venice.kafka.validation;

import com.google.common.collect.Maps;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import com.linkedin.venice.utils.ByteUtils;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * This class maintains state about what an upstream producer has written into Kafka.
 * It keeps track of the last segment, last sequence number and incrementally computed
 * checksum for any given partition.
 *
 * This class is not thread safe, or at least, not for concurrent calls to the same
 * partition. It is intended to be used in a single-threaded tight loop.
 */
@NotThreadSafe
public class ProducerTracker {
  private static final Logger LOGGER = Logger.getLogger(ProducerTracker.class);

  private final GUID producerGUID;
  private final Map<Integer, Segment> segments = Maps.newHashMap();

  public ProducerTracker(GUID producerGUID) {
    this.producerGUID = producerGUID;
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
      LOGGER.info(this.toString() + " will overwrite previous state for partition: " + partition +
          "\nPrevious state: " + segments.get(partition) +
          "\nNew state: " + segment);
    } else {
      LOGGER.info(this.toString() + " will set state for partition: " + partition +
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
  public OffsetRecordTransformer addMessage(int partition, KafkaKey key, KafkaMessageEnvelope messageEnvelope)
      throws DataValidationException {

    Segment segment = trackSegment(partition, messageEnvelope);
    trackSequenceNumber(segment, messageEnvelope);

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

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("ProducerPartitionState updated.");
      }

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
   * @see #initializeNewSegment(int, KafkaMessageEnvelope)
   *
   * @param partition for which the incoming message belongs to
   * @param messageEnvelope of the incoming message
   * @throws DuplicateDataException if the incoming segment is lower than the previously seen segment.
   */
  private Segment trackSegment(int partition, KafkaMessageEnvelope messageEnvelope) throws DuplicateDataException {
    int incomingSegment = messageEnvelope.producerMetadata.segmentNumber;
    Segment previousSegment = segments.get(partition);
    if (previousSegment == null) {
      if (incomingSegment == 0) {
        return initializeNewSegment(partition, messageEnvelope);
      } else {
        throw new MissingDataException(exceptionMessage(DataFaultType.MISSING, null, messageEnvelope));
      }
    } else {
      int previousSegmentNumber = previousSegment.getSegmentNumber();
      if (incomingSegment == previousSegmentNumber) {
        return previousSegment;
      } else if (incomingSegment == previousSegmentNumber + 1) {
        if (previousSegment.isEnded()) {
          return initializeNewSegment(partition, messageEnvelope);
        } else {
          throw new MissingDataException(exceptionMessage(DataFaultType.MISSING, previousSegment, messageEnvelope));
        }
      } else if (incomingSegment > previousSegmentNumber + 1) {
        throw new MissingDataException(exceptionMessage(DataFaultType.MISSING, previousSegment, messageEnvelope));
      } else if (incomingSegment < previousSegmentNumber) {
        throw new DuplicateDataException(exceptionMessage(DataFaultType.DUPLICATE, previousSegment, messageEnvelope));
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
   * @return the newly initialized {@link Segment}
   * @throws IllegalStateException if called for a message other than a {@link ControlMessageType#START_OF_SEGMENT}
   */
  private Segment initializeNewSegment(int partition, KafkaMessageEnvelope messageEnvelope) {
    switch (MessageType.valueOf(messageEnvelope)) {
      case CONTROL_MESSAGE:
        ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
        switch (ControlMessageType.valueOf(controlMessage)) {
          case START_OF_SEGMENT:
            StartOfSegment startOfSegment = (StartOfSegment) controlMessage.controlMessageUnion;
            Segment newSegment = new Segment(
                partition,
                messageEnvelope.producerMetadata.segmentNumber,
                CheckSumType.valueOf(startOfSegment.checksumType)
            );
            segments.put(partition, newSegment);
            return newSegment;
        }
    }
    throw new IllegalStateException("Cannot initialize a new segment on anything else but a START_OF_SEGMENT control message.");
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
  private void trackSequenceNumber(Segment segment, KafkaMessageEnvelope messageEnvelope)
      throws MissingDataException, DuplicateDataException {

    int previousSequenceNumber = segment.getSequenceNumber();
    int incomingSequenceNumber = messageEnvelope.producerMetadata.messageSequenceNumber;

    if (!segment.isStarted() && previousSequenceNumber == 0) {
      // Expected case, at the start of a segment
      segment.start();
    } else if (incomingSequenceNumber == previousSequenceNumber + 1) {
      // Expected case, in steady state
      segment.getAndIncrementSequenceNumber();
      // TODO: Record metrics for messages consumed as expected.
    } else if (incomingSequenceNumber <= previousSequenceNumber) {
      // This is a duplicate message, which we can safely ignore.
      // TODO: Record metrics for duplicate messages.

      // Although data duplication is a benign fault, we need to bubble up for two reasons:
      // 1. We want to short-circuit data validation, because the running checksum depends on exactly-once guarantees.
      // 2. The upstream caller can choose to avoid writing duplicate data, as an optimization.
      throw new DuplicateDataException(exceptionMessage(DataFaultType.DUPLICATE, segment, messageEnvelope));
    } else if (incomingSequenceNumber > previousSequenceNumber + 1) {
      // There is a gap in the sequence, so we are missing some data!
      // TODO: Record metrics for missing messages.
      throw new MissingDataException(exceptionMessage(DataFaultType.MISSING, segment, messageEnvelope));
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
        throw new CorruptDataException(exceptionMessage(DataFaultType.CORRUPT, segment, messageEnvelope));
      }
    }
  }

  private String exceptionMessage(DataFaultType fault, Segment segment, KafkaMessageEnvelope messageEnvelope) {
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

    return fault.toString() + " data detected for producer GUID: " + producerMetadata.producerGUID + ",\n"
        + "message type: " + messageTypeString + ",\n"
        + "partition: " + partition + ",\n"
        + "previous segment: " + previousSegment + ",\n"
        + "incoming segment: " + producerMetadata.segmentNumber + ",\n"
        + "previous sequence number: " + previousSequenceNumber + ",\n"
        + "incoming sequence number: " + producerMetadata.messageSequenceNumber;
  }

  enum DataFaultType {
    DUPLICATE,
    MISSING,
    CORRUPT
  }
}
