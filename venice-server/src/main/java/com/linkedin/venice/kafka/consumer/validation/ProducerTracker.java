package com.linkedin.venice.kafka.consumer.validation;

import com.google.common.collect.Maps;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import org.apache.http.annotation.NotThreadSafe;

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
  private final Map<Integer, Integer> segmentsMap = Maps.newHashMap();
  private final Map<Integer, Integer> sequenceNumbersMap = Maps.newHashMap();
  // TODO: Add another map for checksum state

  /**
   * In some cases, such as when resetting offsets or unsubscribing from a partition,
   * the {@link ProducerTracker} should forget about the state that it accumulated
   * for a given partition.
   *
   * @param partition to clear state for
   */
  public void clearPartition(int partition) {
    segmentsMap.remove(partition);
    sequenceNumbersMap.remove(partition);
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
   * @param messageEnvelope which was consumed
   * @throws DataValidationException
   */
  public void addMessage(int partition, KafkaMessageEnvelope messageEnvelope) throws DataValidationException {
    ProducerMetadata producerMetadata = messageEnvelope.producerMetadata;

    try {
      trackSegment(partition, producerMetadata);
      verifySequenceNumber(partition, producerMetadata);

      // This is the last step, because we want failures in the previous steps to short-circuit execution.
      computeChecksum(partition, messageEnvelope);
    } finally {
      // Regardless of what happened during validation, we update our sequence number.
      sequenceNumbersMap.put(partition, producerMetadata.messageSequenceNumber);
    }
  }

  /**
   * This function ensures that the segment number is either equal or greater than the previous segment
   * seen for this specific partition.
   *
   * If the previous segment does not exist, or if the incoming segment is greater than the previous one,
   * then this function has the side-effect of initializing a new segment.
   *
   * @see #initializeNewSegment(int, int)
   *
   * @param partition for which the incoming message belongs to
   * @param producerMetadata of the incoming message
   * @throws DuplicateDataException if the incoming segment is lower than the previously seen segment.
   */
  private void trackSegment(int partition, ProducerMetadata producerMetadata) throws DuplicateDataException {
    int incomingSegment = producerMetadata.segmentNumber;
    Integer previousSegment = segmentsMap.get(partition);
    if (previousSegment == null) {
      initializeNewSegment(partition, incomingSegment);
    } else if (incomingSegment > previousSegment) {
      initializeNewSegment(partition, incomingSegment);

      // TODO: Handle checksum validation.
    } else if (incomingSegment < previousSegment) {
      throw new DuplicateDataException(exceptionMessage(DataFaultType.DUPLICATE, partition, producerMetadata));
    }
  }

  private void initializeNewSegment(int partition, int incomingSegment) {
    segmentsMap.put(partition, incomingSegment);

    // When starting a new segment, we need to discard whatever sequence number
    // we had for a previous segment of the same partition.
    sequenceNumbersMap.remove(partition);
  }

  /**
   * This function ensures that the sequence number is strictly one greater than the previous incoming
   * message for this specific partition.
   *
   * @param partition for which the incoming message belongs to
   * @param producerMetadata of the incoming message
   * @throws MissingDataException if the incoming sequence number is greater than the previous sequence number + 1
   * @throws DuplicateDataException if the incoming sequence number is equal to or smaller than the previous sequence number
   */
  private void verifySequenceNumber(int partition, ProducerMetadata producerMetadata)
      throws MissingDataException, DuplicateDataException {

    if (sequenceNumbersMap.containsKey(partition)) {
      // If we've already seen the incoming partition/segment before, we verify its sequence number

      int previousSequenceNumber = sequenceNumbersMap.get(partition);
      int incomingSequenceNumber = producerMetadata.messageSequenceNumber;

      if (incomingSequenceNumber == previousSequenceNumber + 1) {
        // Expected case

        // TODO: Record metrics for messages consumed as expected
      } else if (incomingSequenceNumber <= previousSequenceNumber) {
        // This is a duplicate message, which we can safely ignore.

        // TODO: Record metrics for duplicate messages

        // Although data duplication is a benign fault, we need to bubble up for two reasons:
        // 1. We want to short-circuit data validation, because the running checksum depends on exactly-once guarantees.
        // 2. The upstream caller can choose to avoid writing duplicate data, as an optimization.
        throw new DuplicateDataException(exceptionMessage(DataFaultType.DUPLICATE, partition, producerMetadata));
      } else if (incomingSequenceNumber > previousSequenceNumber + 1) {
        // There is a gap in the sequence, so we are missing some data!

        // TODO: Record metrics for missing messages

        throw new MissingDataException(exceptionMessage(DataFaultType.MISSING, partition, producerMetadata));
      } else {
        // Defensive coding, to prevent regressions in the above code from causing silent failures
        throw new IllegalStateException("Unreachable code!");
      }
    }
  }

  /**
   * This function maintains a running checksum of the data seen so far for this specific partition.
   *
   * @param partition for which the incoming message belongs to
   * @param messageEnvelope of the incoming message
   * @throws CorruptDataException if the data is corrupt. Can only happen when processing control message of type:
   *                              {@link ControlMessageType#END_OF_SEGMENT}
   */
  private void computeChecksum(int partition, KafkaMessageEnvelope messageEnvelope) throws CorruptDataException {
    switch (MessageType.valueOf(messageEnvelope)) {
      case CONTROL_MESSAGE:
        ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
        switch (ControlMessageType.valueOf(controlMessage)) {
          case END_OF_SEGMENT:
            // Compare the checksum that was computed upstream with the running checksum computed locally
            break;
          default:
            // No-op
            break;
        }
        break;
      case PUT:
        // Add to running checksum
        break;
      case DELETE:
        // Add to running checksum
        break;
      default:
        // No-op
        break;
    }
  }

  private String exceptionMessage(DataFaultType fault, int partition, ProducerMetadata producerMetadata) {
    return fault.toString() + " data detected for producer GUID: " + producerMetadata.producerGUID
        + ", partition: " + partition
        + ", previous segment: " + segmentsMap.get(partition)
        + ", incoming segment: " + producerMetadata.segmentNumber
        + ", previous sequence number: " + sequenceNumbersMap.get(partition)
        + ", incoming sequence number: " + producerMetadata.messageSequenceNumber;
  }

  enum DataFaultType {
    DUPLICATE,
    MISSING,
    CORRUPT
  }
}
