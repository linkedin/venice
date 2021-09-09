package com.linkedin.venice.kafka.validation;

import com.linkedin.venice.exceptions.validation.*;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.linkedin.venice.kafka.validation.SegmentStatus.*;

/**
 * A segment is a sequence of messages sent by a single producer into a single partition.
 *
 * The same producer will maintain a different segment in each of the partitions it writes
 * into. On the other hand, many distinct producers can maintain their own segment for the
 * same partition, in which case, the messages contained in these various segments will be
 * interleaved.
 *
 * This class keeps track of the state of a segment:
 * - The partition it belongs to.
 * - Its segmentNumber number within its partition.
 * - Whether it has started.
 * - Whether it has ended.
 * - The current sequence number.
 * - The running checksum.
 */
public class Segment {
  // Immutable state
  private final int partition;
  private final int segmentNumber;
  private final Optional<CheckSum> checkSum;
  private final AtomicInteger sequenceNumber;
  private final Map<CharSequence, CharSequence> debugInfo;
  private final Map<CharSequence, Long> aggregates;

  // Mutable state
  private boolean registered;
  private boolean started;
  private boolean ended;
  private boolean finalSegment;
  private long lastSuccessfulOffset;
  private long lastRecordTimestamp = -1;
  private long lastRecordProducerTimestamp = -1;

  public Segment(
      int partition,
      int segmentNumber,
      int sequenceNumber,
      CheckSumType checkSumType,
      Map<CharSequence, CharSequence> debugInfo,
      Map<CharSequence, Long> aggregates) {
    this.partition = partition;
    this.segmentNumber = segmentNumber;
    this.checkSum = CheckSum.getInstance(checkSumType);
    this.sequenceNumber = new AtomicInteger(sequenceNumber);
    this.started = (sequenceNumber > 0);
    this.ended = false;
    this.finalSegment = false;
    this.debugInfo = debugInfo;
    this.aggregates = aggregates;
  }

  public Segment(int partition, int segmentNumber, CheckSumType checkSumType) {
    this(partition, segmentNumber, 0, checkSumType, new HashMap<>(), new HashMap<>());
  }

  public Segment(int partition, ProducerPartitionState state) {
    this.partition = partition;
    this.segmentNumber = state.segmentNumber;
    this.checkSum = CheckSum.getInstance(CheckSumType.valueOf(state.checksumType), state.checksumState.array());
    this.sequenceNumber = new AtomicInteger(state.messageSequenceNumber);

    /** TODO: Decide if we should only hang on to this SegmentStatus here, rather the more granular states. */
    SegmentStatus segmentStatus = SegmentStatus.valueOf(state.segmentStatus);
    this.started = segmentStatus != NOT_STARTED;
    this.ended = segmentStatus.isTerminal();
    this.finalSegment = segmentStatus == END_OF_FINAL_SEGMENT;
    this.debugInfo = state.debugInfo;
    this.aggregates = state.aggregates;
    this.registered = state.isRegistered;
    this.lastRecordProducerTimestamp = state.messageTimestamp;
  }

  public int getSegmentNumber() {
    return this.segmentNumber;
  }

  public int getPartition() {
    return this.partition;
  }

  public int getAndIncrementSequenceNumber() {
    return this.sequenceNumber.getAndIncrement();
  }

  /**
   * This function should only be used for hybrid store after receiving 'EOP' when skipping a message in the sequence.
   * @param sequenceNum
   */
  public void setSequenceNumber(int sequenceNum) {
    this.sequenceNumber.set(sequenceNum);
  }

  public int getSequenceNumber() {
    return this.sequenceNumber.get();
  }

  public Map<CharSequence, CharSequence> getDebugInfo() {
    return this.debugInfo;
  }

  public Map<CharSequence, Long> getAggregates() {
    return this.aggregates;
  }

  /**
   * `synchronized` keyword will guarantee the caller will always get the checksum after processing
   * the full record in function: {@link #addToCheckSum(KafkaKey, KafkaMessageEnvelope)}.
   * @return
   */
  public synchronized byte[] getCheckSumState() {
    if (this.checkSum.isPresent()) {
      return this.checkSum.get().getEncodedState();
    } else {
      return new byte[]{};
    }
  }

  public CheckSumType getCheckSumType() {
    return this.checkSum.map(CheckSum::getType).orElse(CheckSumType.NONE);
  }

  public boolean isStarted() {
    return this.started;
  }

  public boolean isEnded() {
    return this.ended;
  }

  public boolean isRegistered() {
    return this.registered;
  }

  public long getLastSuccessfulOffset() {
    return lastSuccessfulOffset;
  }

  public void setLastSuccessfulOffset(long lastSuccessfulOffset) {
    this.lastSuccessfulOffset = lastSuccessfulOffset;
  }

  public long getLastRecordTimestamp() {
    return lastRecordTimestamp;
  }

  public void setLastRecordTimestamp(long lastRecordTimestamp) {
    this.lastRecordTimestamp = lastRecordTimestamp;
  }

  public long getLastRecordProducerTimestamp() {
    return lastRecordProducerTimestamp;
  }

  public void setLastRecordProducerTimestamp(long lastRecordProducerTimestamp) {
    this.lastRecordProducerTimestamp = lastRecordProducerTimestamp;
  }

  public void start() {
    this.started = true;
  }

  public void end(boolean finalSegment) {
    this.ended = true;
    this.finalSegment = finalSegment;
  }

  public void registeredSegment() {
    this.registered = true;
  }

  /**
   * This function updates the running checksum as follows, depending on the {@link MessageType}:
   *
   * 1. {@link MessageType#CONTROL_MESSAGE}, depending on the specific type:
   *    1.1. {@link ControlMessageType#END_OF_SEGMENT}: No-op.
   *    1.2. All others: Message type, control message type.
   * 2. {@link MessageType#PUT}: Message type, key, schema ID and value.
   * 3. {@link MessageType#DELETE}: Message type, key.
   *
   * Both Producer and Consumer should use this same function in order to ensure coherent behavior.
   *
   * @param key of the message to add into the running checksum
   * @param messageEnvelope to add into the running checksum
   * @return true if something was added to checksum,
   *         false otherwise (which happens when hitting an {@link ControlMessageType#END_OF_SEGMENT}).
   * @throws UnsupportedMessageTypeException if the {@link MessageType} or {@link ControlMessageType} is unknown.
   */
  public synchronized boolean addToCheckSum(KafkaKey key, KafkaMessageEnvelope messageEnvelope) throws
                                                                                   UnsupportedMessageTypeException {
    // Some of the instances could be re-used and clobbered in a single-threaded setting. TODO: Explore GC tuning later.
    switch (MessageType.valueOf(messageEnvelope)) {
      case CONTROL_MESSAGE:
        ControlMessage controlMessage = (ControlMessage) messageEnvelope.payloadUnion;
        switch (ControlMessageType.valueOf(controlMessage)) {
          case END_OF_SEGMENT:
            // No-op for an end of segment.
            return false;
          case START_OF_SEGMENT:
          case START_OF_PUSH:
          case END_OF_PUSH:
          case START_OF_BUFFER_REPLAY:
          case START_OF_INCREMENTAL_PUSH:
          case END_OF_INCREMENTAL_PUSH:
          case TOPIC_SWITCH:
            // All other control messages are handled the same way.
            updateCheckSum(messageEnvelope.messageType);
            updateCheckSum(controlMessage.controlMessageType);
            return true;
          default:
            throw new UnsupportedMessageTypeException(
                "This version of Venice does not support the following control message type: " +
                    controlMessage.controlMessageType);
        }
      case PUT:
        updateCheckSum(messageEnvelope.messageType);
        updateCheckSum(key.getKey());
        Put putPayload = (Put) messageEnvelope.payloadUnion;
        updateCheckSum(putPayload.schemaId);
        ByteBuffer putValue = putPayload.putValue;
        updateCheckSum(putValue.array(), putValue.position(), putValue.remaining());
        return true;
      case UPDATE:
        updateCheckSum(messageEnvelope.messageType);
        updateCheckSum(key.getKey());
        Update updatePayload = (Update) messageEnvelope.payloadUnion;
        updateCheckSum(updatePayload.schemaId);
        updateCheckSum(updatePayload.updateSchemaId);
        ByteBuffer updateValue = updatePayload.updateValue;
        updateCheckSum(updateValue.array(), updateValue.position(), updateValue.remaining());
        return true;
      case DELETE:
        updateCheckSum(messageEnvelope.messageType);
        updateCheckSum(key.getKey());
        return true;
      default:
        throw new UnsupportedMessageTypeException(
            "This version of Venice does not support the following message type: " +
                messageEnvelope.messageType);
    }
  }

  /**
   * This is a simple safeguard in case {@link CheckSumType#NONE} is selected, in which case,
   * the {@link CheckSum} instance is null.
   *
   * @param content to add into the running checksum
   */
  private void updateCheckSum(byte[] content) {
    updateCheckSum(content, 0, content.length);
  }

  private void updateCheckSum(byte[] content, int startIndex, int length) {
    if (checkSum.isPresent()) {
      checkSum.get().update(content, startIndex, length);
    }
  }

  /**
   * This is a simple safeguard in case {@link CheckSumType#NONE} is selected, in which case,
   * the {@link CheckSum} instance is null.
   *
   * @param content to add into the running checksum
   */
  private void updateCheckSum(int content) {
    if (checkSum.isPresent()) {
      checkSum.get().update(content);
    }
  }

  public synchronized byte[] getFinalCheckSum() {
    if (this.checkSum.isPresent()) {
      return checkSum.get().getCheckSum();
    } else {
      return new byte[]{};
    }
  }

  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + partition;
    hash = hash * 31 + segmentNumber;
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Segment) {
      Segment otherSegment = (Segment) obj;
      return otherSegment.partition == this.partition && otherSegment.segmentNumber == this.segmentNumber;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "Segment(partition: " + partition
        + ", segment: " + segmentNumber
        + ", sequence: " + sequenceNumber
        + ", started: " + started
        + ", ended: " + ended
        + ", checksum: " + checkSum + ")";
  }

  public SegmentStatus getStatus() {
    if (!started) {
      return SegmentStatus.NOT_STARTED;
    } else if (ended) {
      if (finalSegment) {
        return SegmentStatus.END_OF_FINAL_SEGMENT;
      } else {
        return SegmentStatus.END_OF_INTERMEDIATE_SEGMENT;
      }
    } else {
      return SegmentStatus.IN_PROGRESS;
    }
  }
}
