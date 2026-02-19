package com.linkedin.venice.kafka.validation;

import static com.linkedin.venice.kafka.validation.SegmentStatus.END_OF_FINAL_SEGMENT;
import static com.linkedin.venice.kafka.validation.SegmentStatus.NOT_STARTED;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
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
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.utils.CollectionUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;


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
@NotThreadsafe
public class Segment {
  /**
   * This cache is to reduce the size on heap of debug info, which is very repetitive in nature. Using this cache, each
   * unique CharSequence should exist only once on the heap, with many segments referring to it.
   *
   * We use weak values so that if there are no more segments referencing a given entry, it will also be cleared from
   * the cache, and thus avoid a mem leak.
   */
  private static final Cache<CharSequence, CharSequence> DEDUPED_DEBUG_INFO =
      Caffeine.newBuilder().weakValues().build();

  // Immutable state
  private final int partition;
  private final int segmentNumber;
  private final CheckSum checkSum;
  private final Map<CharSequence, CharSequence> debugInfo;
  private final Map<CharSequence, Long> aggregates;

  // Mutable state
  private volatile int sequenceNumber;
  private volatile boolean registered;
  private volatile boolean started;
  private volatile boolean ended;
  private volatile boolean finalSegment;
  /**
   * Set this field to true when building a new segment for an incoming message, and update this flag to false immediately
   * after checking incoming message's sequence number.
   */
  private volatile boolean newSegment;
  private volatile PubSubPosition lastSuccessfulPosition;
  // record the last timestamp that a validation for this segment happened and passed.
  private volatile long lastRecordTimestamp = -1;
  // record the last producer message time stamp passed within the ConsumerRecord
  private volatile long lastRecordProducerTimestamp = -1;

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
    this.sequenceNumber = sequenceNumber;
    this.started = (sequenceNumber > 0);
    this.ended = false;
    this.finalSegment = false;
    this.newSegment = true;
    this.debugInfo = getDedupedDebugInfo(debugInfo);
    this.aggregates = aggregates;
  }

  public Segment(int partition, int segmentNumber, CheckSumType checkSumType) {
    this(partition, segmentNumber, 0, checkSumType, Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Build a segment with checkpoint producer state on disk.
   */
  public Segment(int partition, ProducerPartitionState state) {
    this.partition = partition;
    this.segmentNumber = state.segmentNumber;
    this.checkSum = CheckSum.getInstance(CheckSumType.valueOf(state.checksumType), state.checksumState.array());
    this.sequenceNumber = state.getMessageSequenceNumber();

    /** TODO: Decide if we should only hang on to this SegmentStatus here, rather the more granular states. */
    SegmentStatus segmentStatus = SegmentStatus.valueOf(state.segmentStatus);
    this.started = segmentStatus != NOT_STARTED;
    this.ended = segmentStatus.isTerminal();
    this.finalSegment = segmentStatus == END_OF_FINAL_SEGMENT;
    this.newSegment = false;
    this.debugInfo = getDedupedDebugInfo(state.getDebugInfo());
    this.aggregates = CollectionUtils.substituteEmptyMap(state.getAggregates());
    this.registered = state.isRegistered;
    this.lastRecordProducerTimestamp = state.messageTimestamp;
    this.lastRecordTimestamp = state.messageTimestamp;
  }

  public Segment(Segment segment) {
    this.partition = segment.partition;
    this.segmentNumber = segment.segmentNumber;
    this.checkSum = CheckSum.getInstance(segment.getCheckSumType(), segment.getCheckSumState());
    this.sequenceNumber = segment.sequenceNumber;

    this.started = segment.started;
    this.ended = segment.ended;
    this.finalSegment = segment.finalSegment;
    this.newSegment = false;
    /**
     * N.B. No need to call {@link #getDedupedDebugInfo(Map)} here since we assume the other {@link Segment} instance we
     * are copying from was already deduped, having come from one of the other constructors.
     */
    this.debugInfo = segment.debugInfo;
    this.aggregates = segment.aggregates;
    this.registered = segment.registered;
    this.lastRecordProducerTimestamp = segment.lastRecordProducerTimestamp;
    this.lastRecordTimestamp = segment.lastRecordTimestamp;
  }

  public int getSegmentNumber() {
    return this.segmentNumber;
  }

  public int getPartition() {
    return this.partition;
  }

  /** N.B. This function is not threadsafe. Locking must be handled by the caller. */
  public int getAndIncrementSequenceNumber() {
    return this.sequenceNumber++;
  }

  /**
   * This function should only be used for hybrid store after receiving 'EOP' when skipping a message in the sequence.
   * @param sequenceNum
   */
  public void setSequenceNumber(int sequenceNum) {
    this.sequenceNumber = sequenceNum;
  }

  public int getSequenceNumber() {
    return this.sequenceNumber;
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
    if (this.checkSum != null) {
      return this.checkSum.getEncodedState();
    } else {
      return new byte[] {};
    }
  }

  public CheckSumType getCheckSumType() {
    return this.checkSum == null ? CheckSumType.NONE : this.checkSum.getType();
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

  public PubSubPosition getLastSuccessfulPosition() {
    return lastSuccessfulPosition;
  }

  public void setLastSuccessfulPosition(PubSubPosition lastSuccessfulPosition) {
    this.lastSuccessfulPosition = lastSuccessfulPosition;
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

  public boolean isNewSegment() {
    return this.newSegment;
  }

  public void setNewSegment(boolean newSegment) {
    this.newSegment = newSegment;
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
  public synchronized boolean addToCheckSum(KafkaKey key, KafkaMessageEnvelope messageEnvelope)
      throws UnsupportedMessageTypeException {
    // Some of the instances could be re-used and clobbered in a single-threaded setting. TODO: Explore GC tuning later.
    switch (MessageType.valueOf(messageEnvelope)) {
      case CONTROL_MESSAGE:
        ControlMessage controlMessage = (ControlMessage) messageEnvelope.getPayloadUnion();
        switch (ControlMessageType.valueOf(controlMessage)) {
          case END_OF_SEGMENT:
            // No-op for an end of segment.
            return false;
          case START_OF_SEGMENT:
          case START_OF_PUSH:
          case END_OF_PUSH:
          case START_OF_INCREMENTAL_PUSH:
          case END_OF_INCREMENTAL_PUSH:
          case TOPIC_SWITCH:
          case VERSION_SWAP:
            // All other control messages are handled the same way.
            updateCheckSum(messageEnvelope.getMessageType());
            updateCheckSum(controlMessage.getControlMessageType());
            return true;
          default:
            throw new UnsupportedMessageTypeException(
                "This version of Venice does not support the following control message type: "
                    + controlMessage.getControlMessageType());
        }
      case PUT:
      case GLOBAL_RT_DIV: // GLOBAL_RT_DIV is the same as PUT, but contains a DIV object rather than user data
        // TODO: revisit to see if the GLOBAL_RT_DIV message is needed as part of the checksum
        updateCheckSum(messageEnvelope.getMessageType());
        updateCheckSum(key.getKey());
        Put putPayload = (Put) messageEnvelope.getPayloadUnion();
        updateCheckSum(putPayload.getSchemaId());
        ByteBuffer putValue = putPayload.getPutValue();
        updateCheckSum(putValue.array(), putValue.position(), putValue.remaining());
        return true;
      case UPDATE:
        updateCheckSum(messageEnvelope.getMessageType());
        updateCheckSum(key.getKey());
        Update updatePayload = (Update) messageEnvelope.getPayloadUnion();
        updateCheckSum(updatePayload.getSchemaId());
        updateCheckSum(updatePayload.getUpdateSchemaId());
        ByteBuffer updateValue = updatePayload.getUpdateValue();
        updateCheckSum(updateValue.array(), updateValue.position(), updateValue.remaining());
        return true;
      case DELETE:
        updateCheckSum(messageEnvelope.getMessageType());
        updateCheckSum(key.getKey());
        return true;
      default:
        throw new UnsupportedMessageTypeException(
            "This version of Venice does not support the following message type: " + messageEnvelope.getMessageType());
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
    if (checkSum != null) {
      checkSum.update(content, startIndex, length);
    }
  }

  /**
   * This is a simple safeguard in case {@link CheckSumType#NONE} is selected, in which case,
   * the {@link CheckSum} instance is null.
   *
   * @param content to add into the running checksum
   */
  private void updateCheckSum(int content) {
    if (checkSum != null) {
      checkSum.update(content);
    }
  }

  public synchronized byte[] getFinalCheckSum() {
    if (this.checkSum != null) {
      return checkSum.getCheckSum();
    } else {
      return new byte[] {};
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
    return "Segment(partition: " + partition + ", segment: " + segmentNumber + ", sequence: " + sequenceNumber
        + ", started: " + started + ", ended: " + ended + ", checksum: " + checkSum + ")";
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

  private Map<CharSequence, CharSequence> getDedupedDebugInfo(Map<CharSequence, CharSequence> original) {
    if (original == null || original.isEmpty()) {
      return Collections.emptyMap();
    }
    /**
     * The {@link Object2ObjectArrayMap} has an O(N) performance on lookups, but we don't care about the performance
     * of the debug info map, so it is fine. The main concern is to make it as compact as possible, which this
     * implementation achieves by minimizing per-element overhead (e.g. there is no {@link HashMap.Node} wrapping each
     * entry).
     */
    Map<CharSequence, CharSequence> deduped = new Object2ObjectArrayMap<>(original.size());
    for (Map.Entry<CharSequence, CharSequence> entry: original.entrySet()) {
      deduped.put(DEDUPED_DEBUG_INFO.get(entry.getKey(), k -> k), DEDUPED_DEBUG_INFO.get(entry.getValue(), k -> k));
    }
    return deduped;
  }

  public ProducerPartitionState toProducerPartitionState() {
    ProducerPartitionState pps = new ProducerPartitionState();
    /**
     * The aggregates and debugInfo being stored in the {@link ProducerPartitionState} will add a bit
     * of overhead when we checkpoint this metadata to disk, so we should be careful not to add a very
     * large number of elements to these arbitrary collections.
     * <p>
     * In the case of the debugInfo, it is expected (at the time of writing this comment) that all
     * partitions produced by the same producer GUID would have the same debug values (though nothing
     * precludes us from having per-partition debug values in the future if there is a use case for
     * that). It is redundant that we store the same debug values once per partition. In the future,
     * if we want to eliminate this redundancy, we could move the per-producer debug info to another
     * data structure, though that would increase bookkeeping complexity. This is expected to be a
     * minor overhead, and therefore it appears to be premature to optimize this now.
     */
    pps.aggregates = CollectionUtils.substituteEmptyMap(getAggregates());
    pps.debugInfo = CollectionUtils.substituteEmptyMap(getDebugInfo());
    populateProducerPartitionState(pps);
    return pps;
  }

  public void populateProducerPartitionState(ProducerPartitionState pps) {
    /**
     * {@link MD5Digest#getEncodedState()} is allocating a byte array to contain the intermediate,
     * which is expensive. We should only invoke this closure when necessary.
     */
    pps.checksumState = ByteBuffer.wrap(getCheckSumState());
    pps.checksumType = getCheckSumType().getValue();
    pps.segmentNumber = getSegmentNumber();
    pps.messageSequenceNumber = getSequenceNumber();
    pps.messageTimestamp = getLastRecordProducerTimestamp();
    pps.segmentStatus = getStatus().getValue();
    pps.isRegistered = isRegistered();
  }

  // Only for testing.
  public void setStarted(boolean started) {
    this.started = started;
  }
}
