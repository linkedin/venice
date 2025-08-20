package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.CONTROL_MESSAGE;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.DELETE;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;
import static com.linkedin.venice.memory.InstanceSizeEstimator.getSize;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.util.concurrent.CompletableFuture;


/**
 * This class holds all the necessary context information which is passed from
 * kafka consumer thread -> kafka producer callback thread -> drainer thread.
 *
 * All constructors are private by intention. This object should be created through the static utility function based on use case.
 * Internally these utility function fills up the consumedPosition, messageType, keyBytes, valueUnion.
 *
 * consumer thread and drainer thread creates this object by calling the appropriate static utility function based on message type.
 *
 * kafka producer callback thread fills up the producedOffset.
 *
 * drainer thread completes the persistedToDBFuture.
 */
public class LeaderProducedRecordContext implements Measurable {
  private static final int PARTIAL_CLASS_OVERHEAD =
      getClassOverhead(LeaderProducedRecordContext.class) + getClassOverhead(CompletableFuture.class);
  private static final int NO_UPSTREAM = -1;

  /**
   * Kafka cluster ID where the source kafka consumer record was consumed from.
   */
  private final int consumedKafkaClusterId;
  /**
   * This is the offset of the source PubSub consumer record from upstream PubSub
   * topic ( which could be either Real-Time or Stream-Reprocessing topic or remote VT topic).
   */
  private final PubSubPosition consumedPosition;

  /**
   * Type of message should be only PUT/DELETE/CONTROL_MESSAGE and never be UPDATE.
   */
  private final MessageType messageType;

  // key for the record.
  private byte[] keyBytes;

  /**
   * can refer to {@link com.linkedin.venice.kafka.protocol.Put} for PUT or {@link com.linkedin.venice.kafka.protocol.ControlMessage} or NULL for delete
   */
  private final Object valueUnion;

  /**
   * This is the position at which the message was produced in the Version Topic.
   */
  private PubSubPosition producedPosition = PubSubSymbolicPosition.EARLIEST;

  /**
   * This is the timestamp at which the message was produced in the Version Topic as
   * observed by the kafka producer callback thread.
   */
  private long producedTimestampMs = -1;

  /**
   * This future can be waited on to know when this record has been completely processed and persisted to database by drainer thread.
   * This future may either be created or be passed in through the constructor.
   * For most uses a single ProducedRecord is created in consumer  thread and be passed on to kafka callback thread and then to drainer thread.
   * In some cases the a single ProducedRecord created in consumer thread may get chunked and as a result multiple ProducedRecord will be
   * created to carry each chunks and the chunk value manifest from kafka callback thread to drainer thread. In this case the future in the
   * initial ProducedRecord should be passed on to create the last ProducedRecord carrying the chunk value manifest. This helps in caller wating
   * for the same future to be awakened correctly.
   */
  private final CompletableFuture<Void> persistedToDBFuture;

  public static LeaderProducedRecordContext newControlMessageRecord(
      int consumedKafkaClusterId,
      PubSubPosition consumedOffset,
      byte[] keyBytes,
      ControlMessage valueUnion) {
    checkConsumedOffsetParam(consumedOffset);
    return new LeaderProducedRecordContext(
        consumedKafkaClusterId,
        consumedOffset,
        CONTROL_MESSAGE,
        keyBytes,
        valueUnion);
  }

  public static LeaderProducedRecordContext newControlMessageRecord(byte[] keyBytes, ControlMessage valueUnion) {
    return new LeaderProducedRecordContext(
        NO_UPSTREAM,
        PubSubSymbolicPosition.EARLIEST,
        CONTROL_MESSAGE,
        keyBytes,
        valueUnion);
  }

  public static LeaderProducedRecordContext newPutRecord(
      int consumedKafkaClusterId,
      PubSubPosition consumedOffset,
      byte[] keyBytes,
      Put valueUnion) {
    checkConsumedOffsetParam(consumedOffset);
    return new LeaderProducedRecordContext(consumedKafkaClusterId, consumedOffset, PUT, keyBytes, valueUnion);
  }

  public static LeaderProducedRecordContext newChunkPutRecord(byte[] keyBytes, Put valueUnion) {
    return new LeaderProducedRecordContext(NO_UPSTREAM, PubSubSymbolicPosition.EARLIEST, PUT, keyBytes, valueUnion);
  }

  public static LeaderProducedRecordContext newChunkDeleteRecord(byte[] keyBytes, Delete valueUnion) {
    return new LeaderProducedRecordContext(NO_UPSTREAM, PubSubSymbolicPosition.EARLIEST, DELETE, keyBytes, valueUnion);
  }

  public static LeaderProducedRecordContext newPutRecordWithFuture(
      int consumedKafkaClusterId,
      PubSubPosition consumedOffset,
      byte[] keyBytes,
      Put valueUnion,
      CompletableFuture<Void> persistedToDBFuture) {
    checkConsumedOffsetParam(consumedOffset);
    return new LeaderProducedRecordContext(
        consumedKafkaClusterId,
        consumedOffset,
        PUT,
        keyBytes,
        valueUnion,
        persistedToDBFuture);
  }

  public static LeaderProducedRecordContext newDeleteRecord(
      int consumedKafkaClusterId,
      PubSubPosition consumedOffset,
      byte[] keyBytes,
      Delete valueUnion) {
    checkConsumedOffsetParam(consumedOffset);
    return new LeaderProducedRecordContext(consumedKafkaClusterId, consumedOffset, DELETE, keyBytes, valueUnion);
  }

  private LeaderProducedRecordContext(
      int consumedKafkaClusterId,
      PubSubPosition consumedPosition,
      MessageType messageType,
      byte[] keyBytes,
      Object valueUnion) {
    this(consumedKafkaClusterId, consumedPosition, messageType, keyBytes, valueUnion, new CompletableFuture());
  }

  private LeaderProducedRecordContext(
      int consumedKafkaClusterId,
      PubSubPosition consumedPosition,
      MessageType messageType,
      byte[] keyBytes,
      Object valueUnion,
      CompletableFuture persistedToDBFuture) {
    this.consumedKafkaClusterId = consumedKafkaClusterId;
    this.consumedPosition = consumedPosition;
    this.messageType = messageType;
    this.keyBytes = keyBytes;
    this.valueUnion = valueUnion;
    this.persistedToDBFuture = persistedToDBFuture;
  }

  public void setKeyBytes(byte[] keyBytes) {
    this.keyBytes = keyBytes;
  }

  public void setProducedPosition(PubSubPosition producedPosition) {
    this.producedPosition = producedPosition;
  }

  public int getConsumedKafkaClusterId() {
    return consumedKafkaClusterId;
  }

  public PubSubPosition getConsumedPosition() {
    return consumedPosition;
  }

  public MessageType getMessageType() {
    return messageType;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  public Object getValueUnion() {
    return valueUnion;
  }

  public PubSubPosition getProducedPosition() {
    return producedPosition;
  }

  public void setProducedTimestampMs(long timeMs) {
    this.producedTimestampMs = timeMs;
  }

  public long getProducedTimestampMs() {
    return producedTimestampMs;
  }

  public CompletableFuture<Void> getPersistedToDBFuture() {
    return persistedToDBFuture;
  }

  public void completePersistedToDBFuture(Exception e) {
    if (persistedToDBFuture == null) {
      return;
    }

    if (e == null) {
      persistedToDBFuture.complete(null);
    } else {
      persistedToDBFuture.completeExceptionally(e);
    }
  }

  @Override
  public String toString() {
    return "{ consumedOffset: " + consumedPosition + ", messageType: " + messageType + ", producedOffset: "
        + producedPosition + " }";
  }

  /**
   * Some bookkeeping operations are intended to be performed only on messages produced by the leader which have a
   * directly corresponding upstream message, and should be skipped for messages that are generated by the leader in the
   * absence of a directly corresponding upstream. This function helps disambiguate these cases.
   *
   * @return true if the message produced by the leader has a directly corresponding upstream message
   *         false if the message does not (e.g. happens in cases of leader-generated chunks or TopicSwitch)
   */
  public boolean hasCorrespondingUpstreamMessage() {
    return !PubSubSymbolicPosition.EARLIEST.equals(consumedPosition);
  }

  private static void checkConsumedOffsetParam(PubSubPosition consumedOffset) {
    if (consumedOffset == null || PubSubSymbolicPosition.EARLIEST.equals(consumedOffset)
        || PubSubSymbolicPosition.LATEST.equals(consumedOffset)) {
      throw new IllegalArgumentException("consumedOffset cannot be null or symbolic");
    }
  }

  @Override
  public int getHeapSize() {
    int size = PARTIAL_CLASS_OVERHEAD + getSize(this.keyBytes);
    switch (this.messageType) {
      case PUT:
        size += getSize((Put) this.valueUnion);
        break;
      case CONTROL_MESSAGE:
        size += getSize((ControlMessage) this.valueUnion);
        break;
      default:
        /**
         * Only the above two cases contribute any size.
         *
         * {@link DELETE} contributes nothing, and {@link com.linkedin.venice.kafka.protocol.enums.MessageType.UPDATE}
         * should never happen.
         */
        break;
    }
    return size;
  }
}
