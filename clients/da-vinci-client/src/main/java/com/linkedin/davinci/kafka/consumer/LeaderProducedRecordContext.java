package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import java.util.concurrent.CompletableFuture;


/**
 * This class holds all the necessary context information which is passed from
 * kafka consumer thread -> kafka producer callback thread -> drainer thread.
 *
 * All constructors are private by intention. This object should be created through the static utility function based on usecase.
 * Internally these utility function fills up the consumedOffset, messageType, keyBytes, valueUnion.
 *
 * consumer thread and drainer thread creates this object by calling the appropriate static utility function based on message type.
 *
 * kafka producer callback thread fills up the producedOffset.
 *
 * drainer thread completes the persistedToDBFuture.
 */

public class LeaderProducedRecordContext {
  /**
   * Kafka cluster ID where the source kafka consumer record was consumed from.
   */
  private final int consumedKafkaClusterId;
  /**
   * This is the offset of the source kafka consumer record from upstream kafka
   * topic ( which could be either Real-Time or Stream-Reprocessing topic or remote VT topic).
   */
  private final long consumedOffset;

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
   * This is the offset at which the message was produced in the Version Topic.
   */
  private long producedOffset = -1;

  /**
   * This future can be waited on to know when this record has been completely processed and persisted to database by drainer thread.
   * This future may either be created or be passed in through the constructor.
   * For most uses a single ProducedRecord is created in consumer  thread and be passed on to kafka callback thread and then to drainer thread.
   * In some cases the a single ProducedRecord created in consumer thread may get chunked and as a result multiple ProducedRecord will be
   * created to carry each chunks and the chunk value manifest from kafka callback thread to drainer thread. In this case the future in the
   * initial ProducedRecord should be passed on to create the last ProducedRecord carrying the chunk value manifest. This helps in caller wating
   * for the same future to be awakened correctly.
   */
  private CompletableFuture<Void> persistedToDBFuture = null;

  public static LeaderProducedRecordContext newControlMessageRecord(
      int consumedKafkaClusterId,
      long consumedOffset,
      byte[] keyBytes,
      ControlMessage valueUnion) {
    return new LeaderProducedRecordContext(
        consumedKafkaClusterId,
        consumedOffset,
        MessageType.CONTROL_MESSAGE,
        keyBytes,
        valueUnion,
        true);
  }

  public static LeaderProducedRecordContext newPutRecord(
      int consumedKafkaClusterId,
      long consumedOffset,
      byte[] keyBytes,
      Put valueUnion) {
    return new LeaderProducedRecordContext(
        consumedKafkaClusterId,
        consumedOffset,
        MessageType.PUT,
        keyBytes,
        valueUnion,
        true);
  }

  public static LeaderProducedRecordContext newPutRecordWithFuture(
      int consumedKafkaClusterId,
      long consumedOffset,
      byte[] keyBytes,
      Put valueUnion,
      CompletableFuture<Void> persistedToDBFuture) {
    LeaderProducedRecordContext leaderProducedRecordContext = new LeaderProducedRecordContext(
        consumedKafkaClusterId,
        consumedOffset,
        MessageType.PUT,
        keyBytes,
        valueUnion,
        false);
    leaderProducedRecordContext.persistedToDBFuture = persistedToDBFuture;
    return leaderProducedRecordContext;
  }

  public static LeaderProducedRecordContext newDeleteRecord(
      int consumedKafkaClusterId,
      long consumedOffset,
      byte[] keyBytes,
      Delete valueUnion) {
    return new LeaderProducedRecordContext(
        consumedKafkaClusterId,
        consumedOffset,
        MessageType.DELETE,
        keyBytes,
        valueUnion,
        true);
  }

  private LeaderProducedRecordContext(
      int consumedKafkaClusterId,
      long consumedOffset,
      MessageType messageType,
      byte[] keyBytes,
      Object valueUnion,
      boolean createFuture) {
    this.consumedKafkaClusterId = consumedKafkaClusterId;
    this.consumedOffset = consumedOffset;
    this.messageType = messageType;
    this.keyBytes = keyBytes;
    this.valueUnion = valueUnion;
    if (createFuture) {
      this.persistedToDBFuture = new CompletableFuture<>();
    }
  }

  public void setKeyBytes(byte[] keyBytes) {
    this.keyBytes = keyBytes;
  }

  public void setProducedOffset(long producerOffset) {
    this.producedOffset = producerOffset;
  }

  public int getConsumedKafkaClusterId() {
    return consumedKafkaClusterId;
  }

  public long getConsumedOffset() {
    return consumedOffset;
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

  public long getProducedOffset() {
    return producedOffset;
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
    return "{ consumedOffset: " + consumedOffset + ", messageType: " + messageType + ", producedOffset: "
        + producedOffset + " }";
  }
}
