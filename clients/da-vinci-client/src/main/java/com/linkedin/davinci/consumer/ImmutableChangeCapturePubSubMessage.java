package com.linkedin.davinci.consumer;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;


public class ImmutableChangeCapturePubSubMessage<K, V> implements PubSubMessage<K, V, VeniceChangeCoordinate> {
  private final K key;
  private final V value;
  private final PubSubTopicPartition topicPartition;
  private final VeniceChangeCoordinate changeCoordinate;
  private final long timestamp;
  private final int payloadSize;
  private final boolean isEndOfBootstrap;
  private final int writerSchemaId;
  private final java.nio.ByteBuffer replicationMetadataPayload;
  private final ControlMessage controlMessage;
  private final GenericRecord deserializedReplicationMetadata;

  public ImmutableChangeCapturePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      PubSubPosition pubSubPosition,
      long timestamp,
      int payloadSize,
      boolean isEndOfBootstrap,
      long consumerSequenceId) {
    this(
        key,
        value,
        topicPartition,
        pubSubPosition,
        timestamp,
        payloadSize,
        isEndOfBootstrap,
        consumerSequenceId,
        -1,
        null,
        null,
        null);
  }

  public ImmutableChangeCapturePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      PubSubPosition pubSubPosition,
      long timestamp,
      int payloadSize,
      boolean isEndOfBootstrap,
      long consumerSequenceId,
      int writerSchemaId,
      java.nio.ByteBuffer replicationMetadataPayload) {
    this(
        key,
        value,
        topicPartition,
        pubSubPosition,
        timestamp,
        payloadSize,
        isEndOfBootstrap,
        consumerSequenceId,
        writerSchemaId,
        replicationMetadataPayload,
        null,
        null);
  }

  public ImmutableChangeCapturePubSubMessage(
      K key,
      V value,
      PubSubTopicPartition topicPartition,
      PubSubPosition pubSubPosition,
      long timestamp,
      int payloadSize,
      boolean isEndOfBootstrap,
      long consumerSequenceId,
      int writerSchemaId,
      java.nio.ByteBuffer replicationMetadataPayload,
      ControlMessage controlMessage,
      GenericRecord deserializedReplicationMetadata) {
    this.key = key;
    this.value = value;
    this.topicPartition = Objects.requireNonNull(topicPartition);
    this.timestamp = timestamp;
    this.payloadSize = payloadSize;
    this.changeCoordinate = new VeniceChangeCoordinate(
        this.topicPartition.getPubSubTopic().getName(),
        pubSubPosition,
        this.topicPartition.getPartitionNumber(),
        consumerSequenceId);
    this.isEndOfBootstrap = isEndOfBootstrap;
    this.writerSchemaId = writerSchemaId;
    this.replicationMetadataPayload = replicationMetadataPayload;
    this.controlMessage = controlMessage;
    this.deserializedReplicationMetadata = deserializedReplicationMetadata;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public VeniceChangeCoordinate getPosition() {
    return changeCoordinate;
  }

  @Override
  public long getPubSubMessageTime() {
    return timestamp;
  }

  @Override
  public int getPayloadSize() {
    return payloadSize;
  }

  @Override
  public boolean isEndOfBootstrap() {
    return isEndOfBootstrap;
  }

  public int getWriterSchemaId() {
    return writerSchemaId;
  }

  public java.nio.ByteBuffer getReplicationMetadataPayload() {
    return replicationMetadataPayload;
  }

  public GenericRecord getDeserializedReplicationMetadata() {
    return deserializedReplicationMetadata;
  }

  public ControlMessage getControlMessage() {
    return controlMessage;
  }

  @Override
  public String toString() {
    return "PubSubMessage{" + topicPartition + ", changeCoordinate=" + changeCoordinate + ", timestamp=" + timestamp
        + ", isEndOfBootstrap=" + isEndOfBootstrap + '}';
  }

  @Override
  public int getHeapSize() {
    throw new UnsupportedOperationException("getHeapSize is not supported on " + this.getClass().getSimpleName());
  }
}
