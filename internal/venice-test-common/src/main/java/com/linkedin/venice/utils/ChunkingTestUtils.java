package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;


public final class ChunkingTestUtils {
  private ChunkingTestUtils() {
    // Util class
  }

  public final static int CHUNK_LENGTH = 10;
  public final static KeyWithChunkingSuffixSerializer KEY_WITH_CHUNKING_SUFFIX_SERIALIZER =
      new KeyWithChunkingSuffixSerializer();

  public static byte[] createChunkBytes(int startValue, final int chunkLength) {
    byte[] chunkBytes = new byte[chunkLength];
    for (int i = 0; i < chunkBytes.length; i++) {
      chunkBytes[i] = (byte) startValue;
      startValue++;
    }
    return chunkBytes;
  }

  public static ChunkedKeySuffix createChunkedKeySuffix(
      int firstSegmentNumber,
      int firstSequenceNumber,
      int chunkIndex) {
    ChunkId chunkId = new ChunkId();
    chunkId.segmentNumber = firstSegmentNumber;
    chunkId.messageSequenceNumber = firstSequenceNumber;
    chunkId.chunkIndex = chunkIndex;
    chunkId.producerGUID = new GUID();
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.chunkId = chunkId;
    return chunkedKeySuffix;
  }

  public static KafkaMessageEnvelope createKafkaMessageEnvelope(
      MessageType messageType,
      int segmentNumber,
      int sequenceNumber) {
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = messageType.getValue();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.segmentNumber = segmentNumber;
    messageEnvelope.producerMetadata.messageSequenceNumber = sequenceNumber;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    return messageEnvelope;
  }

  public static DefaultPubSubMessage createChunkedRecord(
      byte[] serializedKey,
      int firstSegmentNumber,
      int firstSequenceNumber,
      int chunkIndex,
      int firstMessageOffset,
      PubSubTopicPartition pubSubTopicPartition) {
    long newOffset = firstMessageOffset + chunkIndex;
    int newSequenceNumber = firstSequenceNumber + chunkIndex;
    ChunkedKeySuffix chunkKeySuffix = createChunkedKeySuffix(firstSegmentNumber, firstSequenceNumber, chunkIndex);
    ByteBuffer chunkKeyWithSuffix =
        KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeChunkedKey(serializedKey, chunkKeySuffix);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, ByteUtils.extractByteArray(chunkKeyWithSuffix));
    KafkaMessageEnvelope messageEnvelope =
        createKafkaMessageEnvelope(MessageType.PUT, firstSegmentNumber, newSequenceNumber);

    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    ByteBuffer valueBytes = ByteBuffer.wrap(createChunkBytes(chunkIndex * CHUNK_LENGTH, CHUNK_LENGTH));
    put.putValue = ByteUtils.prependIntHeaderToByteBuffer(valueBytes, put.schemaId);
    put.replicationMetadataPayload = VeniceWriter.EMPTY_BYTE_BUFFER;
    messageEnvelope.payloadUnion = put;
    return new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(newOffset),
        0,
        20);
  }

  public static DefaultPubSubMessage createChunkValueManifestRecord(
      byte[] serializedKey,
      DefaultPubSubMessage firstMessage,
      int numberOfChunks,
      PubSubTopicPartition pubSubTopicPartition) {
    long newOffset = firstMessage.getPosition().getNumericOffset() + numberOfChunks;
    byte[] chunkKeyWithSuffix = KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = createKafkaMessageEnvelope(
        MessageType.PUT,
        firstMessage.getValue().getProducerMetadata().segmentNumber,
        firstMessage.getValue().getProducerMetadata().messageSequenceNumber + numberOfChunks);

    ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(true);
    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>(numberOfChunks);
    manifest.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    manifest.size = numberOfChunks * CHUNK_LENGTH;
    manifest.keysWithChunkIdSuffix.add(ByteBuffer.wrap(firstMessage.getKey().getKey()));
    ByteBuffer putValueBytes = chunkedValueManifestSerializer.serialize(manifest);

    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    put.putValue = ByteUtils.prependIntHeaderToByteBuffer(putValueBytes, put.schemaId);
    put.replicationMetadataPayload = VeniceWriter.EMPTY_BYTE_BUFFER;
    messageEnvelope.payloadUnion = put;
    return new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(newOffset),
        0,
        20);
  }

  public static ByteBuffer createReplicationMetadataPayload(int size) {
    ChunkedValueManifestSerializer chunkedRmdManifestSerializer = new ChunkedValueManifestSerializer(true);
    ChunkedValueManifest chunkedRmdManifest = new ChunkedValueManifest();
    chunkedRmdManifest.keysWithChunkIdSuffix = new ArrayList<>(1);
    chunkedRmdManifest.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    chunkedRmdManifest.size = size;
    ByteBuffer putValueBytes = chunkedRmdManifestSerializer.serialize(chunkedRmdManifest);
    return ByteUtils.prependIntHeaderToByteBuffer(putValueBytes, chunkedRmdManifest.schemaId);
  }

  public static DefaultPubSubMessage createDeleteRecord(
      byte[] serializedKey,
      byte[] serializedRmd,
      PubSubTopicPartition pubSubTopicPartition) {
    byte[] chunkKeyWithSuffix = KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = createKafkaMessageEnvelope(MessageType.DELETE, 0, 0);

    Delete delete = new Delete();
    delete.schemaId = 1;
    if (serializedRmd != null) {
      delete.replicationMetadataPayload = ByteBuffer.wrap(serializedRmd);
      delete.replicationMetadataVersionId = 1;
    }
    messageEnvelope.payloadUnion = delete;
    return new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(1),
        0,
        20);
  }

  public static DefaultPubSubMessage createPutRecord(
      byte[] serializedKey,
      byte[] serializedValue,
      byte[] serializedRmd,
      PubSubTopicPartition pubSubTopicPartition) {
    byte[] chunkKeyWithSuffix = KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = createKafkaMessageEnvelope(MessageType.PUT, 0, 0);

    Put put = new Put();
    put.schemaId = 1;
    put.putValue = ByteBuffer.wrap(serializedValue);
    if (serializedRmd != null) {
      put.replicationMetadataPayload = ByteBuffer.wrap(serializedRmd);
      put.replicationMetadataVersionId = 1;
    }
    messageEnvelope.payloadUnion = put;
    return new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(1),
        0,
        serializedValue.length);
  }

  public static DefaultPubSubMessage createUpdateRecord(
      byte[] serializedKey,
      byte[] serializedValue,
      PubSubTopicPartition pubSubTopicPartition) {
    byte[] chunkKeyWithSuffix = KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(serializedKey);
    KafkaKey kafkaKey = new KafkaKey(MessageType.UPDATE, chunkKeyWithSuffix);
    KafkaMessageEnvelope messageEnvelope = createKafkaMessageEnvelope(MessageType.UPDATE, 0, 0);

    Update update = new Update();
    update.schemaId = 1;
    update.updateValue = ByteBuffer.wrap(serializedValue);
    update.updateSchemaId = 1;
    messageEnvelope.payloadUnion = update;
    return new ImmutablePubSubMessage(
        kafkaKey,
        messageEnvelope,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(1),
        0,
        serializedValue.length);
  }
}
