package com.linkedin.venice.writer;

import static com.linkedin.venice.message.KafkaKey.HEART_BEAT;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_COMPLETED;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_NOT_COMPLETED;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES;
import static com.linkedin.venice.writer.VeniceWriter.VENICE_DEFAULT_LOGICAL_TS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask;
import com.linkedin.davinci.kafka.consumer.LeaderProducerCallback;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.guid.HeartbeatGuidV3Generator;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceWriterUnitTest {
  @Test(dataProvider = "Chunking-And-Partition-Counts", dataProviderClass = DataProviderUtils.class)
  public void testTargetPartitionIsSameForAllOperationsWithTheSameKey(boolean isChunkingEnabled, int partitionCount) {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);

    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setPartitionCount(partitionCount)
        .setChunkingEnabled(isChunkingEnabled)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);

    String valueString = "value-string";
    String key = "test-key";

    ArgumentCaptor<Integer> putPartitionArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.put(key, valueString, 1, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(anyString(), putPartitionArgumentCaptor.capture(), any(), any(), any(), any());

    ArgumentCaptor<Integer> deletePartitionArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(anyString(), deletePartitionArgumentCaptor.capture(), any(), any(), any(), any());

    ArgumentCaptor<Integer> updatePartitionArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
    writer.delete(key, null);
    verify(mockedProducer, atLeast(2))
        .sendMessage(anyString(), updatePartitionArgumentCaptor.capture(), any(), any(), any(), any());

    assertEquals(putPartitionArgumentCaptor.getValue(), deletePartitionArgumentCaptor.getValue());
    assertEquals(putPartitionArgumentCaptor.getValue(), updatePartitionArgumentCaptor.getValue());
  }

  @Test
  public void testDeleteDeprecatedChunk() {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);
    byte[] serializedKeyBytes = new byte[] { 0xa, 0xb };
    writer.deleteDeprecatedChunk(serializedKeyBytes, 0, null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER, null);
    writer.deleteDeprecatedChunk(
        serializedKeyBytes,
        0,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        new DeleteMetadata(
            AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(),
            1,
            WriterChunkingHelper.EMPTY_BYTE_BUFFER));

    ArgumentCaptor<KafkaKey> keyArgumentCaptor = ArgumentCaptor.forClass(KafkaKey.class);
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(3))
        .sendMessage(any(), any(), keyArgumentCaptor.capture(), kmeArgumentCaptor.capture(), any(), any());
    assertEquals(kmeArgumentCaptor.getAllValues().size(), 3);
    KafkaMessageEnvelope actualValue1 = kmeArgumentCaptor.getAllValues().get(1);
    assertEquals(actualValue1.messageType, MessageType.DELETE.getValue());
    assertEquals(((Delete) actualValue1.payloadUnion).schemaId, -10);
    assertEquals(((Delete) actualValue1.payloadUnion).replicationMetadataVersionId, -1);
    assertEquals(
        ((Delete) actualValue1.payloadUnion).replicationMetadataPayload,
        WriterChunkingHelper.EMPTY_BYTE_BUFFER);
    KafkaMessageEnvelope actualValue2 = kmeArgumentCaptor.getAllValues().get(2);
    assertEquals(actualValue2.messageType, MessageType.DELETE.getValue());
    assertEquals(((Delete) actualValue2.payloadUnion).schemaId, -10);
    assertEquals(((Delete) actualValue2.payloadUnion).replicationMetadataVersionId, 1);
    assertEquals(
        ((Delete) actualValue2.payloadUnion).replicationMetadataPayload,
        WriterChunkingHelper.EMPTY_BYTE_BUFFER);
  }

  @Test(timeOut = 10000)
  public void testReplicationMetadataChunking() {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);

    ByteBuffer replicationMetadata = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    PutMetadata putMetadata = new PutMetadata(1, replicationMetadata);

    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      stringBuilder.append("abcdefghabcdefghabcdefghabcdefgh");
    }
    String valueString = stringBuilder.toString();

    LeaderProducerCallback leaderProducerCallback = mock(LeaderProducerCallback.class);
    PartitionConsumptionState.TransientRecord transientRecord =
        new PartitionConsumptionState.TransientRecord(new byte[] { 0xa }, 0, 0, 0, 0, 0);
    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    when(leaderProducerCallback.getPartitionConsumptionState()).thenReturn(partitionConsumptionState);
    when(partitionConsumptionState.getTransientRecord(any())).thenReturn(transientRecord);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = mock(PubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    when(record.getKey()).thenReturn(kafkaKey);
    when(kafkaKey.getKey()).thenReturn(new byte[] { 0xa });
    when(leaderProducerCallback.getSourceConsumerRecord()).thenReturn(record);
    LeaderFollowerStoreIngestionTask storeIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    when(storeIngestionTask.isTransientRecordBufferUsed()).thenReturn(true);
    when(leaderProducerCallback.getIngestionTask()).thenReturn(storeIngestionTask);
    doCallRealMethod().when(leaderProducerCallback).setChunkingInfo(any(), any(), any(), any(), any(), any(), any());
    writer.put(
        Integer.toString(1),
        valueString,
        1,
        leaderProducerCallback,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        putMetadata);
    ArgumentCaptor<KafkaKey> keyArgumentCaptor = ArgumentCaptor.forClass(KafkaKey.class);
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(2))
        .sendMessage(any(), any(), keyArgumentCaptor.capture(), kmeArgumentCaptor.capture(), any(), any());

    Assert.assertNotNull(transientRecord.getValueManifest());
    Assert.assertNotNull(transientRecord.getRmdManifest());
    assertEquals(transientRecord.getValueManifest().getKeysWithChunkIdSuffix().size(), 2);
    assertEquals(transientRecord.getRmdManifest().getKeysWithChunkIdSuffix().size(), 1);

    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    byte[] serializedKey = serializer.serialize(testTopic, Integer.toString(1));
    byte[] serializedValue = serializer.serialize(testTopic, valueString);
    byte[] serializedRmd = replicationMetadata.array();
    int availableMessageSize = DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES - serializedKey.length;

    // The order should be SOS, valueChunk1, valueChunk2, replicationMetadataChunk1, manifest for value and RMD.
    assertEquals(kmeArgumentCaptor.getAllValues().size(), 5);

    // Verify value of the 1st chunk.
    KafkaMessageEnvelope actualValue1 = kmeArgumentCaptor.getAllValues().get(1);
    assertEquals(actualValue1.messageType, MessageType.PUT.getValue());
    assertEquals(((Put) actualValue1.payloadUnion).schemaId, -10);
    assertEquals(((Put) actualValue1.payloadUnion).replicationMetadataVersionId, -1);
    assertEquals(((Put) actualValue1.payloadUnion).replicationMetadataPayload, ByteBuffer.allocate(0));
    assertEquals(((Put) actualValue1.payloadUnion).putValue.array().length, availableMessageSize + 4);
    assertEquals(actualValue1.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // Verify value of the 2nd chunk.
    KafkaMessageEnvelope actualValue2 = kmeArgumentCaptor.getAllValues().get(2);
    assertEquals(actualValue2.messageType, MessageType.PUT.getValue());
    assertEquals(((Put) actualValue2.payloadUnion).schemaId, -10);
    assertEquals(((Put) actualValue2.payloadUnion).replicationMetadataVersionId, -1);
    assertEquals(((Put) actualValue2.payloadUnion).replicationMetadataPayload, ByteBuffer.allocate(0));
    assertEquals(
        ((Put) actualValue2.payloadUnion).putValue.array().length,
        (serializedValue.length - availableMessageSize) + 4);
    assertEquals(actualValue2.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(true);

    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.schemaId = 1;
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(2);
    chunkedValueManifest.size = serializedValue.length;

    // Verify key of the 1st value chunk.
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    chunkedKeySuffix.chunkId.chunkIndex = 0;
    ProducerMetadata producerMetadata = actualValue1.producerMetadata;
    chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;

    ByteBuffer keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey1 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey1 = keyArgumentCaptor.getAllValues().get(1);
    assertEquals(actualKey1.getKey(), expectedKey1.getKey());

    // Verify key of the 2nd value chunk.
    chunkedKeySuffix.chunkId.chunkIndex = 1;
    keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey2 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey2 = keyArgumentCaptor.getAllValues().get(2);
    assertEquals(actualKey2.getKey(), expectedKey2.getKey());

    // Check value of the 1st RMD chunk.
    KafkaMessageEnvelope actualValue3 = kmeArgumentCaptor.getAllValues().get(3);
    assertEquals(actualValue3.messageType, MessageType.PUT.getValue());
    assertEquals(((Put) actualValue3.payloadUnion).schemaId, -10);
    assertEquals(((Put) actualValue3.payloadUnion).replicationMetadataVersionId, -1);
    assertEquals(((Put) actualValue3.payloadUnion).putValue, ByteBuffer.allocate(0));
    assertEquals(((Put) actualValue3.payloadUnion).replicationMetadataPayload.array().length, serializedRmd.length + 4);
    assertEquals(actualValue3.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // Check key of the 1st RMD chunk.
    ChunkedValueManifest chunkedRmdManifest = new ChunkedValueManifest();
    chunkedRmdManifest.schemaId = 1;
    chunkedRmdManifest.keysWithChunkIdSuffix = new ArrayList<>(1);
    chunkedRmdManifest.size = serializedRmd.length;
    chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    producerMetadata = actualValue3.producerMetadata;
    chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;
    // The chunkIndex of the first RMD should be the number of value chunks so that key space of value chunk and RMD
    // chunk will not collide.
    chunkedKeySuffix.chunkId.chunkIndex = 2;
    keyWithSuffix = keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix);
    chunkedRmdManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
    KafkaKey expectedKey3 = new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    KafkaKey actualKey3 = keyArgumentCaptor.getAllValues().get(3);
    assertEquals(actualKey3.getKey(), expectedKey3.getKey());

    // Check key of the manifest.
    byte[] topLevelKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey expectedKey4 = new KafkaKey(MessageType.PUT, topLevelKey);
    KafkaKey actualKey4 = keyArgumentCaptor.getAllValues().get(4);
    assertEquals(actualKey4.getKey(), expectedKey4.getKey());

    // Check manifest for both value and rmd.
    KafkaMessageEnvelope actualValue4 = kmeArgumentCaptor.getAllValues().get(4);
    assertEquals(actualValue4.messageType, MessageType.PUT.getValue());
    assertEquals(
        ((Put) actualValue4.payloadUnion).schemaId,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
    assertEquals(((Put) actualValue4.payloadUnion).replicationMetadataVersionId, putMetadata.getRmdVersionId());
    assertEquals(
        ((Put) actualValue4.payloadUnion).replicationMetadataPayload,
        ByteBuffer.wrap(chunkedValueManifestSerializer.serialize(testTopic, chunkedRmdManifest)));
    assertEquals(
        ((Put) actualValue4.payloadUnion).putValue,
        ByteBuffer.wrap(chunkedValueManifestSerializer.serialize(testTopic, chunkedValueManifest)));
    assertEquals(actualValue4.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

  }

  @Test
  public void testReplicationMetadataWrittenCorrectly() {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), mockedProducer);

    // verify the new veniceWriter API's are able to encode the A/A metadat info correctly.
    long ctime = System.currentTimeMillis();
    ByteBuffer replicationMetadata = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    PutMetadata putMetadata = new PutMetadata(1, replicationMetadata);
    DeleteMetadata deleteMetadata = new DeleteMetadata(1, 1, replicationMetadata);

    writer.put(
        Integer.toString(1),
        Integer.toString(1),
        1,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        ctime,
        null);
    writer.put(
        Integer.toString(2),
        Integer.toString(2),
        1,
        null,
        VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        putMetadata);
    writer.update(Integer.toString(3), Integer.toString(2), 1, 1, null, ctime);
    writer.delete(Integer.toString(4), null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER, ctime);
    writer.delete(Integer.toString(5), null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER, deleteMetadata);
    writer.put(Integer.toString(6), Integer.toString(1), 1, null, VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);

    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(2)).sendMessage(any(), any(), any(), kmeArgumentCaptor.capture(), any(), any());

    // first one will be control message SOS, there should not be any aa metadata.
    KafkaMessageEnvelope value0 = kmeArgumentCaptor.getAllValues().get(0);
    assertEquals(value0.producerMetadata.logicalTimestamp, VENICE_DEFAULT_LOGICAL_TS);

    // verify timestamp is encoded correctly.
    KafkaMessageEnvelope value1 = kmeArgumentCaptor.getAllValues().get(1);
    KafkaMessageEnvelope value3 = kmeArgumentCaptor.getAllValues().get(3);
    KafkaMessageEnvelope value4 = kmeArgumentCaptor.getAllValues().get(4);
    for (KafkaMessageEnvelope kme: Arrays.asList(value1, value3, value4)) {
      assertEquals(kme.producerMetadata.logicalTimestamp, ctime);
    }

    // verify default values for replicationMetadata are written correctly
    Put put = (Put) value1.payloadUnion;
    assertEquals(put.schemaId, 1);
    assertEquals(put.replicationMetadataVersionId, VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID);
    assertEquals(put.replicationMetadataPayload, ByteBuffer.wrap(new byte[0]));

    Delete delete = (Delete) value4.payloadUnion;
    assertEquals(delete.schemaId, VeniceWriter.VENICE_DEFAULT_VALUE_SCHEMA_ID);
    assertEquals(delete.replicationMetadataVersionId, VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID);
    assertEquals(delete.replicationMetadataPayload, ByteBuffer.wrap(new byte[0]));

    // verify replicationMetadata is encoded correctly for Put.
    KafkaMessageEnvelope value2 = kmeArgumentCaptor.getAllValues().get(2);
    assertEquals(value2.messageType, MessageType.PUT.getValue());
    put = (Put) value2.payloadUnion;
    assertEquals(put.schemaId, 1);
    assertEquals(put.replicationMetadataVersionId, 1);
    assertEquals(put.replicationMetadataPayload, ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    assertEquals(value2.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

    // verify replicationMetadata is encoded correctly for Delete.
    KafkaMessageEnvelope value5 = kmeArgumentCaptor.getAllValues().get(5);
    assertEquals(value5.messageType, MessageType.DELETE.getValue());
    delete = (Delete) value5.payloadUnion;
    assertEquals(delete.schemaId, 1);
    assertEquals(delete.replicationMetadataVersionId, 1);
    assertEquals(delete.replicationMetadataPayload, ByteBuffer.wrap(new byte[] { 0xa, 0xb }));
    assertEquals(value5.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);

    // verify default logical_ts is encoded correctly
    KafkaMessageEnvelope value6 = kmeArgumentCaptor.getAllValues().get(6);
    assertEquals(value6.messageType, MessageType.PUT.getValue());
    assertEquals(value6.producerMetadata.logicalTimestamp, APP_DEFAULT_LOGICAL_TS);
  }

  @Test
  public void testCloseSegmentBasedOnElapsedTime() {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    writerProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, 0);
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), mockedProducer);
    for (int i = 0; i < 1000; i++) {
      writer.put(Integer.toString(i), Integer.toString(i), 1, null);
    }
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(mockedProducer, atLeast(1000)).sendMessage(any(), any(), any(), kmeArgumentCaptor.capture(), any(), any());
    int segmentNumber = -1;
    for (KafkaMessageEnvelope envelope: kmeArgumentCaptor.getAllValues()) {
      if (segmentNumber == -1) {
        segmentNumber = envelope.producerMetadata.segmentNumber;
      } else {
        // Segment number should not change since we disabled closing segment based on elapsed time.
        assertEquals(envelope.producerMetadata.segmentNumber, segmentNumber);
      }
    }
  }

  @DataProvider(name = "Boolean-LeaderCompleteState")
  public static Object[][] booleanBooleanCompression() {
    return DataProviderUtils
        .allPermutationGenerator(DataProviderUtils.BOOLEAN, new Object[] { LEADER_NOT_COMPLETED, LEADER_COMPLETED });
  }

  @Test(dataProvider = "Boolean-LeaderCompleteState")
  public void testSendHeartbeat(boolean addLeaderCompleteHeader, LeaderCompleteState leaderCompleteState) {
    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockedFuture);
    Properties writerProperties = new Properties();
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer serializer = new VeniceAvroKafkaSerializer(stringSchema);
    String testTopic = "test_rt";
    VeniceWriterOptions veniceWriterOptions = new VeniceWriterOptions.Builder(testTopic).setKeySerializer(serializer)
        .setValueSerializer(serializer)
        .setWriteComputeSerializer(serializer)
        .setPartitioner(new DefaultVenicePartitioner())
        .setTime(SystemTime.INSTANCE)
        .build();
    VeniceWriter<Object, Object, Object> writer =
        new VeniceWriter(veniceWriterOptions, new VeniceProperties(writerProperties), mockedProducer);
    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    PubSubTopic topic = mock(PubSubTopic.class);
    when(topic.getName()).thenReturn(testTopic);
    when(topicPartition.getPubSubTopic()).thenReturn(topic);
    when(topicPartition.getPartitionNumber()).thenReturn(0);
    for (int i = 0; i < 10; i++) {
      writer.sendHeartbeat(
          topicPartition,
          null,
          DEFAULT_LEADER_METADATA_WRAPPER,
          addLeaderCompleteHeader,
          leaderCompleteState,
          System.currentTimeMillis());
    }
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    ArgumentCaptor<KafkaKey> kafkaKeyArgumentCaptor = ArgumentCaptor.forClass(KafkaKey.class);
    ArgumentCaptor<PubSubMessageHeaders> pubSubMessageHeadersArgumentCaptor =
        ArgumentCaptor.forClass(PubSubMessageHeaders.class);
    verify(mockedProducer, times(10)).sendMessage(
        eq(testTopic),
        eq(0),
        kafkaKeyArgumentCaptor.capture(),
        kmeArgumentCaptor.capture(),
        pubSubMessageHeadersArgumentCaptor.capture(),
        any());
    for (KafkaKey key: kafkaKeyArgumentCaptor.getAllValues()) {
      Assert.assertTrue(Arrays.equals(HEART_BEAT.getKey(), key.getKey()));
    }
    for (KafkaMessageEnvelope kme: kmeArgumentCaptor.getAllValues()) {
      assertEquals(kme.messageType, MessageType.CONTROL_MESSAGE.getValue());
      ControlMessage controlMessage = (ControlMessage) kme.payloadUnion;
      assertEquals(controlMessage.controlMessageType, ControlMessageType.START_OF_SEGMENT.getValue());
      ProducerMetadata producerMetadata = kme.producerMetadata;
      assertEquals(producerMetadata.producerGUID, HeartbeatGuidV3Generator.getInstance().getGuid());
      assertEquals(producerMetadata.segmentNumber, 0);
      assertEquals(producerMetadata.messageSequenceNumber, 0);
    }

    for (PubSubMessageHeaders pubSubMessageHeaders: pubSubMessageHeadersArgumentCaptor.getAllValues()) {
      assertEquals(pubSubMessageHeaders.toList().size(), addLeaderCompleteHeader ? 2 : 1);
      if (addLeaderCompleteHeader) {
        // 0: VENICE_TRANSPORT_PROTOCOL_HEADER, 1: VENICE_LEADER_COMPLETION_STATE_HEADER
        PubSubMessageHeader leaderCompleteHeader = pubSubMessageHeaders.toList().get(1);
        assertEquals(leaderCompleteHeader.key(), VENICE_LEADER_COMPLETION_STATE_HEADER);
        assertEquals(leaderCompleteHeader.value()[0], leaderCompleteState.getValue());
      }
    }
  }
}
