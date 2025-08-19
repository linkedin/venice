package com.linkedin.davinci.utils;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.writer.ChunkedPayloadAndManifest;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.WriterChunkingHelper;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ChunkAssemblerTest {
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer =
      new ChunkedValueManifestSerializer(true);

  @Test
  public void testRocksDBChunkAssembler() {
    byte[] serializedKey = Integer.toString(1).getBytes();
    verifyBasicOperations(
        serializedKey,
        (storageEngine) -> new RocksDBChunkAssembler(storageEngine, true),
        (abstractStorageEngine, manifest) -> {
          // Verify the chunks and manifest are deleted after assembly
          for (ByteBuffer chunkKeys: manifest.keysWithChunkIdSuffix) {
            verify(abstractStorageEngine, times(1)).delete(0, ByteUtils.extractByteArray(chunkKeys));
          }
          verify(abstractStorageEngine, times(1)).delete(0, serializedKey);
        });
  }

  @Test
  public void testInMemoryChunkAssembler() {
    byte[] serializedKey = Integer.toString(1).getBytes();
    verifyBasicOperations(serializedKey, InMemoryChunkAssembler::new, (abstractStorageEngine, manifest) -> {
      verify(abstractStorageEngine, times(1)).dropPartition(0);
    });
  }

  @Test
  public void testRocksDBChunkAssemblerAssemblyError() {
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    PubSubTopicPartition mockPubSubPartition = mock(PubSubTopicPartition.class);
    VeniceCompressor compressor = new NoopCompressor();
    doReturn(0).when(mockPubSubPartition).getPartitionNumber();
    PubSubTopic pubSubTopic = mock(PubSubTopic.class);
    doReturn("test-assembly-error-topic-name").when(pubSubTopic).getName();
    doReturn(pubSubTopic).when(mockPubSubPartition).getPubSubTopic();
    Map<KafkaKey, Put> chunksMap = new HashMap<>();
    int writerSchemaId = 1;
    byte[] serializedKey = Integer.toString(1).getBytes();
    ChunkedPayloadAndManifest manifest = newChunksAndManifest(serializedKey, writerSchemaId, chunksMap);
    // Only provide the manifest at first to simulate broken chunks.
    ByteBuffer manifestBytes = chunkedValueManifestSerializer.serialize(manifest.getChunkedValueManifest());
    byte[] manifestByteArray =
        ValueRecord
            .create(
                AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
                ByteUtils.extractByteArray(manifestBytes))
            .serialize();

    PubSubPosition p10 = ApacheKafkaOffsetPosition.of(10L);
    doReturn(manifestByteArray).when(mockStorageEngine).get(0, ByteBuffer.wrap(serializedKey));
    ChunkAssembler throwChunkAssembler = new RocksDBChunkAssembler(mockStorageEngine, false);
    Assert.assertThrows(
        VeniceException.class,
        () -> throwChunkAssembler.bufferAndAssembleRecord(
            mockPubSubPartition,
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
            serializedKey,
            manifestBytes,
            p10,
            compressor));
    ChunkAssembler skipChunkAssembler = new RocksDBChunkAssembler(mockStorageEngine, true);
    Assert.assertNull(
        skipChunkAssembler.bufferAndAssembleRecord(
            mockPubSubPartition,
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
            serializedKey,
            manifestBytes,
            p10,
            compressor));
    // Make the chunks available too.
    for (Map.Entry<KafkaKey, Put> entry: chunksMap.entrySet()) {
      byte[] key = entry.getKey().getKey();
      byte[] value =
          ValueRecord.create(entry.getValue().getSchemaId(), ByteUtils.extractByteArray(entry.getValue().putValue))
              .serialize();
      doReturn(value).when(mockStorageEngine).get(0, ByteBuffer.wrap(key));
    }
    ByteBufferValueRecord<ByteBuffer> valueRecord = throwChunkAssembler.bufferAndAssembleRecord(
        mockPubSubPartition,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
        serializedKey,
        manifestBytes,
        p10,
        compressor);
    Assert.assertNotNull(valueRecord);
    Assert.assertEquals(valueRecord.writerSchemaId(), writerSchemaId);
  }

  private void verifyBasicOperations(
      byte[] serializedKey,
      Function<StorageEngine, ChunkAssembler> initFunction,
      BiConsumer<StorageEngine, ChunkedValueManifest> verifyConsumer) {
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    ChunkAssembler chunkAssembler = initFunction.apply(mockStorageEngine);
    PubSubTopicPartition mockPubSubPartition = mock(PubSubTopicPartition.class);
    VeniceCompressor compressor = new NoopCompressor();
    doReturn(0).when(mockPubSubPartition).getPartitionNumber();
    Map<KafkaKey, Put> chunksMap = new HashMap<>();
    int writerSchemaId = 5;
    ChunkedPayloadAndManifest manifest = newChunksAndManifest(serializedKey, writerSchemaId, chunksMap);
    PubSubPosition p1 = ApacheKafkaOffsetPosition.of(1L);
    PubSubPosition p10 = ApacheKafkaOffsetPosition.of(10L);
    for (Map.Entry<KafkaKey, Put> entry: chunksMap.entrySet()) {
      byte[] key = entry.getKey().getKey();
      byte[] value =
          ValueRecord.create(entry.getValue().getSchemaId(), ByteUtils.extractByteArray(entry.getValue().putValue))
              .serialize();
      doReturn(value).when(mockStorageEngine).get(0, ByteBuffer.wrap(key));
      Assert.assertNull(
          chunkAssembler.bufferAndAssembleRecord(
              mockPubSubPartition,
              AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(),
              key,
              entry.getValue().getPutValue(),
              p1,
              compressor));
      // Verify chunks are persisted
      verify(mockStorageEngine, times(1)).put(eq(0), eq(key), eq(value));
    }
    ByteBuffer manifestBytes = chunkedValueManifestSerializer.serialize(manifest.getChunkedValueManifest());
    byte[] manifestByteArray =
        ValueRecord
            .create(
                AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
                ByteUtils.extractByteArray(manifestBytes))
            .serialize();
    doReturn(manifestByteArray).when(mockStorageEngine).get(0, ByteBuffer.wrap(serializedKey));
    // We should get an assembled record
    ByteBufferValueRecord<ByteBuffer> valueRecord = chunkAssembler.bufferAndAssembleRecord(
        mockPubSubPartition,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
        serializedKey,
        manifestBytes,
        p10,
        compressor);
    Assert.assertNotNull(valueRecord);
    Assert.assertEquals(valueRecord.writerSchemaId(), writerSchemaId);
    // Verify manifest is persisted
    verify(mockStorageEngine, times(1)).put(eq(0), eq(serializedKey), eq(manifestByteArray));
    verifyConsumer.accept(mockStorageEngine, manifest.getChunkedValueManifest());
  }

  private ChunkedPayloadAndManifest newChunksAndManifest(
      byte[] serializedKey,
      int writerSchemaId,
      Map<KafkaKey, Put> chunksMap) {
    int valueSize = 2 * BYTES_PER_MB;
    char[] chars = new char[valueSize];
    byte[] serializedValue = (new String(chars)).getBytes();
    ProducerMetadata producerMetadata = new ProducerMetadata(new GUID(), 0, 0, 0L, 0L);
    return WriterChunkingHelper.chunkPayloadAndSend(
        serializedKey,
        serializedValue,
        MessageType.PUT,
        true,
        writerSchemaId,
        0,
        false,
        () -> "ignored",
        VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES,
        new KeyWithChunkingSuffixSerializer(),
        ((keyProvider, put) -> chunksMap.put(keyProvider.getKey(producerMetadata), put)));
  }

}
