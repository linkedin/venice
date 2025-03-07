package com.linkedin.davinci.utils;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
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
import java.util.Arrays;
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
    verifyBasicOperations(serializedKey, RocksDBChunkAssembler::new, (abstractStorageEngine, manifest) -> {
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

  private void verifyBasicOperations(
      byte[] serializedKey,
      Function<AbstractStorageEngine, ChunkAssembler> initFunction,
      BiConsumer<AbstractStorageEngine, ChunkedValueManifest> verifyConsumer) {
    AbstractStorageEngine mockStorageEngine = mock(AbstractStorageEngine.class);
    ChunkAssembler chunkAssembler = initFunction.apply(mockStorageEngine);
    PubSubTopicPartition mockPubSubPartition = mock(PubSubTopicPartition.class);
    VeniceCompressor compressor = new NoopCompressor();
    doReturn(0).when(mockPubSubPartition).getPartitionNumber();
    int valueSize = 2 * BYTES_PER_MB;
    char[] chars = new char[valueSize];
    Arrays.fill(chars, Integer.toString(1).charAt(0));
    byte[] serializedValue = (new String(chars)).getBytes();
    Map<KafkaKey, Put> chunksMap = new HashMap<>();
    ProducerMetadata producerMetadata = new ProducerMetadata(new GUID(), 0, 0, 0L, 0L);
    int writerSchemaId = 5;
    ChunkedPayloadAndManifest manifest = WriterChunkingHelper.chunkPayloadAndSend(
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
              1,
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
        10,
        compressor);
    Assert.assertNotNull(valueRecord);
    Assert.assertEquals(valueRecord.writerSchemaId(), writerSchemaId);
    // Verify manifest is persisted
    verify(mockStorageEngine, times(1)).put(eq(0), eq(serializedKey), eq(manifestByteArray));
    verifyConsumer.accept(mockStorageEngine, manifest.getChunkedValueManifest());
  }

}
