package com.linkedin.davinci.utils;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.GzipCompressor;
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
import java.io.IOException;
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

  /**
   * Verifies that when isRmdChunkingEnabled=true, RMD chunks (putValue empty, replicationMetadataPayload non-empty)
   * are buffered, then assembled from the RMD manifest on the manifest message, and the assembled RMD bytes are
   * returned in ByteBufferValueRecord.replicationMetadataPayload().
   */
  @Test
  public void testRmdChunkingAssembly() {
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    ChunkAssembler assembler = new RocksDBChunkAssembler(mockStorageEngine, false, true);
    PubSubTopicPartition mockPartition = mock(PubSubTopicPartition.class);
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    doReturn(0).when(mockPartition).getPartitionNumber();
    doReturn("test-rmd-chunking-topic").when(mockTopic).getName();
    doReturn(mockTopic).when(mockPartition).getPubSubTopic();

    VeniceCompressor compressor = new NoopCompressor();
    byte[] serializedKey = "rmd-test-key".getBytes();
    int writerSchemaId = 3;
    Map<KafkaKey, Put> valueCunksMap = new HashMap<>();
    Map<KafkaKey, Put> rmdChunksMap = new HashMap<>();

    // Build value chunks
    ChunkedPayloadAndManifest valueManifestResult = newChunksAndManifest(serializedKey, writerSchemaId, valueCunksMap);
    // Build RMD chunks (isValuePayload=false) with an offset starting after value chunks
    int valueChunkCount = valueManifestResult.getChunkedValueManifest().keysWithChunkIdSuffix.size();
    byte[] rmdPayload = new byte[2 * BYTES_PER_MB];
    for (int i = 0; i < rmdPayload.length; i++) {
      rmdPayload[i] = (byte) (i % 127);
    }
    ProducerMetadata producerMetadata = new ProducerMetadata(new GUID(), 0, 0, 0L, 0L);
    ChunkedPayloadAndManifest rmdManifestResult = WriterChunkingHelper.chunkPayloadAndSend(
        serializedKey,
        rmdPayload,
        MessageType.PUT,
        false, // isValuePayload=false → RMD chunks
        writerSchemaId,
        valueChunkCount, // start chunk index after value chunks
        false,
        () -> "rmd-size-report",
        VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES,
        new KeyWithChunkingSuffixSerializer(),
        ((keyProvider, put) -> rmdChunksMap.put(keyProvider.getKey(producerMetadata), put)));

    PubSubPosition pos = ApacheKafkaOffsetPosition.of(1L);

    // Feed value chunks
    for (Map.Entry<KafkaKey, Put> entry: valueCunksMap.entrySet()) {
      byte[] key = entry.getKey().getKey();
      byte[] stored =
          ValueRecord.create(entry.getValue().getSchemaId(), ByteUtils.extractByteArray(entry.getValue().putValue))
              .serialize();
      doReturn(stored).when(mockStorageEngine).get(0, ByteBuffer.wrap(key));
      Assert.assertNull(
          assembler.bufferAndAssembleRecord(
              mockPartition,
              AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(),
              key,
              entry.getValue().getPutValue(),
              entry.getValue().replicationMetadataPayload,
              pos,
              compressor));
      verify(mockStorageEngine, times(1)).put(eq(0), eq(key), eq(stored));
    }

    // Feed RMD chunks (putValue is EMPTY_BYTE_BUFFER, replicationMetadataPayload has data).
    // RMD chunks are looked up via get(int, byte[]) directly in ChunkAssembler (not via RawBytesChunkingAdapter),
    // so mock the byte[] overload rather than the ByteBuffer overload.
    for (Map.Entry<KafkaKey, Put> entry: rmdChunksMap.entrySet()) {
      byte[] key = entry.getKey().getKey();
      byte[] stored =
          ValueRecord
              .create(
                  entry.getValue().getSchemaId(),
                  ByteUtils.extractByteArray(entry.getValue().replicationMetadataPayload))
              .serialize();
      doReturn(stored).when(mockStorageEngine).get(0, key);
      Assert.assertNull(
          assembler.bufferAndAssembleRecord(
              mockPartition,
              AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(),
              key,
              entry.getValue().getPutValue(),
              entry.getValue().replicationMetadataPayload,
              pos,
              compressor));
      verify(mockStorageEngine, times(1)).put(eq(0), eq(key), eq(stored));
    }

    // Build value manifest bytes and rmd manifest bytes for the manifest message
    ByteBuffer valueManifestBytes =
        chunkedValueManifestSerializer.serialize(valueManifestResult.getChunkedValueManifest());
    byte[] valueManifestByteArray =
        ValueRecord
            .create(
                AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
                ByteUtils.extractByteArray(valueManifestBytes))
            .serialize();
    doReturn(valueManifestByteArray).when(mockStorageEngine).get(0, ByteBuffer.wrap(serializedKey));

    ByteBuffer rmdManifestBytes = chunkedValueManifestSerializer.serialize(rmdManifestResult.getChunkedValueManifest());

    // Send the manifest message with the RMD manifest in replicationMetadataPayload
    ByteBufferValueRecord<ByteBuffer> result = assembler.bufferAndAssembleRecord(
        mockPartition,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
        serializedKey,
        valueManifestBytes,
        rmdManifestBytes,
        ApacheKafkaOffsetPosition.of(10L),
        compressor);

    Assert.assertNotNull(result);
    Assert.assertEquals(result.writerSchemaId(), writerSchemaId);
    // The assembled RMD should equal the original RMD payload
    Assert.assertNotNull(result.replicationMetadataPayload());
    byte[] assembledRmd = ByteUtils.extractByteArray(result.replicationMetadataPayload());
    Assert.assertEquals(assembledRmd, rmdPayload);
  }

  /**
   * Verifies that a missing RMD chunk (null returned from storage) causes a VeniceException when
   * skipFailedToAssembleRecords=false, and returns null when skipFailedToAssembleRecords=true.
   */
  @Test
  public void testRmdChunkingMissingChunkThrows() {
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    PubSubTopicPartition mockPartition = mock(PubSubTopicPartition.class);
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    doReturn(0).when(mockPartition).getPartitionNumber();
    doReturn("test-rmd-missing-chunk-topic").when(mockTopic).getName();
    doReturn(mockTopic).when(mockPartition).getPubSubTopic();
    VeniceCompressor compressor = new NoopCompressor();

    byte[] serializedKey = "missing-rmd-key".getBytes();
    int writerSchemaId = 2;
    Map<KafkaKey, Put> valueCunksMap = new HashMap<>();
    ChunkedPayloadAndManifest valueManifest = newChunksAndManifest(serializedKey, writerSchemaId, valueCunksMap);
    int valueChunkCount = valueManifest.getChunkedValueManifest().keysWithChunkIdSuffix.size();

    // Build a small RMD manifest whose chunks will NOT be registered in mockStorageEngine
    byte[] rmdPayload = new byte[2 * BYTES_PER_MB];
    ProducerMetadata producerMetadata = new ProducerMetadata(new GUID(), 0, 0, 0L, 0L);
    ChunkedPayloadAndManifest rmdManifest = WriterChunkingHelper.chunkPayloadAndSend(
        serializedKey,
        rmdPayload,
        MessageType.PUT,
        false,
        writerSchemaId,
        valueChunkCount,
        false,
        () -> "rmd-size-report",
        VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES,
        new KeyWithChunkingSuffixSerializer(),
        ((keyProvider, put) -> {
          /* intentionally don't register in rmdChunksMap – chunks will be missing */
        }));

    // Register value chunks so the value assembles fine
    ByteBuffer valueManifestBytes = chunkedValueManifestSerializer.serialize(valueManifest.getChunkedValueManifest());
    byte[] valueManifestByteArray =
        ValueRecord
            .create(
                AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
                ByteUtils.extractByteArray(valueManifestBytes))
            .serialize();
    doReturn(valueManifestByteArray).when(mockStorageEngine).get(0, ByteBuffer.wrap(serializedKey));
    for (Map.Entry<KafkaKey, Put> entry: valueCunksMap.entrySet()) {
      byte[] key = entry.getKey().getKey();
      byte[] stored =
          ValueRecord.create(entry.getValue().getSchemaId(), ByteUtils.extractByteArray(entry.getValue().putValue))
              .serialize();
      doReturn(stored).when(mockStorageEngine).get(0, ByteBuffer.wrap(key));
    }

    ByteBuffer rmdManifestBytes = chunkedValueManifestSerializer.serialize(rmdManifest.getChunkedValueManifest());
    PubSubPosition pos = ApacheKafkaOffsetPosition.of(10L);

    ChunkAssembler throwAssembler = new RocksDBChunkAssembler(mockStorageEngine, false, true);
    Assert.assertThrows(
        VeniceException.class,
        () -> throwAssembler.bufferAndAssembleRecord(
            mockPartition,
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
            serializedKey,
            valueManifestBytes,
            rmdManifestBytes,
            pos,
            compressor));

    ChunkAssembler skipAssembler = new RocksDBChunkAssembler(mockStorageEngine, true, true);
    Assert.assertNull(
        skipAssembler.bufferAndAssembleRecord(
            mockPartition,
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
            serializedKey,
            valueManifestBytes,
            rmdManifestBytes,
            pos,
            compressor));
  }

  /**
   * Verifies that when isRmdChunkingEnabled=false (default), a non-empty replicationMetadataPayload on a CHUNK
   * message is ignored (not stored), so chunk messages with RMD bytes are treated as value-less and return null.
   */
  @Test
  public void testRmdChunkingDisabledIgnoresRmdChunks() {
    StorageEngine mockStorageEngine = mock(StorageEngine.class);
    ChunkAssembler assembler = new RocksDBChunkAssembler(mockStorageEngine, false); // isRmdChunkingEnabled=false
    PubSubTopicPartition mockPartition = mock(PubSubTopicPartition.class);
    doReturn(0).when(mockPartition).getPartitionNumber();

    byte[] key = "some-key".getBytes();
    ByteBuffer emptyValue = ByteBuffer.allocate(0);
    ByteBuffer rmdBytes = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    PubSubPosition pos = ApacheKafkaOffsetPosition.of(1L);

    // A CHUNK message with empty putValue and non-empty rmd should be ignored (no put to storage, returns null)
    Assert.assertNull(
        assembler.bufferAndAssembleRecord(
            mockPartition,
            AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(),
            key,
            emptyValue,
            rmdBytes,
            pos,
            new NoopCompressor()));

    verify(mockStorageEngine, times(0)).put(eq(0), eq(key), org.mockito.ArgumentMatchers.<byte[]>any());
  }

  @Test
  public void testDecompressValueIfNeededSkipsDecompressionForChunkedRecords() throws IOException {
    GzipCompressor compressor = new GzipCompressor();
    byte[] raw = "already-decompressed".getBytes();
    ByteBuffer input = ByteBuffer.wrap(raw);
    int manifestSchemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();

    // Chunked record: returns value as-is without attempting GZIP decompression
    ByteBuffer result = ChunkAssembler.decompressValueIfNeeded(input, manifestSchemaId, compressor);
    assertEquals(result, input);

    int chunkSchemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    result = ChunkAssembler.decompressValueIfNeeded(input, chunkSchemaId, compressor);
    assertEquals(result, input);
  }

  @Test(expectedExceptions = IOException.class)
  public void testDecompressValueIfNeededDecompressesNonChunkedRecords() throws IOException {
    GzipCompressor compressor = new GzipCompressor();
    byte[] notGzip = "not-gzip-data".getBytes();
    int regularSchemaId = 1;

    // Non-chunked: actually attempts decompression, fails on non-GZIP bytes (proving it tried)
    ChunkAssembler.decompressValueIfNeeded(ByteBuffer.wrap(notGzip), regularSchemaId, compressor);
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
