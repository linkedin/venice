package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.utils.ByteUtils.SIZE_OF_INT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ActiveActiveStoreIngestionTaskTest {
  @Test
  public void testLeaderCanSendValueChunksIntoDrainer()
      throws ExecutionException, InterruptedException, TimeoutException {
    String testTopic = "test";
    int valueSchemaId = 1;
    int rmdProtocolVersionID = 1;
    int kafkaClusterId = 0;
    int subPartition = 0;
    String kafkaUrl = "kafkaUrl";
    long beforeProcessingRecordTimestamp = 0;
    boolean resultReuseInput = true;

    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    when(ingestionTask.getHostLevelIngestionStats()).thenReturn(mock(HostLevelIngestionStats.class));
    when(ingestionTask.getVersionIngestionStats()).thenReturn(mock(AggVersionedIngestionStats.class));
    when(ingestionTask.getVersionedDIVStats()).thenReturn(mock(AggVersionedDIVStats.class));
    when(ingestionTask.getKafkaVersionTopic()).thenReturn(testTopic);
    when(ingestionTask.createProducerCallback(any(), any(), any(), anyInt(), anyString(), anyLong()))
        .thenCallRealMethod();
    when(ingestionTask.getProduceToTopicFunction(any(), any(), any(), anyInt(), anyBoolean())).thenCallRealMethod();
    when(ingestionTask.getRmdProtocolVersionID()).thenReturn(rmdProtocolVersionID);
    doCallRealMethod().when(ingestionTask)
        .produceToLocalKafka(any(), any(), any(), any(), anyInt(), anyString(), anyInt(), anyLong());
    byte[] key = "foo".getBytes();
    byte[] updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);

    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    Future mockedFuture = mock(Future.class);
    when(mockedProducer.getNumberOfPartitions(any())).thenReturn(1);
    when(mockedProducer.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);
    AtomicLong offset = new AtomicLong(0);

    ArgumentCaptor<KafkaKey> kafkaKeyArgumentCaptor = ArgumentCaptor.forClass(KafkaKey.class);
    ArgumentCaptor<KafkaMessageEnvelope> kmeArgumentCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    when(
        mockedProducer.sendMessage(
            eq(testTopic),
            any(),
            kafkaKeyArgumentCaptor.capture(),
            kmeArgumentCaptor.capture(),
            any(),
            any())).thenAnswer((Answer<Future<PubSubProduceResult>>) invocation -> {
              KafkaKey kafkaKey = invocation.getArgument(2);
              KafkaMessageEnvelope kafkaMessageEnvelope = invocation.getArgument(3);
              PubSubProducerCallback callback = invocation.getArgument(5);
              PubSubProduceResult produceResult = mock(PubSubProduceResult.class);
              offset.addAndGet(1);
              when(produceResult.getOffset()).thenReturn(offset.get());
              MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope.messageType);
              when(produceResult.getSerializedSize()).thenReturn(
                  kafkaKey.getKeyLength() + (messageType.equals(MessageType.PUT)
                      ? ((Put) (kafkaMessageEnvelope.payloadUnion)).putValue.remaining()
                      : 0));
              callback.onCompletion(produceResult, null);
              return mockedFuture;
            });
    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(testTopic).setPartitioner(new DefaultVenicePartitioner())
            .setTime(SystemTime.INSTANCE)
            .setChunkingEnabled(true)
            .build();
    VeniceWriter<byte[], byte[], byte[]> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);
    when(ingestionTask.getVeniceWriter()).thenReturn(Lazy.of(() -> writer));
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      stringBuilder.append("abcdefghabcdefghabcdefghabcdefgh");
    }
    String valueString = stringBuilder.toString();
    byte[] valueBytes = valueString.getBytes();
    byte[] schemaIdPrependedValueBytes = new byte[4 + valueBytes.length];
    ByteUtils.writeInt(schemaIdPrependedValueBytes, 1, 0);
    System.arraycopy(valueBytes, 0, schemaIdPrependedValueBytes, 4, valueBytes.length);
    ByteBuffer updatedValueBytes = ByteBuffer.wrap(schemaIdPrependedValueBytes, 4, valueBytes.length);
    ByteBuffer updatedRmdBytes = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord = mock(PubSubMessage.class);
    when(consumerRecord.getOffset()).thenReturn(100L);

    Put updatedPut = new Put();
    updatedPut.putValue = ByteUtils.prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, resultReuseInput);
    updatedPut.schemaId = valueSchemaId;
    updatedPut.replicationMetadataVersionId = rmdProtocolVersionID;
    updatedPut.replicationMetadataPayload = updatedRmdBytes;
    LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext
        .newPutRecord(kafkaClusterId, consumerRecord.getOffset(), updatedKeyBytes, updatedPut);

    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    ingestionTask.produceToLocalKafka(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        ingestionTask.getProduceToTopicFunction(
            updatedKeyBytes,
            updatedValueBytes,
            updatedRmdBytes,
            valueSchemaId,
            resultReuseInput),
        subPartition,
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingRecordTimestamp);

    // Send 1 SOS, 2 Chunks, 1 Manifest.
    verify(mockedProducer, times(4)).sendMessage(any(), any(), any(), any(), any(), any());
    ArgumentCaptor<LeaderProducedRecordContext> leaderProducedRecordContextArgumentCaptor =
        ArgumentCaptor.forClass(LeaderProducedRecordContext.class);
    verify(ingestionTask, times(3)).produceToStoreBufferService(
        any(),
        leaderProducedRecordContextArgumentCaptor.capture(),
        anyInt(),
        anyString(),
        anyLong(),
        anyLong());

    // Make sure the chunks && manifest are exactly the same for both produced to VT and drain to leader storage.
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(0).getValueUnion(),
        kmeArgumentCaptor.getAllValues().get(1).payloadUnion);
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(1).getValueUnion(),
        kmeArgumentCaptor.getAllValues().get(2).payloadUnion);
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(2).getValueUnion(),
        kmeArgumentCaptor.getAllValues().get(3).payloadUnion);
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(0).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(1).getKey());
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(1).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(2).getKey());
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(2).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(3).getKey());
  }

  @Test
  public void testReadingChunkedRmdFromStorage() {
    String storeName = "testStore";
    String topicName = "testStore_v1";
    int subPartition = 1;
    int valueSchema = 2;
    KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
    ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(true);

    byte[] key1 = "foo".getBytes();
    byte[] key2 = "bar".getBytes();
    byte[] key3 = "ljl".getBytes();

    byte[] topLevelKey1 = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key1);
    byte[] expectedNonChunkedValue = new byte[8];
    ByteUtils.writeInt(expectedNonChunkedValue, valueSchema, 0);
    ByteUtils.writeInt(expectedNonChunkedValue, 666, 4);

    byte[] expectedChunkedValue1 = new byte[12];
    ByteUtils.writeInt(expectedChunkedValue1, valueSchema, 0);
    ByteUtils.writeInt(expectedChunkedValue1, 666, 4);
    ByteUtils.writeInt(expectedChunkedValue1, 777, 8);
    byte[] expectedChunkedValue2 = new byte[24];
    ByteUtils.writeInt(expectedChunkedValue2, valueSchema, 0);
    ByteUtils.writeInt(expectedChunkedValue2, 666, 4);
    ByteUtils.writeInt(expectedChunkedValue2, 777, 8);
    ByteUtils.writeInt(expectedChunkedValue2, 111, 12);
    ByteUtils.writeInt(expectedChunkedValue2, 222, 16);
    ByteUtils.writeInt(expectedChunkedValue2, 333, 20);
    byte[] chunkedValue1 = new byte[12];
    ByteUtils.writeInt(chunkedValue1, AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(), 0);
    ByteUtils.writeInt(chunkedValue1, 666, 4);
    ByteUtils.writeInt(chunkedValue1, 777, 8);

    byte[] chunkedValue2 = new byte[16];
    ByteUtils.writeInt(chunkedValue2, AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(), 0);
    ByteUtils.writeInt(chunkedValue2, 111, 4);
    ByteUtils.writeInt(chunkedValue2, 222, 8);
    ByteUtils.writeInt(chunkedValue2, 333, 12);

    /**
     * The 1st key does not have any chunk but only has a top level key.
     */
    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    String stringSchema = "\"string\"";
    when(schemaRepository.getSupersetOrLatestValueSchema(storeName))
        .thenReturn(new SchemaEntry(valueSchema, stringSchema));
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.isComputeFastAvroEnabled()).thenReturn(false);
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    when(ingestionTask.getRmdProtocolVersionID()).thenReturn(1);
    Lazy<VeniceCompressor> compressor = Lazy.of(NoopCompressor::new);
    when(ingestionTask.getCompressor()).thenReturn(compressor);
    when(ingestionTask.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(ingestionTask.getStoreName()).thenReturn(storeName);
    when(ingestionTask.getStorageEngine()).thenReturn(storageEngine);
    when(ingestionTask.getSchemaRepo()).thenReturn(schemaRepository);
    when(ingestionTask.getServerConfig()).thenReturn(serverConfig);
    when(ingestionTask.getRmdWithValueSchemaByteBufferFromStorage(anyInt(), any(), anyLong())).thenCallRealMethod();
    when(ingestionTask.isChunked()).thenReturn(true);
    when(ingestionTask.getHostLevelIngestionStats()).thenReturn(mock(HostLevelIngestionStats.class));

    when(storageEngine.getReplicationMetadata(subPartition, topLevelKey1)).thenReturn(expectedNonChunkedValue);
    byte[] result = ingestionTask.getRmdWithValueSchemaByteBufferFromStorage(subPartition, key1, 0L);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, expectedNonChunkedValue);

    /**
     * The 2nd key contains 1 chunks with 8 bytes.
     */
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    ProducerMetadata metadata = new ProducerMetadata(new GUID(), 1, 2, 100L, 200L);
    chunkedKeySuffix.chunkId.producerGUID = metadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = metadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = metadata.messageSequenceNumber;
    chunkedKeySuffix.chunkId.chunkIndex = 0;
    ByteBuffer chunkedKeyWithSuffix1 = keyWithChunkingSuffixSerializer.serializeChunkedKey(key2, chunkedKeySuffix);
    ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.schemaId = valueSchema;
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(1);
    chunkedValueManifest.size = 8;
    chunkedValueManifest.keysWithChunkIdSuffix.add(chunkedKeyWithSuffix1);
    byte[] chunkedManifestBytes = chunkedValueManifestSerializer.serialize(topicName, chunkedValueManifest);
    byte[] chunkedManifestWithSchemaBytes = new byte[SIZE_OF_INT + chunkedManifestBytes.length];
    System.arraycopy(chunkedManifestBytes, 0, chunkedManifestWithSchemaBytes, 4, chunkedManifestBytes.length);
    ByteUtils.writeInt(
        chunkedManifestWithSchemaBytes,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
        0);
    byte[] topLevelKey2 = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key2);
    byte[] chunkedKey1InKey2 = chunkedKeyWithSuffix1.array();

    when(storageEngine.getReplicationMetadata(subPartition, topLevelKey2)).thenReturn(chunkedManifestWithSchemaBytes);
    when(storageEngine.getReplicationMetadata(subPartition, chunkedKey1InKey2)).thenReturn(chunkedValue1);
    byte[] result2 = ingestionTask.getRmdWithValueSchemaByteBufferFromStorage(subPartition, key2, 0L);
    Assert.assertNotNull(result2);
    Assert.assertEquals(result2, expectedChunkedValue1);

    /**
     * The 3rd key contains 2 chunks, each contains 8 bytes and 12 bytes respectively.
     */
    chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    chunkedKeySuffix.chunkId.producerGUID = metadata.producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = metadata.segmentNumber;
    chunkedKeySuffix.chunkId.messageSequenceNumber = metadata.messageSequenceNumber;
    chunkedKeySuffix.chunkId.chunkIndex = 0;
    chunkedKeyWithSuffix1 = keyWithChunkingSuffixSerializer.serializeChunkedKey(key3, chunkedKeySuffix);

    chunkedKeySuffix.chunkId.chunkIndex = 1;
    ByteBuffer chunkedKeyWithSuffix2 = keyWithChunkingSuffixSerializer.serializeChunkedKey(key3, chunkedKeySuffix);

    chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.schemaId = valueSchema;
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(2);
    chunkedValueManifest.size = 8 + 12;
    chunkedValueManifest.keysWithChunkIdSuffix.add(chunkedKeyWithSuffix1);
    chunkedValueManifest.keysWithChunkIdSuffix.add(chunkedKeyWithSuffix2);
    chunkedManifestBytes = chunkedValueManifestSerializer.serialize(topicName, chunkedValueManifest);
    chunkedManifestWithSchemaBytes = new byte[SIZE_OF_INT + chunkedManifestBytes.length];
    System.arraycopy(chunkedManifestBytes, 0, chunkedManifestWithSchemaBytes, SIZE_OF_INT, chunkedManifestBytes.length);
    ByteUtils.writeInt(
        chunkedManifestWithSchemaBytes,
        AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(),
        0);
    byte[] topLevelKey3 = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key3);
    byte[] chunkedKey1InKey3 = chunkedKeyWithSuffix1.array();
    byte[] chunkedKey2InKey3 = chunkedKeyWithSuffix2.array();

    when(storageEngine.getReplicationMetadata(subPartition, topLevelKey3)).thenReturn(chunkedManifestWithSchemaBytes);
    when(storageEngine.getReplicationMetadata(subPartition, chunkedKey1InKey3)).thenReturn(chunkedValue1);
    when(storageEngine.getReplicationMetadata(subPartition, chunkedKey2InKey3)).thenReturn(chunkedValue2);
    byte[] result3 = ingestionTask.getRmdWithValueSchemaByteBufferFromStorage(subPartition, key3, 0L);
    Assert.assertNotNull(result3);
    Assert.assertEquals(result3, expectedChunkedValue2);
  }
}
