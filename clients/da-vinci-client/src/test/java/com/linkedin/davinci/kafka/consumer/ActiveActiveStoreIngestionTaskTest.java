package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.blackhole.BlackHoleStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ActiveActiveStoreIngestionTaskTest {
  private static final Logger LOGGER = LogManager.getLogger(ActiveActiveStoreIngestionTaskTest.class);
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  String STORE_NAME = "Thvorusleikir_store";
  String PUSH_JOB_ID = "yule";
  String BOOTSTRAP_SERVER = "Stekkjastaur";
  String TEST_CLUSTER_NAME = "venice-GRYLA";

  @DataProvider(name = "CompressionStrategy")
  public static Object[] compressionStrategyProvider() {
    return new Object[] { CompressionStrategy.NO_OP, CompressionStrategy.GZIP, CompressionStrategy.ZSTD_WITH_DICT };
  }

  @Test
  public void testHandleDeleteBeforeEOP() {
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask)
        .processMessageAndMaybeProduceToKafka(any(), any(), anyInt(), anyString(), anyInt(), anyLong(), anyLong());
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.isEndOfPushReceived()).thenReturn(false);
    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    when(consumerRecord.getKey()).thenReturn(kafkaKey);
    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    when(consumerRecord.getValue()).thenReturn(kafkaValue);
    when(consumerRecord.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(1));
    kafkaValue.messageType = MessageType.DELETE.getValue();
    Delete deletePayload = new Delete();
    kafkaValue.payloadUnion = deletePayload;
    ArgumentCaptor<LeaderProducedRecordContext> leaderProducedRecordContextArgumentCaptor =
        ArgumentCaptor.forClass(LeaderProducedRecordContext.class);
    ingestionTask.processMessageAndMaybeProduceToKafka(
        new PubSubMessageProcessedResultWrapper(consumerRecord),
        pcs,
        0,
        "dummyUrl",
        0,
        0L,
        0L);
    verify(ingestionTask, times(1)).produceToLocalKafka(
        any(),
        any(),
        leaderProducedRecordContextArgumentCaptor.capture(),
        any(),
        anyInt(),
        anyString(),
        anyInt(),
        anyLong());
    Assert.assertEquals(leaderProducedRecordContextArgumentCaptor.getAllValues().get(0).getValueUnion(), deletePayload);
  }

  @Test(dataProvider = "CompressionStrategy")
  public void testGetValueBytesFromTransientRecords(CompressionStrategy strategy) throws IOException {
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    PartitionConsumptionState.TransientRecord transientRecord = mock(PartitionConsumptionState.TransientRecord.class);
    VeniceCompressor compressor = getCompressor(strategy);
    when(ingestionTask.getCompressor()).thenReturn(Lazy.of(() -> compressor));
    when(ingestionTask.getCompressionStrategy()).thenReturn(strategy);
    when(ingestionTask.getCurrentValueFromTransientRecord(any())).thenCallRealMethod();

    byte[] dataBytes = "Hello World".getBytes();
    byte[] transientRecordValueBytes = dataBytes;
    int startPosition = 0;
    int dataLength = dataBytes.length;
    if (strategy != CompressionStrategy.NO_OP) {
      ByteBuffer compressedByteBuffer = compressor.compress(ByteBuffer.wrap(dataBytes), 4);
      transientRecordValueBytes = compressedByteBuffer.array();
      startPosition = compressedByteBuffer.position();
      dataLength = compressedByteBuffer.remaining();
    }
    when(transientRecord.getValue()).thenReturn(transientRecordValueBytes);
    when(transientRecord.getValueOffset()).thenReturn(startPosition);
    when(transientRecord.getValueLen()).thenReturn(dataLength);
    ByteBuffer result = ingestionTask.getCurrentValueFromTransientRecord(transientRecord);
    Assert.assertEquals(result.remaining(), dataBytes.length);
    byte[] resultByteArray = new byte[result.remaining()];
    result.get(resultByteArray);
    Assert.assertEquals("Hello World", new String(resultByteArray));
  }

  @Test
  public void testisReadyToServeAnnouncedWithRTLag() {
    // Setup store/schema/storage repository
    ReadOnlyStoreRepository readOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    ReadOnlySchemaRepository readOnlySchemaRepository = mock(ReadOnlySchemaRepository.class);
    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    when(storageEngineRepository.getLocalStorageEngine(any())).thenReturn(new BlackHoleStorageEngine(STORE_NAME));

    // Setup server config
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.freezeIngestionIfReadyToServeOrLocalDataExists()).thenReturn(false);
    when(serverConfig.getKafkaClusterUrlResolver()).thenReturn(null);
    when(serverConfig.getKafkaClusterUrlToIdMap()).thenReturn(new Object2IntArrayMap<>());
    when(serverConfig.getKafkaClusterIdToUrlMap()).thenReturn(new Int2ObjectArrayMap<>());
    when(serverConfig.getConsumerPoolSizePerKafkaCluster()).thenReturn(1);

    // Set up IngestionTask Builder
    StoreIngestionTaskFactory.Builder builder = new StoreIngestionTaskFactory.Builder();
    builder.setPubSubTopicRepository(TOPIC_REPOSITORY);
    builder.setHostLevelIngestionStats(mock(AggHostLevelIngestionStats.class));
    builder.setAggKafkaConsumerService(mock(AggKafkaConsumerService.class));
    builder.setMetadataRepository(readOnlyStoreRepository);
    builder.setServerConfig(serverConfig);
    builder.setSchemaRepository(readOnlySchemaRepository);

    // Set up version config and store config
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100L, 100L, 100L, BufferReplayPolicy.REWIND_FROM_EOP);

    StorageService storageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(StorageEngine.class), se -> {})).when(storageService)
        .getRefCountedStorageEngine(anyString());

    Store store = new ZKStore(
        STORE_NAME,
        "Felix",
        100L,
        PersistenceType.BLACK_HOLE,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    store.setHybridStoreConfig(hybridStoreConfig);
    Version mockVersion = new VersionImpl(STORE_NAME, 1, PUSH_JOB_ID);
    mockVersion.setHybridStoreConfig(hybridStoreConfig);
    store.setVersions(Collections.singletonList(mockVersion));

    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, BOOTSTRAP_SERVER);
    kafkaConsumerProperties.put(CLUSTER_NAME, TEST_CLUSTER_NAME);
    kafkaConsumerProperties.put(ZOOKEEPER_ADDRESS, BOOTSTRAP_SERVER);
    VeniceStoreVersionConfig storeVersionConfig =
        new VeniceStoreVersionConfig(STORE_NAME + "_v1", new VeniceProperties(kafkaConsumerProperties));
    int port = 123;
    ActiveActiveStoreIngestionTask ingestionTask = new ActiveActiveStoreIngestionTask(
        storageService,
        builder,
        store,
        mockVersion,
        kafkaConsumerProperties,
        () -> true,
        storeVersionConfig,
        1,
        false,
        Optional.empty(),
        null,
        null);

    PartitionConsumptionState badPartitionConsumptionState = mock(PartitionConsumptionState.class);
    when(badPartitionConsumptionState.hasLagCaughtUp()).thenReturn(true);
    // short circuit isReadyToServe
    when(badPartitionConsumptionState.isEndOfPushReceived()).thenReturn(false);
    ingestionTask.addPartitionConsumptionState(1, badPartitionConsumptionState);

    Assert.assertTrue(ingestionTask.isReadyToServeAnnouncedWithRTLag());

    PartitionConsumptionState goodPartitionConsumptionState = mock(PartitionConsumptionState.class);
    when(goodPartitionConsumptionState.hasLagCaughtUp()).thenReturn(true);
    when(goodPartitionConsumptionState.isEndOfPushReceived()).thenReturn(true);
    when(goodPartitionConsumptionState.isWaitingForReplicationLag()).thenReturn(false);
    ingestionTask.addPartitionConsumptionState(1, goodPartitionConsumptionState);

    Assert.assertFalse(ingestionTask.isReadyToServeAnnouncedWithRTLag());

    ingestionTask.addPartitionConsumptionState(2, badPartitionConsumptionState);

    Assert.assertTrue(ingestionTask.isReadyToServeAnnouncedWithRTLag());
  }

  @Test
  public void testMaybeBatchReportEOIP() {
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doCallRealMethod().when(ingestionTask).maybeReportBatchEndOfIncPushStatus(any());

    when(pcs.getPendingReportIncPushVersionList()).thenReturn(Collections.emptyList());
    ingestionTask.maybeReportBatchEndOfIncPushStatus(pcs);
    Mockito.verify(ingestionTask, Mockito.times(0)).getIngestionNotificationDispatcher();

    when(pcs.getPendingReportIncPushVersionList()).thenReturn(Collections.singletonList("test"));
    IngestionNotificationDispatcher ingestionNotificationDispatcher = mock(IngestionNotificationDispatcher.class);
    when(ingestionTask.getIngestionNotificationDispatcher()).thenReturn(ingestionNotificationDispatcher);

    when(pcs.isComplete()).thenReturn(false);
    ingestionTask.maybeReportBatchEndOfIncPushStatus(pcs);
    Mockito.verify(ingestionTask, Mockito.times(0)).getIngestionNotificationDispatcher();

    when(pcs.isComplete()).thenReturn(true);
    ingestionTask.maybeReportBatchEndOfIncPushStatus(pcs);
    Mockito.verify(ingestionTask, Mockito.times(1)).getIngestionNotificationDispatcher();
  }

  @Test
  public void testLeaderCanSendValueChunksIntoDrainer() throws InterruptedException {
    String testTopic = "test";
    int valueSchemaId = 1;
    int rmdProtocolVersionID = 1;
    int kafkaClusterId = 0;
    int partition = 0;
    String kafkaUrl = "kafkaUrl";
    long beforeProcessingRecordTimestamp = 0;
    boolean resultReuseInput = true;

    HostLevelIngestionStats mockHostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    when(ingestionTask.getHostLevelIngestionStats()).thenReturn(mockHostLevelIngestionStats);
    when(ingestionTask.getVersionIngestionStats()).thenReturn(mock(AggVersionedIngestionStats.class));
    when(ingestionTask.getVersionedDIVStats()).thenReturn(mock(AggVersionedDIVStats.class));
    when(ingestionTask.getKafkaVersionTopic()).thenReturn(testTopic);
    when(ingestionTask.createProducerCallback(any(), any(), any(), anyInt(), anyString(), anyLong()))
        .thenCallRealMethod();
    when(ingestionTask.getProduceToTopicFunction(any(), any(), any(), any(), any(), any(), anyInt(), anyBoolean()))
        .thenCallRealMethod();
    when(ingestionTask.getRmdProtocolVersionId()).thenReturn(rmdProtocolVersionID);
    doCallRealMethod().when(ingestionTask)
        .produceToLocalKafka(any(), any(), any(), any(), anyInt(), anyString(), anyInt(), anyLong());
    byte[] key = "foo".getBytes();
    byte[] updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);

    PubSubProducerAdapter mockedProducer = mock(PubSubProducerAdapter.class);
    CompletableFuture mockedFuture = mock(CompletableFuture.class);
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
            .setRmdChunkingEnabled(true)
            .setPartitionCount(1)
            .build();
    VeniceWriter<byte[], byte[], byte[]> writer =
        new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), mockedProducer);
    when(ingestionTask.isTransientRecordBufferUsed(any())).thenReturn(true);
    when(ingestionTask.getVeniceWriter(any())).thenReturn(Lazy.of(() -> writer));
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < 50000; i++) {
      stringBuilder.append("abcdefghabcdefghabcdefghabcdefgh");
    }
    ByteBuffer valueBytes = ByteBuffer.wrap(stringBuilder.toString().getBytes());
    ByteBuffer updatedValueBytes = ByteUtils.prependIntHeaderToByteBuffer(valueBytes, 1);
    ByteBuffer updatedRmdBytes = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    when(consumerRecord.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(100L));

    Put updatedPut = new Put();
    updatedPut.putValue = ByteUtils.prependIntHeaderToByteBuffer(updatedValueBytes, valueSchemaId, resultReuseInput);
    updatedPut.schemaId = valueSchemaId;
    updatedPut.replicationMetadataVersionId = rmdProtocolVersionID;
    updatedPut.replicationMetadataPayload = updatedRmdBytes;
    LeaderProducedRecordContext leaderProducedRecordContext = LeaderProducedRecordContext
        .newPutRecord(kafkaClusterId, consumerRecord.getPosition(), updatedKeyBytes, updatedPut);

    PubSubPosition consumedPositionMock = mock(PubSubPosition.class);
    PartitionConsumptionState.TransientRecord transientRecord =
        new PartitionConsumptionState.TransientRecord(new byte[] { 0xa }, 0, 0, 0, 0, consumedPositionMock);

    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    when(partitionConsumptionState.getTransientRecord(any())).thenReturn(transientRecord);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    when(consumerRecord.getKey()).thenReturn(kafkaKey);
    when(kafkaKey.getKey()).thenReturn(new byte[] { 0xa });
    ingestionTask.produceToLocalKafka(
        consumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        ingestionTask.getProduceToTopicFunction(
            partitionConsumptionState,
            updatedKeyBytes,
            updatedValueBytes,
            updatedRmdBytes,
            null,
            null,
            valueSchemaId,
            resultReuseInput),
        partition,
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingRecordTimestamp);

    // RMD chunking not enabled in this case...
    Assert.assertNotNull(transientRecord.getValueManifest());
    Assert.assertNotNull(transientRecord.getRmdManifest());
    Assert.assertEquals(transientRecord.getValueManifest().getKeysWithChunkIdSuffix().size(), 2);
    Assert.assertEquals(transientRecord.getRmdManifest().getKeysWithChunkIdSuffix().size(), 1);

    // Send 1 SOS, 2 Value Chunks, 1 RMD Chunk, 1 Manifest.
    verify(mockedProducer, times(5)).sendMessage(any(), any(), any(), any(), any(), any());
    ArgumentCaptor<LeaderProducedRecordContext> leaderProducedRecordContextArgumentCaptor =
        ArgumentCaptor.forClass(LeaderProducedRecordContext.class);
    verify(ingestionTask, times(4)).produceToStoreBufferService(
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
    Assert.assertEquals(
        leaderProducedRecordContextArgumentCaptor.getAllValues().get(3).getKeyBytes(),
        kafkaKeyArgumentCaptor.getAllValues().get(4).getKey());
    verify(mockHostLevelIngestionStats).recordLeaderProduceLatency(anyDouble());
  }

  @Test
  public void testReadingChunkedRmdFromStorage() {
    String storeName = "testStore";
    String topicName = "testStore_v1";
    int partition = 1;
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
    StorageEngine storageEngine = mock(StorageEngine.class);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    String stringSchema = "\"string\"";
    when(schemaRepository.getSupersetOrLatestValueSchema(storeName))
        .thenReturn(new SchemaEntry(valueSchema, stringSchema));
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.isComputeFastAvroEnabled()).thenReturn(false);
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    when(ingestionTask.getRmdProtocolVersionId()).thenReturn(1);
    Lazy<VeniceCompressor> compressor = Lazy.of(NoopCompressor::new);
    when(ingestionTask.getCompressor()).thenReturn(compressor);
    when(ingestionTask.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(ingestionTask.getStoreName()).thenReturn(storeName);
    when(ingestionTask.getStorageEngine()).thenReturn(storageEngine);
    when(ingestionTask.getSchemaRepo()).thenReturn(schemaRepository);
    when(ingestionTask.getServerConfig()).thenReturn(serverConfig);
    when(ingestionTask.getRmdWithValueSchemaByteBufferFromStorageInternal(anyInt(), any(), any())).thenCallRealMethod();
    when(ingestionTask.isChunked()).thenReturn(true);
    when(ingestionTask.getHostLevelIngestionStats()).thenReturn(mock(HostLevelIngestionStats.class));
    ChunkedValueManifestContainer container = new ChunkedValueManifestContainer();
    when(storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(topLevelKey1)))
        .thenReturn(expectedNonChunkedValue);
    byte[] result =
        ingestionTask.getRmdWithValueSchemaByteBufferFromStorageInternal(partition, key1, container).serialize();
    Assert.assertNotNull(result);
    Assert.assertNull(container.getManifest());
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
    int manifestSchemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    ByteBuffer chunkedManifestBytes = chunkedValueManifestSerializer.serialize(chunkedValueManifest);
    chunkedManifestBytes = ByteUtils.prependIntHeaderToByteBuffer(chunkedManifestBytes, manifestSchemaId);
    byte[] topLevelKey2 = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key2);
    byte[] chunkedKey1InKey2 = chunkedKeyWithSuffix1.array();
    when(storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(topLevelKey2)))
        .thenReturn(chunkedManifestBytes.array());
    when(storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(chunkedKey1InKey2))).thenReturn(chunkedValue1);
    byte[] result2 =
        ingestionTask.getRmdWithValueSchemaByteBufferFromStorageInternal(partition, key2, container).serialize();
    Assert.assertNotNull(result2);
    Assert.assertNotNull(container.getManifest());
    Assert.assertEquals(container.getManifest().getKeysWithChunkIdSuffix().size(), 1);
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
    chunkedManifestBytes = chunkedValueManifestSerializer.serialize(chunkedValueManifest);
    chunkedManifestBytes = ByteUtils.prependIntHeaderToByteBuffer(chunkedManifestBytes, manifestSchemaId);
    byte[] topLevelKey3 = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key3);
    byte[] chunkedKey1InKey3 = chunkedKeyWithSuffix1.array();
    byte[] chunkedKey2InKey3 = chunkedKeyWithSuffix2.array();
    when(storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(topLevelKey3)))
        .thenReturn(chunkedManifestBytes.array());
    when(storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(chunkedKey1InKey3))).thenReturn(chunkedValue1);
    when(storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(chunkedKey2InKey3))).thenReturn(chunkedValue2);
    byte[] result3 =
        ingestionTask.getRmdWithValueSchemaByteBufferFromStorageInternal(partition, key3, container).serialize();
    Assert.assertNotNull(result3);
    Assert.assertNotNull(container.getManifest());
    Assert.assertEquals(container.getManifest().getKeysWithChunkIdSuffix().size(), 2);
    Assert.assertEquals(result3, expectedChunkedValue2);
  }

  @Test
  public void testUnwrapByteBufferFromOldValueProvider() {
    Lazy<ByteBuffer> lazyBB = ActiveActiveStoreIngestionTask.unwrapByteBufferFromOldValueProvider(Lazy.of(() -> null));
    assertNotNull(lazyBB);
    assertNull(lazyBB.get());

    lazyBB = ActiveActiveStoreIngestionTask.unwrapByteBufferFromOldValueProvider(
        Lazy.of(() -> new ByteBufferValueRecord<>(ByteBuffer.wrap(new byte[1]), 1)));
    assertNotNull(lazyBB);
    assertNotNull(lazyBB.get());
  }

  @Test
  public void testGetUpstreamKafkaUrlFromKafkaValue() {
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("topic"), 0);
    long offset = 100;
    PubSubPosition position = ApacheKafkaOffsetPosition.of(offset);
    long timestamp = System.currentTimeMillis();
    int payloadSize = 200;
    String sourceKafka = "sourceKafkaURL";

    Int2ObjectMap<String> kafkaClusterIdToUrlMap = new Int2ObjectArrayMap<>(2);
    kafkaClusterIdToUrlMap.put(0, "url0");
    kafkaClusterIdToUrlMap.put(1, "url1");

    KafkaMessageEnvelope kmeWithNullLeaderMetadata = new KafkaMessageEnvelope();
    DefaultPubSubMessage pubSubMessageWithNullLeaderMetadata = new ImmutablePubSubMessage(
        new KafkaKey(MessageType.PUT, new byte[] { 1 }),
        kmeWithNullLeaderMetadata,
        partition,
        position,
        timestamp,
        payloadSize);
    try {
      ActiveActiveStoreIngestionTask
          .getUpstreamKafkaUrlFromKafkaValue(pubSubMessageWithNullLeaderMetadata, sourceKafka, kafkaClusterIdToUrlMap);
    } catch (VeniceException e) {
      LOGGER.info("kmeWithNullLeaderMetadata", e);
      assertEquals(e.getMessage(), "leaderMetadataFooter field in KME should have been set.");
    }

    KafkaMessageEnvelope kmeWithAbsentUpstreamCluster = new KafkaMessageEnvelope();
    kmeWithAbsentUpstreamCluster.setLeaderMetadataFooter(new LeaderMetadata());
    kmeWithAbsentUpstreamCluster.getLeaderMetadataFooter().upstreamKafkaClusterId = -1;
    kmeWithAbsentUpstreamCluster.setMessageType(MessageType.PUT.getValue());
    kmeWithAbsentUpstreamCluster.setProducerMetadata(new ProducerMetadata());
    DefaultPubSubMessage msgWithAbsentUpstreamCluster = new ImmutablePubSubMessage(
        new KafkaKey(MessageType.PUT, new byte[] { 1 }),
        kmeWithAbsentUpstreamCluster,
        partition,
        position,
        timestamp,
        payloadSize);
    try {
      ActiveActiveStoreIngestionTask
          .getUpstreamKafkaUrlFromKafkaValue(msgWithAbsentUpstreamCluster, sourceKafka, kafkaClusterIdToUrlMap);
    } catch (VeniceException e) {
      LOGGER.info("kmeWithAbsentUpstreamCluster", e);
      assertTrue(e.getMessage().startsWith("No Kafka cluster ID found in the cluster ID to Kafka URL map."));
      assertTrue(e.getMessage().contains("Message type: " + MessageType.PUT));
    }

    KafkaMessageEnvelope kmeForControlMessage = new KafkaMessageEnvelope();
    kmeForControlMessage.setLeaderMetadataFooter(new LeaderMetadata());
    kmeForControlMessage.getLeaderMetadataFooter().upstreamKafkaClusterId = -1;
    kmeForControlMessage.setMessageType(MessageType.CONTROL_MESSAGE.getValue());
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.setControlMessageType(ControlMessageType.TOPIC_SWITCH.getValue());
    kmeForControlMessage.setPayloadUnion(controlMessage);
    kmeForControlMessage.setProducerMetadata(new ProducerMetadata());
    DefaultPubSubMessage msgForControlMessage = new ImmutablePubSubMessage(
        new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[] { 1 }),
        kmeForControlMessage,
        partition,
        position,
        timestamp,
        payloadSize);
    try {
      ActiveActiveStoreIngestionTask
          .getUpstreamKafkaUrlFromKafkaValue(msgForControlMessage, sourceKafka, kafkaClusterIdToUrlMap);
    } catch (VeniceException e) {
      LOGGER.info("kmeForControlMessage", e);
      assertTrue(e.getMessage().startsWith("No Kafka cluster ID found in the cluster ID to Kafka URL map."));
      assertTrue(
          e.getMessage()
              .contains("Message type: " + MessageType.CONTROL_MESSAGE + "/" + ControlMessageType.TOPIC_SWITCH));
    }

    KafkaMessageEnvelope validKME = new KafkaMessageEnvelope();
    validKME.setLeaderMetadataFooter(new LeaderMetadata());
    validKME.getLeaderMetadataFooter().upstreamKafkaClusterId = 0;
    validKME.setProducerMetadata(new ProducerMetadata());
    DefaultPubSubMessage validMsg = new ImmutablePubSubMessage(
        new KafkaKey(MessageType.PUT, new byte[] { 1 }),
        validKME,
        partition,
        position,
        timestamp,
        payloadSize);
    assertEquals(
        ActiveActiveStoreIngestionTask.getUpstreamKafkaUrlFromKafkaValue(validMsg, sourceKafka, kafkaClusterIdToUrlMap),
        "url0");
  }

  private VeniceCompressor getCompressor(CompressionStrategy strategy) {
    if (Objects.requireNonNull(strategy) == CompressionStrategy.ZSTD_WITH_DICT) {
      byte[] dictionary = ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();
      return new CompressorFactory().createCompressorWithDictionary(dictionary, Zstd.maxCompressionLevel());
    }
    return new CompressorFactory().getCompressor(strategy);
  }

  @Test
  public void getKeyLevelLockMaxPoolSizeBasedOnServerConfigTest() {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    Int2ObjectMap<String> clusterIdToUrlMap = new Int2ObjectOpenHashMap<>();
    clusterIdToUrlMap.put(1, "region_1_url");
    clusterIdToUrlMap.put(2, "region_2_url");
    clusterIdToUrlMap.put(3, "region_3_url");
    when(serverConfig.getConsumerPoolStrategyType())
        .thenReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.DEFAULT);
    when(serverConfig.getConsumerPoolSizePerKafkaCluster()).thenReturn(100);
    when(serverConfig.getKafkaClusterIdToUrlMap()).thenReturn(clusterIdToUrlMap);
    when(serverConfig.getDedicatedConsumerPoolSizeForSepRTLeader()).thenReturn(3);
    when(serverConfig.getDedicatedConsumerPoolSizeForAAWCLeader()).thenReturn(5);
    assertEquals(ActiveActiveStoreIngestionTask.getKeyLevelLockMaxPoolSizeBasedOnServerConfig(serverConfig, 10), 31);
    when(serverConfig.isDedicatedConsumerPoolForAAWCLeaderEnabled()).thenReturn(true);
    assertEquals(ActiveActiveStoreIngestionTask.getKeyLevelLockMaxPoolSizeBasedOnServerConfig(serverConfig, 10), 25);
    assertEquals(ActiveActiveStoreIngestionTask.getKeyLevelLockMaxPoolSizeBasedOnServerConfig(serverConfig, 1), 4);
    // Test when current version prioritization strategy is enabled.
    when(serverConfig.isDedicatedConsumerPoolForAAWCLeaderEnabled()).thenReturn(false);
    when(serverConfig.getConsumerPoolStrategyType())
        .thenReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION);
    when(serverConfig.getConsumerPoolSizeForCurrentVersionAAWCLeader()).thenReturn(10);
    when(serverConfig.getConsumerPoolSizeForCurrentVersionSepRTLeader()).thenReturn(10);
    when(serverConfig.getConsumerPoolSizeForNonCurrentVersionAAWCLeader()).thenReturn(20);
    when(serverConfig.getConsumerPoolSizeForCurrentVersionNonAAWCLeader()).thenReturn(30);
    when(serverConfig.getConsumerPoolSizeForNonCurrentVersionNonAAWCLeader()).thenReturn(40);
    assertEquals(ActiveActiveStoreIngestionTask.getKeyLevelLockMaxPoolSizeBasedOnServerConfig(serverConfig, 1000), 121);

    // Test with parallel compute is enabled
    when(serverConfig.getAAWCWorkloadParallelProcessingThreadPoolSize()).thenReturn(8);
    when(serverConfig.isAAWCWorkloadParallelProcessingEnabled()).thenReturn(true);
    assertEquals(ActiveActiveStoreIngestionTask.getKeyLevelLockMaxPoolSizeBasedOnServerConfig(serverConfig, 1000), 961);
  }

  @Test
  public void testConsumerSubscribeValidatesPubSubAddress() {
    // Case 1: AA store ingestion task with invalid pubsub address
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);

    when(ingestionTask.getServerConfig()).thenReturn(mockServerConfig);
    when(ingestionTask.isDaVinciClient()).thenReturn(false);
    Object2IntMap<String> kafkaClusterUrlToIdMap = Object2IntMaps.singleton("validPubSubAddress", 1);
    when(mockServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(kafkaClusterUrlToIdMap);

    // Set up real method call
    doCallRealMethod().when(ingestionTask).consumerSubscribe(any(), anyLong(), anyString());

    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test"), 0);
    VeniceException exception = expectThrows(
        VeniceException.class,
        () -> ingestionTask.consumerSubscribe(pubSubTopicPartition, 100L, "invalidPubSubAddress"));
    assertNotNull(exception.getMessage(), "Exception message should not be null");
    assertTrue(
        exception.getMessage().contains("is not in the pubsub cluster map"),
        "Exception message should contain the expected message but found: " + exception.getMessage());

    verify(ingestionTask, times(1)).consumerSubscribe(pubSubTopicPartition, 100L, "invalidPubSubAddress");

    // Case 2: DaVinci client
    ActiveActiveStoreIngestionTask dvcIngestionTask = mock(ActiveActiveStoreIngestionTask.class);
    doCallRealMethod().when(dvcIngestionTask).consumerSubscribe(any(), anyLong(), anyString());
    when(dvcIngestionTask.getServerConfig()).thenReturn(mockServerConfig);
    when(dvcIngestionTask.isDaVinciClient()).thenReturn(true);
    when(mockServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(Object2IntMaps.emptyMap());
    try {
      dvcIngestionTask.consumerSubscribe(pubSubTopicPartition, 100L, "validPubSubAddress");
    } catch (Exception e) {
      if (e.getMessage() != null) {
        assertFalse(
            e.getMessage().contains("is not in the pubsub cluster map"),
            "Exception message should not contain the expected message but found: " + e.getMessage());
      }
    }
    verify(dvcIngestionTask, times(1)).consumerSubscribe(pubSubTopicPartition, 100L, "validPubSubAddress");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testGetStorageOperationTypeForDaVinci(boolean isDeferredWrite) {
    ByteBuffer payload = ByteBuffer.wrap("abc".getBytes());
    ByteBuffer emptyPayload = ByteBuffer.allocate(0);
    Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    partitionConsumptionStateMap.put(1, pcs);
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).checkStorageOperationCommonInvalidPattern(any(), any());
    doCallRealMethod().when(ingestionTask).getStorageOperationTypeForPut(anyInt(), any());
    doCallRealMethod().when(ingestionTask).getStorageOperationTypeForDelete(anyInt(), any());
    Put putWithEmptyPayloadAndWithoutRmd = new Put();
    putWithEmptyPayloadAndWithoutRmd.putValue = emptyPayload;
    Put putWithEmptyPayloadAndWithEmptyRmd = new Put();
    putWithEmptyPayloadAndWithEmptyRmd.putValue = emptyPayload;
    putWithEmptyPayloadAndWithEmptyRmd.replicationMetadataPayload = emptyPayload;
    Put putWithEmptyPayloadAndWithRmd = new Put();
    putWithEmptyPayloadAndWithRmd.putValue = emptyPayload;
    putWithEmptyPayloadAndWithRmd.replicationMetadataPayload = payload;
    Put putWithPayloadAndWithoutRmd = new Put();
    putWithPayloadAndWithoutRmd.putValue = payload;
    Put putWithPayloadAndWithEmptyRmd = new Put();
    putWithPayloadAndWithEmptyRmd.putValue = payload;
    putWithPayloadAndWithEmptyRmd.replicationMetadataPayload = emptyPayload;
    Put putWithPayloadAndWithRmd = new Put();
    putWithPayloadAndWithRmd.putValue = payload;
    putWithPayloadAndWithRmd.replicationMetadataPayload = payload;

    Delete deleteWithoutRmd = new Delete();
    Delete deleteWithEmptyRmd = new Delete();
    deleteWithEmptyRmd.replicationMetadataPayload = emptyPayload;
    Delete deleteWithRmd = new Delete();
    deleteWithRmd.replicationMetadataPayload = payload;

    doReturn(partitionConsumptionStateMap).when(ingestionTask).getPartitionConsumptionStateMap();
    // PCS == null should not persist.
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(0, deleteWithoutRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.SKIP);
    /**
     * Da Vinci case
     */
    doReturn(true).when(ingestionTask).isDaVinciClient();

    doReturn(isDeferredWrite).when(pcs).isDeferredWrite();
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForDelete(1, deleteWithoutRmd));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForPut(1, putWithEmptyPayloadAndWithoutRmd));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForPut(1, putWithPayloadAndWithoutRmd));

    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(1, deleteWithEmptyRmd),
        isDeferredWrite
            ? ActiveActiveStoreIngestionTask.StorageOperationType.SKIP
            : ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForPut(1, putWithEmptyPayloadAndWithEmptyRmd));
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForPut(1, putWithPayloadAndWithEmptyRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);

    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(1, deleteWithRmd),
        isDeferredWrite
            ? ActiveActiveStoreIngestionTask.StorageOperationType.SKIP
            : ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForPut(1, putWithEmptyPayloadAndWithRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.SKIP);
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForPut(1, putWithPayloadAndWithRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testGetStorageOperationTypeForServer(boolean isEndOfPush) {
    ByteBuffer payload = ByteBuffer.wrap("abc".getBytes());
    ByteBuffer emptyPayload = ByteBuffer.allocate(0);
    Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    partitionConsumptionStateMap.put(1, pcs);
    ActiveActiveStoreIngestionTask ingestionTask = mock(ActiveActiveStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).checkStorageOperationCommonInvalidPattern(any(), any());
    doCallRealMethod().when(ingestionTask).getStorageOperationTypeForPut(anyInt(), any());
    doCallRealMethod().when(ingestionTask).getStorageOperationTypeForDelete(anyInt(), any());
    Put putWithEmptyPayloadAndWithoutRmd = new Put();
    putWithEmptyPayloadAndWithoutRmd.putValue = emptyPayload;
    Put putWithEmptyPayloadAndWithEmptyRmd = new Put();
    putWithEmptyPayloadAndWithEmptyRmd.putValue = emptyPayload;
    putWithEmptyPayloadAndWithEmptyRmd.replicationMetadataPayload = emptyPayload;
    Put putWithEmptyPayloadAndWithRmd = new Put();
    putWithEmptyPayloadAndWithRmd.putValue = emptyPayload;
    putWithEmptyPayloadAndWithRmd.replicationMetadataPayload = payload;
    Put putWithPayloadAndWithoutRmd = new Put();
    putWithPayloadAndWithoutRmd.putValue = payload;
    Put putWithPayloadAndWithEmptyRmd = new Put();
    putWithPayloadAndWithEmptyRmd.putValue = payload;
    putWithPayloadAndWithEmptyRmd.replicationMetadataPayload = emptyPayload;
    Put putWithPayloadAndWithRmd = new Put();
    putWithPayloadAndWithRmd.putValue = payload;
    putWithPayloadAndWithRmd.replicationMetadataPayload = payload;

    Delete deleteWithoutRmd = new Delete();
    Delete deleteWithEmptyRmd = new Delete();
    deleteWithEmptyRmd.replicationMetadataPayload = emptyPayload;
    Delete deleteWithRmd = new Delete();
    deleteWithRmd.replicationMetadataPayload = payload;

    doReturn(partitionConsumptionStateMap).when(ingestionTask).getPartitionConsumptionStateMap();
    // PCS == null should not persist.
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(0, deleteWithoutRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.SKIP);

    doReturn(false).when(ingestionTask).isDaVinciClient();
    // deferred write = false.
    doReturn(isEndOfPush).when(pcs).isEndOfPushReceived();
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForDelete(1, deleteWithoutRmd));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForPut(1, putWithEmptyPayloadAndWithoutRmd));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForPut(1, putWithPayloadAndWithoutRmd));

    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(1, deleteWithEmptyRmd),
        isEndOfPush
            ? ActiveActiveStoreIngestionTask.StorageOperationType.VALUE_AND_RMD
            : ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> ingestionTask.getStorageOperationTypeForPut(1, putWithEmptyPayloadAndWithEmptyRmd));
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForPut(1, putWithPayloadAndWithEmptyRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE);

    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForDelete(1, deleteWithRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE_AND_RMD);
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForPut(1, putWithEmptyPayloadAndWithRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.RMD_CHUNK);
    Assert.assertEquals(
        ingestionTask.getStorageOperationTypeForPut(1, putWithPayloadAndWithRmd),
        ActiveActiveStoreIngestionTask.StorageOperationType.VALUE_AND_RMD);
  }

}
