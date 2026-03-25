package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.VIEW_WRITER_CLOSE_TIMEOUT_IN_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferStatusTrackingManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatKey;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.RawBytesChunkingAdapter;
import com.linkedin.davinci.store.DelegatingStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.view.MaterializedViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.GlobalRtDivState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ConfigCommonUtils.ActivationState;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.VeniceWriter;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.verification.VerificationModeFactory;
import org.mockito.verification.Timeout;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LeaderFollowerStoreIngestionTaskTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final int GLOBAL_RT_DIV_VERSION =
      AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion();

  private Store mockStore;
  private LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask;
  private PartitionConsumptionState mockPartitionConsumptionState;
  private PubSubTopicPartition mockTopicPartition;
  private ConsumerAction mockConsumerAction;
  private StorageService mockStorageService;
  private StoreBufferService mockStoreBufferService;
  private Properties mockProperties;
  private BooleanSupplier mockBooleanSupplier;
  private VeniceStoreVersionConfig mockVeniceStoreVersionConfig;

  private VeniceViewWriterFactory mockVeniceViewWriterFactory;
  private HostLevelIngestionStats hostLevelIngestionStats;
  private StorageMetadataService mockStorageMetadataService;
  private ReadOnlyStoreRepository storeRepository;
  private VeniceServerConfig mockVeniceServerConfig;
  private TopicManagerRepository mockTopicManagerRepository;

  @Test
  public void testCheckWhetherToCloseUnusedVeniceWriter() {
    VeniceWriter<byte[], byte[], byte[]> writer1 = mock(VeniceWriter.class);
    VeniceWriter<byte[], byte[], byte[]> writer2 = mock(VeniceWriter.class);
    PartitionConsumptionState pcsForLeaderBeforeEOP = mock(PartitionConsumptionState.class);
    doReturn(LeaderFollowerStateType.LEADER).when(pcsForLeaderBeforeEOP).getLeaderFollowerState();
    doReturn(false).when(pcsForLeaderBeforeEOP).isEndOfPushReceived();
    PartitionConsumptionState pcsForLeaderAfterEOP = mock(PartitionConsumptionState.class);
    doReturn(LeaderFollowerStateType.LEADER).when(pcsForLeaderAfterEOP).getLeaderFollowerState();
    doReturn(true).when(pcsForLeaderAfterEOP).isEndOfPushReceived();
    PartitionConsumptionState pcsForFollowerBeforeEOP = mock(PartitionConsumptionState.class);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcsForFollowerBeforeEOP).getLeaderFollowerState();
    doReturn(false).when(pcsForFollowerBeforeEOP).isEndOfPushReceived();
    PartitionConsumptionState pcsForFollowerAfterEOP = mock(PartitionConsumptionState.class);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcsForFollowerAfterEOP).getLeaderFollowerState();
    doReturn(true).when(pcsForLeaderAfterEOP).isEndOfPushReceived();

    String versionTopicName = "store_v1";
    // Some writers are not available.
    assertFalse(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            Lazy.of(() -> writer1),
            Lazy.of(() -> writer1),
            mock(Map.class),
            () -> {},
            versionTopicName));
    Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterWithInitializedValue1 = Lazy.of(() -> writer1);
    veniceWriterWithInitializedValue1.get();
    assertFalse(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            veniceWriterWithInitializedValue1,
            Lazy.of(() -> writer1),
            mock(Map.class),
            () -> {},
            versionTopicName));
    assertFalse(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            Lazy.of(() -> writer1),
            veniceWriterWithInitializedValue1,
            mock(Map.class),
            () -> {},
            versionTopicName));

    // Same writers
    assertFalse(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            veniceWriterWithInitializedValue1,
            veniceWriterWithInitializedValue1,
            mock(Map.class),
            () -> {},
            versionTopicName));

    Lazy<VeniceWriter<byte[], byte[], byte[]>> veniceWriterWithInitializedValue2 = Lazy.of(() -> writer2);
    veniceWriterWithInitializedValue2.get();
    // No leader
    Map<Integer, PartitionConsumptionState> noLeaderPCSMap = new HashMap<>();
    noLeaderPCSMap.put(0, pcsForFollowerAfterEOP);
    noLeaderPCSMap.put(1, pcsForFollowerBeforeEOP);
    Runnable runnable = mock(Runnable.class);

    assertTrue(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            veniceWriterWithInitializedValue1,
            veniceWriterWithInitializedValue2,
            noLeaderPCSMap,
            runnable,
            versionTopicName));
    verify(runnable).run();

    // One leader before EOP and some follower
    Map<Integer, PartitionConsumptionState> oneLeaderBeforeEOPPCSMap = new HashMap<>();
    oneLeaderBeforeEOPPCSMap.put(0, pcsForLeaderBeforeEOP);
    oneLeaderBeforeEOPPCSMap.put(1, pcsForFollowerBeforeEOP);
    runnable = mock(Runnable.class);
    assertFalse(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            veniceWriterWithInitializedValue1,
            veniceWriterWithInitializedValue2,
            oneLeaderBeforeEOPPCSMap,
            runnable,
            versionTopicName));
    verify(runnable, never()).run();

    // One leader before EOP and one leader after EOP and some follower
    Map<Integer, PartitionConsumptionState> oneLeaderBeforeEOPAndOneLeaderAfterEOPPCSMap = new HashMap<>();
    oneLeaderBeforeEOPAndOneLeaderAfterEOPPCSMap.put(0, pcsForLeaderBeforeEOP);
    oneLeaderBeforeEOPAndOneLeaderAfterEOPPCSMap.put(1, pcsForLeaderAfterEOP);
    oneLeaderBeforeEOPAndOneLeaderAfterEOPPCSMap.put(2, pcsForFollowerAfterEOP);
    runnable = mock(Runnable.class);
    assertFalse(
        LeaderFollowerStoreIngestionTask.checkWhetherToCloseUnusedVeniceWriter(
            veniceWriterWithInitializedValue1,
            veniceWriterWithInitializedValue2,
            oneLeaderBeforeEOPAndOneLeaderAfterEOPPCSMap,
            runnable,
            versionTopicName));
    verify(runnable, never()).run();
  }

  public void setUp() throws InterruptedException {
    setUp(false);
  }

  public void setUp(boolean isHybrid) throws InterruptedException {
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 1;
    mockStorageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(DelegatingStorageEngine.class), se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(anyString());
    mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    hostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    AggHostLevelIngestionStats aggHostLevelIngestionStats = mock(AggHostLevelIngestionStats.class);
    doReturn(hostLevelIngestionStats).when(aggHostLevelIngestionStats).getStoreStats(storeName);
    StorageMetadataService inMemoryStorageMetadataService = new InMemoryStorageMetadataService();
    StoreIngestionTaskFactory.Builder builder =
        getStoreIngestionTaskBuilder(isHybrid, storeName, inMemoryStorageMetadataService, aggHostLevelIngestionStats);
    when(builder.getSchemaRepo().getKeySchema(storeName)).thenReturn(new SchemaEntry(1, "\"string\""));
    mockStore = builder.getMetadataRepo().getStoreOrThrow(storeName);
    mockStoreBufferService = (StoreBufferService) builder.getStoreBufferService();
    Version version = mockStore.getVersion(versionNumber);
    assert version != null; // helps the IDE understand that version is not null, so it won't complain
    version.setCompressionStrategy(CompressionStrategy.GZIP);
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    String viewName = "testView";
    MaterializedViewParameters.Builder viewParamBuilder = new MaterializedViewParameters.Builder(viewName);
    viewParamBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName()).setPartitionCount(3);
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), viewParamBuilder.build());
    viewConfigMap.put(viewName, viewConfig);
    when(mockStore.getViewConfigs()).thenReturn(viewConfigMap);

    mockPartitionConsumptionState = mock(PartitionConsumptionState.class);
    mockConsumerAction = mock(ConsumerAction.class);

    mockProperties = new Properties();
    mockProperties.put(KAFKA_BOOTSTRAP_SERVERS, "bootStrapServers");
    mockBooleanSupplier = mock(BooleanSupplier.class);
    mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    String versionTopic = version.kafkaTopicName();
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();
    mockStorageMetadataService = builder.getStorageMetadataService();
    storeRepository = builder.getMetadataRepo();
    PubSubContext pubSubContext = builder.getPubSubContext();
    mockTopicManagerRepository = pubSubContext.getTopicManagerRepository();
    leaderFollowerStoreIngestionTask = spy(
        new LeaderFollowerStoreIngestionTask(
            mockStorageService,
            builder,
            mockStore,
            version,
            mockProperties,
            mockBooleanSupplier,
            mockVeniceStoreVersionConfig,
            0,
            Optional.empty(),
            null,
            null));

    leaderFollowerStoreIngestionTask.addPartitionConsumptionState(0, mockPartitionConsumptionState);
  }

  public StoreIngestionTaskFactory.Builder getStoreIngestionTaskBuilder(
      boolean isHybrid,
      String storeName,
      StorageMetadataService inMemoryStorageMetadataService,
      AggHostLevelIngestionStats aggHostLevelIngestionStats) {
    if (isHybrid) {
      return TestUtils.getStoreIngestionTaskBuilder(storeName, true)
          .setServerConfig(mockVeniceServerConfig)
          .setVeniceViewWriterFactory(mockVeniceViewWriterFactory)
          .setHeartbeatMonitoringService(mock(HeartbeatMonitoringService.class))
          .setCompressorFactory(new StorageEngineBackedCompressorFactory(inMemoryStorageMetadataService))
          .setHostLevelIngestionStats(aggHostLevelIngestionStats);
    }
    return TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setServerConfig(mockVeniceServerConfig)
        .setVeniceViewWriterFactory(mockVeniceViewWriterFactory)
        .setHeartbeatMonitoringService(mock(HeartbeatMonitoringService.class))
        .setCompressorFactory(new StorageEngineBackedCompressorFactory(inMemoryStorageMetadataService))
        .setHostLevelIngestionStats(aggHostLevelIngestionStats);
  }

  /**
   * Test veniceWriterLazyRef in PartitionConsumptionState can handle NPE in processConsumerAction.
   *
   * 1. No VeniceWriter is set in PCS, processConsumerAction doesn't have NPE thrown.
   * 2. VeniceWriter is set, but not initialized, closePartition is not invoked.
   * 3. VeniceWriter is set and initialized. closePartition is invoked once.
   */
  @Test
  public void testVeniceWriterInProcessConsumerAction() throws InterruptedException {
    setUp();
    when(mockConsumerAction.getType()).thenReturn(ConsumerActionType.LEADER_TO_STANDBY);
    when(mockConsumerAction.getTopic()).thenReturn("test-topic");
    when(mockConsumerAction.getPartition()).thenReturn(0);
    LeaderFollowerPartitionStateModel.LeaderSessionIdChecker mockLeaderSessionIdChecker =
        mock(LeaderFollowerPartitionStateModel.LeaderSessionIdChecker.class);
    when(mockConsumerAction.getLeaderSessionIdChecker()).thenReturn(mockLeaderSessionIdChecker);
    when(mockLeaderSessionIdChecker.isSessionIdValid()).thenReturn(true);
    mockTopicPartition = mock(PubSubTopicPartition.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    when(mockConsumerAction.getTopicPartition()).thenReturn(mockTopicPartition);
    when(mockPartitionConsumptionState.getOffsetRecord()).thenReturn(mockOffsetRecord);

    // case 1: No VeniceWriter is set in PCS, processConsumerAction doesn't have NPE.
    when(mockPartitionConsumptionState.getVeniceWriterLazyRef()).thenReturn(null);
    when(mockPartitionConsumptionState.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);

    leaderFollowerStoreIngestionTask.processConsumerAction(mockConsumerAction, mockStore);
    verify(mockPartitionConsumptionState, times(1)).setLeaderFollowerState(LeaderFollowerStateType.STANDBY);

    // case 2: VeniceWriter is set, but not initialized, closePartition is not invoked.
    VeniceWriter mockWriter = mock(VeniceWriter.class);
    Lazy<VeniceWriter<byte[], byte[], byte[]>> lazyMockWriter = Lazy.of(() -> mockWriter);
    when(mockPartitionConsumptionState.getVeniceWriterLazyRef()).thenReturn(lazyMockWriter);
    leaderFollowerStoreIngestionTask.processConsumerAction(mockConsumerAction, mockStore);
    verify(mockWriter, times(0)).closePartition(0);

    // case 3: VeniceWriter is set and initialized. closePartition is invoked once.
    lazyMockWriter.get();
    leaderFollowerStoreIngestionTask.processConsumerAction(mockConsumerAction, mockStore);
    verify(mockWriter, times(1)).closePartition(0);
  }

  /**
   * Verify that leaderTopic is persisted to storage metadata service during LEADER_TO_STANDBY transition.
   * This prevents blob transfer from copying stale leaderTopic (VT instead of RT) to new hosts.
   */
  @Test
  public void testLeaderTopicPersistedDuringLeaderToStandby() throws InterruptedException {
    setUp(true); // hybrid store
    when(mockConsumerAction.getType()).thenReturn(ConsumerActionType.LEADER_TO_STANDBY);
    when(mockConsumerAction.getTopic()).thenReturn("test-topic");
    when(mockConsumerAction.getPartition()).thenReturn(0);
    LeaderFollowerPartitionStateModel.LeaderSessionIdChecker mockLeaderSessionIdChecker =
        mock(LeaderFollowerPartitionStateModel.LeaderSessionIdChecker.class);
    when(mockConsumerAction.getLeaderSessionIdChecker()).thenReturn(mockLeaderSessionIdChecker);
    when(mockLeaderSessionIdChecker.isSessionIdValid()).thenReturn(true);

    mockTopicPartition = mock(PubSubTopicPartition.class);
    PubSubTopic vtTopic = TOPIC_REPOSITORY.getTopic("test-topic_v1");
    when(mockConsumerAction.getTopicPartition()).thenReturn(mockTopicPartition);
    when(mockTopicPartition.getPubSubTopic()).thenReturn(vtTopic);

    // Set up OffsetRecord with no leaderTopic (null means consuming from VT — the stale state)
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    when(mockPartitionConsumptionState.getOffsetRecord()).thenReturn(mockOffsetRecord);
    when(mockOffsetRecord.getLeaderTopic(any())).thenReturn(null);

    // PCS starts as LEADER; when setLeaderFollowerState(STANDBY) is called, subsequent
    // getLeaderFollowerState() returns STANDBY so updateLeaderTopicOnFollower doesn't skip
    when(mockPartitionConsumptionState.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);
    doAnswer(invocation -> {
      doReturn(invocation.getArgument(0)).when(mockPartitionConsumptionState).getLeaderFollowerState();
      return null;
    }).when(mockPartitionConsumptionState).setLeaderFollowerState(any());

    when(mockPartitionConsumptionState.getVeniceWriterLazyRef()).thenReturn(null);
    when(mockPartitionConsumptionState.getPartition()).thenReturn(0);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test-topic_v1-0");

    // Set up topicSwitch pointing to RT — this is what updateLeaderTopicOnFollower reads
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("test-topic_rt");
    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = new ArrayList<>();
    topicSwitch.sourceKafkaServers.add("kafka-server");
    topicSwitch.sourceTopicName = rtTopic.getName();
    topicSwitch.rewindStartTimestamp = System.currentTimeMillis();
    TopicSwitchWrapper topicSwitchWrapper = new TopicSwitchWrapper(topicSwitch, rtTopic);
    when(mockPartitionConsumptionState.getTopicSwitch()).thenReturn(topicSwitchWrapper);

    // Process LEADER_TO_STANDBY transition
    leaderFollowerStoreIngestionTask.processConsumerAction(mockConsumerAction, mockStore);

    // Verify leaderTopic was updated to RT by updateLeaderTopicOnFollower
    verify(mockOffsetRecord).setLeaderTopic(rtTopic);

    // Verify offset record was persisted under the correct version topic — the fix that prevents
    // stale leaderTopic in blob transfer from being copied to new hosts.
    // mockStorageMetadataService is a Mockito mock (created by TestUtils.getStoreIngestionTaskBuilder).
    verify(mockStorageMetadataService)
        .put(eq(leaderFollowerStoreIngestionTask.kafkaVersionTopic), eq(0), eq(mockOffsetRecord));
  }

  @Test
  public void testQueueUpVersionTopicWritesWithViewWriters() throws InterruptedException {
    mockVeniceViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Map<String, VeniceViewWriter> viewWriterMap = new HashMap<>();
    MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);
    when(materializedViewWriter.getViewWriterType()).thenReturn(VeniceViewWriter.ViewWriterType.MATERIALIZED_VIEW);
    viewWriterMap.put("testView", materializedViewWriter);
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt())).thenReturn(viewWriterMap);
    CompletableFuture<Void> viewWriterFuture = new CompletableFuture<>();
    when(materializedViewWriter.processRecord(any(), any(), anyInt(), any(), any())).thenReturn(viewWriterFuture);
    setUp();
    WriteComputeResultWrapper mockResult = mock(WriteComputeResultWrapper.class);
    Put put = new Put();
    put.schemaId = 1;
    when(mockResult.getNewPut()).thenReturn(put);
    AtomicBoolean writeToVersionTopic = new AtomicBoolean(false);
    when(mockPartitionConsumptionState.getLastVTProduceCallFuture())
        .thenReturn(CompletableFuture.completedFuture(null));
    leaderFollowerStoreIngestionTask.queueUpVersionTopicWritesWithViewWriters(
        mockPartitionConsumptionState,
        (viewWriter, viewPartitionSet) -> viewWriter
            .processRecord(mock(ByteBuffer.class), new byte[1], 1, viewPartitionSet, Lazy.of(() -> null)),
        null,
        () -> writeToVersionTopic.set(true));
    verify(mockPartitionConsumptionState, times(1)).getLastVTProduceCallFuture();
    ArgumentCaptor<CompletableFuture> vtWriteFutureCaptor = ArgumentCaptor.forClass(CompletableFuture.class);
    verify(mockPartitionConsumptionState, times(1)).setLastVTProduceCallFuture(vtWriteFutureCaptor.capture());
    verify(materializedViewWriter, times(1)).processRecord(any(), any(), anyInt(), any(), any());
    verify(hostLevelIngestionStats, times(1)).recordViewProducerLatency(anyDouble());
    verify(hostLevelIngestionStats, never()).recordViewProducerAckLatency(anyDouble());
    assertFalse(writeToVersionTopic.get());
    assertFalse(vtWriteFutureCaptor.getValue().isDone());
    assertFalse(vtWriteFutureCaptor.getValue().isCompletedExceptionally());
    viewWriterFuture.complete(null);
    // The whenCompleteAsync callback runs on the ForkJoinPool: it completes the VT future, sets writeToVersionTopic,
    // and THEN records ack latency. Use timeout() on the verify because isDone() becomes true before the metric call.
    verify(hostLevelIngestionStats, timeout(5000).times(1)).recordViewProducerAckLatency(anyDouble());
    assertTrue(vtWriteFutureCaptor.getValue().isDone());
    assertFalse(vtWriteFutureCaptor.getValue().isCompletedExceptionally());
    assertTrue(writeToVersionTopic.get());
  }

  @Test(timeOut = 30000)
  public void testCloseVeniceViewWriters() throws InterruptedException {
    mockVeniceViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Map<String, VeniceViewWriter> viewWriterMap = new HashMap<>();
    MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);
    viewWriterMap.put("testView", materializedViewWriter);
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt())).thenReturn(viewWriterMap);
    setUp();
    CompletableFuture<Void> lastVTProduceCallFuture = new CompletableFuture<>();
    doReturn(lastVTProduceCallFuture).when(mockPartitionConsumptionState).getLastVTProduceCallFuture();
    // gracefulClose/doFlush is false when close is called for a killed ingestion. The lastVTProduceCallFuture should be
    // short-circuited without any exception to reduce unnecessary exception logging.
    leaderFollowerStoreIngestionTask.closeVeniceViewWriters(false);
    verify(materializedViewWriter, times(1)).close(false);
    assertTrue(lastVTProduceCallFuture.isDone());
    assertFalse(lastVTProduceCallFuture.isCompletedExceptionally());

    clearInvocations(materializedViewWriter);
    setUp();
    Time mockTime = mock(Time.class);
    when(mockTime.getMilliseconds()).thenReturn(0L).thenReturn(VIEW_WRITER_CLOSE_TIMEOUT_IN_MS - 100L);
    leaderFollowerStoreIngestionTask.setTime(mockTime);
    CompletableFuture<Void> timedOutLastVTProduceCallFuture = new CompletableFuture<>();
    doReturn(timedOutLastVTProduceCallFuture).when(mockPartitionConsumptionState).getLastVTProduceCallFuture();
    // gracefulClose/doFlush is true. We will mimic lastVTProduceCallFuture is still not complete after some time and
    // the ingestion task will wait for 100ms before completing it exceptionally to shortcircuit and unblock any threads
    // waiting on the future.
    leaderFollowerStoreIngestionTask.closeVeniceViewWriters(true);
    verify(materializedViewWriter, times(1)).close(true);
    assertTrue(timedOutLastVTProduceCallFuture.isDone());
    assertTrue(timedOutLastVTProduceCallFuture.isCompletedExceptionally());
    try {
      timedOutLastVTProduceCallFuture.get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getMessage().contains("Exception caught when closing"));
    }

    // Finally verify a forcefully short-circuited lastVTProduceCallFuture due to already exceeding the graceful close
    // timeout.
    clearInvocations(materializedViewWriter);
    setUp();
    mockTime = mock(Time.class);
    when(mockTime.getMilliseconds()).thenReturn(0L).thenReturn(VIEW_WRITER_CLOSE_TIMEOUT_IN_MS + 100L);
    leaderFollowerStoreIngestionTask.setTime(mockTime);
    CompletableFuture<Void> forcefullyCompletedLastVTProduceCallFuture = new CompletableFuture<>();
    doReturn(forcefullyCompletedLastVTProduceCallFuture).when(mockPartitionConsumptionState)
        .getLastVTProduceCallFuture();
    leaderFollowerStoreIngestionTask.closeVeniceViewWriters(true);
    verify(materializedViewWriter, times(1)).close(true);
    assertTrue(forcefullyCompletedLastVTProduceCallFuture.isDone());
    assertTrue(forcefullyCompletedLastVTProduceCallFuture.isCompletedExceptionally());
    try {
      forcefullyCompletedLastVTProduceCallFuture.get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getMessage().contains("Completing the future forcefully"));
    }
  }

  /**
   * This test is to ensure if there are view writers the CMs produced to the VT don't get out of order due previous
   * writes to the VT getting delayed by corresponding view writers. Since during NR we write to view topic(s) before VT
   */
  @Test
  public void testControlMessagesAreInOrderWithPassthroughDIV() throws InterruptedException {
    mockVeniceViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Map<String, VeniceViewWriter> viewWriterMap = new HashMap<>();
    MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);
    viewWriterMap.put("testView", materializedViewWriter);
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt())).thenReturn(viewWriterMap);
    setUp();
    PubSubMessageProcessedResultWrapper firstCM = getMockMessage(1);
    PubSubMessageProcessedResultWrapper secondCM = getMockMessage(2);
    CompletableFuture<Void> lastVTWriteFuture = new CompletableFuture<>();
    CompletableFuture<Void> nextVTWriteFuture = new CompletableFuture<>();
    when(mockPartitionConsumptionState.getLastVTProduceCallFuture()).thenReturn(lastVTWriteFuture)
        .thenReturn(nextVTWriteFuture);
    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    doReturn(Lazy.of(() -> veniceWriter)).when(mockPartitionConsumptionState).getVeniceWriterLazyRef();
    leaderFollowerStoreIngestionTask.delegateConsumerRecord(firstCM, 0, "testURL", 0, 0, 0);
    leaderFollowerStoreIngestionTask.delegateConsumerRecord(secondCM, 0, "testURL", 0, 0, 0);
    // The CM write should be queued but not executed yet since the previous VT write future is still incomplete
    verify(veniceWriter, never()).put(any(), any(), any(), anyInt(), any());
    lastVTWriteFuture.complete(null);
    verify(veniceWriter, timeout(1000)).put(any(), any(), any(), anyInt(), any());
    nextVTWriteFuture.complete(null);
    // The CM should be written once the previous VT write is completed
    ArgumentCaptor<KafkaMessageEnvelope> kafkaValueCaptor = ArgumentCaptor.forClass(KafkaMessageEnvelope.class);
    verify(veniceWriter, new Timeout(1000, VerificationModeFactory.times(2)))
        .put(any(), kafkaValueCaptor.capture(), any(), anyInt(), any());
    int seqNumber = 1;
    for (KafkaMessageEnvelope value: kafkaValueCaptor.getAllValues()) {
      assertEquals(seqNumber++, value.getProducerMetadata().getMessageSequenceNumber());
    }
  }

  private PubSubMessageProcessedResultWrapper getMockMessage(int seqNumber) {
    PubSubMessageProcessedResultWrapper pubSubMessageProcessedResultWrapper =
        mock(PubSubMessageProcessedResultWrapper.class);
    DefaultPubSubMessage pubSubMessage = mock(DefaultPubSubMessage.class);
    doReturn(pubSubMessage).when(pubSubMessageProcessedResultWrapper).getMessage();
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(kafkaKey).when(pubSubMessage).getKey();
    KafkaMessageEnvelope kafkaValue = mock(KafkaMessageEnvelope.class);
    doReturn(MessageType.CONTROL_MESSAGE.getValue()).when(kafkaValue).getMessageType();
    ProducerMetadata producerMetadata = mock(ProducerMetadata.class);
    doReturn(seqNumber).when(producerMetadata).getMessageSequenceNumber();
    doReturn(producerMetadata).when(kafkaValue).getProducerMetadata();
    doReturn(kafkaValue).when(pubSubMessage).getValue();
    doReturn(true).when(mockPartitionConsumptionState).consumeRemotely();
    doReturn(LeaderFollowerStateType.LEADER).when(mockPartitionConsumptionState).getLeaderFollowerState();
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(mockPartitionConsumptionState).getOffsetRecord();
    PubSubTopicPartition pubSubTopicPartition = mock(PubSubTopicPartition.class);
    doReturn(pubSubTopicPartition).when(pubSubMessage).getTopicPartition();
    PubSubTopic pubSubTopic = mock(PubSubTopic.class);
    doReturn(pubSubTopic).when(pubSubTopicPartition).getPubSubTopic();
    doReturn(false).when(pubSubTopic).isRealTime();
    doReturn(true).when(kafkaKey).isControlMessage();
    ControlMessage controlMessage = mock(ControlMessage.class);
    doReturn(controlMessage).when(kafkaValue).getPayloadUnion();
    doReturn(ControlMessageType.START_OF_SEGMENT.getValue()).when(controlMessage).getControlMessageType();
    doReturn(ApacheKafkaOffsetPosition.of(seqNumber)).when(pubSubMessage).getPosition();
    return pubSubMessageProcessedResultWrapper;
  }

  @Test(timeOut = 60_000)
  public void testIsRecordSelfProduced() throws InterruptedException {
    setUp();
    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope kme = mock(KafkaMessageEnvelope.class);
    when(consumerRecord.getValue()).thenReturn(kme);

    // Case 0: Function does not throw when LeaderMetadata is null
    when(kme.getLeaderMetadataFooter()).thenReturn(null);
    assertFalse(leaderFollowerStoreIngestionTask.isRecordSelfProduced(consumerRecord));

    LeaderMetadata leaderMetadata = mock(LeaderMetadata.class);
    when(kme.getLeaderMetadataFooter()).thenReturn(leaderMetadata);

    // Case 1: HostName is different
    when(leaderMetadata.getHostName()).thenReturn("notlocalhost");
    assertFalse(leaderFollowerStoreIngestionTask.isRecordSelfProduced(consumerRecord));

    // Case 2: HostName is the same
    when(leaderMetadata.getHostName()).thenReturn(Utils.getHostName());
    assertFalse(leaderFollowerStoreIngestionTask.isRecordSelfProduced(consumerRecord));

    // Case 3: HostName is same and there is a port
    when(leaderMetadata.getHostName()).thenReturn(Utils.getHostName() + ":12345");
    assertFalse(leaderFollowerStoreIngestionTask.isRecordSelfProduced(consumerRecord));

    // Case 4: HostName and port is the same (listener port is 0)
    when(leaderMetadata.getHostName()).thenReturn(Utils.getHostName() + ":0");
    assertTrue(leaderFollowerStoreIngestionTask.isRecordSelfProduced(consumerRecord));
  }

  @Test
  public void testGetIngestionProgressPercentage() throws InterruptedException {
    setUp();

    // Mock the necessary components
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    PubSubTopicPartition mockTopicPartition = mock(PubSubTopicPartition.class);
    TopicManager mockTopicManager = mock(TopicManager.class);

    // Setup the mocks
    doReturn(mockPosition).when(mockPcs).getLatestProcessedVtPosition();
    doReturn(mockTopicPartition).when(mockPcs).getReplicaTopicPartition();
    doReturn(true).when(mockVeniceServerConfig).isIngestionProgressLoggingEnabled();
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager(anyString());
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getLocalTopicManager();
    doReturn(75).when(mockTopicManager).getIngestionProgressPercentage(mockTopicPartition, mockPosition);

    // Call the method under test
    int percentage = leaderFollowerStoreIngestionTask.getIngestionProgressPercentage(mockPcs);

    // Verify the result
    assertEquals(75, percentage, "Ingestion progress percentage should match the expected value");
    verify(mockTopicManager).getIngestionProgressPercentage(mockTopicPartition, mockPosition);

    // Test when progress percentage is disabled
    doReturn(false).when(mockVeniceServerConfig).isIngestionProgressLoggingEnabled();
    percentage = leaderFollowerStoreIngestionTask.getIngestionProgressPercentage(mockPcs);
    assertEquals(-1, percentage, "Ingestion progress percentage should be -1 when disabled");

    // No additional calls to getProgressPercentage should be made
    verify(mockTopicManager, times(1)).getIngestionProgressPercentage(any(), any());
  }

  @Test
  public void testSendGlobalRtDivMessage() throws InterruptedException, IOException {
    setUp();
    int partition = 1;
    long offset = 3L;
    long messageTime = 5;
    DefaultPubSubMessage mockMessage = mock(DefaultPubSubMessage.class);
    PubSubTopicPartition mockTopicPartition = mock(PubSubTopicPartition.class);
    LeaderProducedRecordContext context = mock(LeaderProducedRecordContext.class);
    PubSubPosition p3 = ApacheKafkaOffsetPosition.of(offset);
    doReturn(p3).when(context).getConsumedPosition();
    doReturn(partition).when(mockTopicPartition).getPartitionNumber();
    doReturn(p3).when(mockMessage).getPosition();
    doReturn(mockTopicPartition).when(mockMessage).getTopicPartition();
    doReturn(messageTime).when(mockMessage).getPubSubMessageTime();
    VeniceWriter mockWriter = mock(VeniceWriter.class);
    Lazy<VeniceWriter<byte[], byte[], byte[]>> lazyMockWriter = Lazy.of(() -> mockWriter);
    doReturn(lazyMockWriter).when(mockPartitionConsumptionState).getVeniceWriterLazyRef();
    doReturn(mock(KafkaMessageEnvelope.class)).when(mockWriter)
        .getKafkaMessageEnvelope(any(), anyBoolean(), anyInt(), anyBoolean(), any(), anyLong());
    String brokerUrl = "localhost:1234";
    byte[] keyBytes =
        LeaderFollowerStoreIngestionTask.getGlobalRtDivKeyName(partition, brokerUrl).getBytes(StandardCharsets.UTF_8);

    leaderFollowerStoreIngestionTask
        .sendGlobalRtDivMessage(mockMessage, mockPartitionConsumptionState, partition, brokerUrl, 0L, null, context);

    ArgumentCaptor<byte[]> valueBytesArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<LeaderProducerCallback> callbackArgumentCaptor =
        ArgumentCaptor.forClass(LeaderProducerCallback.class);
    verify(mockWriter, times(1)).put(
        eq(keyBytes),
        valueBytesArgumentCaptor.capture(),
        eq(partition),
        anyInt(),
        callbackArgumentCaptor.capture(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        eq(true));

    // Verify that GlobalRtDivState is correctly compressed and serialized from the VeniceWriter#put() call
    byte[] compressedBytes = valueBytesArgumentCaptor.getValue();
    VeniceCompressor compressor = leaderFollowerStoreIngestionTask.getCompressor().get();
    byte[] valueBytes = ByteUtils.extractByteArray(compressor.decompress(ByteBuffer.wrap(compressedBytes)));
    InternalAvroSpecificSerializer<GlobalRtDivState> serializer =
        leaderFollowerStoreIngestionTask.globalRtDivStateSerializer;
    GlobalRtDivState globalRtDiv = serializer.deserialize(valueBytes, GLOBAL_RT_DIV_VERSION);
    assertNotNull(globalRtDiv);

    // Verify the callback has DivSnapshot (VT + RT DIV)
    LeaderProducerCallback callback = callbackArgumentCaptor.getValue();
    DefaultPubSubMessage callbackPayload = callback.getSourceConsumerRecord();
    assertEquals(callbackPayload.getKey().getKey(), keyBytes);
    assertEquals(callbackPayload.getKey().getKeyHeaderByte(), MessageType.GLOBAL_RT_DIV.getKeyHeaderByte());
    assertEquals(callbackPayload.getValue().getMessageType(), MessageType.PUT.getValue());
    assertEquals(callbackPayload.getPartition(), partition);
    assertTrue(callbackPayload.getValue().payloadUnion instanceof Put);
    Put put = (Put) callbackPayload.getValue().payloadUnion;
    assertEquals(put.getSchemaId(), GLOBAL_RT_DIV_VERSION);
    assertNotNull(put.getPutValue());

    // Verify that completing the future from put() causes execSyncOffsetFromSnapshotAsync to be called
    // and that produceResult should override the LCVP of the VT DIV sent to the drainer
    verify(mockStoreBufferService, never()).execSyncOffsetFromSnapshotAsync(any(), any(), any(), any());
    PubSubProduceResult produceResult = mock(PubSubProduceResult.class);
    PubSubPosition specificPosition = InMemoryPubSubPosition.of(11L);
    when(produceResult.getPubSubPosition()).thenReturn(specificPosition);
    when(produceResult.getSerializedSize()).thenReturn(keyBytes.length + put.putValue.remaining());
    callback.onCompletion(produceResult, null);
    ArgumentCaptor<PartitionTracker> vtDivCaptor = ArgumentCaptor.forClass(PartitionTracker.class);
    verify(mockStoreBufferService, times(1))
        .execSyncOffsetFromSnapshotAsync(any(), vtDivCaptor.capture(), any(), any());
    assertEquals(vtDivCaptor.getValue().getLatestConsumedVtPosition(), specificPosition);
  }

  @Test
  public void testUpdateLatestConsumedVtOffset() throws InterruptedException {
    setUp();
    LeaderFollowerStoreIngestionTask mockIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    PubSubMessageProcessedResultWrapper cm = getMockMessage(1);
    PubSubTopic mockTopic = cm.getMessage().getTopicPartition().getPubSubTopic();
    doReturn(false).when(mockTopic).isRealTime();
    doReturn(mockPartitionConsumptionState).when(mockIngestionTask).getPartitionConsumptionState(anyInt());
    doReturn(LeaderFollowerStateType.STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();
    doCallRealMethod().when(mockIngestionTask)
        .delegateConsumerRecord(any(), anyInt(), any(), anyInt(), anyLong(), anyLong());
    DataIntegrityValidator consumerDiv = mock(DataIntegrityValidator.class);
    doReturn(consumerDiv).when(mockIngestionTask).getConsumerDiv();
    doReturn(true).when(mockIngestionTask).isGlobalRtDivEnabled();

    // delegateConsumerRecord() should cause updateLatestConsumedVtPosition() to be called
    mockIngestionTask.delegateConsumerRecord(cm, 0, "testURL", 0, 0, 0);
    verify(consumerDiv, times(1)).updateLatestConsumedVtPosition(0, cm.getMessage().getPosition());
  }

  @Test
  public void testShouldSyncOffsetFromSnapshot() throws InterruptedException {
    setUp();
    LeaderFollowerStoreIngestionTask mockIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(mockIngestionTask).shouldSyncOffsetFromSnapshot(any(), any());
    doCallRealMethod().when(mockIngestionTask).shouldSyncOffset(any(), any(), any());
    // Stub early so size-based branch can call getVersionTopic().getName()
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic("test-topic_v1");
    doReturn(versionTopic).when(mockIngestionTask).getVersionTopic();
    VeniceConcurrentHashMap<String, Long> consumedBytesSinceLastSync = new VeniceConcurrentHashMap<>();
    doReturn(consumedBytesSinceLastSync).when(mockIngestionTask).getConsumedBytesSinceLastSync();

    // Set up Global RT DIV message
    final DefaultPubSubMessage globalRtDivMessage = getMockMessage(1).getMessage();
    KafkaKey mockKey = globalRtDivMessage.getKey();
    doReturn(false).when(mockKey).isControlMessage();
    Put mockPut = mock(Put.class);
    KafkaMessageEnvelope mockKme = globalRtDivMessage.getValue();

    // The method should only return true for non-chunked Global RT DIV messages
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));
    doReturn(true).when(mockKey).isGlobalRtDiv();
    doReturn(mockPut).when(mockKme).getPayloadUnion();
    doReturn(GLOBAL_RT_DIV_VERSION).when(mockPut).getSchemaId();
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));
    doReturn(AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()).when(mockPut).getSchemaId();
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));
    doReturn(AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()).when(mockPut).getSchemaId();
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));

    // The method should not error when a non-Put value is passed in
    Delete mockDelete = mock(Delete.class);
    doReturn(mockDelete).when(mockKme).getPayloadUnion();
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));

    // Set up Control Message
    final DefaultPubSubMessage nonSegmentControlMessage = getMockMessage(2).getMessage();
    mockKey = nonSegmentControlMessage.getKey();
    mockKme = nonSegmentControlMessage.getValue();
    doReturn(false).when(mockKey).isGlobalRtDiv();
    ControlMessage mockControlMessage = mock(ControlMessage.class);

    // The method should only return true for non-segment control messages
    assertFalse(
        mockIngestionTask.shouldSyncOffsetFromSnapshot(nonSegmentControlMessage, mockPartitionConsumptionState));
    doReturn(ControlMessageType.START_OF_PUSH.getValue()).when(mockControlMessage).getControlMessageType();
    doReturn(true).when(mockKey).isControlMessage();
    doReturn(mockControlMessage).when(mockKme).getPayloadUnion();
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(nonSegmentControlMessage, mockPartitionConsumptionState));
    doReturn(ControlMessageType.START_OF_SEGMENT.getValue()).when(mockControlMessage).getControlMessageType();
    assertFalse(
        mockIngestionTask.shouldSyncOffsetFromSnapshot(nonSegmentControlMessage, mockPartitionConsumptionState));

    // Mock the getSyncBytesInterval method to return a specific value
    doReturn(1000L).when(mockIngestionTask).getSyncBytesInterval(any());

    // Create a mock message that is neither a Global RT DIV nor a control message
    final DefaultPubSubMessage regularMessage = getMockMessage(3).getMessage();
    KafkaKey regularMockKey = regularMessage.getKey();
    doReturn(false).when(regularMockKey).isGlobalRtDiv();
    doReturn(false).when(regularMockKey).isControlMessage();

    // Test case 1: When VT consumed bytes since last sync is less than 2*syncBytesInterval
    consumedBytesSinceLastSync.put(versionTopic.getName(), 1500L);
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(regularMessage, mockPartitionConsumptionState));

    // Test case 2: When VT consumed bytes since last sync is equal to 2*syncBytesInterval
    consumedBytesSinceLastSync.put(versionTopic.getName(), 2000L);
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(regularMessage, mockPartitionConsumptionState));

    // Test case 3: When VT consumed bytes since last sync is greater than 2*syncBytesInterval
    consumedBytesSinceLastSync.put(versionTopic.getName(), 2500L);
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(regularMessage, mockPartitionConsumptionState));

    // Test case 4: When syncBytesInterval is 0 (disabled)
    doReturn(0L).when(mockIngestionTask).getSyncBytesInterval(any());
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(regularMessage, mockPartitionConsumptionState));
  }

  @Test
  public void testFutureVersionLatchStatus() throws InterruptedException {
    setUp(true);

    // Setup subscribe action
    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_v1"), 0);
    when(mockConsumerAction.getTopicPartition()).thenReturn(partition0);
    when(mockConsumerAction.getType()).thenReturn(ConsumerActionType.SUBSCRIBE);
    LeaderFollowerPartitionStateModel.LeaderSessionIdChecker mockLeaderSessionIdChecker =
        mock(LeaderFollowerPartitionStateModel.LeaderSessionIdChecker.class);
    when(mockConsumerAction.getLeaderSessionIdChecker()).thenReturn(mockLeaderSessionIdChecker);
    when(mockLeaderSessionIdChecker.isSessionIdValid()).thenReturn(true);

    // Setup partition
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    when(mockOffsetRecord.getLeaderTopic()).thenReturn("test_rt");
    when(mockOffsetRecord.isEndOfPushReceived()).thenReturn(true);
    PubSubPosition p0 = ApacheKafkaOffsetPosition.of(0L);
    doReturn(p0).when(mockOffsetRecord).getCheckpointedLocalVtPosition();

    when(mockStorageMetadataService.getLastOffset(any(), anyInt(), any())).thenReturn(mockOffsetRecord);
    when(mockConsumerAction.getTopicPartition()).thenReturn(partition0);
    when(mockPartitionConsumptionState.getOffsetRecord()).thenReturn(mockOffsetRecord);

    // Setup store. Mark current version as false and make future version ONLINE to trigger Utils.isFutureVersionReady
    when(mockBooleanSupplier.getAsBoolean()).thenReturn(false);
    when(storeRepository.getStore(anyString())).thenReturn(mockStore);
    Version mockVersion = mock(Version.class);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.ONLINE);
    when(mockStore.getVersion(1)).thenReturn(mockVersion);
    when(mockStore.getCurrentVersion()).thenReturn(0);
    when(mockVersion.isHybrid()).thenReturn(true);

    // Setup server properties
    VeniceProperties properties = new VeniceProperties();
    when(mockVeniceServerConfig.getClusterProperties()).thenReturn(properties);
    when(mockVeniceServerConfig.getKafkaConsumerConfigsForLocalConsumption()).thenReturn(properties);

    // Setup topic manager
    TopicManager mockTopicManager = mock(TopicManager.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getLocalTopicManager();
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager(anyString());
    ApacheKafkaOffsetPosition p10 = ApacheKafkaOffsetPosition.of(10L);
    doReturn(p10).when(mockTopicManager).getLatestPositionCached(any(PubSubTopicPartition.class));

    // Run SIT to process the mock consumer action
    leaderFollowerStoreIngestionTask.processCommonConsumerAction(mockConsumerAction);

    // Verify that we enter the block to release the latch
    verify(leaderFollowerStoreIngestionTask, times(1)).measureLagWithCallToPubSub(any(), any(), any());
  }

  /**
   * Verifies that when recordTransformer is enabled and blob transfer is skipped,
   * processCommonConsumerAction stores the original consumerAction (with pubSubPosition) on PCS.
   * This ensures seekToCheckpoint position is preserved through record transformer recovery.
   */
  @Test
  public void testRecordTransformerPathPreservesConsumerActionWithPubSubPosition() throws Exception {
    setUp(false);

    // Setup subscribe action with a pubSubPosition (seekToCheckpoint)
    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_v1"), 0);
    ConsumerAction consumerAction = new ConsumerAction(ConsumerActionType.SUBSCRIBE, partition0, 1, false);
    PubSubPosition seekPosition = ApacheKafkaOffsetPosition.of(42L);
    consumerAction.setPubSubPosition(seekPosition);

    // Setup storage metadata to return an offset record
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    when(mockStorageMetadataService.getLastOffset(any(), anyInt(), any())).thenReturn(mockOffsetRecord);

    // Set recordTransformer to a non-null mock so the transformer path is taken
    InternalDaVinciRecordTransformer mockRecordTransformer = mock(InternalDaVinciRecordTransformer.class);
    setField(leaderFollowerStoreIngestionTask, "recordTransformer", mockRecordTransformer);

    // Set up a thread pool so submitRecordTransformerRecoveryAsync can execute
    ExecutorService threadPool = Executors.newSingleThreadExecutor();
    setField(leaderFollowerStoreIngestionTask, "recordTransformerOnRecoveryThreadPool", threadPool);

    // Set up blobTransferHelper with a non-null BlobTransferManager so the pubSubPosition
    // check (not the null check) is what causes blob transfer to be skipped.
    BlobTransferManager mockBlobTransferManager = mock(BlobTransferManager.class);
    BlobTransferIngestionHelper blobTransferHelper = spy(
        new BlobTransferIngestionHelper(
            mockBlobTransferManager,
            mockStorageService,
            mockStorageMetadataService,
            mockVeniceServerConfig,
            Collections.emptySet()));
    setField(leaderFollowerStoreIngestionTask, "blobTransferHelper", blobTransferHelper);

    // Process the subscribe action — should take the transformer path (not blob transfer, not default)
    leaderFollowerStoreIngestionTask.processCommonConsumerAction(consumerAction);

    // Verify the PCS was created and has the consumerAction stored
    PartitionConsumptionState pcs =
        leaderFollowerStoreIngestionTask.getPartitionConsumptionStateMap().get(partition0.getPartitionNumber());
    assertNotNull(pcs, "PCS should be created for the partition");

    // Verify blob transfer was skipped due to pubSubPosition on consumerAction, not due to null helper
    assertFalse(pcs.isBlobTransferInProgress(), "Blob transfer should not be in progress");
    verify(blobTransferHelper).shouldStartBlobTransfer(
        eq(consumerAction),
        any(),
        anyString(),
        anyInt(),
        anyInt(),
        anyString(),
        anyBoolean(),
        anyBoolean(),
        anyString(),
        any());
    verify(blobTransferHelper, never())
        .startBlobTransferAsyncForPartition(anyInt(), any(), any(), anyString(), anyInt(), any(), anyString());

    assertTrue(pcs.isRecordTransformerRecoveryInProgress(), "Record transformer recovery should be in progress");

    ConsumerAction storedAction = pcs.getPostRecordTransformerConsumerAction();
    assertNotNull(storedAction, "ConsumerAction should be stored on PCS for post-recovery subscribe");
    assertEquals(storedAction, consumerAction, "Stored consumerAction should be the original one");
    assertEquals(
        storedAction.getPubSubPosition(),
        seekPosition,
        "pubSubPosition (seekToCheckpoint) should be preserved on the stored consumerAction");

    threadPool.shutdownNow();
  }

  /**
   * Verifies that the transformer-only SUBSCRIBE path creates a placeholder PCS with an empty
   * OffsetRecord (no storageMetadataService.getLastOffset call) and does NOT set DIV state.
   */
  @Test
  public void testPlaceholderPcsHasEmptyOffsetAndNoDivState() throws Exception {
    setUp(false);

    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_v1"), 0);
    ConsumerAction consumerAction = new ConsumerAction(ConsumerActionType.SUBSCRIBE, partition0, 1, false);

    // Setup storage metadata -- should NOT be called for placeholder path
    OffsetRecord realOffsetRecord = mock(OffsetRecord.class);
    when(mockStorageMetadataService.getLastOffset(any(), anyInt(), any())).thenReturn(realOffsetRecord);

    // Set recordTransformer to non-null so the transformer path is taken
    InternalDaVinciRecordTransformer mockRecordTransformer = mock(InternalDaVinciRecordTransformer.class);
    setField(leaderFollowerStoreIngestionTask, "recordTransformer", mockRecordTransformer);

    ExecutorService threadPool = Executors.newSingleThreadExecutor();
    setField(leaderFollowerStoreIngestionTask, "recordTransformerOnRecoveryThreadPool", threadPool);

    // Spy on the data integrity validator to verify setPartitionState is NOT called
    DataIntegrityValidator mockDiv = spy(leaderFollowerStoreIngestionTask.getDataIntegrityValidator());
    setField(leaderFollowerStoreIngestionTask, "drainerDiv", mockDiv);

    // Remove the mock PCS that setUp added for partition 0 so the real SUBSCRIBE flow runs
    leaderFollowerStoreIngestionTask.getPartitionConsumptionStateMap().remove(0);

    leaderFollowerStoreIngestionTask.processCommonConsumerAction(consumerAction);

    // Verify the placeholder PCS was created
    PartitionConsumptionState pcs =
        leaderFollowerStoreIngestionTask.getPartitionConsumptionStateMap().get(partition0.getPartitionNumber());
    assertNotNull(pcs, "Placeholder PCS should be installed in the map");

    // Verify the OffsetRecord is empty (not the real one from storage)
    OffsetRecord offsetRecord = pcs.getOffsetRecord();
    assertNotNull(offsetRecord, "Placeholder PCS should have a non-null OffsetRecord");
    assertFalse(
        offsetRecord == realOffsetRecord,
        "Placeholder PCS should use an empty OffsetRecord, not the one from storageMetadataService");
    assertFalse(offsetRecord.isEndOfPushReceived(), "Empty OffsetRecord should not have EOP received");

    // Verify storageMetadataService.getLastOffset was NOT called (placeholder skips this)
    verify(mockStorageMetadataService, never()).getLastOffset(any(), anyInt(), any());

    // Verify DIV setPartitionState was NOT called for the placeholder
    verify(mockDiv, never()).setPartitionState(any(), anyInt(), any(OffsetRecord.class));

    assertTrue(pcs.isRecordTransformerRecoveryInProgress(), "Recovery should be in progress on placeholder PCS");

    threadPool.shutdownNow();
  }

  /**
   * Verifies that reinitializePartitionConsumptionStateFromStorage preserves the leader/follower
   * state and DolStamp from the old PCS when creating the fresh PCS.
   * Uses the transformer path to create a real PCS first, then simulates recovery completion.
   */
  @Test
  public void testReinitializePartitionConsumptionStatePreservesLfStateAndDolStamp() throws Exception {
    setUp(false);

    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_v1"), 0);
    ConsumerAction consumerAction = new ConsumerAction(ConsumerActionType.SUBSCRIBE, partition0, 1, false);

    // Set recordTransformer so the transformer path creates a real PCS
    InternalDaVinciRecordTransformer mockRecordTransformer = mock(InternalDaVinciRecordTransformer.class);
    setField(leaderFollowerStoreIngestionTask, "recordTransformer", mockRecordTransformer);
    ExecutorService threadPool = Executors.newSingleThreadExecutor();
    setField(leaderFollowerStoreIngestionTask, "recordTransformerOnRecoveryThreadPool", threadPool);

    // Remove mock PCS so SUBSCRIBE creates a real one
    leaderFollowerStoreIngestionTask.getPartitionConsumptionStateMap().remove(0);
    leaderFollowerStoreIngestionTask.processCommonConsumerAction(consumerAction);

    // Get the real PCS that was created by SUBSCRIBE (placeholder with empty offset)
    PartitionConsumptionState oldPcs = leaderFollowerStoreIngestionTask.getPartitionConsumptionStateMap().get(0);
    assertNotNull(oldPcs, "SUBSCRIBE should have created a PCS");

    // Simulate a STANDBY_TO_LEADER transition during recovery
    oldPcs.setLeaderFollowerState(LeaderFollowerStateType.LEADER);

    // Simulate a DoL stamp that was set during recovery
    DolStamp dolStamp = new DolStamp(42L, "test-host");
    oldPcs.setDolState(dolStamp);

    // Override storageMetadataService with a mock that returns a real OffsetRecord for reinitialize
    StorageMetadataService mockSms = mock(StorageMetadataService.class);
    OffsetRecord freshOffsetRecord = mock(OffsetRecord.class);
    when(mockSms.getLastOffset(any(), anyInt(), any())).thenReturn(freshOffsetRecord);
    setField(leaderFollowerStoreIngestionTask, "storageMetadataService", mockSms);

    // Call reinitializePartitionConsumptionStateFromStorage (what checkLongRunningTaskState calls after recovery)
    PartitionConsumptionState freshPcs =
        leaderFollowerStoreIngestionTask.reinitializePartitionConsumptionStateFromStorage(partition0, 0);

    // Verify the fresh PCS is a new instance
    assertNotNull(freshPcs, "Fresh PCS should be created");
    assertTrue(freshPcs != oldPcs, "Fresh PCS should be a new instance, not the old one");

    // Verify leader/follower state was preserved
    assertEquals(
        freshPcs.getLeaderFollowerState(),
        LeaderFollowerStateType.LEADER,
        "Leader/follower state should be preserved from old PCS");

    // Verify DolStamp was preserved
    assertNotNull(freshPcs.getDolState(), "DolStamp should be preserved from old PCS");
    assertEquals(freshPcs.getDolState().getLeadershipTerm(), 42L, "DolStamp leadership term should be preserved");
    assertEquals(freshPcs.getDolState().getHostId(), "test-host", "DolStamp host ID should be preserved");

    // Verify the fresh PCS is installed in the map
    assertEquals(
        leaderFollowerStoreIngestionTask.getPartitionConsumptionStateMap().get(0),
        freshPcs,
        "Fresh PCS should replace the old one in the map");

    threadPool.shutdownNow();
  }

  @DataProvider(name = "isVtFullyConsumedCases")
  public Object[][] fullyConsumedCases() {
    PubSubPosition p0 = ApacheKafkaOffsetPosition.of(0L);
    PubSubPosition p1 = ApacheKafkaOffsetPosition.of(1L);
    PubSubPosition p2 = ApacheKafkaOffsetPosition.of(2L);
    PubSubPosition p5 = ApacheKafkaOffsetPosition.of(5L);
    PubSubPosition p10 = ApacheKafkaOffsetPosition.of(10L);
    PubSubPosition p20 = ApacheKafkaOffsetPosition.of(20L);
    PubSubPosition p49 = ApacheKafkaOffsetPosition.of(49L);
    PubSubPosition p50 = ApacheKafkaOffsetPosition.of(50L);
    PubSubPosition p51 = ApacheKafkaOffsetPosition.of(51L);
    PubSubPosition p98 = ApacheKafkaOffsetPosition.of(98L);
    PubSubPosition p99 = ApacheKafkaOffsetPosition.of(99L);
    PubSubPosition p100 = ApacheKafkaOffsetPosition.of(100L);
    PubSubPosition p1000 = ApacheKafkaOffsetPosition.of(1000L);

    // vtPosition, endPosition, isFullyConsumed, message
    return new Object[][] {
        // === Path 1: LATEST end position cases ===
        { p50, PubSubSymbolicPosition.LATEST, false, "Any VT position with LATEST end always returns false" },
        { p0, PubSubSymbolicPosition.LATEST, false, "VT at start with LATEST end returns false" },
        { PubSubSymbolicPosition.EARLIEST, PubSubSymbolicPosition.LATEST, false,
            "EARLIEST VT with LATEST end returns false" },

        // === Path 2: EARLIEST VT position cases (uses countRecordsUntil) ===
        { PubSubSymbolicPosition.EARLIEST, p0, true, "Empty partition: EARLIEST VT, end=0, numRecords=0" },
        { PubSubSymbolicPosition.EARLIEST, p1, false, "Single message partition: EARLIEST VT, end=1, numRecords=1" },
        { PubSubSymbolicPosition.EARLIEST, p10, false, "Non-empty partition: EARLIEST VT, end=10, numRecords=10" },
        { PubSubSymbolicPosition.EARLIEST, p100, false, "Large partition: EARLIEST VT, end=100, numRecords=100" },

        // === Path 3: Normal diff calculation cases (diff <= 1) ===
        // Exact match cases (diff = 1, fully consumed)
        { p99, p100, true, "Exact match: VT=99, end=100, diff=1 (fully consumed)" },
        { p0, p1, true, "Single message consumed: VT=0, end=1, diff=1" },
        { p49, p50, true, "Mid-range exact match: VT=49, end=50, diff=1" },

        // Equal position cases (diff = 0, over-consumed but still considered fully consumed)
        { p50, p50, true, "Equal positions: VT=50, end=50, diff=0 (over-consumed)" },
        { p100, p100, true, "Large equal positions: VT=100, end=100, diff=0" },

        // Under-consumed cases (diff > 1, not fully consumed)
        { p50, p100, false, "Under-consumed: VT=50, end=100, diff=50" },
        { p0, p10, false, "Far behind: VT=0, end=10, diff=10" },
        { p5, p20, false, "Multiple messages behind: VT=5, end=20, diff=15" },
        { p1, p1000, false, "Very far behind: VT=1, end=1000, diff=999" },

        // Over-consumed cases (negative diff, still considered fully consumed)
        // Ideally, this shouldn't happen
        { p51, p50, true, "Over-consumed by 1: VT=51, end=50, diff=-1" },
        { p100, p50, true, "Over-consumed by many: VT=100, end=50, diff=-50" },
        { p20, p10, true, "Over-consumed mid-range: VT=20, end=10, diff=-10" },

        // Boundary cases around diff = 1
        { p98, p100, false, "Boundary: VT=98, end=100, diff=2 (not fully consumed)" },
        { p1, p2, true, "Small boundary: VT=1, end=2, diff=1 (fully consumed)" },
        { p0, p2, false, "Small boundary: VT=0, end=2, diff=2 (not fully consumed)" },

        // Edge cases with position 0
        { p0, p0, true, "Both zero: VT=0, end=0, diff=0" },
        { p1, p0, true, "VT ahead of zero end: VT=1, end=0, diff=-1" }, };
  }

  @Test(timeOut = 60_000, dataProvider = "isVtFullyConsumedCases")
  public void testIsLocalVersionTopicPartitionFullyConsumed(
      PubSubPosition vtPosition,
      PubSubPosition endPosition,
      boolean expected,
      String msg) throws InterruptedException {

    setUp();
    TopicManager mockLocalTopicManager = mock(TopicManager.class);

    doAnswer(inv -> {
      ApacheKafkaOffsetPosition end = inv.getArgument(1);
      return end.getInternalOffset();
    }).when(mockLocalTopicManager).countRecordsUntil(any(), any());

    doAnswer(inv -> {
      ApacheKafkaOffsetPosition a = inv.getArgument(1);
      ApacheKafkaOffsetPosition b = inv.getArgument(2);
      return a.getInternalOffset() - b.getInternalOffset();
    }).when(mockLocalTopicManager).diffPosition(any(), any(), any());

    when(mockPartitionConsumptionState.getLatestProcessedVtPosition()).thenReturn(vtPosition);
    when(mockPartitionConsumptionState.getPartition()).thenReturn(0);
    when(mockPartitionConsumptionState.getReplicaTopicPartition())
        .thenReturn(new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_v1"), 0));

    doReturn(mockLocalTopicManager).when(leaderFollowerStoreIngestionTask).getTopicManager(anyString());
    doReturn(endPosition).when(leaderFollowerStoreIngestionTask).getTopicPartitionEndPosition(anyString(), any());
    boolean actual =
        leaderFollowerStoreIngestionTask.isLocalVersionTopicPartitionFullyConsumed(mockPartitionConsumptionState);
    assertEquals(actual, expected, msg);

    if (PubSubSymbolicPosition.EARLIEST.equals(vtPosition) && PubSubSymbolicPosition.LATEST.equals(endPosition)) {
      verify(mockLocalTopicManager, never()).countRecordsUntil(any(), any());
      verify(mockLocalTopicManager, never()).diffPosition(any(), any(), any());
      return;
    }

    if (PubSubSymbolicPosition.EARLIEST.equals(vtPosition)) {
      verify(mockLocalTopicManager, times(1)).countRecordsUntil(any(), eq(endPosition));
      verify(mockLocalTopicManager, never()).diffPosition(any(), any(), any());
    } else if (!PubSubSymbolicPosition.LATEST.equals(endPosition)) {
      verify(mockLocalTopicManager, times(1)).diffPosition(any(), eq(endPosition), eq(vtPosition));
      verify(mockLocalTopicManager, never()).countRecordsUntil(any(), any());
    }
  }

  @Test(timeOut = 60_000)
  public void testIsLocalVersionTopicPartitionFullyConsumedWhenTopicDoesNotExist() throws InterruptedException {
    setUp();
    TopicManager mockLocalTopicManager = mock(TopicManager.class);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(100L);

    // Case 1: diffPosition throws PubSubTopicDoesNotExistException -> should return false
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(mockLocalTopicManager)
        .diffPosition(any(), any(), any());
    when(mockPartitionConsumptionState.getLatestProcessedVtPosition()).thenReturn(ApacheKafkaOffsetPosition.of(99L));
    when(mockPartitionConsumptionState.getPartition()).thenReturn(0);
    when(mockPartitionConsumptionState.getReplicaTopicPartition())
        .thenReturn(new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_v1"), 0));
    doReturn(mockLocalTopicManager).when(leaderFollowerStoreIngestionTask).getTopicManager(anyString());
    doReturn(endPosition).when(leaderFollowerStoreIngestionTask).getTopicPartitionEndPosition(anyString(), any());

    assertFalse(
        leaderFollowerStoreIngestionTask.isLocalVersionTopicPartitionFullyConsumed(mockPartitionConsumptionState),
        "Should return false when diffPosition throws PubSubTopicDoesNotExistException");

    // Case 2: countRecordsUntil throws PubSubTopicDoesNotExistException (EARLIEST path) -> should return false
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(mockLocalTopicManager)
        .countRecordsUntil(any(), any());
    when(mockPartitionConsumptionState.getLatestProcessedVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);

    assertFalse(
        leaderFollowerStoreIngestionTask.isLocalVersionTopicPartitionFullyConsumed(mockPartitionConsumptionState),
        "Should return false when countRecordsUntil throws PubSubTopicDoesNotExistException");
  }

  @Test(timeOut = 60_000)
  public void testCheckAndHandleUpstreamOffsetRewind() throws InterruptedException {
    setUp();
    TopicManager mockTopicManager = mock(TopicManager.class);
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("test-topic_rt"), 0);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    PubSubPosition newPosition = ApacheKafkaOffsetPosition.of(50L);
    PubSubPosition prevPosition = ApacheKafkaOffsetPosition.of(100L); // rewind scenario

    // Case 1: EARLIEST previous position -> skip diffPosition
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        tp,
        pcs,
        record,
        newPosition,
        PubSubSymbolicPosition.EARLIEST,
        leaderFollowerStoreIngestionTask);
    verify(mockTopicManager, never()).diffPosition(any(), any(), any());

    // Case 2: versionBootstrapCompleted=true and EOP not received -> skip diffPosition
    doReturn(false).when(pcs).isEndOfPushReceived();
    leaderFollowerStoreIngestionTask.versionBootstrapCompleted = true;
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        tp,
        pcs,
        record,
        newPosition,
        prevPosition,
        leaderFollowerStoreIngestionTask);
    verify(mockTopicManager, never()).diffPosition(any(), any(), any());

    // Case 3: versionBootstrapCompleted=true and EOP received -> should call diffPosition
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(-50L).when(mockTopicManager).diffPosition(any(), any(), any());
    doReturn(false).when(leaderFollowerStoreIngestionTask).isHybridMode();
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        tp,
        pcs,
        record,
        newPosition,
        prevPosition,
        leaderFollowerStoreIngestionTask);
    verify(mockTopicManager, times(1)).diffPosition(any(), any(), any());

    // Case 4: diffPosition throws PubSubTopicDoesNotExistException -> should not throw
    leaderFollowerStoreIngestionTask.versionBootstrapCompleted = false;
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(mockTopicManager)
        .diffPosition(any(), any(), any());
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        tp,
        pcs,
        record,
        newPosition,
        prevPosition,
        leaderFollowerStoreIngestionTask);
  }

  @Test(timeOut = 60_000)
  public void testSyncConsumedUpstreamRTOffsetMapWhenTopicDoesNotExist() throws InterruptedException {
    setUp();
    TopicManager throwingTopicManager = mock(TopicManager.class);
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(throwingTopicManager)
        .diffPosition(any(), any(), any());
    doReturn(throwingTopicManager).when(leaderFollowerStoreIngestionTask).getTopicManager(anyString());

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("test-topic_rt");
    doReturn(rtTopic).when(mockOffsetRecord).getLeaderTopic(any());
    doReturn(mockOffsetRecord).when(mockPartitionConsumptionState).getOffsetRecord();
    PubSubTopicPartition leaderTopicPartition = new PubSubTopicPartitionImpl(rtTopic, 0);
    doReturn(leaderTopicPartition).when(mockPartitionConsumptionState).getSourceTopicPartition(rtTopic);

    // Set latest consumed position to non-EARLIEST so the diffPosition path is hit
    doReturn(ApacheKafkaOffsetPosition.of(5L)).when(leaderFollowerStoreIngestionTask)
        .getLatestConsumedUpstreamPositionForHybridOffsetLagMeasurement(any(), anyString());

    Map<String, PubSubPosition> upstreamStartPositionByUrl = new HashMap<>();
    upstreamStartPositionByUrl.put("kafka-url", ApacheKafkaOffsetPosition.of(10L));

    // Should not throw - exception is caught and position update is skipped
    leaderFollowerStoreIngestionTask
        .syncConsumedUpstreamRTOffsetMapIfNeeded(mockPartitionConsumptionState, upstreamStartPositionByUrl);

    // Verify that updateLatestConsumedRtPositions was NOT called since the exception was caught
    verify(leaderFollowerStoreIngestionTask, never()).updateLatestConsumedRtPositions(any(), anyString(), any());
  }

  @Test(timeOut = 60_000)
  public void testShouldProcessRecordWhenTopicDoesNotExist() throws InterruptedException {
    setUp();
    // Setup for the default (STANDBY/follower) branch
    doReturn(LeaderFollowerStateType.STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();

    PubSubTopic versionTopic = leaderFollowerStoreIngestionTask.getVersionTopic();
    PubSubTopicPartition vtp = new PubSubTopicPartitionImpl(versionTopic, 0);

    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    doReturn(0).when(record).getPartition();
    doReturn(vtp).when(record).getTopicPartition();
    KafkaKey kafkaKey = mock(KafkaKey.class);
    doReturn(false).when(kafkaKey).isControlMessage();
    doReturn(kafkaKey).when(record).getKey();
    doReturn(ApacheKafkaOffsetPosition.of(5L)).when(record).getPosition();

    // Set lastProcessedVtPos to non-EARLIEST so the diffPosition path is hit
    doReturn(ApacheKafkaOffsetPosition.of(3L)).when(mockPartitionConsumptionState).getLatestProcessedVtPosition();

    // Make diffPosition throw PubSubTopicDoesNotExistException
    TopicManager throwingTopicManager = mock(TopicManager.class);
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(throwingTopicManager)
        .diffPosition(any(), any(), any());
    doReturn(throwingTopicManager).when(mockTopicManagerRepository).getLocalTopicManager();

    // Should process the record (not skip it) when topic doesn't exist
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).shouldProcessRecord(record);
    boolean result = leaderFollowerStoreIngestionTask.shouldProcessRecord(record);
    assertTrue(result, "Should process the record when diffPosition throws PubSubTopicDoesNotExistException");
  }

  @Test(timeOut = 60_000)
  public void testMeasureRtLagForSingleRegionWhenTopicDoesNotExist() throws InterruptedException {
    setUp();
    TopicManager throwingTopicManager = mock(TopicManager.class);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(100L);

    doReturn(throwingTopicManager).when(leaderFollowerStoreIngestionTask).getTopicManager(anyString());
    doReturn(endPosition).when(leaderFollowerStoreIngestionTask).getTopicPartitionEndPosition(anyString(), any());

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("test-topic_rt");
    doReturn(rtTopic).when(mockOffsetRecord).getLeaderTopic(any());
    doReturn(mockOffsetRecord).when(mockPartitionConsumptionState).getOffsetRecord();
    doReturn(0).when(mockPartitionConsumptionState).getPartition();
    doReturn("replica-0").when(mockPartitionConsumptionState).getReplicaId();

    doReturn(rtTopic).when(leaderFollowerStoreIngestionTask).resolveRtTopicWithPubSubBrokerAddress(any(), anyString());

    // Case 1: diffPosition throws exception (non-EARLIEST path)
    doReturn(ApacheKafkaOffsetPosition.of(50L)).when(leaderFollowerStoreIngestionTask)
        .getLatestPersistedRtPositionForLagMeasurement(any(), anyString());
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(throwingTopicManager)
        .diffPosition(any(), any(), any());

    long lag =
        leaderFollowerStoreIngestionTask.measureRtLagForSingleRegion("kafka-url", mockPartitionConsumptionState, false);
    assertEquals(
        lag,
        Long.MAX_VALUE,
        "Should return Long.MAX_VALUE when diffPosition throws PubSubTopicDoesNotExistException");

    // Case 2: countRecordsUntil throws exception (EARLIEST path)
    doReturn(PubSubSymbolicPosition.EARLIEST).when(leaderFollowerStoreIngestionTask)
        .getLatestPersistedRtPositionForLagMeasurement(any(), anyString());
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(throwingTopicManager)
        .countRecordsUntil(any(), any());

    lag =
        leaderFollowerStoreIngestionTask.measureRtLagForSingleRegion("kafka-url", mockPartitionConsumptionState, false);
    assertEquals(
        lag,
        Long.MAX_VALUE,
        "Should return Long.MAX_VALUE when countRecordsUntil throws PubSubTopicDoesNotExistException");
  }

  @Test(timeOut = 60_000)
  public void testExtractUpstreamClusterId() throws InterruptedException {
    setUp();

    // Case 1: Normal case with valid LeaderMetadata and upstreamKafkaClusterId
    DefaultPubSubMessage consumerRecord1 = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope envelope1 = mock(KafkaMessageEnvelope.class);
    LeaderMetadata leaderMetadata1 = new LeaderMetadata();
    leaderMetadata1.upstreamKafkaClusterId = 42;
    envelope1.leaderMetadataFooter = leaderMetadata1;
    when(consumerRecord1.getValue()).thenReturn(envelope1);
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(consumerRecord1);
    int clusterId1 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(consumerRecord1);
    assertEquals(clusterId1, 42, "Should extract correct upstream cluster ID");

    // Case 2: Null consumerRecord
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(null);
    int clusterId2 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(null);
    assertEquals(clusterId2, -1, "Should return -1 for null consumerRecord");

    // Case 3: ConsumerRecord with null value (envelope)
    DefaultPubSubMessage consumerRecord3 = mock(DefaultPubSubMessage.class);
    when(consumerRecord3.getValue()).thenReturn(null);
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(consumerRecord3);
    int clusterId3 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(consumerRecord3);
    assertEquals(clusterId3, -1, "Should return -1 for null envelope");

    // Case 4: Envelope with null leaderMetadataFooter
    DefaultPubSubMessage consumerRecord4 = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope envelope4 = mock(KafkaMessageEnvelope.class);
    envelope4.leaderMetadataFooter = null;
    when(consumerRecord4.getValue()).thenReturn(envelope4);
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(consumerRecord4);
    int clusterId4 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(consumerRecord4);
    assertEquals(clusterId4, -1, "Should return -1 for null leaderMetadataFooter");

    // Case 5: Zero upstream cluster ID (valid edge case)
    DefaultPubSubMessage consumerRecord5 = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope envelope5 = mock(KafkaMessageEnvelope.class);
    LeaderMetadata leaderMetadata5 = new LeaderMetadata();
    leaderMetadata5.upstreamKafkaClusterId = 0;
    envelope5.leaderMetadataFooter = leaderMetadata5;
    when(consumerRecord5.getValue()).thenReturn(envelope5);
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(consumerRecord5);
    int clusterId5 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(consumerRecord5);
    assertEquals(clusterId5, 0, "Should extract zero cluster ID correctly");

    // Case 6: Negative upstream cluster ID (edge case)
    DefaultPubSubMessage consumerRecord6 = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope envelope6 = mock(KafkaMessageEnvelope.class);
    LeaderMetadata leaderMetadata6 = new LeaderMetadata();
    leaderMetadata6.upstreamKafkaClusterId = -5;
    envelope6.leaderMetadataFooter = leaderMetadata6;
    when(consumerRecord6.getValue()).thenReturn(envelope6);
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(consumerRecord6);
    int clusterId6 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(consumerRecord6);
    assertEquals(clusterId6, -5, "Should extract negative cluster ID correctly");

    // Case 7: Large upstream cluster ID value
    DefaultPubSubMessage consumerRecord7 = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope envelope7 = mock(KafkaMessageEnvelope.class);
    LeaderMetadata leaderMetadata7 = new LeaderMetadata();
    leaderMetadata7.upstreamKafkaClusterId = Integer.MAX_VALUE;
    envelope7.leaderMetadataFooter = leaderMetadata7;
    when(consumerRecord7.getValue()).thenReturn(envelope7);
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).extractUpstreamClusterId(consumerRecord7);
    int clusterId7 = leaderFollowerStoreIngestionTask.extractUpstreamClusterId(consumerRecord7);
    assertEquals(clusterId7, Integer.MAX_VALUE, "Should extract large cluster ID correctly");
  }

  @Test
  public void testHeartbeatRecord() {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);

    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doReturn("foo").when(ingestionTask).getStoreName();
    doReturn(1).when(ingestionTask).getVersionNumber();
    doCallRealMethod().when(ingestionTask).recordHeartbeatReceived(any(), any(), anyString());

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(100).when(pcs).getPartition();
    doReturn(false).when(pcs).isComplete();

    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.messageTimestamp = 123L;
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    doReturn(kafkaMessageEnvelope).when(consumerRecord).getValue();

    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    Map<String, String> urlMap = Collections.singletonMap("abc:123", "c1");
    doReturn(urlMap).when(veniceServerConfig).getKafkaClusterUrlToAliasMap();
    doReturn(veniceServerConfig).when(ingestionTask).getServerConfig();

    // Set up PCS cached heartbeat keys
    HeartbeatKey leaderKey = new HeartbeatKey("foo", 1, 100, "c1");
    doReturn(leaderKey).when(pcs).getOrCreateCachedHeartbeatKey("c1");
    HeartbeatKey followerLocalKey = new HeartbeatKey("foo", 1, 100, "local");
    doReturn(followerLocalKey).when(pcs).getOrCreateCachedHeartbeatKey("local");

    // Monitoring service is null.
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, never()).recordLeaderHeartbeat(any(HeartbeatKey.class), anyLong(), anyBoolean());
    verify(heartbeatMonitoringService, never())
        .recordFollowerHeartbeat(any(HeartbeatKey.class), anyLong(), anyBoolean());

    // Verify Leader
    doReturn(heartbeatMonitoringService).when(ingestionTask).getHeartbeatMonitoringService();
    doReturn(LeaderFollowerStateType.LEADER).when(pcs).getLeaderFollowerState();
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1)).recordLeaderHeartbeat(eq(leaderKey), eq(123L), eq(false));
    verify(heartbeatMonitoringService, never())
        .recordFollowerHeartbeat(any(HeartbeatKey.class), anyLong(), anyBoolean());
    // Verify Follower
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1)).recordFollowerHeartbeat(eq(leaderKey), eq(123L), eq(false));
    // Verify Da Vinci
    doReturn(true).when(ingestionTask).isDaVinciClient();
    doReturn("local").when(veniceServerConfig).getRegionName();
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1)).recordFollowerHeartbeat(eq(followerLocalKey), eq(123L), eq(false));
  }

  @Test
  public void testIngestionTimeoutHandling() throws Exception {
    LeaderFollowerStoreIngestionTask storeIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);

    // Set up fields accessed directly by the real checkLongRunningTaskState method
    AggVersionedIngestionStats mockVersionedIngestionStats = mock(AggVersionedIngestionStats.class);
    HostLevelIngestionStats mockHostLevelStats = mock(HostLevelIngestionStats.class);
    AtomicBoolean emitTehutiMetrics = new AtomicBoolean(false);
    setField(storeIngestionTask, "versionedIngestionStats", mockVersionedIngestionStats);
    setField(storeIngestionTask, "hostLevelIngestionStats", mockHostLevelStats);
    setField(storeIngestionTask, "emitTehutiMetrics", emitTehutiMetrics);
    setField(storeIngestionTask, "storeName", "foo");

    doReturn("foo").when(storeIngestionTask).getStoreName();
    doReturn(Lazy.of(() -> mock(VeniceWriter.class))).when(storeIngestionTask).getVeniceWriter();
    doReturn(Lazy.of(() -> mock(VeniceWriter.class))).when(storeIngestionTask).getVeniceWriterForRealTime();
    doCallRealMethod().when(storeIngestionTask).isEmitTehutiMetricsEnabled();
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(storeRepository).when(storeIngestionTask).getStoreRepository();
    Store store = mock(Store.class);
    doReturn(5).when(store).getCurrentVersion();
    doReturn(store).when(storeRepository).getStoreOrThrow(anyString());

    doReturn(TimeUnit.DAYS.toMillis(1)).when(storeIngestionTask).getBootstrapTimeoutInMs();
    PubSubTopic topic = new PubSubTopicImpl("foo_v1");
    doReturn(topic).when(storeIngestionTask).getVersionTopic();
    doCallRealMethod().when(storeIngestionTask).checkLongRunningTaskState();
    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    doReturn(pcsMap).when(storeIngestionTask).getPartitionConsumptionStateMap();

    addStandbyPcs(pcsMap, 1, TimeUnit.DAYS.toMillis(2)); // timed out (2 days > 1 day timeout)
    addStandbyPcs(pcsMap, 2, TimeUnit.HOURS.toMillis(2)); // not timed out (2 hours < 1 day timeout)
    addStandbyPcs(pcsMap, 3, TimeUnit.DAYS.toMillis(2)); // timed out (2 days > 1 day timeout)

    // Future version (v10 > currentVersion=5): should throw
    setVersion(storeIngestionTask, 10);
    Assert.assertThrows(VeniceTimeoutException.class, storeIngestionTask::checkLongRunningTaskState);
    // OTel check-time is recorded before the throw
    verify(mockVersionedIngestionStats, times(1)).recordLongRunningTaskCheckTime(eq("foo"), eq(10), anyDouble());
    // Future version throws, so ingestion failure is not recorded
    verify(mockVersionedIngestionStats, never()).recordIngestionFailureCount(anyString(), anyInt(), any());
    // emitTehutiMetrics=false, so Tehuti calls should not fire
    verify(mockHostLevelStats, never()).recordCheckLongRunningTasksLatency(anyDouble());
    verify(mockHostLevelStats, never()).recordIngestionFailure();

    // Current version (v5 <= currentVersion=5): reports errors per partition, does not throw
    setVersion(storeIngestionTask, 5);
    storeIngestionTask.checkLongRunningTaskState();
    verify(storeIngestionTask, times(1)).reportError(anyString(), eq(1), any());
    verify(storeIngestionTask, times(0)).reportError(anyString(), eq(2), any());
    verify(storeIngestionTask, times(1)).reportError(anyString(), eq(3), any());
    // OTel: check-time for v5 + ingestion failure
    verify(mockVersionedIngestionStats, times(1)).recordLongRunningTaskCheckTime(eq("foo"), eq(5), anyDouble());
    verify(mockVersionedIngestionStats, times(1)).recordIngestionFailureCount(eq("foo"), eq(5), any());
    // emitTehutiMetrics still false: no Tehuti
    verify(mockHostLevelStats, never()).recordCheckLongRunningTasksLatency(anyDouble());
    verify(mockHostLevelStats, never()).recordIngestionFailure();

    // Backup version (v1 <= currentVersion=5): same behavior as current
    setVersion(storeIngestionTask, 1);
    storeIngestionTask.checkLongRunningTaskState();
    verify(storeIngestionTask, times(2)).reportError(anyString(), eq(1), any());
    verify(storeIngestionTask, times(0)).reportError(anyString(), eq(2), any());
    verify(storeIngestionTask, times(2)).reportError(anyString(), eq(3), any());
    // OTel: check-time for v1 + ingestion failure
    verify(mockVersionedIngestionStats, times(1)).recordLongRunningTaskCheckTime(eq("foo"), eq(1), anyDouble());
    verify(mockVersionedIngestionStats, times(1)).recordIngestionFailureCount(eq("foo"), eq(1), any());

    // Now enable Tehuti emission and call again for current version to verify Tehuti gating
    emitTehutiMetrics.set(true);
    setVersion(storeIngestionTask, 5);
    storeIngestionTask.checkLongRunningTaskState();
    verify(mockVersionedIngestionStats, times(2)).recordLongRunningTaskCheckTime(eq("foo"), eq(5), anyDouble());
    verify(mockVersionedIngestionStats, times(2)).recordIngestionFailureCount(eq("foo"), eq(5), any());
    // Tehuti: now fires because emitTehutiMetrics=true
    verify(mockHostLevelStats, times(1)).recordCheckLongRunningTasksLatency(anyDouble());
    verify(mockHostLevelStats, times(1)).recordIngestionFailure();
  }

  @Test
  public void testStopTrackingCurrentVersionIngestionOnDemotion() throws Exception {
    LeaderFollowerStoreIngestionTask storeIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(serverConfig).isResubscriptionTriggeredByVersionIngestionContextChangeEnabled();
    doReturn(0).when(serverConfig).getResubscriptionCheckIntervalInSeconds();
    setField(storeIngestionTask, "serverConfig", serverConfig);
    setField(storeIngestionTask, "versionRole", VersionRole.CURRENT);
    setVersion(storeIngestionTask, 1);
    doReturn(true).when(storeIngestionTask).isHybridMode();
    doCallRealMethod().when(storeIngestionTask).refreshIngestionContextIfChanged(any(Store.class));

    Store store = mock(Store.class);
    doReturn(5).when(store).getCurrentVersion();

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs).isLatchCreated();
    doReturn(false).when(pcs).isLatchReleased();
    VeniceConcurrentHashMap<Integer, PartitionConsumptionState> pcsMap = new VeniceConcurrentHashMap<>();
    pcsMap.put(1, pcs);
    setField(storeIngestionTask, "partitionConsumptionStateMap", pcsMap);

    IngestionNotificationDispatcher mockDispatcher = mock(IngestionNotificationDispatcher.class);
    setField(storeIngestionTask, "ingestionNotificationDispatcher", mockDispatcher);

    // First call: versionRole transitions CURRENT -> BACKUP, reportStopped should be called once
    storeIngestionTask.refreshIngestionContextIfChanged(store);
    verify(mockDispatcher, times(1)).reportStopped(eq(pcs));

    // Second call: versionRole is already BACKUP, no further latch release
    storeIngestionTask.refreshIngestionContextIfChanged(store);
    verify(mockDispatcher, times(1)).reportStopped(eq(pcs));
  }

  private static void addStandbyPcs(Map<Integer, PartitionConsumptionState> pcsMap, int partition, long ageMs) {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    pcsMap.put(partition, pcs);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
    doReturn(false).when(pcs).isComplete();
    doReturn(partition).when(pcs).getPartition();
    doReturn(System.currentTimeMillis() - ageMs).when(pcs).getConsumptionStartTimeInMs();
  }

  /** Sets both the {@code versionNumber} field and the getter stub on a mocked StoreIngestionTask. */
  private static void setVersion(StoreIngestionTask task, int version)
      throws NoSuchFieldException, IllegalAccessException {
    setField(task, "versionNumber", version);
    doReturn(version).when(task).getVersionNumber();
  }

  private static void setField(Object target, String fieldName, Object value)
      throws NoSuchFieldException, IllegalAccessException {
    Class<?> cls = StoreIngestionTask.class;
    while (cls != null) {
      try {
        Field field = cls.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        cls = cls.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName + " not found in " + StoreIngestionTask.class.getName() + " hierarchy");
  }

  /** Holds a mocked task and the common mock/field objects used across gating tests. */
  private static class MockTaskContext {
    final LeaderFollowerStoreIngestionTask task;
    final AggVersionedIngestionStats versionedStats;
    final HostLevelIngestionStats hostLevelStats;
    final AtomicBoolean emitTehutiMetrics;

    MockTaskContext(
        LeaderFollowerStoreIngestionTask task,
        AggVersionedIngestionStats versionedStats,
        HostLevelIngestionStats hostLevelStats,
        AtomicBoolean emitTehutiMetrics) {
      this.task = task;
      this.versionedStats = versionedStats;
      this.hostLevelStats = hostLevelStats;
      this.emitTehutiMetrics = emitTehutiMetrics;
    }
  }

  /**
   * Creates a mocked {@link LeaderFollowerStoreIngestionTask} with the common fields set for
   * OTel/Tehuti gating tests: versionedIngestionStats, hostLevelIngestionStats, emitTehutiMetrics,
   * storeName ("testStore"), and versionNumber (3).
   */
  private static MockTaskContext createMockTaskForGatingTests() throws NoSuchFieldException, IllegalAccessException {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    AggVersionedIngestionStats versionedStats = mock(AggVersionedIngestionStats.class);
    HostLevelIngestionStats hostLevelStats = mock(HostLevelIngestionStats.class);
    AtomicBoolean emitTehutiMetrics = new AtomicBoolean(false);
    setField(task, "versionedIngestionStats", versionedStats);
    setField(task, "hostLevelIngestionStats", hostLevelStats);
    setField(task, "emitTehutiMetrics", emitTehutiMetrics);
    setField(task, "storeName", "testStore");
    setField(task, "versionNumber", 3);
    doCallRealMethod().when(task).isEmitTehutiMetricsEnabled();
    return new MockTaskContext(task, versionedStats, hostLevelStats, emitTehutiMetrics);
  }

  @Test
  public void testRecordMaxIdleTimeGating() throws Exception {
    MockTaskContext ctx = createMockTaskForGatingTests();
    VeniceConcurrentHashMap<Integer, PartitionConsumptionState> pcsMap = new VeniceConcurrentHashMap<>();
    setField(ctx.task, "partitionConsumptionStateMap", pcsMap);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(System.currentTimeMillis() - 5000).when(pcs).getLatestPolledMessageTimestampInMs();
    pcsMap.put(0, pcs);

    Method recordMaxIdleTime = StoreIngestionTask.class.getDeclaredMethod("recordMaxIdleTime");
    recordMaxIdleTime.setAccessible(true);

    // emitTehutiMetrics=false: OTel fires, Tehuti does not
    recordMaxIdleTime.invoke(ctx.task);
    verify(ctx.versionedStats, times(1)).recordMaxIdleTime(eq("testStore"), eq(3), anyLong(), eq(false));

    // emitTehutiMetrics=true: both fire (emitTehuti=true passed to single method)
    ctx.emitTehutiMetrics.set(true);
    recordMaxIdleTime.invoke(ctx.task);
    verify(ctx.versionedStats, times(1)).recordMaxIdleTime(eq("testStore"), eq(3), anyLong(), eq(true));
  }

  @Test
  public void testProcessConsumerActionsGating() throws Exception {
    MockTaskContext ctx = createMockTaskForGatingTests();
    PriorityBlockingQueue<ConsumerAction> emptyQueue = new PriorityBlockingQueue<>();
    setField(ctx.task, "consumerActionsQueue", emptyQueue);

    doCallRealMethod().when(ctx.task).processConsumerActions(any(Store.class));

    // emitTehutiMetrics=false: OTel fires, Tehuti does not
    ctx.task.processConsumerActions(mock(Store.class));
    verify(ctx.versionedStats, times(1)).recordConsumerActionTime(eq("testStore"), eq(3), anyDouble());
    verify(ctx.hostLevelStats, never()).recordProcessConsumerActionLatency(anyDouble());

    // emitTehutiMetrics=true: both fire
    ctx.emitTehutiMetrics.set(true);
    ctx.task.processConsumerActions(mock(Store.class));
    verify(ctx.versionedStats, times(2)).recordConsumerActionTime(eq("testStore"), eq(3), anyDouble());
    verify(ctx.hostLevelStats, times(1)).recordProcessConsumerActionLatency(anyDouble());
  }

  @Test
  public void testShouldProcessRecordUnexpectedMessageGating() throws Exception {
    MockTaskContext ctx = createMockTaskForGatingTests();
    VeniceConcurrentHashMap<Integer, PartitionConsumptionState> pcsMap = new VeniceConcurrentHashMap<>();
    setField(ctx.task, "partitionConsumptionStateMap", pcsMap);
    setField(ctx.task, "kafkaVersionTopic", "testStore_v3");

    // Set up versionTopic field and pub sub topic repository for the LF shouldProcessRecord override
    PubSubTopic versionTopic = new PubSubTopicImpl("testStore_v3");
    setField(ctx.task, "versionTopic", versionTopic);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    setField(ctx.task, "pubSubTopicRepository", pubSubTopicRepository);

    // Set up a batch-only STANDBY partition that has received EOP
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(pcs).isErrorReported();
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(true).when(pcs).isBatchOnly();
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
    doReturn(PubSubSymbolicPosition.EARLIEST).when(pcs).getLatestProcessedVtPosition();
    pcsMap.put(0, pcs);

    // Create a non-control-message record for partition 0 (same topic as versionTopic)
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(versionTopic, 0);
    KafkaKey dataKey = new KafkaKey(MessageType.PUT, new byte[] { 1, 2, 3 });
    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.payloadUnion = new Put();
    DefaultPubSubMessage record = new ImmutablePubSubMessage(dataKey, envelope, topicPartition, null, 0, 100);

    doCallRealMethod().when(ctx.task).shouldProcessRecord(any(DefaultPubSubMessage.class));

    // emitTehutiMetrics=false: OTel fires, Tehuti does not
    ctx.task.shouldProcessRecord(record);
    verify(ctx.versionedStats, times(1)).recordUnexpectedMessageCount(eq("testStore"), eq(3));
    verify(ctx.hostLevelStats, never()).recordUnexpectedMessage();

    // emitTehutiMetrics=true: both fire
    ctx.emitTehutiMetrics.set(true);
    ctx.task.shouldProcessRecord(record);
    verify(ctx.versionedStats, times(2)).recordUnexpectedMessageCount(eq("testStore"), eq(3));
    verify(ctx.hostLevelStats, times(1)).recordUnexpectedMessage();
  }

  @Test
  public void testRecordAssembledRecordSizeGating() throws Exception {
    MockTaskContext ctx = createMockTaskForGatingTests();
    setField(ctx.task, "isRmdChunked", false);

    // Use a real serializer and create a valid ChunkedValueManifest
    ChunkedValueManifestSerializer serializer = new ChunkedValueManifestSerializer(true);
    setField(ctx.task, "manifestSerializer", serializer);

    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>();
    manifest.schemaId = 1;
    manifest.size = 500;
    ByteBuffer valueBytes = serializer.serialize(manifest);

    doCallRealMethod().when(ctx.task).recordAssembledRecordSize(anyInt(), any(ByteBuffer.class), any(), anyLong());
    // Enable the ratio path: stub getMaxRecordSizeBytes to a positive limit so ratio > 0
    doCallRealMethod().when(ctx.task).calculateAssembledRecordSizeRatio(anyLong());
    doCallRealMethod().when(ctx.task).recordAssembledRecordSizeRatio(anyDouble(), anyLong());
    doReturn(1000).when(ctx.task).getMaxRecordSizeBytes();

    long currentTimeMs = System.currentTimeMillis();
    // With recordSize=600 (100 + manifest.size) and maxRecordSizeBytes=1000, ratio = 0.6
    double expectedRatio = 600.0 / 1000;

    // emitTehutiMetrics=false: OTel fires, Tehuti does not
    ctx.task.recordAssembledRecordSize(100, valueBytes.duplicate(), null, currentTimeMs);
    verify(ctx.versionedStats, times(1))
        .recordAssembledSize(eq("testStore"), eq(3), eq(VeniceRecordType.DATA), eq(600L));
    verify(ctx.hostLevelStats, never()).recordAssembledRecordSize(anyLong(), anyLong());
    // Ratio: OTel fires, Tehuti does not
    verify(ctx.versionedStats, times(1)).recordAssembledSizeRatio(eq("testStore"), eq(3), eq(expectedRatio));
    verify(ctx.hostLevelStats, never()).recordAssembledRecordSizeRatio(anyDouble(), anyLong());

    // emitTehutiMetrics=true: both fire
    ctx.emitTehutiMetrics.set(true);
    ctx.task.recordAssembledRecordSize(100, valueBytes.duplicate(), null, currentTimeMs);
    verify(ctx.versionedStats, times(2))
        .recordAssembledSize(eq("testStore"), eq(3), eq(VeniceRecordType.DATA), eq(600L));
    verify(ctx.hostLevelStats, times(1)).recordAssembledRecordSize(eq(600L), eq(currentTimeMs));
    // Ratio: both fire
    verify(ctx.versionedStats, times(2)).recordAssembledSizeRatio(eq("testStore"), eq(3), eq(expectedRatio));
    verify(ctx.hostLevelStats, times(1)).recordAssembledRecordSizeRatio(eq(expectedRatio), eq(currentTimeMs));
  }

  @Test
  public void testRecordAssembledRmdSizeGating() throws Exception {
    MockTaskContext ctx = createMockTaskForGatingTests();
    setField(ctx.task, "isRmdChunked", false);

    ChunkedValueManifestSerializer serializer = new ChunkedValueManifestSerializer(true);
    setField(ctx.task, "manifestSerializer", serializer);

    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>();
    manifest.schemaId = 1;
    manifest.size = 500;
    ByteBuffer valueBytes = serializer.serialize(manifest);

    // Non-chunked RMD: rmdSize = rmdBytes.remaining() (no deserialization)
    ByteBuffer rmdBytes = ByteBuffer.wrap(new byte[150]);

    doCallRealMethod().when(ctx.task).recordAssembledRecordSize(anyInt(), any(ByteBuffer.class), any(), anyLong());
    doCallRealMethod().when(ctx.task).calculateAssembledRecordSizeRatio(anyLong());
    doCallRealMethod().when(ctx.task).recordAssembledRecordSizeRatio(anyDouble(), anyLong());
    doReturn(1000).when(ctx.task).getMaxRecordSizeBytes();

    long currentTimeMs = System.currentTimeMillis();

    // emitTehutiMetrics=false: OTel fires for both DATA and RMD, Tehuti does not
    ctx.task.recordAssembledRecordSize(100, valueBytes.duplicate(), rmdBytes.duplicate(), currentTimeMs);
    verify(ctx.versionedStats, times(1))
        .recordAssembledSize(eq("testStore"), eq(3), eq(VeniceRecordType.REPLICATION_METADATA), eq(150L));
    verify(ctx.hostLevelStats, never()).recordAssembledRmdSize(anyLong(), anyLong());

    // emitTehutiMetrics=true: both OTel and Tehuti fire for RMD
    ctx.emitTehutiMetrics.set(true);
    ctx.task.recordAssembledRecordSize(100, valueBytes.duplicate(), rmdBytes.duplicate(), currentTimeMs);
    verify(ctx.versionedStats, times(2))
        .recordAssembledSize(eq("testStore"), eq(3), eq(VeniceRecordType.REPLICATION_METADATA), eq(150L));
    verify(ctx.hostLevelStats, times(1)).recordAssembledRmdSize(eq(150L), eq(currentTimeMs));
  }

  @Test
  public void testDolStampProduceCallbackSuccess() {
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    long leadershipTerm = 42L;
    DolStamp dolStamp = new DolStamp(leadershipTerm, "test-host");
    doReturn(dolStamp).when(mockPcs).getDolState();
    doReturn("test-replica-id").when(mockPcs).getReplicaId();

    // Verify DolStamp is not marked as produced before callback
    assertFalse(dolStamp.isDolProduced());

    // Create callback and invoke onCompletion with success
    PubSubProducerCallback callback =
        new LeaderFollowerStoreIngestionTask.DolStampProduceCallback(mockPcs, leadershipTerm);
    PubSubProduceResult mockResult = mock(PubSubProduceResult.class);
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    doReturn(mockPosition).when(mockResult).getPubSubPosition();

    callback.onCompletion(mockResult, null);

    // Verify DolStamp is marked as produced
    assertTrue(dolStamp.isDolProduced());
    // Verify clearDolState was NOT called
    verify(mockPcs, never()).clearDolState();
  }

  @Test
  public void testDolStampProduceCallbackFailure() {
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    long leadershipTerm = 42L;
    DolStamp dolStamp = new DolStamp(leadershipTerm, "test-host");
    doReturn(dolStamp).when(mockPcs).getDolState();
    doReturn("test-replica-id").when(mockPcs).getReplicaId();

    // Create callback and invoke onCompletion with failure
    PubSubProducerCallback callback =
        new LeaderFollowerStoreIngestionTask.DolStampProduceCallback(mockPcs, leadershipTerm);

    callback.onCompletion(null, new RuntimeException("Test exception"));

    // Verify DolStamp is NOT marked as produced
    assertFalse(dolStamp.isDolProduced());
    // Verify clearDolState WAS called to fall back to legacy mechanism
    verify(mockPcs, times(1)).clearDolState();
  }

  @Test
  public void testDolStampProduceCallbackTermMismatch() {
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    long callbackTerm = 42L;
    long dolStateTerm = 100L; // Different term
    DolStamp dolStamp = new DolStamp(dolStateTerm, "test-host");
    doReturn(dolStamp).when(mockPcs).getDolState();
    doReturn("test-replica-id").when(mockPcs).getReplicaId();

    // Create callback with different term than DolStamp
    PubSubProducerCallback callback =
        new LeaderFollowerStoreIngestionTask.DolStampProduceCallback(mockPcs, callbackTerm);
    PubSubProduceResult mockResult = mock(PubSubProduceResult.class);
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    doReturn(mockPosition).when(mockResult).getPubSubPosition();

    callback.onCompletion(mockResult, null);

    // Verify DolStamp is NOT marked as produced due to term mismatch
    assertFalse(dolStamp.isDolProduced());
  }

  @Test
  public void testDolStampProduceCallbackNullDolState() {
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    long leadershipTerm = 42L;
    doReturn(null).when(mockPcs).getDolState();
    doReturn("test-replica-id").when(mockPcs).getReplicaId();

    // Create callback and invoke onCompletion - should not throw with null DolState
    PubSubProducerCallback callback =
        new LeaderFollowerStoreIngestionTask.DolStampProduceCallback(mockPcs, leadershipTerm);
    PubSubProduceResult mockResult = mock(PubSubProduceResult.class);
    PubSubPosition mockPosition = mock(PubSubPosition.class);
    doReturn(mockPosition).when(mockResult).getPubSubPosition();

    callback.onCompletion(mockResult, null);

    // No exception thrown, clearDolState not called
    verify(mockPcs, never()).clearDolState();
  }

  @Test
  public void testDolStampIsDolComplete() {
    // Test DolStamp complete state
    DolStamp dolStampComplete = new DolStamp(42L, "test-host");
    assertFalse(dolStampComplete.isDolComplete());

    dolStampComplete.setDolProduced(true);
    assertFalse(dolStampComplete.isDolComplete());

    dolStampComplete.setDolConsumed(true);
    assertTrue(dolStampComplete.isDolComplete());

    // Test incomplete state (only consumed)
    DolStamp dolStampIncomplete = new DolStamp(42L, "test-host");
    dolStampIncomplete.setDolConsumed(true);
    assertFalse(dolStampIncomplete.isDolComplete());
  }

  @Test
  public void testBatchUnsubscribeCollectsLeaderTopics() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).consumerBatchUnsubscribeAllTopics();

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("store_v1");
    PubSubTopic leaderTopic = topicRepo.getTopic("store_rt");

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(leaderTopic).when(offsetRecord).getLeaderTopic(topicRepo);
    doReturn(LeaderFollowerStateType.LEADER).when(pcs).getLeaderFollowerState();
    doReturn(3).when(pcs).getPartition();
    doReturn(null).when(pcs).getVeniceWriterLazyRef();

    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    pcsMap.put(3, pcs);
    doReturn(pcsMap).when(task).getPartitionConsumptionStateMap();
    doReturn(versionTopic).when(task).getVersionTopic();
    doReturn(topicRepo).when(task).getPubSubTopicRepository();

    task.consumerBatchUnsubscribeAllTopics();

    ArgumentCaptor<Set<PubSubTopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(task).consumerBatchUnsubscribe(captor.capture());
    Set<PubSubTopicPartition> captured = captor.getValue();
    assertEquals(captured.size(), 1);
    PubSubTopicPartition tp = captured.iterator().next();
    assertEquals(tp.getPubSubTopic(), leaderTopic);
    assertEquals(tp.getPartitionNumber(), 3);
  }

  @Test
  public void testBatchUnsubscribeIncludesSeparateRtTopic() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).consumerBatchUnsubscribeAllTopics();

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("store_v1");
    PubSubTopic leaderTopic = topicRepo.getTopic("store_rt");
    PubSubTopic separateRtTopic = topicRepo.getTopic("store_rt_separate");

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(leaderTopic).when(offsetRecord).getLeaderTopic(topicRepo);
    doReturn(LeaderFollowerStateType.LEADER).when(pcs).getLeaderFollowerState();
    doReturn(4).when(pcs).getPartition();
    doReturn(null).when(pcs).getVeniceWriterLazyRef();

    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    pcsMap.put(4, pcs);
    doReturn(pcsMap).when(task).getPartitionConsumptionStateMap();
    doReturn(versionTopic).when(task).getVersionTopic();
    doReturn(topicRepo).when(task).getPubSubTopicRepository();
    doReturn(true).when(task).isSeparatedRealtimeTopicEnabled();
    doReturn(separateRtTopic).when(task).getSeparateRealTimeTopic();

    task.consumerBatchUnsubscribeAllTopics();

    ArgumentCaptor<Set<PubSubTopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(task).consumerBatchUnsubscribe(captor.capture());
    Set<PubSubTopicPartition> captured = captor.getValue();
    assertEquals(captured.size(), 2);
    assertTrue(
        captured.contains(new PubSubTopicPartitionImpl(leaderTopic, 4)),
        "Should contain leader topic partition");
    assertTrue(
        captured.contains(new PubSubTopicPartitionImpl(separateRtTopic, 4)),
        "Should contain separate RT topic partition");
  }

  @Test
  public void testBatchUnsubscribeLeaderWithNullLeaderTopicUsesVersionTopic() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).consumerBatchUnsubscribeAllTopics();

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("store_v1");

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(null).when(offsetRecord).getLeaderTopic(topicRepo);
    doReturn(LeaderFollowerStateType.LEADER).when(pcs).getLeaderFollowerState();
    doReturn(2).when(pcs).getPartition();
    doReturn(null).when(pcs).getVeniceWriterLazyRef();

    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    pcsMap.put(2, pcs);
    doReturn(pcsMap).when(task).getPartitionConsumptionStateMap();
    doReturn(versionTopic).when(task).getVersionTopic();
    doReturn(topicRepo).when(task).getPubSubTopicRepository();

    task.consumerBatchUnsubscribeAllTopics();

    ArgumentCaptor<Set<PubSubTopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(task).consumerBatchUnsubscribe(captor.capture());
    Set<PubSubTopicPartition> captured = captor.getValue();
    assertEquals(captured.size(), 1);
    PubSubTopicPartition tp = captured.iterator().next();
    assertEquals(tp.getPubSubTopic(), versionTopic);
    assertEquals(tp.getPartitionNumber(), 2);
  }

  @Test
  public void testBatchUnsubscribeCollectsFollowerTopics() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).consumerBatchUnsubscribeAllTopics();

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("store_v1");

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(null).when(offsetRecord).getLeaderTopic(topicRepo);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
    doReturn(5).when(pcs).getPartition();
    doReturn(null).when(pcs).getVeniceWriterLazyRef();

    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    pcsMap.put(5, pcs);
    doReturn(pcsMap).when(task).getPartitionConsumptionStateMap();
    doReturn(versionTopic).when(task).getVersionTopic();
    doReturn(topicRepo).when(task).getPubSubTopicRepository();

    task.consumerBatchUnsubscribeAllTopics();

    ArgumentCaptor<Set<PubSubTopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(task).consumerBatchUnsubscribe(captor.capture());
    Set<PubSubTopicPartition> captured = captor.getValue();
    assertEquals(captured.size(), 1);
    PubSubTopicPartition tp = captured.iterator().next();
    assertEquals(tp.getPubSubTopic(), versionTopic);
    assertEquals(tp.getPartitionNumber(), 5);
  }

  @Test
  public void testBatchUnsubscribeClosesVeniceWriterPartitions() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).consumerBatchUnsubscribeAllTopics();

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("store_v1");

    VeniceWriter<byte[], byte[], byte[]> veniceWriter = mock(VeniceWriter.class);
    Lazy<VeniceWriter<byte[], byte[], byte[]>> writerLazyRef = Lazy.of(() -> veniceWriter);
    // Force initialization so ifPresent triggers
    writerLazyRef.get();

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(null).when(offsetRecord).getLeaderTopic(topicRepo);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
    doReturn(7).when(pcs).getPartition();
    doReturn(writerLazyRef).when(pcs).getVeniceWriterLazyRef();

    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    pcsMap.put(7, pcs);
    doReturn(pcsMap).when(task).getPartitionConsumptionStateMap();
    doReturn(versionTopic).when(task).getVersionTopic();
    doReturn(topicRepo).when(task).getPubSubTopicRepository();

    task.consumerBatchUnsubscribeAllTopics();

    verify(veniceWriter).closePartition(7);
  }

  @Test
  public void testBatchUnsubscribeNoOpWhenEmpty() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(task).consumerBatchUnsubscribeAllTopics();

    PubSubTopicRepository topicRepo = new PubSubTopicRepository();
    PubSubTopic versionTopic = topicRepo.getTopic("store_v1");

    doReturn(new HashMap<>()).when(task).getPartitionConsumptionStateMap();
    doReturn(versionTopic).when(task).getVersionTopic();
    doReturn(topicRepo).when(task).getPubSubTopicRepository();

    task.consumerBatchUnsubscribeAllTopics();

    verify(task, never()).consumerBatchUnsubscribe(any());
  }

  @Test
  public void testUnknownRegionFallback() {
    // Verify normalizeRegionName returns UNKNOWN_REGION for unmapped cluster IDs
    Int2ObjectMap<String> clusterIdToAlias = new Int2ObjectOpenHashMap<>();
    clusterIdToAlias.put(0, "dc-1");

    // Cluster ID 99 is not in the map — get() returns null, normalizeRegionName returns UNKNOWN_REGION
    String result = RegionUtils.normalizeRegionName(clusterIdToAlias.get(99));
    assertEquals(result, RegionUtils.UNKNOWN_REGION, "Unknown kafkaClusterId should map to 'unknown' region");

    // Cluster ID 0 is in the map — get() returns "dc-1", normalizeRegionName passes through
    String known = RegionUtils.normalizeRegionName(clusterIdToAlias.get(0));
    assertEquals(known, "dc-1", "Known kafkaClusterId should map to its alias");

    // Empty alias — normalizeRegionName returns UNKNOWN_REGION
    clusterIdToAlias.put(2, "");
    String empty = RegionUtils.normalizeRegionName(clusterIdToAlias.get(2));
    assertEquals(empty, RegionUtils.UNKNOWN_REGION, "Empty alias should normalize to 'unknown' region");
  }

  /**
   * Tests that {@link StoreIngestionTask#putGlobalRtDivStateInMetadata} writes the provided value
   * into the underlying storage engine via {@link DelegatingStorageEngine#putGlobalRtDivMetadata}
   * using the raw key bytes (no prefix validation, no storageMetadataService involvement).
   * The stored value must be {@code [schemaId (4 bytes)][payload bytes]}.
   * Production keys arrive already serialized with the non-chunk suffix from VeniceWriter.
   */
  @Test
  public void testPutGlobalRtDivStateInMetadata() throws Exception {
    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).putGlobalRtDivStateInMetadata(anyInt(), any(), any());

    DelegatingStorageEngine<?> mockStorageEngine = mock(DelegatingStorageEngine.class);
    injectField(ingestionTask, StoreIngestionTask.class, "storageEngine", mockStorageEngine);

    byte[] testValueBytes = "test-value".getBytes(StandardCharsets.UTF_8);
    Put put = new Put();
    put.schemaId = GLOBAL_RT_DIV_VERSION;
    ByteBuffer valueWithHeader = ByteBuffer.allocate(ValueRecord.SCHEMA_HEADER_LENGTH + testValueBytes.length);
    valueWithHeader.putInt(GLOBAL_RT_DIV_VERSION);
    valueWithHeader.put(testValueBytes);
    valueWithHeader.position(ValueRecord.SCHEMA_HEADER_LENGTH);
    put.putValue = valueWithHeader;

    String brokerUrl = "localhost:9092";
    // In production VeniceWriter serializes the key with the non-chunk suffix; use that here too.
    byte[] rawKeyBytes =
        (StoreIngestionTask.GLOBAL_RT_DIV_KEY_PREFIX + "0." + brokerUrl).getBytes(StandardCharsets.UTF_8);
    byte[] suffixedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(rawKeyBytes);
    ingestionTask.putGlobalRtDivStateInMetadata(0, suffixedKeyBytes, put);

    // Verify putGlobalRtDivMetadata was called with the suffixed key and value=[schemaId][payload].
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(mockStorageEngine, times(1))
        .putGlobalRtDivMetadata(argThat(k -> Arrays.equals(k, suffixedKeyBytes)), valueCaptor.capture());
    byte[] stored = valueCaptor.getValue();
    Assert.assertEquals(stored.length, ValueRecord.SCHEMA_HEADER_LENGTH + testValueBytes.length);
    Assert.assertEquals(ByteUtils.readInt(stored, 0), GLOBAL_RT_DIV_VERSION);
    Assert.assertEquals(Arrays.copyOfRange(stored, ValueRecord.SCHEMA_HEADER_LENGTH, stored.length), testValueBytes);
  }

  /**
   * Tests {@link LeaderFollowerStoreIngestionTask#readGlobalRtDivState} via the unified RawBytesChunkingAdapter path:
   * - When storage has a non-chunked value, it is returned deserialized.
   * - When storage returns null, null is returned.
   * - When the key does not start with the GLOBAL_RT_DIV_KEY_PREFIX, null is returned immediately.
   * - When storage throws VeniceException, null is returned without propagating.
   */
  @Test
  public void testReadGlobalRtDivStateMetadataPath() throws Exception {
    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask)
        .readGlobalRtDivState(any(), anyInt(), any(), any(ChunkedValueManifestContainer.class));

    String versionTopic = "testStore_v1";
    String brokerUrl = "localhost:9092";
    int partitionId = 0;
    // Key format now includes partition ID: GLOBAL_RT_DIV_KEY.{partitionId}.{brokerUrl}
    byte[] keyBytes =
        (StoreIngestionTask.GLOBAL_RT_DIV_KEY_PREFIX + partitionId + "." + brokerUrl).getBytes(StandardCharsets.UTF_8);
    byte[] manifestKey = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(keyBytes);

    InternalAvroSpecificSerializer<GlobalRtDivState> serializer =
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getSerializer();
    injectField(ingestionTask, LeaderFollowerStoreIngestionTask.class, "globalRtDivStateSerializer", serializer);

    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    doReturn(partitionId).when(topicPartition).getPartitionNumber();
    ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();

    GlobalRtDivState expectedState =
        new GlobalRtDivState(brokerUrl, Collections.emptyMap(), InMemoryPubSubPosition.of(5).toWireFormatBuffer());
    byte[] serializedState = serializer.serialize(null, expectedState);

    // Build the stored value with schema header prepended (what putGlobalRtDivMetadata stores).
    byte[] storedNonChunked = new byte[ValueRecord.SCHEMA_HEADER_LENGTH + serializedState.length];
    ByteUtils.writeInt(storedNonChunked, GLOBAL_RT_DIV_VERSION, 0);
    System.arraycopy(serializedState, 0, storedNonChunked, ValueRecord.SCHEMA_HEADER_LENGTH, serializedState.length);

    DelegatingStorageEngine<?> mockStorageEngine = mock(DelegatingStorageEngine.class);
    doReturn(versionTopic).when(mockStorageEngine).getStoreVersionName();
    injectField(ingestionTask, StoreIngestionTask.class, "storageEngine", mockStorageEngine);
    injectField(ingestionTask, StoreIngestionTask.class, "kafkaVersionTopic", versionTopic);
    injectField(ingestionTask, StoreIngestionTask.class, "compressor", Lazy.of(() -> new NoopCompressor()));

    // Case 1: non-chunked value present → returns deserialized state
    doReturn(storedNonChunked).when(mockStorageEngine)
        .getGlobalRtDivMetadata(argThat(k -> Arrays.equals(k, manifestKey)));
    GlobalRtDivState result =
        ingestionTask.readGlobalRtDivState(keyBytes, GLOBAL_RT_DIV_VERSION, topicPartition, manifestContainer);
    assertNotNull(result);
    assertEquals(result.srcUrl.toString(), brokerUrl);

    // Case 2: value absent → returns null
    doReturn(null).when(mockStorageEngine).getGlobalRtDivMetadata(argThat(k -> Arrays.equals(k, manifestKey)));
    GlobalRtDivState nullResult =
        ingestionTask.readGlobalRtDivState(keyBytes, GLOBAL_RT_DIV_VERSION, topicPartition, manifestContainer);
    Assert.assertNull(nullResult);

    // Case 3: key does not start with the prefix → returns null immediately, no storage call made
    clearInvocations(mockStorageEngine);
    byte[] nonPrefixKey = "REGULAR_KEY.localhost:9092".getBytes(StandardCharsets.UTF_8);
    GlobalRtDivState nonPrefixResult =
        ingestionTask.readGlobalRtDivState(nonPrefixKey, GLOBAL_RT_DIV_VERSION, topicPartition, manifestContainer);
    Assert.assertNull(nonPrefixResult);
    verify(mockStorageEngine, never()).getGlobalRtDivMetadata(any());

    // Case 4: storage throws VeniceException → returns null without propagating
    doThrow(new VeniceException("storage not initialized")).when(mockStorageEngine).getGlobalRtDivMetadata(any());
    GlobalRtDivState exceptionResult =
        ingestionTask.readGlobalRtDivState(keyBytes, GLOBAL_RT_DIV_VERSION, topicPartition, manifestContainer);
    Assert.assertNull(exceptionResult);
  }

  /**
   * Tests that {@link LeaderFollowerStoreIngestionTask#readGlobalRtDivState} correctly assembles
   * a chunked GlobalRtDivState at read time using {@link RawBytesChunkingAdapter}.
   * Verifies the full round-trip: manifest + one chunk stored in the storage engine → assembled → deserialized.
   */
  @Test
  public void testReadGlobalRtDivStateChunkedAssembly() throws Exception {
    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask)
        .readGlobalRtDivState(any(), anyInt(), any(), any(ChunkedValueManifestContainer.class));

    String versionTopic = "testStore_v1";
    String brokerUrl = "localhost:9092";
    int partitionId = 0;
    // Key format includes partition ID: GLOBAL_RT_DIV_KEY.{partitionId}.{brokerUrl}
    byte[] keyBytes =
        (StoreIngestionTask.GLOBAL_RT_DIV_KEY_PREFIX + partitionId + "." + brokerUrl).getBytes(StandardCharsets.UTF_8);

    InternalAvroSpecificSerializer<GlobalRtDivState> serializer =
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getSerializer();
    injectField(ingestionTask, LeaderFollowerStoreIngestionTask.class, "globalRtDivStateSerializer", serializer);
    injectField(ingestionTask, StoreIngestionTask.class, "kafkaVersionTopic", versionTopic);

    PubSubTopicPartition topicPartition = mock(PubSubTopicPartition.class);
    doReturn(0).when(topicPartition).getPartitionNumber();

    // Serialize the expected state into raw bytes (what the chunk will contain).
    GlobalRtDivState expectedState =
        new GlobalRtDivState(brokerUrl, Collections.emptyMap(), InMemoryPubSubPosition.of(7).toWireFormatBuffer());
    byte[] serializedState = serializer.serialize(null, expectedState);

    // Build chunk 0: [CHUNK_SCHEMA_ID (4 bytes)] + serializedState
    int chunkSchemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    byte[] chunkValue = new byte[ValueRecord.SCHEMA_HEADER_LENGTH + serializedState.length];
    ByteUtils.writeInt(chunkValue, chunkSchemaId, 0);
    System.arraycopy(serializedState, 0, chunkValue, ValueRecord.SCHEMA_HEADER_LENGTH, serializedState.length);

    // Build chunk key using ChunkingUtils serializer.
    ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();
    chunkedKeySuffix.chunkId.producerGUID = new ProducerMetadata(new GUID(), 0, 0, 0L, 0L).producerGUID;
    chunkedKeySuffix.chunkId.segmentNumber = 0;
    chunkedKeySuffix.chunkId.messageSequenceNumber = 0;
    chunkedKeySuffix.chunkId.chunkIndex = 0;
    ByteBuffer chunkKeyBuf =
        ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeChunkedKey(keyBytes, chunkedKeySuffix);
    byte[] chunkKeyBytes = ByteUtils.extractByteArray(chunkKeyBuf);

    // Build manifest: keysWithChunkIdSuffix=[chunkKeyBuf], size=serializedState.length
    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>();
    manifest.keysWithChunkIdSuffix.add(chunkKeyBuf);
    manifest.schemaId = GLOBAL_RT_DIV_VERSION;
    manifest.size = serializedState.length;

    // Serialize manifest WITHOUT header, then prepend CHUNK_MANIFEST_SCHEMA_ID header.
    int chunkManifestSchemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    byte[] manifestPayload = ByteUtils.extractByteArray(new ChunkedValueManifestSerializer(true).serialize(manifest));
    byte[] manifestWithHeader = new byte[ValueRecord.SCHEMA_HEADER_LENGTH + manifestPayload.length];
    ByteUtils.writeInt(manifestWithHeader, chunkManifestSchemaId, 0);
    System.arraycopy(manifestPayload, 0, manifestWithHeader, ValueRecord.SCHEMA_HEADER_LENGTH, manifestPayload.length);

    // Compute expected manifest key (with non-chunk suffix) for storage engine routing.
    byte[] manifestStorageKey = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(keyBytes);

    // Set up storage engine mock: getGlobalRtDivMetadata routes by key bytes.
    // manifest key (non-chunk suffix) → manifestWithHeader; chunk key → chunkValue.
    DelegatingStorageEngine<?> mockStorageEngine = mock(DelegatingStorageEngine.class);
    doReturn(versionTopic).when(mockStorageEngine).getStoreVersionName();
    doReturn(manifestWithHeader).when(mockStorageEngine)
        .getGlobalRtDivMetadata(argThat(k -> Arrays.equals(k, manifestStorageKey)));
    doReturn(chunkValue).when(mockStorageEngine).getGlobalRtDivMetadata(argThat(k -> Arrays.equals(k, chunkKeyBytes)));
    injectField(ingestionTask, StoreIngestionTask.class, "storageEngine", mockStorageEngine);
    injectField(ingestionTask, StoreIngestionTask.class, "compressor", Lazy.of(() -> new NoopCompressor()));

    ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();
    GlobalRtDivState result =
        ingestionTask.readGlobalRtDivState(keyBytes, GLOBAL_RT_DIV_VERSION, topicPartition, manifestContainer);

    assertNotNull(result, "Assembled chunked GlobalRtDivState should not be null");
    assertEquals(result.srcUrl.toString(), brokerUrl);
    // Verify the manifest container was populated (so callers can clean up chunks).
    assertNotNull(manifestContainer.getManifest(), "ManifestContainer should be populated after chunked assembly");
    assertEquals(manifestContainer.getManifest().keysWithChunkIdSuffix.size(), 1);
  }

  private static void injectField(Object target, Class<?> declaringClass, String fieldName, Object value)
      throws Exception {
    Field field = declaringClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  @Test
  public void testShouldStartBlobTransferReturnsFalseWhenManagerIsNull() throws InterruptedException {
    setUp();
    // blobTransferManager is null by default in test setup
    assertFalse(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test-topic_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferReturnsFalseForSeekSubscribe() throws InterruptedException {
    setUp();
    // When consumerAction has a non-null PubSubPosition, blob transfer should be skipped
    when(mockConsumerAction.getPubSubPosition()).thenReturn(mock(PubSubPosition.class));
    assertFalse(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test-topic_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferReturnsFalseForNullConsumerAction() throws InterruptedException {
    setUp();
    // null consumerAction should not throw; with null blobTransferManager returns false
    assertFalse(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test-topic_v1-0", null));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerDisabledByStorePolicy() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("DISABLED");
    assertFalse(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerEnabledByStorePolicy() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    assertTrue(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerDisabledByServerPolicy() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn(null);
    when(mockVeniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(ActivationState.DISABLED);
    assertFalse(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerEnabledByServerPolicy() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn(null);
    when(mockVeniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(ActivationState.ENABLED);
    assertTrue(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerNotSpecifiedPolicy() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn(null);
    when(mockVeniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(ActivationState.NOT_SPECIFIED);
    assertFalse(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  @Test
  public void testCancelPendingBlobTransfer() throws InterruptedException {
    setUpWithBlobTransfer(false);
    CompletableFuture<Void> pendingFuture = new CompletableFuture<>();
    when(mockPartitionConsumptionState.getPendingBlobTransfer()).thenReturn(pendingFuture);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");

    leaderFollowerStoreIngestionTask.blobTransferHelper
        .requestPendingBlobTransferCancellation(mockPartitionConsumptionState);

    // Verify the pending transfer is cleared
    verify(mockPartitionConsumptionState).setPendingBlobTransfer(null);
  }

  @Test
  public void testCancelPendingBlobTransferNoOpsWhenNoPendingTransfer() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockPartitionConsumptionState.getPendingBlobTransfer()).thenReturn(null);

    leaderFollowerStoreIngestionTask.blobTransferHelper
        .requestPendingBlobTransferCancellation(mockPartitionConsumptionState);

    // Should not try to clear since there's nothing to cancel
    verify(mockPartitionConsumptionState, never()).setPendingBlobTransfer(any());
  }

  @Test
  public void testStopBlobTransferAndWaitAlreadyInFinalState() throws InterruptedException {
    setUpWithBlobTransfer(false);
    BlobTransferStatusTrackingManager mockTrackingManager = mock(BlobTransferStatusTrackingManager.class);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(mockTrackingManager);
    when(mockTrackingManager.isTransferInFinalState("test_v1-0")).thenReturn(true);

    leaderFollowerStoreIngestionTask.blobTransferHelper.cancelBlobTransferAndAwaitTermination(0, 5, "test_v1-0");

    // Already in final state, should not poll or clear
    verify(mockTrackingManager, never()).getTransferStatus(anyString());
    verify(mockTrackingManager, never()).clearTransferStatusEnum(anyString());
  }

  @Test
  public void testStopBlobTransferAndWaitPollsUntilFinalState() throws InterruptedException {
    setUpWithBlobTransfer(false);
    BlobTransferStatusTrackingManager mockTrackingManager = mock(BlobTransferStatusTrackingManager.class);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(mockTrackingManager);
    // First call: not final, second call (after poll): final
    when(mockTrackingManager.isTransferInFinalState("test_v1-0")).thenReturn(false, true);
    when(mockTrackingManager.getTransferStatus("test_v1-0")).thenReturn(BlobTransferStatus.TRANSFER_STARTED);

    leaderFollowerStoreIngestionTask.blobTransferHelper.cancelBlobTransferAndAwaitTermination(0, 5, "test_v1-0");

    verify(mockTrackingManager).clearTransferStatusEnum("test_v1-0");
  }

  @Test
  public void testStopBlobTransferAndWaitNoOpsWhenNullTrackingManager() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(null);

    // Should not throw
    leaderFollowerStoreIngestionTask.blobTransferHelper.cancelBlobTransferAndAwaitTermination(0, 5, "test_v1-0");
  }

  // --- Tests requiring non-null BlobTransferManager ---

  private BlobTransferManager mockBlobTransferManager;

  /**
   * Sets up the test with a non-null BlobTransferManager so that shouldStartBlobTransfer
   * can proceed past the null check and exercise isReplicaLaggedAndNeedBlobTransfer.
   */
  public void setUpWithBlobTransfer(boolean isHybrid) throws InterruptedException {
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 1;
    mockStorageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(DelegatingStorageEngine.class), se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(anyString());
    mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    hostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    AggHostLevelIngestionStats aggHostLevelIngestionStats = mock(AggHostLevelIngestionStats.class);
    doReturn(hostLevelIngestionStats).when(aggHostLevelIngestionStats).getStoreStats(storeName);
    StorageMetadataService inMemoryStorageMetadataService = new InMemoryStorageMetadataService();
    StoreIngestionTaskFactory.Builder builder =
        getStoreIngestionTaskBuilder(isHybrid, storeName, inMemoryStorageMetadataService, aggHostLevelIngestionStats);
    when(builder.getSchemaRepo().getKeySchema(storeName)).thenReturn(new SchemaEntry(1, "\"string\""));

    // Set up blob transfer manager on builder
    mockBlobTransferManager = mock(BlobTransferManager.class);
    builder.setBlobTransferManagerSupplier(() -> mockBlobTransferManager);

    mockStore = builder.getMetadataRepo().getStoreOrThrow(storeName);
    mockStoreBufferService = (StoreBufferService) builder.getStoreBufferService();
    Version version = mockStore.getVersion(versionNumber);
    assert version != null;
    version.setCompressionStrategy(CompressionStrategy.GZIP);
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    String viewName = "testView";
    MaterializedViewParameters.Builder viewParamBuilder = new MaterializedViewParameters.Builder(viewName);
    viewParamBuilder.setPartitioner(DefaultVenicePartitioner.class.getCanonicalName()).setPartitionCount(3);
    ViewConfig viewConfig = new ViewConfigImpl(MaterializedView.class.getCanonicalName(), viewParamBuilder.build());
    viewConfigMap.put(viewName, viewConfig);
    when(mockStore.getViewConfigs()).thenReturn(viewConfigMap);

    mockPartitionConsumptionState = mock(PartitionConsumptionState.class);
    mockConsumerAction = mock(ConsumerAction.class);

    mockProperties = new Properties();
    mockProperties.put(KAFKA_BOOTSTRAP_SERVERS, "bootStrapServers");
    mockBooleanSupplier = mock(BooleanSupplier.class);
    mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    String versionTopic = version.kafkaTopicName();
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();
    mockStorageMetadataService = builder.getStorageMetadataService();
    storeRepository = builder.getMetadataRepo();
    PubSubContext pubSubContext = builder.getPubSubContext();
    mockTopicManagerRepository = pubSubContext.getTopicManagerRepository();
    leaderFollowerStoreIngestionTask = spy(
        new LeaderFollowerStoreIngestionTask(
            mockStorageService,
            builder,
            mockStore,
            version,
            mockProperties,
            mockBooleanSupplier,
            mockVeniceStoreVersionConfig,
            0,
            Optional.empty(),
            null,
            null));

    leaderFollowerStoreIngestionTask.addPartitionConsumptionState(0, mockPartitionConsumptionState);
  }

  @Test
  public void testShouldStartBlobTransferReturnsTrueWhenNullOffsetRecord() throws InterruptedException {
    setUpWithBlobTransfer(false);
    // Mock: store has blob transfer enabled (DaVinci path)
    when(mockStore.isBlobTransferEnabled()).thenReturn(true);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    // storageMetadataService.getLastOffset returns null by default for non-partition-0 — use partition 0 which
    // returns mock OffsetRecord. Override to return null.
    doReturn(null).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());

    // isDaVinciClient is false by default; need to set store policy for server mode
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");

    assertTrue(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferNegativeThresholdAlwaysReturnsTrue() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    // getLastOffset returns a mock OffsetRecord
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    // Negative threshold means always use blob transfer
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(-1L);

    assertTrue(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferBatchStoreEOPReceived() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(false);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(100L);
    // Batch store with EOP received — should bootstrap from Kafka, not blob transfer
    when(mockOffset.isEndOfPushReceived()).thenReturn(true);

    assertFalse(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferBatchStoreEOPNotReceived() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(false);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(100L);
    when(mockOffset.isEndOfPushReceived()).thenReturn(false);

    assertTrue(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferHybridStoreOffsetLagBelowThreshold() throws InterruptedException {
    setUpWithBlobTransfer(true);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(true);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(1000L);
    when(mockVeniceServerConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(0);
    // Offset lag below threshold — should bootstrap from Kafka
    when(mockOffset.getOffsetLag()).thenReturn(500L);
    when(mockOffset.getCheckpointedLocalVtPosition()).thenReturn(new ApacheKafkaOffsetPosition(100L));

    assertFalse(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferHybridStoreOffsetLagAboveThreshold() throws InterruptedException {
    setUpWithBlobTransfer(true);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(true);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(1000L);
    when(mockVeniceServerConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(0);
    // Offset lag above threshold — should use blob transfer
    when(mockOffset.getOffsetLag()).thenReturn(5000L);
    when(mockOffset.getCheckpointedLocalVtPosition()).thenReturn(new ApacheKafkaOffsetPosition(100L));

    assertTrue(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferHybridStoreZeroLagEarliestPosition() throws InterruptedException {
    setUpWithBlobTransfer(true);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(true);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(1000L);
    when(mockVeniceServerConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(0);
    // Zero offset lag but EARLIEST position — needs blob transfer
    when(mockOffset.getOffsetLag()).thenReturn(0L);
    when(mockOffset.getCheckpointedLocalVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);

    assertTrue(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreDaVinciClient() throws Exception {
    setUpWithBlobTransfer(false);

    when(mockStore.isBlobTransferEnabled()).thenReturn(true);
    assertTrue(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, true));

    when(mockStore.isBlobTransferEnabled()).thenReturn(false);
    assertFalse(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, true));
  }

  @Test
  public void testCancelPendingBlobTransferWithTrackingManager() throws InterruptedException {
    setUpWithBlobTransfer(false);
    CompletableFuture<Void> pendingFuture = new CompletableFuture<>();
    when(mockPartitionConsumptionState.getPendingBlobTransfer()).thenReturn(pendingFuture);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");

    BlobTransferStatusTrackingManager mockTrackingManager = mock(BlobTransferStatusTrackingManager.class);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(mockTrackingManager);

    leaderFollowerStoreIngestionTask.blobTransferHelper
        .requestPendingBlobTransferCancellation(mockPartitionConsumptionState);

    verify(mockTrackingManager).cancelTransfer("test_v1-0");
    verify(mockPartitionConsumptionState).setPendingBlobTransfer(null);
  }

  @Test
  public void testCancelPendingBlobTransferWithNullTrackingManager() throws InterruptedException {
    setUpWithBlobTransfer(false);
    CompletableFuture<Void> pendingFuture = new CompletableFuture<>();
    when(mockPartitionConsumptionState.getPendingBlobTransfer()).thenReturn(pendingFuture);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");

    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(null);

    // Should not throw even when tracking manager is null
    leaderFollowerStoreIngestionTask.blobTransferHelper
        .requestPendingBlobTransferCancellation(mockPartitionConsumptionState);

    verify(mockPartitionConsumptionState).setPendingBlobTransfer(null);
  }

  @Test
  public void testStopBlobTransferAndWaitWithTrackingManagerCompletedTransfer() throws InterruptedException {
    setUpWithBlobTransfer(false);
    BlobTransferStatusTrackingManager mockTrackingManager = mock(BlobTransferStatusTrackingManager.class);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(mockTrackingManager);
    // Not in final state initially, then transitions to final
    when(mockTrackingManager.isTransferInFinalState("test_v1-0")).thenReturn(false, true);
    when(mockTrackingManager.getTransferStatus("test_v1-0"))
        .thenReturn(BlobTransferStatus.TRANSFER_STARTED, BlobTransferStatus.TRANSFER_COMPLETED);

    leaderFollowerStoreIngestionTask.blobTransferHelper.cancelBlobTransferAndAwaitTermination(0, 5, "test_v1-0");

    verify(mockTrackingManager).clearTransferStatusEnum("test_v1-0");
  }

  @Test
  public void testStopBlobTransferAndWaitWithNullTrackingManagerIsNoOp() throws InterruptedException {
    setUpWithBlobTransfer(false);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(null);

    // Should not throw
    leaderFollowerStoreIngestionTask.blobTransferHelper.cancelBlobTransferAndAwaitTermination(0, 5, "test_v1-0");
  }

  @Test
  public void testShouldStartBlobTransferHybridStoreTimeLagThreshold() throws InterruptedException {
    setUpWithBlobTransfer(true);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(true);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(1000L);
    // Set time lag threshold > 0 to exercise that branch
    when(mockVeniceServerConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(10);
    // Recent heartbeat — within threshold, so should NOT need blob transfer
    when(mockOffset.getHeartbeatTimestamp()).thenReturn(System.currentTimeMillis());

    assertFalse(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testShouldStartBlobTransferHybridStoreTimeLagExceedsThreshold() throws InterruptedException {
    setUpWithBlobTransfer(true);
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockStore.isHybrid()).thenReturn(true);
    when(mockPartitionConsumptionState.getReplicaId()).thenReturn("test_v1-0");
    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(mockOffset).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt(), any());
    when(mockVeniceServerConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(1000L);
    when(mockVeniceServerConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(10);
    // Old heartbeat — exceeds 10 minute threshold
    when(mockOffset.getHeartbeatTimestamp()).thenReturn(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30));

    assertTrue(leaderFollowerStoreIngestionTask.shouldStartBlobTransfer(0, "test_v1-0", mockConsumerAction));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerDisabledByServerPolicyWithNonNullStorePolicy()
      throws InterruptedException {
    setUpWithBlobTransfer(false);
    // Store policy is non-null but not DISABLED/ENABLED, server policy is DISABLED
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("SOMETHING_ELSE");
    when(mockVeniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(ActivationState.DISABLED);
    assertFalse(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  @Test
  public void testIsBlobTransferEnabledForStoreServerEnabledByStorePolicyWithServerDisabled()
      throws InterruptedException {
    setUpWithBlobTransfer(false);
    // Store policy ENABLED takes precedence even when server policy is NOT_SPECIFIED
    when(mockStore.getBlobTransferInServerEnabled()).thenReturn("ENABLED");
    when(mockVeniceServerConfig.getBlobTransferReceiverServerPolicy()).thenReturn(ActivationState.NOT_SPECIFIED);
    assertTrue(leaderFollowerStoreIngestionTask.blobTransferHelper.shouldEnableBlobTransfer(mockStore, false));
  }

  /**
   * Helper to set up common mocks needed by completeBlobTransferAndSubscribe tests.
   */
  private void setUpCompleteBlobTransferMocks() {
    // Mock blobTransferManager methods used by the helper
    BlobTransferStatusTrackingManager mockTrackingManager = mock(BlobTransferStatusTrackingManager.class);
    when(mockBlobTransferManager.getTransferStatusTrackingManager()).thenReturn(mockTrackingManager);
    when(mockBlobTransferManager.getAggVersionedBlobTransferStats()).thenReturn(null);

    // Mock serverConfig methods needed by helper and checkConsumptionStateWhenStart
    when(mockVeniceServerConfig.getRocksDBPath()).thenReturn("/tmp/test-rocksdb");
    VeniceProperties mockClusterProps = mock(VeniceProperties.class);
    when(mockClusterProps.getPropertiesCopy()).thenReturn(new Properties());
    when(mockVeniceServerConfig.getClusterProperties()).thenReturn(mockClusterProps);

    // The storageEngine was already set as a mock DelegatingStorageEngine during setup.
    // Configure it to return the version topic name for adjustStoragePartition.
    PubSubTopic versionTopic = leaderFollowerStoreIngestionTask.getVersionTopic();
    StorageEngine storageEngine = leaderFollowerStoreIngestionTask.getStorageEngine();
    when(storageEngine.getStoreVersionName()).thenReturn(versionTopic.getName());

    // Mock the DataIntegrityValidator
    DataIntegrityValidator mockDiv = mock(DataIntegrityValidator.class);
    doReturn(mockDiv).when(leaderFollowerStoreIngestionTask).getDataIntegrityValidator();

    // Stub consumerSubscribe to avoid deep Kafka consumer creation
    doNothing().when(leaderFollowerStoreIngestionTask)
        .consumerSubscribe(any(PubSubTopic.class), any(PartitionConsumptionState.class), any(), anyString());
  }

  @Test
  public void testCompleteBlobTransferAndSubscribeSuccess() throws Exception {
    setUpWithBlobTransfer(false);
    setUpCompleteBlobTransferMocks();

    PubSubTopic versionTopic = leaderFollowerStoreIngestionTask.getVersionTopic();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(versionTopic, 0);

    // Create a PCS with a successfully completed blob transfer future
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getPartition()).thenReturn(0);
    when(pcs.getReplicaId()).thenReturn(versionTopic.getName() + "-0");
    when(pcs.getReplicaTopicPartition()).thenReturn(topicPartition);
    CompletableFuture<Void> succeededFuture = CompletableFuture.completedFuture(null);
    when(pcs.getPendingBlobTransfer()).thenReturn(succeededFuture);
    when(pcs.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.STANDBY);
    when(pcs.getDolState()).thenReturn(null);

    // Set isCurrentVersion to return true so the latch branch is covered
    when(mockBooleanSupplier.getAsBoolean()).thenReturn(true);

    leaderFollowerStoreIngestionTask.completeBlobTransferAndSubscribe(pcs);

    // Verify tracking status was cleared
    BlobTransferStatusTrackingManager trackingManager = mockBlobTransferManager.getTransferStatusTrackingManager();
    verify(trackingManager).clearTransferStatusEnum(versionTopic.getName() + "-0");

    // Verify storage partition was adjusted
    StorageEngine storageEngine = leaderFollowerStoreIngestionTask.getStorageEngine();
    verify(storageEngine).adjustStoragePartition(eq(0), any(), any());
  }

  @Test
  public void testCompleteBlobTransferAndSubscribeFailure() throws Exception {
    setUpWithBlobTransfer(false);
    setUpCompleteBlobTransferMocks();

    PubSubTopic versionTopic = leaderFollowerStoreIngestionTask.getVersionTopic();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(versionTopic, 0);

    // Create a PCS with a failed blob transfer future
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getPartition()).thenReturn(0);
    when(pcs.getReplicaId()).thenReturn(versionTopic.getName() + "-0");
    when(pcs.getReplicaTopicPartition()).thenReturn(topicPartition);
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("transfer failed"));
    when(pcs.getPendingBlobTransfer()).thenReturn(failedFuture);
    when(pcs.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);
    DolStamp mockDolStamp = mock(DolStamp.class);
    when(pcs.getDolState()).thenReturn(mockDolStamp);

    // Not current version, so latch branch is not taken
    when(mockBooleanSupplier.getAsBoolean()).thenReturn(false);

    leaderFollowerStoreIngestionTask.completeBlobTransferAndSubscribe(pcs);

    // Verify tracking status was cleared
    BlobTransferStatusTrackingManager trackingManager = mockBlobTransferManager.getTransferStatusTrackingManager();
    verify(trackingManager).clearTransferStatusEnum(versionTopic.getName() + "-0");

    // Verify storage partition was adjusted (happens for both success and failure)
    StorageEngine storageEngine = leaderFollowerStoreIngestionTask.getStorageEngine();
    verify(storageEngine).adjustStoragePartition(eq(0), any(), any());
  }

  @Test
  public void testTrackRecordReceivedSkipsBeforeEOP() throws Exception {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);

    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).trackRecordReceived(any(), any(), anyString());
    doReturn(heartbeatMonitoringService).when(ingestionTask).getHeartbeatMonitoringService();

    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);

    // Batch-only store before EOP — should skip
    PartitionConsumptionState batchPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(batchPcs).isEndOfPushReceived();
    ingestionTask.trackRecordReceived(batchPcs, consumerRecord, "abc:123");

    // Hybrid store before EOP — should also skip
    PartitionConsumptionState hybridPcs = mock(PartitionConsumptionState.class);
    doReturn(false).when(hybridPcs).isEndOfPushReceived();
    ingestionTask.trackRecordReceived(hybridPcs, consumerRecord, "abc:123");

    verify(heartbeatMonitoringService, never())
        .recordLeaderRecordTimestamp(any(HeartbeatKey.class), anyLong(), anyBoolean());
    verify(heartbeatMonitoringService, never())
        .recordFollowerRecordTimestamp(any(HeartbeatKey.class), anyLong(), anyBoolean());
  }

  @Test
  public void testTrackRecordReceivedEmitsAfterEOP() throws Exception {
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);

    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).trackRecordReceived(any(), any(), anyString());
    doReturn(heartbeatMonitoringService).when(ingestionTask).getHeartbeatMonitoringService();
    doReturn(false).when(ingestionTask).isDaVinciClient();

    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    Map<String, String> urlMap = Collections.singletonMap("abc:123", "c1");
    doReturn(urlMap).when(veniceServerConfig).getKafkaClusterUrlToAliasMap();
    setField(ingestionTask, "serverConfig", veniceServerConfig);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(100).when(pcs).getPartition();
    doReturn(true).when(pcs).isEndOfPushReceived();
    doReturn(false).when(pcs).isComplete();
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();

    long producerTimestamp = 1000L;
    DefaultPubSubMessage consumerRecord = mock(DefaultPubSubMessage.class);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.messageTimestamp = producerTimestamp;
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    doReturn(kafkaMessageEnvelope).when(consumerRecord).getValue();

    HeartbeatKey followerKey = new HeartbeatKey("foo", 1, 100, "c1");
    doReturn(followerKey).when(pcs).getOrCreateCachedHeartbeatKey("c1");

    // EOP received — should emit even during hybrid rewind
    ingestionTask.trackRecordReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1))
        .recordFollowerRecordTimestamp(eq(followerKey), eq(producerTimestamp), eq(false));
  }
}
