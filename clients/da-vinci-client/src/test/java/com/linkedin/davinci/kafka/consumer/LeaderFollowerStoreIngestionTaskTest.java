package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.VIEW_WRITER_CLOSE_TIMEOUT_IN_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
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

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.DelegatingStorageEngine;
import com.linkedin.davinci.store.view.MaterializedViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
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
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.VeniceWriter;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
            false,
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

  @Test
  public void testQueueUpVersionTopicWritesWithViewWriters() throws InterruptedException {
    mockVeniceViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Map<String, VeniceViewWriter> viewWriterMap = new HashMap<>();
    MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);
    viewWriterMap.put("testView", materializedViewWriter);
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt(), any())).thenReturn(viewWriterMap);
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
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> assertTrue(vtWriteFutureCaptor.getValue().isDone()));
    assertFalse(vtWriteFutureCaptor.getValue().isCompletedExceptionally());
    assertTrue(writeToVersionTopic.get());
    verify(hostLevelIngestionStats, times(1)).recordViewProducerAckLatency(anyDouble());
  }

  @Test(timeOut = 30000)
  public void testCloseVeniceViewWriters() throws InterruptedException {
    mockVeniceViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Map<String, VeniceViewWriter> viewWriterMap = new HashMap<>();
    MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);
    viewWriterMap.put("testView", materializedViewWriter);
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt(), any())).thenReturn(viewWriterMap);
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
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt(), any())).thenReturn(viewWriterMap);
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
    byte[] keyBytes = LeaderFollowerStoreIngestionTask.getGlobalRtDivKeyName(brokerUrl).getBytes();

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

    // Monitoring service is null.
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, never())
        .recordLeaderHeartbeat(anyString(), anyInt(), anyInt(), anyString(), anyLong(), anyBoolean());
    verify(heartbeatMonitoringService, never())
        .recordFollowerHeartbeat(anyString(), anyInt(), anyInt(), anyString(), anyLong(), anyBoolean());

    // Verify Leader
    doReturn(heartbeatMonitoringService).when(ingestionTask).getHeartbeatMonitoringService();
    doReturn(LeaderFollowerStateType.LEADER).when(pcs).getLeaderFollowerState();
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1))
        .recordLeaderHeartbeat(eq("foo"), eq(1), eq(100), eq("c1"), eq(123L), eq(false));
    verify(heartbeatMonitoringService, never())
        .recordFollowerHeartbeat(anyString(), anyInt(), anyInt(), anyString(), anyLong(), anyBoolean());
    // Verify Follower
    doReturn(false).when(ingestionTask).isDaVinciClient();
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1))
        .recordFollowerHeartbeat(eq("foo"), eq(1), eq(100), eq("c1"), eq(123L), eq(false));
    // Verify Da Vinci
    doReturn(true).when(ingestionTask).isDaVinciClient();
    doReturn("local").when(veniceServerConfig).getRegionName();
    ingestionTask.recordHeartbeatReceived(pcs, consumerRecord, "abc:123");
    verify(heartbeatMonitoringService, times(1))
        .recordFollowerHeartbeat(eq("foo"), eq(1), eq(100), eq("local"), eq(123L), eq(false));
  }

  @Test
  public void testIngestionTimeoutHandling() throws InterruptedException {
    LeaderFollowerStoreIngestionTask storeIngestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doReturn("foo").when(storeIngestionTask).getStoreName();
    doReturn(Lazy.of(() -> mock(VeniceWriter.class))).when(storeIngestionTask).getVeniceWriter();
    doReturn(Lazy.of(() -> mock(VeniceWriter.class))).when(storeIngestionTask).getVeniceWriterForRealTime();
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(storeRepository).when(storeIngestionTask).getStoreRepository();
    Store store = mock(Store.class);
    doReturn(5).when(store).getCurrentVersion();
    doReturn(store).when(storeRepository).getStoreOrThrow(anyString());

    // Timeout replica
    doReturn(TimeUnit.DAYS.toMillis(1)).when(storeIngestionTask).getBootstrapTimeoutInMs();
    PubSubTopic topic = new PubSubTopicImpl("foo_v1");
    doReturn(topic).when(storeIngestionTask).getVersionTopic();
    doCallRealMethod().when(storeIngestionTask).checkLongRunningTaskState();
    Map<Integer, PartitionConsumptionState> pcsMap = new HashMap<>();
    doReturn(pcsMap).when(storeIngestionTask).getPartitionConsumptionStateMap();

    // Not yet timeout replica
    PartitionConsumptionState pcs1 = mock(PartitionConsumptionState.class);
    pcsMap.put(1, pcs1);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs1).getLeaderFollowerState();
    doReturn(false).when(pcs1).isComplete();
    doReturn(1).when(pcs1).getPartition();
    doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2)).when(pcs1).getConsumptionStartTimeInMs();

    // Timeout replica
    PartitionConsumptionState pcs2 = mock(PartitionConsumptionState.class);
    pcsMap.put(2, pcs2);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs2).getLeaderFollowerState();
    doReturn(false).when(pcs2).isComplete();
    doReturn(2).when(pcs2).getPartition();
    doReturn(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2)).when(pcs2).getConsumptionStartTimeInMs();

    PartitionConsumptionState pcs3 = mock(PartitionConsumptionState.class);
    pcsMap.put(3, pcs3);
    doReturn(LeaderFollowerStateType.STANDBY).when(pcs3).getLeaderFollowerState();
    doReturn(false).when(pcs3).isComplete();
    doReturn(3).when(pcs3).getPartition();
    doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2)).when(pcs3).getConsumptionStartTimeInMs();

    // For future version it should be throwing exception.
    doReturn(10).when(storeIngestionTask).getVersionNumber();
    Assert.assertThrows(VeniceTimeoutException.class, storeIngestionTask::checkLongRunningTaskState);

    // For current version we should report error and only failing this partition instead of throwing exception and stop
    // SIT.
    doReturn(5).when(storeIngestionTask).getVersionNumber();
    storeIngestionTask.checkLongRunningTaskState();
    verify(storeIngestionTask, times(1)).reportError(anyString(), eq(1), any());
    verify(storeIngestionTask, times(0)).reportError(anyString(), eq(2), any());
    verify(storeIngestionTask, times(1)).reportError(anyString(), eq(3), any());

    // Same for the backup version.
    doReturn(1).when(storeIngestionTask).getVersionNumber();
    storeIngestionTask.checkLongRunningTaskState();
    verify(storeIngestionTask, times(2)).reportError(anyString(), eq(1), any());
    verify(storeIngestionTask, times(0)).reportError(anyString(), eq(2), any());
    verify(storeIngestionTask, times(2)).reportError(anyString(), eq(3), any());
  }
}
