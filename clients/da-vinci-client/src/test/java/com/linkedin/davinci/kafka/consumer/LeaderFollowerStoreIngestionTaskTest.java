package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask.VIEW_WRITER_CLOSE_TIMEOUT_IN_MS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
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
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.view.MaterializedViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.GlobalRtDivState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.VeniceWriter;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.testng.annotations.Test;


public class LeaderFollowerStoreIngestionTaskTest {
  Store mockStore;
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
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 1;
    mockStorageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(StorageEngine.class), se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(anyString());
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    hostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    AggHostLevelIngestionStats aggHostLevelIngestionStats = mock(AggHostLevelIngestionStats.class);
    doReturn(hostLevelIngestionStats).when(aggHostLevelIngestionStats).getStoreStats(storeName);
    StorageMetadataService inMemoryStorageMetadataService = new InMemoryStorageMetadataService();
    StoreIngestionTaskFactory.Builder builder = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setServerConfig(mockVeniceServerConfig)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setVeniceViewWriterFactory(mockVeniceViewWriterFactory)
        .setHeartbeatMonitoringService(mock(HeartbeatMonitoringService.class))
        .setCompressorFactory(new StorageEngineBackedCompressorFactory(inMemoryStorageMetadataService))
        .setHostLevelIngestionStats(aggHostLevelIngestionStats);
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
    mockBooleanSupplier = mock(BooleanSupplier.class);
    mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    String versionTopic = version.kafkaTopicName();
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

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

  @Test(timeOut = 10000)
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

  @Test
  public void testSendGlobalRtDivMessage() throws InterruptedException, IOException {
    setUp();
    int partition = 1;
    long offset = 3L;
    long messageTime = 5;
    DefaultPubSubMessage mockMessage = mock(DefaultPubSubMessage.class);
    PubSubTopicPartition mockTopicPartition = mock(PubSubTopicPartition.class);
    LeaderProducedRecordContext context = mock(LeaderProducedRecordContext.class);
    PubSubPosition positionMock = mock(PubSubPosition.class);
    doReturn(positionMock).when(context).getConsumedPosition();
    doReturn(partition).when(mockTopicPartition).getPartitionNumber();
    doReturn(offset).when(positionMock).getNumericOffset();
    doReturn(positionMock).when(mockMessage).getPosition();
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
        eq(false));

    // Verify that GlobalRtDivState is correctly compressed and serialized from the VeniceWriter#put() call
    byte[] compressedBytes = valueBytesArgumentCaptor.getValue();
    VeniceCompressor compressor = leaderFollowerStoreIngestionTask.getCompressor().get();
    byte[] valueBytes = ByteUtils.extractByteArray(compressor.decompress(ByteBuffer.wrap(compressedBytes)));
    InternalAvroSpecificSerializer<GlobalRtDivState> serializer =
        leaderFollowerStoreIngestionTask.globalRtDivStateSerializer;
    GlobalRtDivState globalRtDiv =
        serializer.deserialize(valueBytes, AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion());
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
    assertEquals(put.getSchemaId(), AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion());
    assertNotNull(put.getPutValue());
    PubSubProduceResult produceResult = mock(PubSubProduceResult.class);
    when(produceResult.getOffset()).thenReturn(0L);
    when(produceResult.getSerializedSize()).thenReturn(keyBytes.length + put.putValue.remaining());
    callback.onCompletion(produceResult, null);
    verify(mockStoreBufferService, times(1)).execSyncOffsetFromSnapshotAsync(any(), any(), any());
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

    // delegateConsumerRecord() should cause updateLatestConsumedVtOffset() to be called
    mockIngestionTask.delegateConsumerRecord(cm, 0, "testURL", 0, 0, 0);
    verify(consumerDiv, times(1)).updateLatestConsumedVtOffset(0, 1L);
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

    // The method should only return true for non-chunk Global RT DIV messages
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));
    doReturn(true).when(mockKey).isGlobalRtDiv();
    doReturn(mockPut).when(mockKme).getPayloadUnion();
    doReturn(AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion()).when(mockPut).getSchemaId();
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));
    doReturn(AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()).when(mockPut).getSchemaId();
    assertFalse(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));
    doReturn(AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()).when(mockPut).getSchemaId();
    assertTrue(mockIngestionTask.shouldSyncOffsetFromSnapshot(globalRtDivMessage, mockPartitionConsumptionState));

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
}
