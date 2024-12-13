package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.view.MaterializedViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.VeniceWriter;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.testng.annotations.Test;


public class LeaderFollowerStoreIngestionTaskTest {
  Store mockStore;
  private LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask;
  private PartitionConsumptionState mockPartitionConsumptionState;
  private PubSubTopicPartition mockTopicPartition;
  private ConsumerAction mockConsumerAction;
  private StorageService mockStorageService;
  private Properties mockProperties;
  private BooleanSupplier mockBooleanSupplier;
  private VeniceStoreVersionConfig mockVeniceStoreVersionConfig;

  private VeniceViewWriterFactory mockVeniceViewWriterFactory;

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
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    StoreIngestionTaskFactory.Builder builder = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setServerConfig(mockVeniceServerConfig)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setVeniceViewWriterFactory(mockVeniceViewWriterFactory);
    when(builder.getSchemaRepo().getKeySchema(storeName)).thenReturn(new SchemaEntry(1, "\"string\""));
    mockStore = builder.getMetadataRepo().getStoreOrThrow(storeName);
    Version version = mockStore.getVersion(versionNumber);
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

    leaderFollowerStoreIngestionTask = new LeaderFollowerStoreIngestionTask(
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
        null);

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
  public void testProcessViewWriters() throws InterruptedException {
    mockVeniceViewWriterFactory = mock(VeniceViewWriterFactory.class);
    Map<String, VeniceViewWriter> viewWriterMap = new HashMap<>();
    MaterializedViewWriter materializedViewWriter = mock(MaterializedViewWriter.class);
    viewWriterMap.put("testView", materializedViewWriter);
    when(mockVeniceViewWriterFactory.buildStoreViewWriters(any(), anyInt(), any())).thenReturn(viewWriterMap);
    CompletableFuture<PubSubProduceResult> viewWriterFuture = new CompletableFuture<>();
    when(materializedViewWriter.processRecord(any(), any(), anyInt())).thenReturn(viewWriterFuture);
    setUp();
    WriteComputeResultWrapper mockResult = mock(WriteComputeResultWrapper.class);
    Put put = new Put();
    put.schemaId = 1;
    when(mockResult.getNewPut()).thenReturn(put);
    CompletableFuture[] futures =
        leaderFollowerStoreIngestionTask.processViewWriters(mockPartitionConsumptionState, new byte[1], mockResult);
    assertEquals(futures.length, 2);
    verify(mockPartitionConsumptionState, times(1)).getLastVTProduceCallFuture();
    verify(materializedViewWriter, times(1)).processRecord(any(), any(), anyInt());
  }
}
