package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceHelixAdmin.VERSION_ID_UNSET;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_SOP;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD;
import static com.linkedin.venice.meta.Version.DEFAULT_RT_VERSION_NUMBER;
import static com.linkedin.venice.meta.Version.VERSION_SEPARATOR;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.ETLStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.lingeringjob.LingeringStoreVersionChecker;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ConcurrentBatchPushException;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.ConcurrentPushDetectionStrategy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.VeniceETLStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.partitioner.InvalidKeySchemaPartitioner;
import com.linkedin.venice.pubsub.adapter.SimplePubSubProduceResultImpl;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpStatus;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestVeniceParentHelixAdmin extends AbstractTestVeniceParentHelixAdmin {
  String storeName = Utils.getUniqueString("test_store");
  static final int NUM_REGIONS = 3;
  static final long LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION =
      AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;

  @BeforeMethod
  public void setupTestCase() {
    setupInternalMocks();
    initializeParentAdmin(Optional.empty(), Optional.empty());
  }

  @AfterMethod
  public void cleanupTestCase() {
    super.cleanupTestCase();
  }

  @Test
  public void testSendPushJobDetailsToChildRegions() {
    parentAdmin.initStorageCluster(clusterName);
    PushJobStatusRecordKey key = new PushJobStatusRecordKey();
    key.versionNumber = 1;
    key.storeName = "abc";
    PushJobDetails value = new PushJobDetails();
    doCallRealMethod().when(internalAdmin).sendPushJobDetails(any(), any());
    ControllerClient controllerClient = mock(ControllerClient.class);
    ControllerResponse controllerResponse = mock(ControllerResponse.class);
    when(controllerResponse.isError()).thenReturn(false);
    doReturn(controllerResponse).when(controllerClient).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    Assert.assertNotNull(controllerClient.sendPushJobDetails("abc", 1, "abc".getBytes()));
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    controllerClientMap.put("test-region", controllerClient);
    doReturn(controllerClientMap).when(internalAdmin).getControllerClientMap(anyString());
    doReturn(true).when(internalAdmin).isParent();
    InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsInternalAvroSpecificSerializer =
        mock(InternalAvroSpecificSerializer.class);
    doReturn("abc".getBytes()).when(pushJobDetailsInternalAvroSpecificSerializer).serialize(any(), any());
    doReturn(pushJobDetailsInternalAvroSpecificSerializer).when(internalAdmin).getPushJobDetailsSerializer();
    doReturn("test-cluster").when(internalAdmin).getPushJobStatusStoreClusterName();
    parentAdmin.sendPushJobDetails(key, value);
    verify(internalAdmin, atLeast(1)).sendPushJobDetails(key, value);
    // Parent does not write push job details to parent region RT anymore.
    verify(internalAdmin, never()).sendPushJobDetailsToLocalRT(any(), any());
    verify(controllerClient, atLeast(1)).sendPushJobDetails(anyString(), anyInt(), any());
    doReturn(controllerClients).when(internalAdmin).getControllerClientMap(any());

  }

  @Test
  public void testStartWithTopicExists() {
    parentAdmin.initStorageCluster(clusterName);
    verify(internalAdmin).getTopicManager();
    verify(topicManager, never()).createTopic(
        pubSubTopicRepository.getTopic(topicName),
        AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC,
        KAFKA_REPLICA_FACTOR,
        true,
        false,
        Optional.empty());
  }

  @Test
  public void testStartWhenTopicNotExists() {
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    doReturn(false).when(topicManager).containsTopicAndAllPartitionsAreOnline(pubSubTopic);
    parentAdmin.initStorageCluster(clusterName);
    verify(internalAdmin).getTopicManager();
    verify(topicManager).createTopic(
        pubSubTopic,
        AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC,
        KAFKA_REPLICA_FACTOR,
        true,
        false,
        Optional.empty());
  }

  /**
   * Partially stubbed class to verify async setup behavior.
   */
  private static class AsyncSetupMockVeniceParentHelixAdmin extends VeniceParentHelixAdmin {
    private Map<String, Store> systemStores = new VeniceConcurrentHashMap<>();

    public AsyncSetupMockVeniceParentHelixAdmin(
        VeniceHelixAdmin veniceHelixAdmin,
        VeniceControllerClusterConfig config) {
      super(veniceHelixAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config), mock(MetricsRepository.class));
    }

    public boolean isAsyncSetupRunning(String clusterName) {
      return asyncSetupEnabledMap.get(clusterName);
    }

    @Override
    public void createStore(
        String clusterName,
        String storeName,
        String owner,
        String keySchema,
        String valueSchema,
        boolean isSystemStore) {
      if (!(VeniceSystemStoreUtils.isSystemStore(storeName) && isSystemStore)) {
        throw new VeniceException("Invalid store name and isSystemStore combination. Got store name: " + storeName);
      }
      if (systemStores.containsKey(storeName)) {
        // no op
        return;
      }
      Store newStore = new ZKStore(
          storeName,
          owner,
          System.currentTimeMillis(),
          PersistenceType.IN_MEMORY,
          RoutingStrategy.HASH,
          ReadStrategy.ANY_OF_ONLINE,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
          1);
      systemStores.put(storeName, newStore);
    }

    @Override
    public Store getStore(String clusterName, String storeName) {
      if (!systemStores.containsKey(storeName)) {
        return null;
      }
      return systemStores.get(storeName).cloneStore();
    }

    @Override
    public void updateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
      Optional<Long> hybridRewindSeconds = params.getHybridRewindSeconds();
      Optional<Long> hybridOffsetLagThreshold = params.getHybridOffsetLagThreshold();
      Optional<Long> hybridTimeLagThreshold = params.getHybridTimeLagThreshold();
      Optional<BufferReplayPolicy> hybridBufferReplayPolicy = params.getHybridBufferReplayPolicy();

      if (!systemStores.containsKey(storeName)) {
        throw new VeniceNoStoreException("Cannot update store " + storeName + " because it's missing.");
      }
      if (hybridRewindSeconds.isPresent() && hybridOffsetLagThreshold.isPresent()) {
        final long finalHybridTimeLagThreshold = hybridTimeLagThreshold.orElse(DEFAULT_HYBRID_TIME_LAG_THRESHOLD);
        final BufferReplayPolicy finalHybridBufferReplayPolicy =
            hybridBufferReplayPolicy.orElse(BufferReplayPolicy.REWIND_FROM_EOP);
        systemStores.get(storeName)
            .setHybridStoreConfig(
                new HybridStoreConfigImpl(
                    hybridRewindSeconds.get(),
                    hybridOffsetLagThreshold.get(),
                    finalHybridTimeLagThreshold,
                    finalHybridBufferReplayPolicy));
      }
    }

    @Override
    public Version incrementVersionIdempotent(
        String clusterName,
        String storeName,
        String pushJobId,
        int numberOfPartition,
        int replicationFactor) {
      if (!systemStores.containsKey(storeName)) {
        throw new VeniceNoStoreException("Cannot add version to store " + storeName + " because it's missing.");
      }
      Version version = new VersionImpl(storeName, 1, "test-id");
      version.setReplicationFactor(replicationFactor);
      List<Version> versions = new ArrayList<>();
      versions.add(version);
      systemStores.get(storeName).setVersions(versions);
      return version;
    }
  }

  @Test
  public void testAddStore() {
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";

    // To support the store update during store creation.
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    parentAdmin.createStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    verify(internalAdmin)
        .checkPreConditionForCreateStore(clusterName, storeName, keySchemaStr, valueSchemaStr, false, false);
    verify(veniceWriter, times(2)).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(3)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter, times(2)).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getAllValues().get(0);
    byte[] valueBytes = valueCaptor.getAllValues().get(0);
    int schemaId = schemaCaptor.getAllValues().get(0);
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.STORE_CREATION.getValue());

    StoreCreation storeCreationMessage = (StoreCreation) adminMessage.payloadUnion;
    assertEquals(storeCreationMessage.clusterName.toString(), clusterName);
    assertEquals(storeCreationMessage.storeName.toString(), storeName);
    assertEquals(storeCreationMessage.owner.toString(), owner);
    assertEquals(storeCreationMessage.keySchema.definition.toString(), keySchemaStr);
    assertEquals(storeCreationMessage.valueSchema.definition.toString(), valueSchemaStr);
  }

  @Test
  public void testCreateStoreForMultiCluster() {
    String secondCluster = "testCreateStoreForMultiCluster";
    VeniceControllerClusterConfig configForSecondCluster = mockConfig(secondCluster);
    mockResources(configForSecondCluster, secondCluster);
    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);
    configMap.put(secondCluster, configForSecondCluster);
    parentAdmin = new VeniceParentHelixAdmin(
        internalAdmin,
        new VeniceControllerMultiClusterConfig(configMap),
        mock(MetricsRepository.class));
    Map<String, VeniceWriter> writerMap = new HashMap<>();
    for (String cluster: configMap.keySet()) {
      ControllerClient mockControllerClient = mock(ControllerClient.class);
      doReturn(new ControllerResponse()).when(mockControllerClient).checkResourceCleanupForStoreCreation(anyString());
      doReturn(internalAdmin.getHelixVeniceClusterResources(clusterName)).when(internalAdmin)
          .getHelixVeniceClusterResources(cluster);

      parentAdmin.getAdminCommandExecutionTracker(cluster)
          .get()
          .getFabricToControllerClientsMap()
          .put("test-fabric", mockControllerClient);
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      // Need to bypass VeniceWriter initialization
      parentAdmin.setVeniceWriterForCluster(cluster, veniceWriter);
      writerMap.put(cluster, veniceWriter);
      parentAdmin.initStorageCluster(cluster);
    }

    for (String cluster: configMap.keySet()) {
      String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(cluster);
      String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(cluster);

      VeniceWriter veniceWriter = writerMap.get(cluster);

      // Return offset -1 before writing any data into topic.
      when(zkClient.readData(metadataPath, null)).thenReturn(null)
          .thenReturn(
              AdminTopicMetadataAccessor.generateMetadataMap(
                  Optional.of(1L),
                  Optional.of(-1L),
                  Optional.of(1L),
                  Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

      String storeName = "test-store-" + cluster;
      String owner = "test-owner-" + cluster;
      String keySchemaStr = "\"string\"";
      String valueSchemaStr = "\"string\"";
      when(veniceWriter.put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any()))
          .then(invocation -> {
            // Once we send message to topic through venice writer, return offset 1
            CompletableFuture future = mock(CompletableFuture.class);
            doReturn(new SimplePubSubProduceResultImpl(adminTopic, partitionId, mock(PubSubPosition.class), -1))
                .when(future)
                .get();
            return future;
          });

      // To support the store update during store creation.
      Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
      doReturn(store).when(internalAdmin).getStore(cluster, storeName);

      parentAdmin.createStore(cluster, storeName, owner, keySchemaStr, valueSchemaStr);

      verify(internalAdmin)
          .checkPreConditionForCreateStore(cluster, storeName, keySchemaStr, valueSchemaStr, false, false);
      verify(veniceWriter, times(2)).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
      verify(zkClient, times(3)).readData(metadataPath, null);
      ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
      verify(veniceWriter, times(2)).put(
          keyCaptor.capture(),
          valueCaptor.capture(),
          schemaCaptor.capture(),
          any(),
          any(),
          anyLong(),
          any(),
          any(),
          any(),
          any());
      byte[] keyBytes = keyCaptor.getAllValues().get(0);
      byte[] valueBytes = valueCaptor.getAllValues().get(0);
      int schemaId = schemaCaptor.getAllValues().get(0);
      assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      assertEquals(keyBytes.length, 0);
      AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
      assertEquals(adminMessage.operationType, AdminMessageType.STORE_CREATION.getValue());
      StoreCreation storeCreationMessage = (StoreCreation) adminMessage.payloadUnion;
      assertEquals(storeCreationMessage.clusterName.toString(), cluster);
      assertEquals(storeCreationMessage.storeName.toString(), storeName);
      assertEquals(storeCreationMessage.owner.toString(), owner);
      assertEquals(storeCreationMessage.keySchema.definition.toString(), keySchemaStr);
      assertEquals(storeCreationMessage.valueSchema.definition.toString(), valueSchemaStr);
    }

  }

  @Test
  public void testCreateStoreWhenExists() {
    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    doThrow(new VeniceStoreAlreadyExistsException(storeName, clusterName)).when(internalAdmin)
        .checkPreConditionForCreateStore(clusterName, storeName, keySchemaStr, valueSchemaStr, false, false);

    parentAdmin.initStorageCluster(clusterName);

    assertThrows(
        VeniceStoreAlreadyExistsException.class,
        () -> parentAdmin.createStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr));
  }

  @Test
  public void testCreateStoreWhenLastExceptionIsNotNull() {
    String storeName = "test-store";
    when(internalAdmin.getLastExceptionForStore(clusterName, storeName)).thenReturn(null)
        .thenReturn(null)
        .thenReturn(new VeniceException("mock exception"));
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.initStorageCluster(clusterName);
    // To support the store update during store creation.
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    parentAdmin.createStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    // Add store again now with an existing exception
    assertThrows(
        VeniceException.class,
        () -> parentAdmin.createStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr));
    verify(zkClient, times(3)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testSetStorePartitionCount() {
    String storeName = "test-store";
    when(internalAdmin.getLastExceptionForStore(clusterName, storeName)).thenReturn(null);
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(
        AdminTopicMetadataAccessor.generateMetadataMap(
            Optional.of(1L),
            Optional.of(-1L),
            Optional.of(1L),
            Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.initStorageCluster(clusterName);
    // To support the store update during store creation.
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    parentAdmin.createStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
    parentAdmin.setStorePartitionCount(clusterName, storeName, MAX_PARTITION_NUM);
    assertThrows(
        ConfigurationException.class,
        () -> parentAdmin.setStorePartitionCount(clusterName, storeName, MAX_PARTITION_NUM + 1));
    assertThrows(ConfigurationException.class, () -> parentAdmin.setStorePartitionCount(clusterName, storeName, -1));
    verify(zkClient, times(4)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testAddValueSchema() {
    String storeName = "test-store";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    int valueSchemaId = 10;
    String valueSchemaStr = "\"string\"";
    doReturn(valueSchemaId).when(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(
            clusterName,
            storeName,
            valueSchemaStr,
            DirectionalSchemaCompatibilityType.FULL);
    doReturn(valueSchemaId).when(internalAdmin).getValueSchemaId(clusterName, storeName, valueSchemaStr);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);

    verify(internalAdmin).checkPreConditionForAddValueSchemaAndGetNewSchemaId(
        clusterName,
        storeName,
        valueSchemaStr,
        DirectionalSchemaCompatibilityType.FULL);
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.VALUE_SCHEMA_CREATION.getValue());

    ValueSchemaCreation valueSchemaCreationMessage = (ValueSchemaCreation) adminMessage.payloadUnion;
    assertEquals(valueSchemaCreationMessage.clusterName.toString(), clusterName);
    assertEquals(valueSchemaCreationMessage.storeName.toString(), storeName);
    assertEquals(valueSchemaCreationMessage.schema.definition.toString(), valueSchemaStr);
    assertEquals(valueSchemaCreationMessage.schemaId, valueSchemaId);
  }

  @Test
  public void testAddDerivedSchema() {
    String storeName = "test-store";
    String derivedSchemaStr = "\"string\"";
    int valueSchemaId = 10;
    int derivedSchemaId = 1;

    doReturn(derivedSchemaId).when(internalAdmin)
        .checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    doReturn(new GeneratedSchemaID(valueSchemaId, derivedSchemaId)).when(internalAdmin)
        .getDerivedSchemaId(clusterName, storeName, derivedSchemaStr);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addDerivedSchema(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter)
        .put(any(), valueCaptor.capture(), schemaCaptor.capture(), any(), any(), anyLong(), any(), any(), any(), any());

    AdminOperation adminMessage =
        adminOperationSerializer.deserialize(ByteBuffer.wrap(valueCaptor.getValue()), schemaCaptor.getValue());
    DerivedSchemaCreation derivedSchemaCreation = (DerivedSchemaCreation) adminMessage.payloadUnion;

    assertEquals(derivedSchemaCreation.clusterName.toString(), clusterName);
    assertEquals(derivedSchemaCreation.storeName.toString(), storeName);
    assertEquals(derivedSchemaCreation.schema.definition.toString(), derivedSchemaStr);
    assertEquals(derivedSchemaCreation.valueSchemaId, valueSchemaId);
    assertEquals(derivedSchemaCreation.derivedSchemaId, derivedSchemaId);
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testDisableStoreRead() {
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    String storeName = "test-store";
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.setStoreReadability(clusterName, storeName, false);

    verify(internalAdmin).checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.DISABLE_STORE_READ.getValue());

    DisableStoreRead disableStoreRead = (DisableStoreRead) adminMessage.payloadUnion;
    assertEquals(disableStoreRead.clusterName.toString(), clusterName);
    assertEquals(disableStoreRead.storeName.toString(), storeName);
  }

  @Test
  public void testDisableStoreWrite() {
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    String storeName = "test-store";
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.setStoreWriteability(clusterName, storeName, false);

    verify(internalAdmin).checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.DISABLE_STORE_WRITE.getValue());

    PauseStore pauseStore = (PauseStore) adminMessage.payloadUnion;
    assertEquals(pauseStore.clusterName.toString(), clusterName);
    assertEquals(pauseStore.storeName.toString(), storeName);
  }

  @Test
  public void testDisableStoreWriteWhenStoreDoesNotExist() {
    String storeName = "test-store";
    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

    parentAdmin.initStorageCluster(clusterName);
    assertThrows(VeniceNoStoreException.class, () -> parentAdmin.setStoreWriteability(clusterName, storeName, false));
  }

  @Test
  public void testEnableStoreRead() {
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    String storeName = "test-store";
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.setStoreReadability(clusterName, storeName, true);

    verify(internalAdmin).checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.ENABLE_STORE_READ.getValue());

    EnableStoreRead enableStoreRead = (EnableStoreRead) adminMessage.payloadUnion;
    assertEquals(enableStoreRead.clusterName.toString(), clusterName);
    assertEquals(enableStoreRead.storeName.toString(), storeName);
  }

  @Test
  public void testEnableStoreWrite() {
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
    String storeName = "test-store";
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.setStoreWriteability(clusterName, storeName, true);

    verify(internalAdmin).checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.ENABLE_STORE_WRITE.getValue());

    ResumeStore resumeStore = (ResumeStore) adminMessage.payloadUnion;
    assertEquals(resumeStore.clusterName.toString(), clusterName);
    assertEquals(resumeStore.storeName.toString(), storeName);
  }

  @Test
  public void testKillOfflinePushJob() {
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic("test_store_v1");
    doReturn(new HashSet<>(Arrays.asList(pubSubTopic))).when(topicManager).listTopics();

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
    Store store = mock(Store.class);
    doReturn(store).when(internalAdmin).getStore(clusterName, pubSubTopic.getStoreName());
    doReturn(ConcurrentPushDetectionStrategy.TOPIC_BASED_ONLY).when(config).getConcurrentPushDetectionStrategy();

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.killOfflinePush(clusterName, pubSubTopic.getName(), false);

    verify(internalAdmin).checkPreConditionForKillOfflinePush(clusterName, pubSubTopic.getName());
    verify(internalAdmin).truncateKafkaTopic(pubSubTopic.getName());
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.KILL_OFFLINE_PUSH_JOB.getValue());

    KillOfflinePushJob killJob = (KillOfflinePushJob) adminMessage.payloadUnion;
    assertEquals(killJob.clusterName.toString(), clusterName);
    assertEquals(killJob.kafkaTopic.toString(), pubSubTopic.getName());
  }

  @Test
  public void testIdempotentIncrementVersionWhenNoPreviousTopics() {
    String pushJobId = Utils.getUniqueString("push_job_id");
    doReturn(new Pair<>(true, new VersionImpl(storeName, 1, pushJobId))).when(internalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeName,
            pushJobId,
            VERSION_ID_UNSET,
            1,
            1,
            true,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            0,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(0).when(store).getLargestUsedRTVersionNumber();
    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        new PartialMockVeniceParentHelixAdmin(internalAdmin, config)) {
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);

      doReturn(
          CompletableFuture.completedFuture(
              new SimplePubSubProduceResultImpl(topicName, partitionId, mock(PubSubPosition.class), -1)))
                  .when(veniceWriter)
                  .put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
      when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
          .thenReturn(
              AdminTopicMetadataAccessor.generateMetadataMap(
                  Optional.of(1L),
                  Optional.of(-1L),
                  Optional.of(1L),
                  Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
      partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1);
      verify(internalAdmin).addVersionAndTopicOnly(
          clusterName,
          storeName,
          pushJobId,
          VERSION_ID_UNSET,
          1,
          1,
          true,
          false,
          Version.PushType.BATCH,
          null,
          null,
          Optional.empty(),
          -1,
          0,
          Optional.empty(),
          false,
          null,
          -1,
          DEFAULT_RT_VERSION_NUMBER,
          -1);
      verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
    }
  }

  /**
   * This class is used to assist unit test for {@link VeniceParentHelixAdmin#incrementVersionIdempotent(String, String, String, int, int)}
   * to mock various offline job status.
   */
  private static class PartialMockVeniceParentHelixAdmin extends VeniceParentHelixAdmin {
    private ExecutionStatus offlineJobStatus = ExecutionStatus.NOT_CREATED;

    /**
     * Key: store version
     * Value: True -> the corresponding push job is killed
     *        False -> the corresponding push job is still running
     */
    private Map<String, Boolean> storeVersionToKillJobStatus = new HashMap<>();

    public PartialMockVeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerClusterConfig config) {
      super(veniceHelixAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config), mock(MetricsRepository.class));
    }

    public void setOfflineJobStatus(ExecutionStatus executionStatus) {
      this.offlineJobStatus = executionStatus;
    }

    @Override
    public void killOfflinePush(String clusterName, String kafkaTopic, boolean isForcedKill) {
      storeVersionToKillJobStatus.put(kafkaTopic, true);
    }

    public boolean isJobKilled(String kafkaTopic) {
      if (storeVersionToKillJobStatus.containsKey(kafkaTopic)) {
        return storeVersionToKillJobStatus.get(kafkaTopic);
      }
      return false;
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
      return new OfflinePushStatusInfo(offlineJobStatus);
    }
  }

  @Test
  public void testIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsStillRunning() {
    String storeName = Utils.getUniqueString("test_store");
    PubSubTopic previousKafkaTopic = pubSubTopicRepository.getTopic(storeName + "_v1");
    PubSubTopic unknownTopic = pubSubTopicRepository.getTopic("1unknown_topic_v1");
    doReturn(new HashSet<>(Arrays.asList(unknownTopic, previousKafkaTopic))).when(topicManager).listTopics();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    String pushJobId = "test_push_id";
    String pushJobId2 = "test_push_id2";
    store.addVersion(new VersionImpl(storeName, 1, pushJobId));
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(new StoreVersionInfo(store, store.getVersion(1))).when(internalAdmin)
        .waitVersion(eq(clusterName), eq(storeName), eq(1), any());

    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        new PartialMockVeniceParentHelixAdmin(internalAdmin, config)) {
      partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.PROGRESS);

      assertThrows(
          VeniceException.class,
          () -> partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId2, 1, 1));
    }
  }

  /**
   * Idempotent increment version should work because existing topic uses the same push ID as the request
   */
  @Test
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsNotDoneForSamePushId() {
    String storeName = Utils.getUniqueString("test_store");
    String pushJobId = Utils.getUniqueString("push_job_id");
    PubSubTopic previousPubSubTopic = pubSubTopicRepository.getTopic(storeName + "_v1");
    doReturn(new HashSet<>(Arrays.asList(previousPubSubTopic))).when(topicManager).listTopics();
    Store store = spy(
        new ZKStore(
            storeName,
            "owner",
            System.currentTimeMillis(),
            PersistenceType.IN_MEMORY,
            RoutingStrategy.CONSISTENT_HASH,
            ReadStrategy.ANY_OF_ONLINE,
            OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
            1));
    Version version = new VersionImpl(storeName, 1, pushJobId);
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(version).when(store).getVersion(1);
    doReturn(new StoreVersionInfo(store, version)).when(internalAdmin)
        .waitVersion(eq(clusterName), eq(storeName), eq(version.getNumber()), any());
    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        new PartialMockVeniceParentHelixAdmin(internalAdmin, config)) {
      partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
      doReturn(
          CompletableFuture.completedFuture(
              new SimplePubSubProduceResultImpl(topicName, partitionId, mock(PubSubPosition.class), -1)))
                  .when(veniceWriter)
                  .put(any(), any(), anyInt());
      Version newVersion = partialMockParentAdmin.incrementVersionIdempotent(
          clusterName,
          storeName,
          pushJobId,
          1,
          1,
          Version.PushType.BATCH,
          false,
          false,
          null,
          Optional.empty(),
          Optional.empty(),
          -1,
          Optional.empty(),
          false,
          null,
          -1,
          -1);
      verify(internalAdmin, never()).addVersionAndTopicOnly(
          clusterName,
          storeName,
          pushJobId,
          VERSION_ID_UNSET,
          1,
          1,
          false,
          false,
          Version.PushType.BATCH,
          null,
          null,
          Optional.empty(),
          -1,
          1,
          Optional.empty(),
          false,
          null,
          -1,
          DEFAULT_RT_VERSION_NUMBER,
          -1);
      assertEquals(newVersion, version);
    }
  }

  /**
   * Idempotent increment version should work because existing topic is truncated
   */
  @Test(expectedExceptions = ConcurrentBatchPushException.class)
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistButTruncated() {
    String storeName = Utils.getUniqueString("test_store");
    String pushJobId = Utils.getUniqueString("push_job_id");
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    Version version = new VersionImpl(storeName, 1, pushJobId + "_different");
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(new Pair<>(true, new VersionImpl(storeName, 1, pushJobId))).when(internalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeName,
            pushJobId,
            VERSION_ID_UNSET,
            1,
            1,
            false,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            1,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);
    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        new PartialMockVeniceParentHelixAdmin(internalAdmin, config)) {
      partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
      doReturn(
          CompletableFuture.completedFuture(
              new SimplePubSubProduceResultImpl(topicName, partitionId, mock(PubSubPosition.class), -1)))
                  .when(veniceWriter)
                  .put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
      when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
          .thenReturn(
              AdminTopicMetadataAccessor.generateMetadataMap(
                  Optional.of(1L),
                  Optional.of(-1L),
                  Optional.of(1L),
                  Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
      partialMockParentAdmin.incrementVersionIdempotent(
          clusterName,
          storeName,
          pushJobId,
          1,
          1,
          Version.PushType.BATCH,
          false,
          false,
          null,
          Optional.empty(),
          Optional.empty(),
          -1,
          Optional.empty(),
          false,
          null,
          -1,
          -1);
      verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
    }
  }

  /**
   * Idempotent increment version should NOT work because existing topic uses different push ID than the request
   */
  @Test
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsNotDoneForDifferentPushId() {
    String storeName = Utils.getUniqueString("test_store");
    String pushJobId = Utils.getUniqueString("push_job_id");
    PubSubTopic previousPubSubTopic = pubSubTopicRepository.getTopic(storeName + "_v1");
    doReturn(new HashSet<>(Arrays.asList(previousPubSubTopic))).when(topicManager).listTopics();
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    Version version = new VersionImpl(storeName, 1, Version.guidBasedDummyPushId());
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(new StoreVersionInfo(store, version)).when(internalAdmin)
        .waitVersion(eq(clusterName), eq(storeName), eq(version.getNumber()), any());
    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        new PartialMockVeniceParentHelixAdmin(internalAdmin, config)) {
      partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
      try {
        partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1);
      } catch (VeniceException e) {
        Assert.assertTrue(
            e.getMessage().contains(pushJobId),
            "Exception for topic exists when increment version should contain requested pushId");
      }
    }
  }

  @Test
  public void testIdempotentIncrementVersionPreviousTopicsDoNotExistVersionExistsForSamePushId() {
    String storeName = Utils.getUniqueString("test_store");
    String pushJobId = Utils.getUniqueString("push_job_id");
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    Version version = new VersionImpl(storeName, 1, pushJobId);
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(new Pair<>(false, version)).when(internalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeName,
            pushJobId,
            VERSION_ID_UNSET,
            1,
            1,
            false,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            1,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);
    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        spy(new PartialMockVeniceParentHelixAdmin(internalAdmin, config))) {
      Version newVersion = partialMockParentAdmin.incrementVersionIdempotent(
          clusterName,
          storeName,
          pushJobId,
          1,
          1,
          Version.PushType.BATCH,
          false,
          false,
          null,
          Optional.empty(),
          Optional.empty(),
          -1,
          Optional.empty(),
          false,
          null,
          -1,
          -1);
      verify(partialMockParentAdmin, never()).sendAddVersionAdminMessage(
          clusterName,
          storeName,
          pushJobId,
          newVersion,
          1,
          Version.PushType.BATCH,
          null,
          -1,
          DEFAULT_RT_VERSION_NUMBER);
      assertEquals(newVersion.getNumber(), version.getNumber());
    }
  }

  @Test
  public void testIdempotentIncrementVersionWhenPreviousPushIsARepushAndIncomingPushIsABatchPush() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin mockInternalAdmin = mock(VeniceHelixAdmin.class);

    doReturn(mockInternalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    Version version = new VersionImpl(storeName, 1, Version.generateRePushId("test_push_id"));
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);

    doReturn(
        (LingeringStoreVersionChecker) (
            store1,
            version1,
            time,
            controllerAdmin,
            requesterCert,
            identityParser) -> false).when(mockParentAdmin).getLingeringStoreVersionChecker();
    doReturn(mock(UserSystemStoreLifeCycleHelper.class)).when(mockParentAdmin).getSystemStoreLifeCycleHelper();
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    doReturn(Optional.of(version.kafkaTopicName())).when(mockParentAdmin)
        .getTopicForCurrentPushJob(eq(clusterName), eq(storeName), anyBoolean(), anyBoolean());

    String incomingPushId = "TEST_BATCH_PUSH";
    doCallRealMethod().when(mockParentAdmin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1);

    Version version2 = new VersionImpl(storeName, 2, incomingPushId);
    doReturn(new Pair(true, version2)).when(mockInternalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeName,
            incomingPushId,
            VERSION_ID_UNSET,
            1,
            1,
            false,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            1,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);

    HelixVeniceClusterResources mockHelixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(mockHelixVeniceClusterResources).when(mockInternalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(mock(VeniceAdminStats.class)).when(mockHelixVeniceClusterResources).getVeniceAdminStats();

    mockParentAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        incomingPushId,
        1,
        1,
        Version.PushType.BATCH,
        false,
        false,
        null,
        Optional.empty(),
        Optional.empty(),
        -1,
        Optional.empty(),
        false,
        null,
        -1,
        -1);

    verify(mockParentAdmin, times(1)).killOfflinePush(clusterName, version.kafkaTopicName(), true);
  }

  @Test
  public void testIdempotentIncrementVersionWhenPreviousPushIsARepushAndIncomingPushIsARepush() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin mockInternalAdmin = mock(VeniceHelixAdmin.class);

    doReturn(mockInternalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    Version version = new VersionImpl(storeName, 1, Version.generateRePushId("test_push_id"));
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);

    doReturn(
        (LingeringStoreVersionChecker) (
            store1,
            version1,
            time,
            controllerAdmin,
            requesterCert,
            identityParser) -> false).when(mockParentAdmin).getLingeringStoreVersionChecker();
    doReturn(mock(UserSystemStoreLifeCycleHelper.class)).when(mockParentAdmin).getSystemStoreLifeCycleHelper();
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    doReturn(Optional.of(version.kafkaTopicName())).when(mockParentAdmin)
        .getTopicForCurrentPushJob(eq(clusterName), eq(storeName), anyBoolean(), anyBoolean());

    String incomingPushId = Version.generateRePushId("TEST_BATCH_PUSH");
    doCallRealMethod().when(mockParentAdmin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1);

    Version version2 = new VersionImpl(storeName, 2, incomingPushId);
    doReturn(new Pair(true, version2)).when(mockInternalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeName,
            incomingPushId,
            VERSION_ID_UNSET,
            1,
            1,
            false,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            1,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);

    HelixVeniceClusterResources mockHelixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(mockHelixVeniceClusterResources).when(mockInternalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(mock(VeniceAdminStats.class)).when(mockHelixVeniceClusterResources).getVeniceAdminStats();

    assertThrows(
        VeniceException.class,
        () -> mockParentAdmin.incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1));

    verify(mockParentAdmin, never()).killOfflinePush(clusterName, version.kafkaTopicName(), true);
  }

  @Test
  public void testIdempotentIncrementVersionWhenPreviousPushIsARepushAndIncomingPushIsAnIncPushToRT() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin mockInternalAdmin = mock(VeniceHelixAdmin.class);

    doReturn(mockInternalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    Version version = new VersionImpl(storeName, 1, Version.generateRePushId("test_push_id"));
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);

    doReturn(
        (LingeringStoreVersionChecker) (
            store1,
            version1,
            time,
            controllerAdmin,
            requesterCert,
            identityParser) -> false).when(mockParentAdmin).getLingeringStoreVersionChecker();
    doReturn(mock(UserSystemStoreLifeCycleHelper.class)).when(mockParentAdmin).getSystemStoreLifeCycleHelper();
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    doReturn(Optional.of(version.kafkaTopicName())).when(mockParentAdmin)
        .getTopicForCurrentPushJob(eq(clusterName), eq(storeName), anyBoolean(), anyBoolean());

    String incomingPushId = "TEST_INCREMENTAL_PUSH";
    doCallRealMethod().when(mockParentAdmin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1);

    HelixVeniceClusterResources mockHelixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(mockHelixVeniceClusterResources).when(mockInternalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(mock(VeniceAdminStats.class)).when(mockHelixVeniceClusterResources).getVeniceAdminStats();

    mockParentAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        incomingPushId,
        1,
        1,
        Version.PushType.INCREMENTAL,
        false,
        false,
        null,
        Optional.empty(),
        Optional.empty(),
        -1,
        Optional.empty(),
        false,
        null,
        -1,
        -1);

    verify(mockParentAdmin, never()).killOfflinePush(clusterName, version.kafkaTopicName(), true);
  }

  @Test
  public void testIdempotentIncrementVersionWhenPreviousPushIsACompliancePushAndIncomingPushIsABatchPush() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin mockInternalAdmin = mock(VeniceHelixAdmin.class);

    doReturn(mockInternalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    // Create an ongoing compliance push
    Version version = new VersionImpl(storeName, 1, Version.generateCompliancePushId("compliance_push_id"));
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);

    doReturn(
        (LingeringStoreVersionChecker) (
            store1,
            version1,
            time,
            controllerAdmin,
            requesterCert,
            identityParser) -> false).when(mockParentAdmin).getLingeringStoreVersionChecker();
    doReturn(mock(UserSystemStoreLifeCycleHelper.class)).when(mockParentAdmin).getSystemStoreLifeCycleHelper();
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    doReturn(Optional.of(version.kafkaTopicName())).when(mockParentAdmin)
        .getTopicForCurrentPushJob(eq(clusterName), eq(storeName), anyBoolean(), anyBoolean());

    // User-initiated batch push should be able to kill the compliance push
    String incomingPushId = "USER_BATCH_PUSH";
    doCallRealMethod().when(mockParentAdmin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1);

    Version version2 = new VersionImpl(storeName, 2, incomingPushId);
    doReturn(new Pair(true, version2)).when(mockInternalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeName,
            incomingPushId,
            VERSION_ID_UNSET,
            1,
            1,
            false,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            1,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);

    HelixVeniceClusterResources mockHelixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(mockHelixVeniceClusterResources).when(mockInternalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(mock(VeniceAdminStats.class)).when(mockHelixVeniceClusterResources).getVeniceAdminStats();

    mockParentAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        incomingPushId,
        1,
        1,
        Version.PushType.BATCH,
        false,
        false,
        null,
        Optional.empty(),
        Optional.empty(),
        -1,
        Optional.empty(),
        false,
        null,
        -1,
        -1);

    // Verify that the compliance push was killed
    verify(mockParentAdmin, times(1)).killOfflinePush(clusterName, version.kafkaTopicName(), true);
  }

  @Test
  public void testIdempotentIncrementVersionWhenPreviousPushIsACompliancePushAndIncomingPushIsAlsoCompliancePush() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin mockInternalAdmin = mock(VeniceHelixAdmin.class);

    doReturn(mockInternalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    // Create an ongoing compliance push
    Version version = new VersionImpl(storeName, 1, Version.generateCompliancePushId("compliance_push_id"));
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);

    doReturn(
        (LingeringStoreVersionChecker) (
            store1,
            version1,
            time,
            controllerAdmin,
            requesterCert,
            identityParser) -> false).when(mockParentAdmin).getLingeringStoreVersionChecker();
    doReturn(mock(UserSystemStoreLifeCycleHelper.class)).when(mockParentAdmin).getSystemStoreLifeCycleHelper();
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    doReturn(Optional.of(version.kafkaTopicName())).when(mockParentAdmin)
        .getTopicForCurrentPushJob(eq(clusterName), eq(storeName), anyBoolean(), anyBoolean());

    // Another compliance push should NOT be able to kill the existing compliance push
    String incomingPushId = Version.generateCompliancePushId("another_compliance_push");
    doCallRealMethod().when(mockParentAdmin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1);

    HelixVeniceClusterResources mockHelixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(mockHelixVeniceClusterResources).when(mockInternalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(mock(VeniceAdminStats.class)).when(mockHelixVeniceClusterResources).getVeniceAdminStats();

    // Should throw ConcurrentBatchPushException
    assertThrows(
        VeniceException.class,
        () -> mockParentAdmin.incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1));

    // Verify that killOfflinePush was never called
    verify(mockParentAdmin, never()).killOfflinePush(clusterName, version.kafkaTopicName(), true);
  }

  @Test
  public void testCompliancePushCannotKillUserPush() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin mockInternalAdmin = mock(VeniceHelixAdmin.class);

    doReturn(mockInternalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);

    // Create an ongoing user push
    String userPushId = System.currentTimeMillis() + "_https://example.com/user-job";
    Version version = new VersionImpl(storeName, 1, userPushId);
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);

    doReturn(
        (LingeringStoreVersionChecker) (
            store1,
            version1,
            time,
            controllerAdmin,
            requesterCert,
            identityParser) -> false).when(mockParentAdmin).getLingeringStoreVersionChecker();
    doReturn(mock(UserSystemStoreLifeCycleHelper.class)).when(mockParentAdmin).getSystemStoreLifeCycleHelper();
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    doReturn(Optional.of(version.kafkaTopicName())).when(mockParentAdmin)
        .getTopicForCurrentPushJob(eq(clusterName), eq(storeName), anyBoolean(), anyBoolean());

    // Compliance push should NOT be able to kill the user push
    String incomingPushId = Version.generateCompliancePushId("compliance_push");
    doCallRealMethod().when(mockParentAdmin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            incomingPushId,
            1,
            1,
            Version.PushType.BATCH,
            false,
            false,
            null,
            Optional.empty(),
            Optional.empty(),
            -1,
            Optional.empty(),
            false,
            null,
            -1,
            -1);

    HelixVeniceClusterResources mockHelixVeniceClusterResources = mock(HelixVeniceClusterResources.class);
    doReturn(mockHelixVeniceClusterResources).when(mockInternalAdmin).getHelixVeniceClusterResources(clusterName);
    doReturn(mock(VeniceAdminStats.class)).when(mockHelixVeniceClusterResources).getVeniceAdminStats();

    // Should throw VeniceException because compliance push cannot kill user push
    try {
      mockParentAdmin.incrementVersionIdempotent(
          clusterName,
          storeName,
          incomingPushId,
          1,
          1,
          Version.PushType.BATCH,
          false,
          false,
          null,
          Optional.empty(),
          Optional.empty(),
          -1,
          Optional.empty(),
          false,
          null,
          -1,
          -1);
      fail("Expected VeniceException to be thrown");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("is found and it must be terminated before another push can be started"));
    }

    // Verify that killOfflinePush was never called
    verify(mockParentAdmin, never()).killOfflinePush(clusterName, version.kafkaTopicName(), true);
  }

  @Test
  public void testStoreVersionCleanUpWithFewerVersions() {
    String storeName = "test_store";
    Store testStore = new ZKStore(
        storeName,
        "test_owner",
        -1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    testStore.addVersion(new VersionImpl(storeName, 1));
    testStore.addVersion(new VersionImpl(storeName, 2));
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(testStore).when(storeRepo).getStore(storeName);
    doReturn(storeRepo).when(resources).getStoreMetadataRepository();
    doReturn(5).when(config).getUserStoreVersionRetentionCount();
    parentAdmin.cleanupHistoricalVersions(clusterName, storeName);
    verify(storeRepo).getStore(storeName);
    verify(storeRepo, never()).updateStore(any());
  }

  @Test
  public void testStoreVersionCleanUpWithMoreVersions() {
    String storeName = "test_store";
    Store testStore = new ZKStore(
        storeName,
        "test_owner",
        -1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    for (int i = 1; i <= 10; ++i) {
      testStore.addVersion(new VersionImpl(storeName, i));
    }
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(testStore).when(storeRepo).getStore(storeName);
    doReturn(storeRepo).when(resources).getStoreMetadataRepository();
    doReturn(5).when(config).getUserStoreVersionRetentionCount();
    mockControllerClients(storeName);

    parentAdmin.cleanupHistoricalVersions(clusterName, storeName);
    verify(storeRepo).getStore(storeName);
    ArgumentCaptor<Store> storeCaptor = ArgumentCaptor.forClass(Store.class);
    verify(storeRepo).updateStore(storeCaptor.capture());
    Store capturedStore = storeCaptor.getValue();
    int storeVersionRetentionCount =
        parentAdmin.getMultiClusterConfigs().getCommonConfig().getUserStoreVersionRetentionCount();
    assertEquals(capturedStore.getVersions().size(), storeVersionRetentionCount);

    for (int i = 1; i <= 3; ++i) {
      Assert.assertFalse(capturedStore.containsVersion(i));
    }
    // child region current versions 4,5,6 are persisted
    for (int i = 4; i <= 6; ++i) {
      Assert.assertTrue(capturedStore.containsVersion(i));
    }
    // last two probably failed pushes are persisted.
    for (int i = 9; i <= 10; ++i) {
      Assert.assertTrue(capturedStore.containsVersion(i));
    }
  }

  private void mockControllerClients(String storeName) {
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    Map<String, String> map = new HashMap<>();
    map.put(storeName, "1");

    for (int i = 0; i < NUM_REGIONS; i++) {
      ControllerClient client = mock(ControllerClient.class);
      StoreResponse storeResponse = new StoreResponse();
      Store s = TestUtils.createTestStore("s" + i, "test", System.currentTimeMillis());
      s.setCurrentVersion(i + 4); // child region current versions 4,5,6
      storeResponse.setStore(StoreInfo.fromStore(s));
      MultiStoreStatusResponse storeStatusResponse = mock(MultiStoreStatusResponse.class);
      doReturn(map).when(storeStatusResponse).getStoreStatusMap();
      doReturn(storeStatusResponse).when(client).getFutureVersions(anyString(), anyString());
      doReturn(storeResponse).when(client).getStore(anyString());
      controllerClientMap.put("region" + i, client);
    }

    doReturn(controllerClientMap).when(internalAdmin).getControllerClientMap(anyString());
  }

  // get a map of mock client that can return vairable execution status
  Map<ExecutionStatus, ControllerClient> getMockJobStatusQueryClient() {
    Map<ExecutionStatus, ControllerClient> clientMap = new HashMap<>();
    for (ExecutionStatus status: ExecutionStatus.values()) {
      JobStatusQueryResponse response = new JobStatusQueryResponse();
      response.setStatus(status.toString());
      ControllerClient statusClient = mock(ControllerClient.class);
      doReturn(response).when(statusClient).queryJobStatus(anyString(), any());
      clientMap.put(status, statusClient);
    }

    return clientMap;
  }

  @Test
  public void testGetIncrementalPushVersion() {
    String storeName = "testStore";
    parentAdmin.getStore(storeName, clusterName);
    Version incrementalPushVersion = new VersionImpl(storeName, 1);
    assertEquals(
        parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.COMPLETED),
        incrementalPushVersion);

    try {
      parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.STARTED);
      fail();
    } catch (VeniceException e) {
    }

    try {
      parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.ERROR);
      fail();
    } catch (VeniceException e) {
    }

    doReturn(false).when(internalAdmin).isTopicTruncated(eq(Utils.composeRealTimeTopic(storeName)));
    assertEquals(
        parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.COMPLETED),
        incrementalPushVersion);

    doReturn(true).when(internalAdmin).isTopicTruncated(anyString());
    assertThrows(
        VeniceException.class,
        () -> parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.COMPLETED));
  }

  @Test
  public void testGetExecutionStatus() {
    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put("IGNORED", config);
    configMap.put("mycluster", config);
    parentAdmin = new VeniceParentHelixAdmin(
        internalAdmin,
        new VeniceControllerMultiClusterConfig(configMap),
        mock(MetricsRepository.class));

    Map<ExecutionStatus, ControllerClient> clientMap = getMockJobStatusQueryClient();
    JobStatusQueryResponse failResponse = new JobStatusQueryResponse();
    failResponse.setError("error");
    ControllerClient failClient = mock(ControllerClient.class);
    doReturn(failResponse).when(failClient).queryJobStatus(anyString(), any());
    clientMap.put(null, failClient);

    // Completely failing client that cannot even complete leadership discovery.
    ControllerClient completelyFailingClient = mock(ControllerClient.class);
    doReturn(failResponse).when(completelyFailingClient).queryJobStatus(anyString(), any());
    String completelyFailingExceptionMessage = "Unable to discover leader controller";
    doThrow(new VeniceException(completelyFailingExceptionMessage)).when(completelyFailingClient)
        .getLeaderControllerUrl();

    // Verify clients work as expected
    for (ExecutionStatus status: ExecutionStatus.values()) {
      assertEquals(clientMap.get(status).queryJobStatus("topic", Optional.empty()).getStatus(), status.toString());
    }
    Assert.assertTrue(clientMap.get(null).queryJobStatus("topic", Optional.empty()).isError());

    Map<String, ControllerClient> completeMap = new HashMap<>();
    completeMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    completeMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    completeMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    Store store = mock(Store.class);
    doReturn(false).when(store).isIncrementalPushEnabled();
    doReturn(null).when(store).getVersion(anyInt());
    doReturn(VersionStatus.STARTED).when(store).getVersionStatus(anyInt());
    doReturn(store).when(internalAdmin).getStore(anyString(), anyString());
    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    doReturn(mock(ClusterLockManager.class)).when(resources).getClusterLockManager();
    doReturn(resources).when(internalAdmin).getHelixVeniceClusterResources(anyString());
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(store).when(repository).getStore(anyString());
    Version version = mock(Version.class);
    doReturn(version).when(store).getVersion(anyInt());
    doReturn(VersionStatus.CREATED).when(version).getStatus();
    doReturn(Version.PushType.BATCH).when(version).getPushType();
    Admin.OfflinePushStatusInfo offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic1_v1", completeMap);
    Map<String, String> extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.COMPLETED);
    assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());

    completeMap.put("cluster-slow", clientMap.get(ExecutionStatus.NOT_CREATED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic2_v1", completeMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NOT_CREATED); // Do we want this to be
                                                                                      // Progress? limitation of
                                                                                      // ordering used in
                                                                                      // aggregation code
    assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster-slow"), ExecutionStatus.NOT_CREATED.toString());

    Map<String, ControllerClient> progressMap = new HashMap<>();
    progressMap.put("cluster", clientMap.get(ExecutionStatus.NOT_CREATED));
    progressMap.put("cluster3", clientMap.get(ExecutionStatus.NOT_CREATED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic3_v1", progressMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NOT_CREATED);
    verify(internalAdmin, never()).truncateKafkaTopic("topic3_v1");
    assertEquals(extraInfo.get("cluster"), ExecutionStatus.NOT_CREATED.toString());
    assertEquals(extraInfo.get("cluster3"), ExecutionStatus.NOT_CREATED.toString());

    progressMap.put("cluster5", clientMap.get(ExecutionStatus.NEW));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic4_v1", progressMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NEW);
    verify(internalAdmin, never()).truncateKafkaTopic("topic4_v1");
    assertEquals(extraInfo.get("cluster"), ExecutionStatus.NOT_CREATED.toString());
    assertEquals(extraInfo.get("cluster3"), ExecutionStatus.NOT_CREATED.toString());
    assertEquals(extraInfo.get("cluster5"), ExecutionStatus.NEW.toString());

    progressMap.put("cluster7", clientMap.get(ExecutionStatus.PROGRESS));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic5_v1", progressMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic5_v1");
    ;
    assertEquals(extraInfo.get("cluster7"), ExecutionStatus.PROGRESS.toString());

    progressMap.put("cluster9", clientMap.get(ExecutionStatus.STARTED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic6_v1", progressMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic6_v1");
    assertEquals(extraInfo.get("cluster9"), ExecutionStatus.STARTED.toString());

    progressMap.put("cluster11", clientMap.get(ExecutionStatus.END_OF_PUSH_RECEIVED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic7_v1", progressMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic7_v1");
    assertEquals(extraInfo.get("cluster11"), ExecutionStatus.END_OF_PUSH_RECEIVED.toString());

    progressMap.put("cluster13", clientMap.get(ExecutionStatus.COMPLETED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic8_v1", progressMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic8_v1");
    assertEquals(extraInfo.get("cluster13"), ExecutionStatus.COMPLETED.toString());

    Map<String, ControllerClient> failCompleteMap = new HashMap<>();
    failCompleteMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("failcluster", clientMap.get(null));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic8_v1", failCompleteMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.UNKNOWN);
    assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("failcluster"), ExecutionStatus.UNKNOWN.toString());

    // 2 problematic fabrics. One is failing completely and one is returning error response. It should still get the
    // status of other fabrics and return UNKNOWN for the unreachable fabrics.
    failCompleteMap.clear();
    failCompleteMap.put("fabric1", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("fabric2", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("failFabric", clientMap.get(null));
    failCompleteMap.put("completelyFailingFabric", completelyFailingClient);
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic8_v1", failCompleteMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(extraInfo.get("fabric1"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("fabric2"), ExecutionStatus.COMPLETED.toString());
    assertEquals(extraInfo.get("failFabric"), ExecutionStatus.UNKNOWN.toString());
    assertEquals(extraInfo.get("completelyFailingFabric"), ExecutionStatus.UNKNOWN.toString());
    Assert.assertTrue(
        offlineJobStatus.getExtraDetails().get("completelyFailingFabric").contains(completelyFailingExceptionMessage));

    Map<String, ControllerClient> errorMap = new HashMap<>();
    errorMap.put("cluster-err", clientMap.get(ExecutionStatus.ERROR));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic10_v1", errorMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    assertEquals(extraInfo.get("cluster-err"), ExecutionStatus.ERROR.toString());

    errorMap.put("cluster-complete", clientMap.get(ExecutionStatus.COMPLETED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic11_v1", errorMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    assertEquals(extraInfo.get("cluster-complete"), ExecutionStatus.COMPLETED.toString());

    // Test whether errored topics will be truncated or not when 'maxErroredTopicNumToKeep' is > 0.
    parentAdmin.setMaxErroredTopicNumToKeep(2);
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic12_v1", errorMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    verify(internalAdmin, never()).truncateKafkaTopic("topic12_v1");
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    // Reset
    parentAdmin.setMaxErroredTopicNumToKeep(0);

    errorMap.put("cluster-new", clientMap.get(ExecutionStatus.NEW));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic13_v1", errorMap);
    extraInfo = offlineJobStatus.getExtraInfo();
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NEW); // Do we want this to be Progress?
                                                                              // limitation of ordering used in
                                                                              // aggregation code
    assertEquals(extraInfo.get("cluster-new"), ExecutionStatus.NEW.toString());

    doReturn(true).when(store).isIncrementalPushEnabled();
    doReturn(store).when(internalAdmin).getStore(anyString(), anyString());
    completeMap.remove("cluster-slow");
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic2_v1", completeMap);
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.COMPLETED);
  }

  @Test
  public void testKilledVersionExecutionStatus() {
    Map<ExecutionStatus, ControllerClient> clientMap = getMockJobStatusQueryClient();
    TopicManager topicManager = mock(TopicManager.class);

    Map<String, ControllerClient> notCreatedMap = new HashMap<>();
    notCreatedMap.put("cluster", clientMap.get(ExecutionStatus.ERROR));
    notCreatedMap.put("cluster2", clientMap.get(ExecutionStatus.NOT_CREATED));
    notCreatedMap.put("cluster3", clientMap.get(ExecutionStatus.ERROR));

    Set<PubSubTopic> pubSubTopics = new HashSet<>();
    pubSubTopics.add(pubSubTopicRepository.getTopic("topic_v1"));
    doReturn(pubSubTopics).when(topicManager).listTopics();

    Store store = mock(Store.class);
    doReturn(false).when(store).isIncrementalPushEnabled();
    doReturn(store).when(internalAdmin).getStore(anyString(), anyString());
    doReturn(VersionStatus.STARTED).when(store).getVersionStatus(anyInt());

    Version version = mock(Version.class);
    doReturn(version).when(store).getVersion(anyInt());
    doReturn(VersionStatus.KILLED).when(version).getStatus();
    doReturn(Version.PushType.BATCH).when(version).getPushType();

    HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    doReturn(mock(ClusterLockManager.class)).when(resources).getClusterLockManager();
    doReturn(resources).when(internalAdmin).getHelixVeniceClusterResources(anyString());
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    doReturn(repository).when(resources).getStoreMetadataRepository();
    doReturn(store).when(repository).getStore(anyString());

    Admin.OfflinePushStatusInfo offlineJobStatus =
        parentAdmin.getOffLineJobStatus("IGNORED", "topic1_v1", notCreatedMap);
    assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
  }

  @Test
  public void testUpdateStore() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    UpdateStoreQueryParams storeQueryParams1 = new UpdateStoreQueryParams().setBlobTransferEnabled(true);
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.updateStore(clusterName, storeName, storeQueryParams1);

    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.UPDATE_STORE.getValue());

    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertTrue(updateStore.blobTransferEnabled);

    long readQuota = 100L;
    boolean readability = true;
    boolean accessControlled = true;
    Map<String, String> testPartitionerParams = new HashMap<>();

    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setEnableReads(readability)
        .setIncrementalPushEnabled(false)
        .setPartitionCount(64)
        .setPartitionerClass("com.linkedin.venice.partitioner.DefaultVenicePartitioner")
        .setPartitionerParams(testPartitionerParams)
        .setReadQuotaInCU(readQuota)
        .setAccessControlled(accessControlled)
        .setCompressionStrategy(CompressionStrategy.GZIP)
        .setHybridRewindSeconds(135L)
        .setHybridOffsetLagThreshold(2000)
        .setHybridBufferReplayPolicy(REWIND_FROM_SOP)
        .setBootstrapToOnlineTimeoutInHours(48)
        .setReplicationFactor(2)
        .setBlobTransferEnabled(false)
        .setMaxRecordSizeBytes(7777)
        .setMaxNearlineRecordSizeBytes(6666);

    parentAdmin.updateStore(clusterName, storeName, updateStoreQueryParams);

    verify(veniceWriter, times(2)).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());
    valueBytes = valueCaptor.getValue();
    schemaId = schemaCaptor.getValue();
    adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(updateStore.clusterName.toString(), clusterName);
    assertEquals(updateStore.storeName.toString(), storeName);
    assertEquals(updateStore.readQuotaInCU, readQuota, "New read quota should be written into kafka message.");
    assertEquals(updateStore.enableReads, readability, "New read readability should be written into kafka message.");
    assertEquals(
        updateStore.currentVersion,
        AdminConsumptionTask.IGNORED_CURRENT_VERSION,
        "As we don't pass any current version into updateStore, a magic version number should be used to prevent current version being overrided in prod region.");
    Assert.assertNotNull(
        updateStore.hybridStoreConfig,
        "Hybrid store config should result in something not null in the avro object");
    assertEquals(updateStore.hybridStoreConfig.rewindTimeInSeconds, 135L);
    assertEquals(updateStore.hybridStoreConfig.offsetLagThresholdToGoOnline, 2000L);
    assertEquals(updateStore.hybridStoreConfig.bufferReplayPolicy, REWIND_FROM_SOP.getValue());
    assertEquals(updateStore.accessControlled, accessControlled);
    assertEquals(updateStore.bootstrapToOnlineTimeoutInHours, 48);
    assertEquals(updateStore.partitionerConfig.amplificationFactor, 1);
    assertEquals(updateStore.partitionerConfig.partitionerParams.toString(), testPartitionerParams.toString());
    assertEquals(
        updateStore.partitionerConfig.partitionerClass.toString(),
        "com.linkedin.venice.partitioner.DefaultVenicePartitioner");
    assertEquals(updateStore.replicationFactor, 2);
    Assert.assertFalse(updateStore.blobTransferEnabled);
    Assert.assertEquals(updateStore.maxRecordSizeBytes, 7777);
    Assert.assertEquals(updateStore.maxNearlineRecordSizeBytes, 6666);
    Assert.assertNull(updateStore.targetSwapRegion);
    // Disable Access Control
    accessControlled = false;
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setAccessControlled(accessControlled));

    verify(veniceWriter, times(3)).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());
    valueBytes = valueCaptor.getValue();
    schemaId = schemaCaptor.getValue();
    adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(updateStore.accessControlled, accessControlled);

    // Update the store twice with the same parameter to make sure get methods in UpdateStoreQueryParams class can work
    // properly.
    parentAdmin.updateStore(clusterName, storeName, updateStoreQueryParams);
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setNativeReplicationEnabled(true));

    // Verify the correct config was sent via veniceWriter
    verify(veniceWriter, times(5)).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());
    valueBytes = valueCaptor.getValue();
    schemaId = schemaCaptor.getValue();
    adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertTrue(
        updateStore.nativeReplicationEnabled,
        "Native replication was not set to true after updating the store!");
    // Test exception thrown for unsuccessful partitioner instance creation inside store update.
    try {
      parentAdmin.updateStore(
          clusterName,
          storeName,
          new UpdateStoreQueryParams().setPartitionerClass(InvalidKeySchemaPartitioner.class.getName()));
      fail("The partitioner creation should not be successful");
    } catch (Exception e) {
      Assert.assertTrue(e.getClass().isAssignableFrom(VeniceHttpException.class));
      Assert.assertTrue(e instanceof VeniceHttpException);
      VeniceHttpException veniceHttpException = (VeniceHttpException) e;
      assertEquals(veniceHttpException.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
      assertEquals(veniceHttpException.getErrorType(), ErrorType.INVALID_SCHEMA);
    }
  }

  @Test
  public void testUpdateStoreNativeReplicationSourceFabric() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setNativeReplicationSourceFabric("dc1"));

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(
        updateStore.nativeReplicationSourceFabric.toString(),
        "dc1",
        "Native replication source fabric does not match after updating the store!");
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
  }

  @Test(description = "Test that update store sets target region swap configs correctly")
  public void testUpdateStoreTargetSwapRegion() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setTargetRegionSwap("prod")
        .setTargetRegionSwapWaitTime(100)
        .setIsDavinciHeartbeatReported(false);
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.updateStore(clusterName, storeName, updateStoreQueryParams);

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertEquals(updateStore.targetSwapRegion.toString(), "prod");
    Assert.assertEquals(updateStore.targetSwapRegionWaitTime, 100);
    Assert.assertEquals(updateStore.isDaVinciHeartBeatReported, false);
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testDisableHybridConfigWhenActiveActiveOrIncPushConfigIsEnabled() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());

    store.setHybridStoreConfig(new HybridStoreConfigImpl(1000, 100, -1, BufferReplayPolicy.REWIND_FROM_EOP));
    store.setActiveActiveReplicationEnabled(true);
    store.setIncrementalPushEnabled(true);
    store.setNativeReplicationEnabled(true);
    store.setChunkingEnabled(true);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    // When user disable hybrid but also try to manually turn on A/A or Incremental Push, update operation should fail
    // loudly.
    assertThrows(
        () -> parentAdmin.updateStore(
            clusterName,
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(-1)
                .setHybridOffsetLagThreshold(-1)
                .setActiveActiveReplicationEnabled(true)));
    assertThrows(
        () -> parentAdmin.updateStore(
            clusterName,
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(-1)
                .setHybridOffsetLagThreshold(-1)
                .setIncrementalPushEnabled(true)));

    parentAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(-1).setHybridRewindSeconds(-1));

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertFalse(internalAdmin.isHybrid(updateStore.getHybridStoreConfig()));
    Assert.assertFalse(updateStore.incrementalPushEnabled);
    Assert.assertFalse(updateStore.activeActiveReplicationEnabled);
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testSetStoreViewConfig() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    setupForStoreViewConfigUpdateTest(storeName);
    Map<String, String> viewConfig = new HashMap<>();
    viewConfig.put(
        "changeCapture",
        "{\"viewClassName\" : \"" + ChangeCaptureView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStoreViews(viewConfig));

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertTrue(updateStore.getViews().containsKey("changeCapture"));
  }

  @Test
  public void testSetRePartitionViewConfig() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    setupForStoreViewConfigUpdateTest(storeName);
    Map<String, String> viewConfig = new HashMap<>();
    String rePartitionViewConfigString = "{\"viewClassName\" : \"%s\", \"viewParameters\" : {\"%s\":\"%s\"}}";
    String rePartitionViewName = "rePartitionViewA";
    int rePartitionViewPartitionCount = 10;
    String viewString = String.format(
        rePartitionViewConfigString,
        MaterializedView.class.getCanonicalName(),
        MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(),
        rePartitionViewPartitionCount);

    // Invalid re-partition view name
    viewConfig.put(rePartitionViewName + VERSION_SEPARATOR, viewString);
    Assert.assertThrows(
        () -> parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStoreViews(viewConfig)));

    viewConfig.clear();
    viewConfig.put(rePartitionViewName, viewString);
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStoreViews(viewConfig));

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertTrue(updateStore.getViews().containsKey(rePartitionViewName));
    Map<String, CharSequence> rePartitionViewParameters =
        updateStore.getViews().get(rePartitionViewName).viewParameters;
    Assert.assertNotNull(rePartitionViewParameters.get(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name()));
    Assert.assertEquals(
        rePartitionViewParameters.get(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name()).toString(),
        rePartitionViewName);
    Assert.assertEquals(
        Integer.parseInt(
            rePartitionViewParameters.get(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name())
                .toString()),
        rePartitionViewPartitionCount);
  }

  @Test
  public void testInsertStoreViewConfig() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = setupForStoreViewConfigUpdateTest(storeName);
    store.setViewConfigs(
        Collections.singletonMap("testView", new ViewConfigImpl("testViewClassDummyName", Collections.emptyMap())));
    parentAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setViewName("changeCapture")
            .setViewClassName(ChangeCaptureView.class.getCanonicalName()));

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(updateStore.getViews().size(), 2);
    Assert.assertTrue(updateStore.getViews().containsKey("changeCapture"));
    assertEquals(
        updateStore.getViews().get("changeCapture").viewClassName.toString(),
        ChangeCaptureView.class.getCanonicalName());
    Assert.assertTrue(updateStore.getViews().get("changeCapture").viewParameters.isEmpty());
  }

  @Test
  public void testInsertMaterializedViewConfig() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = setupForStoreViewConfigUpdateTest(storeName);
    store.setViewConfigs(
        Collections.singletonMap("testView", new ViewConfigImpl("testViewClassDummyName", Collections.emptyMap())));
    String rePartitionViewName = "rePartitionViewA";
    int rePartitionViewPartitionCount = 10;
    Map<String, String> viewClassParams = new HashMap<>();
    viewClassParams.put(
        MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name(),
        Integer.toString(rePartitionViewPartitionCount));

    // Invalid re-partition view name
    Assert.assertThrows(
        () -> parentAdmin.updateStore(
            clusterName,
            storeName,
            new UpdateStoreQueryParams().setViewName(rePartitionViewName + VERSION_SEPARATOR)
                .setViewClassName(MaterializedView.class.getCanonicalName())
                .setViewClassParams(viewClassParams)));

    parentAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setViewName(rePartitionViewName)
            .setViewClassName(MaterializedView.class.getCanonicalName())
            .setViewClassParams(viewClassParams));

    AdminOperation adminMessage = verifyAndGetSingleAdminOperation();
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(updateStore.getViews().size(), 2);
    Assert.assertTrue(updateStore.getViews().containsKey(rePartitionViewName));
    Map<String, CharSequence> rePartitionViewParameters =
        updateStore.getViews().get(rePartitionViewName).viewParameters;
    Assert.assertNotNull(rePartitionViewParameters.get(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name()));
    Assert.assertEquals(
        rePartitionViewParameters.get(MaterializedViewParameters.MATERIALIZED_VIEW_NAME.name()).toString(),
        rePartitionViewName);
    Assert.assertEquals(
        Integer.parseInt(
            rePartitionViewParameters.get(MaterializedViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name())
                .toString()),
        rePartitionViewPartitionCount);
  }

  @Test
  public void testRemoveStoreViewConfig() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    store.setActiveActiveReplicationEnabled(true);
    store.setChunkingEnabled(true);
    store.setViewConfigs(
        Collections.singletonMap(
            "changeCapture",
            new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), Collections.emptyMap())));
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setViewName("changeCapture").setDisableStoreView());

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);

    verify(veniceWriter, times(1)).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(updateStore.getViews().size(), 0);
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testUpdateStoreWithBadPartitionerConfigs() {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    parentAdmin.initStorageCluster(clusterName);

    assertThrows(
        () -> parentAdmin.updateStore(
            clusterName,
            storeName,
            new UpdateStoreQueryParams().setPartitionerClass("com.linkedin.im.a.bad.man")));
    verify(veniceWriter, times(0)).put(any(), any(), anyInt());

    Assert.assertThrows(
        () -> parentAdmin
            .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true)));
    verify(veniceWriter, times(0)).put(any(), any(), anyInt());

    Assert.assertThrows(
        () -> parentAdmin
            .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)));
    verify(veniceWriter, times(0)).put(any(), any(), anyInt());
  }

  @Test
  public void testAbortMigrationDeleteStore() {
    String storeName = "test-testAbortMigrationCreateStore";
    String owner = "unitTest";
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());

    doReturn(store).when(internalAdmin).getStore(eq(clusterName), eq(storeName));
    doReturn(store).when(internalAdmin).checkPreConditionForDeletion(eq(clusterName), eq(storeName));
    assertTrue(!store.isMigrating());
    parentAdmin.initStorageCluster(clusterName);
    Exception exp = Assert
        .expectThrows(VeniceException.class, () -> parentAdmin.deleteStore(clusterName, storeName, true, 0, true));
    assertEquals(
        "Store test-testAbortMigrationCreateStore's migrating flag is false. Not safe to delete a store "
            + "that is assumed to be migrating without the migrating flag setup as true.",
        exp.getMessage());
  }

  @Test
  public void testDeleteStore() {
    String storeName = "test-testReCreateStore";
    String owner = "unittest";
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(eq(clusterName), eq(storeName));
    doReturn(store).when(internalAdmin).checkPreConditionForDeletion(eq(clusterName), eq(storeName));

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.deleteStore(clusterName, storeName, false, 0, true);

    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.DELETE_STORE.getValue());

    DeleteStore deleteStore = (DeleteStore) adminMessage.payloadUnion;
    assertEquals(deleteStore.clusterName.toString(), clusterName);
    assertEquals(deleteStore.storeName.toString(), storeName);
    assertEquals(deleteStore.largestUsedVersionNumber, 0);
    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testGetCurrentVersionForMultiRegions() {
    int regionCount = 4;
    Map<String, ControllerClient> controllerClientMap = prepareForCurrentVersionTest(regionCount);
    Map<String, Integer> result =
        parentAdmin.getCurrentVersionForMultiRegions(clusterName, "test", controllerClientMap);
    assertEquals(result.size(), regionCount, "Should return the current versions for all regions.");
    for (int i = 0; i < regionCount; i++) {
      assertEquals(result.get("region" + i).intValue(), i);
    }
  }

  @Test
  public void testGetCurrentVersionForMultiRegionsWithError() {
    int regionCount = 4;
    Map<String, ControllerClient> controllerClientMap = prepareForCurrentVersionTest(regionCount - 1);
    ControllerClient errorClient = mock(ControllerClient.class);
    StoreResponse errorResponse = new StoreResponse();
    errorResponse.setError("Error getting store for testing.");
    doReturn(errorResponse).when(errorClient).getStore(anyString());
    controllerClientMap.put("region4", errorClient);

    Map<String, Integer> result =
        parentAdmin.getCurrentVersionForMultiRegions(clusterName, "test", controllerClientMap);
    assertEquals(result.size(), regionCount, "Should return the current versions for all regions.");
    for (int i = 0; i < regionCount - 1; i++) {
      assertEquals(result.get("region" + i).intValue(), i);
    }
    assertEquals(
        result.get("region4").intValue(),
        AdminConsumptionTask.IGNORED_CURRENT_VERSION,
        "Met an error while querying a current version from a region, should return -1.");
  }

  private Map<String, ControllerClient> prepareForCurrentVersionTest(int regionCount) {
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();

    for (int i = 0; i < regionCount; i++) {
      ControllerClient client = mock(ControllerClient.class);
      StoreResponse storeResponse = new StoreResponse();
      Store s = TestUtils.createTestStore("s" + i, "test", System.currentTimeMillis());
      s.setCurrentVersion(i);
      storeResponse.setStore(StoreInfo.fromStore(s));
      doReturn(storeResponse).when(client).getStore(anyString());
      controllerClientMap.put("region" + i, client);
    }
    return controllerClientMap;
  }

  @Test
  public void testGetKafkaTopicsByAge() {
    String storeName = Utils.getUniqueString("test-store");
    List<PubSubTopic> versionTopics = parentAdmin.getKafkaTopicsByAge(storeName);
    Assert.assertTrue(versionTopics.isEmpty());

    Set<PubSubTopic> topicList = new HashSet<>();
    topicList.add(pubSubTopicRepository.getTopic(storeName + "_v1"));
    topicList.add(pubSubTopicRepository.getTopic(storeName + "_v2"));
    topicList.add(pubSubTopicRepository.getTopic(storeName + "_v3"));
    doReturn(topicList).when(topicManager).listTopics();
    versionTopics = parentAdmin.getKafkaTopicsByAge(storeName);
    Assert.assertFalse(versionTopics.isEmpty());
    PubSubTopic latestTopic = versionTopics.get(0);
    assertEquals(latestTopic, pubSubTopicRepository.getTopic(storeName + "_v3"));
    Assert.assertTrue(topicList.containsAll(versionTopics));
    Assert.assertTrue(versionTopics.containsAll(topicList));
  }

  @Test
  public void testGetTopicForCurrentPushJob() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(internalAdmin).when(mockParentAdmin).getVeniceHelixAdmin();
    doReturn(new ArrayList<String>()).when(mockParentAdmin).getKafkaTopicsByAge(any());
    ControllerClient client = mock(ControllerClient.class);
    Map<String, ControllerClient> map = new HashMap<>();
    map.put("dc-0", client);
    doReturn(map).when(internalAdmin).getControllerClientMap(anyString());
    Map<String, VeniceControllerClusterConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);
    doReturn(new VeniceControllerMultiClusterConfig(configMap)).when(mockParentAdmin).getMultiClusterConfigs();
    HelixVeniceClusterResources clusterResources = internalAdmin.getHelixVeniceClusterResources(clusterName);
    doReturn(clusterResources).when(internalAdmin).getHelixVeniceClusterResources(clusterName);
    doCallRealMethod().when(mockParentAdmin).getTopicForCurrentPushJob(clusterName, storeName, false, false);
    doCallRealMethod().when(mockParentAdmin)
        .getTopicForCurrentPushJobParentVersionStatusBasedTracking(clusterName, storeName);

    Store store = new ZKStore(
        storeName,
        "test_owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    VersionImpl version = new VersionImpl(storeName, 1, "test_push_id");
    version.setStatus(VersionStatus.ONLINE);
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);
    StoreResponse response = mock(StoreResponse.class);
    StoreInfo info = mock(StoreInfo.class);
    doReturn(response).when(client).getStore(anyString());
    doReturn(info).when(response).getStore();
    doReturn(new StoreVersionInfo(store, store.getVersion(1))).when(internalAdmin)
        .waitVersion(eq(clusterName), eq(storeName), eq(1), any());

    String latestTopic = storeName + "_v1";

    // When there is a regular topic and the job status is terminal
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED)).when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    doReturn(false).when(mockParentAdmin).isTopicTruncated(latestTopic);
    Assert.assertFalse(mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false, false).isPresent());
    // When there is a regular topic and the job status is not terminal
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS)).when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    Optional<String> currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false, false);
    Assert.assertTrue(currentPush.isPresent());
    assertEquals(currentPush.get(), latestTopic);
    verify(mockParentAdmin, times(2)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is 'UNKNOWN' in some region,
    // but overall status is 'COMPLETED'
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("cluster1", ExecutionStatus.UNKNOWN.toString());
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED, extraInfo)).when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    doCallRealMethod().when(mockParentAdmin).setTimer(any());
    mockParentAdmin.setTimer(new TestMockTime());
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false, false);
    Assert.assertFalse(currentPush.isPresent());
    verify(mockParentAdmin, times(7)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is 'UNKNOWN' in some region,
    // but overall status is 'PROGRESS'
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS, extraInfo)).when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false, false);
    Assert.assertTrue(currentPush.isPresent());
    assertEquals(currentPush.get(), latestTopic);
    verify(mockParentAdmin, times(12)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is 'UNKNOWN' in some region for the first time,
    // but overall status is 'PROGRESS'
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS, extraInfo)).when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    when(mockParentAdmin.getOffLinePushStatus(clusterName, latestTopic))
        .thenReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS, extraInfo))
        .thenReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS));
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false, false);
    Assert.assertTrue(currentPush.isPresent());
    assertEquals(currentPush.get(), latestTopic);
    verify(mockParentAdmin, times(14)).getOffLinePushStatus(clusterName, latestTopic);

    version = new VersionImpl(storeName, 2, "test_push_id");
    version.setStatus(VersionStatus.KILLED);
    store.addVersion(version);
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);
    response = mock(StoreResponse.class);
    info = mock(StoreInfo.class);
    doReturn(response).when(client).getStore(anyString());
    doReturn(info).when(response).getStore();
    doReturn(new StoreVersionInfo(store, store.getVersion(1))).when(internalAdmin)
        .waitVersion(eq(clusterName), eq(storeName), eq(1), any());
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false, false);
    Assert.assertFalse(currentPush.isPresent());
  }

  @Test
  public void testTruncateTopicsBasedOnMaxErroredTopicNumToKeep() {
    String storeName = Utils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    List<String> topics = new ArrayList<>();
    topics.add(storeName + "_v1");
    topics.add(storeName + "_v10");
    topics.add(storeName + "_v8");
    topics.add(storeName + "_v5");
    topics.add(storeName + "_v7");
    doReturn(topics).when(mockParentAdmin).existingVersionTopicsForStore(storeName);
    // isTopicTruncated will return false for other topics
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName + "_v8");
    doCallRealMethod().when(mockParentAdmin).truncateTopicsBasedOnMaxErroredTopicNumToKeep(any(), anyBoolean(), any());
    doCallRealMethod().when(mockParentAdmin).setMaxErroredTopicNumToKeep(anyInt());
    mockParentAdmin.setMaxErroredTopicNumToKeep(2);
    mockParentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(topics, false, null);
    /**
     * Since the max error version topics we would like to keep is 2 and the non-truncated version
     * topics include v1, v5, v7 and v10 (v8 is truncated already), we will truncate v1, v5 and keep
     * 2 error non-truncated version topics v7 and v10.
     */
    verify(mockParentAdmin).truncateKafkaTopic(storeName + "_v1");
    verify(mockParentAdmin).truncateKafkaTopic(storeName + "_v5");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName + "_v7");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName + "_v8");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName + "_v10");

    // Test with more truncated topics
    String storeName1 = Utils.getUniqueString("test-store");
    List<String> topics1 = new ArrayList<>();
    topics1.add(storeName1 + "_v1");
    topics1.add(storeName1 + "_v10");
    topics1.add(storeName1 + "_v8");
    topics1.add(storeName1 + "_v5");
    topics1.add(storeName1 + "_v7");
    doReturn(topics1).when(mockParentAdmin).existingVersionTopicsForStore(storeName1);
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "_v10");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "_v7");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "_v8");
    doCallRealMethod().when(mockParentAdmin).truncateTopicsBasedOnMaxErroredTopicNumToKeep(any(), anyBoolean(), any());
    mockParentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(topics1, false, null);
    /**
     * Since the max error version topics we would like to keep is 2 and we only have 2 non-truncated version
     * topics v1 and v5 (v7, v8 and v10 are truncated already), we will not truncate anything.
     */
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v1");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v5");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v7");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v8");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v10");
  }

  @Test
  public void testAdminCanCleanupLeakingTopics() {
    String storeName = "test_store";

    List<PubSubTopic> pubSubTopics = Arrays.asList(
        pubSubTopicRepository.getTopic(storeName + "_v1"),
        pubSubTopicRepository.getTopic(storeName + "_v2"),
        pubSubTopicRepository.getTopic(storeName + "_v3"));
    List<String> topics = Arrays.asList(storeName + "_v1", storeName + "_v2", storeName + "_v3");
    doReturn(new HashSet(pubSubTopics)).when(topicManager).listTopics();

    parentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(topics, false, null);
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v1");
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v2");
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v3");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAdminCanKillLingeringVersion(boolean isIncrementalPush) {
    doReturn(ConcurrentPushDetectionStrategy.TOPIC_BASED_ONLY).when(config).getConcurrentPushDetectionStrategy();
    try (PartialMockVeniceParentHelixAdmin partialMockParentAdmin =
        new PartialMockVeniceParentHelixAdmin(internalAdmin, config)) {
      long startTime = System.currentTimeMillis();
      TestMockTime mockTime = new TestMockTime(startTime);
      partialMockParentAdmin.setTimer(mockTime);
      mockTime.addMilliseconds(TimeUnit.HOURS.toMillis(30));
      String storeName = "test_store";
      String existingTopicName = storeName + "_v1";
      Store store = mock(Store.class);
      Version version = new VersionImpl(storeName, 1, "test-push");
      partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.STARTED);
      String newPushJobId = "new-test-push";
      Version newVersion = new VersionImpl(storeName, 2, newPushJobId);

      doReturn(24).when(store).getBootstrapToOnlineTimeoutInHours();
      doReturn(-1).when(store).getRmdVersion();
      doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
      doReturn(version).when(store).getVersion(1);
      doReturn(new StoreVersionInfo(store, version)).when(internalAdmin)
          .waitVersion(eq(clusterName), eq(storeName), eq(version.getNumber()), any());
      List<PubSubTopic> pubSubTopics =
          Arrays.asList(pubSubTopicRepository.getTopic(topicName), pubSubTopicRepository.getTopic(existingTopicName));
      doReturn(new HashSet<>(pubSubTopics)).when(topicManager).listTopics();
      doReturn(new Pair<>(true, newVersion)).when(internalAdmin)
          .addVersionAndTopicOnly(
              clusterName,
              storeName,
              newPushJobId,
              VERSION_ID_UNSET,
              3,
              3,
              false,
              true,
              Version.PushType.BATCH,
              null,
              null,
              Optional.empty(),
              -1,
              1,
              Optional.empty(),
              false,
              null,
              -1,
              DEFAULT_RT_VERSION_NUMBER,
              -1);

      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
      doReturn(
          CompletableFuture.completedFuture(
              new SimplePubSubProduceResultImpl(topicName, partitionId, mock(PubSubPosition.class), -1)))
                  .when(veniceWriter)
                  .put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
      mockControllerClients(storeName);

      when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
          .thenReturn(
              AdminTopicMetadataAccessor.generateMetadataMap(
                  Optional.of(1L),
                  Optional.of(-1L),
                  Optional.of(1L),
                  Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

      if (isIncrementalPush) {
        /**
         * Parent shouldn't allow an incremental push happen on an incompleted batch push;
         * notice that the batch push might already be completed but is reported as incompleted
         * due to transient issues. Either way, incremental push should fail.
         */
        try {
          partialMockParentAdmin.incrementVersionIdempotent(
              clusterName,
              storeName,
              newPushJobId,
              3,
              3,
              Version.PushType.INCREMENTAL,
              false,
              true,
              null,
              Optional.empty(),
              Optional.empty(),
              -1,
              Optional.empty(),
              false,
              null,
              -1,
              -1);
          fail("Incremental push should fail if the previous batch push is not in COMPLETE state.");
          verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
        } catch (Exception e) {
          /**
           * Make sure that parent will not kill previous batch push.
           */
          Assert.assertFalse(partialMockParentAdmin.isJobKilled(version.kafkaTopicName()));
        }
      } else {
        assertEquals(
            partialMockParentAdmin.incrementVersionIdempotent(
                clusterName,
                storeName,
                newPushJobId,
                3,
                3,
                Version.PushType.BATCH,
                false,
                true,
                null,
                Optional.empty(),
                Optional.empty(),
                -1,
                Optional.empty(),
                false,
                null,
                -1,
                -1),
            newVersion,
            "Unexpected new version returned by incrementVersionIdempotent");
        // Parent should kill the lingering job.
        Assert.assertTrue(partialMockParentAdmin.isJobKilled(version.kafkaTopicName()));
        verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
      }
    }
  }

  @Test
  public void testAdminMessageIsolation() {
    String storeA = "test_store_A";
    String storeB = "test_store_B";
    Version storeAVersion = new VersionImpl(storeA, 1, "");
    Version storeBVersion = new VersionImpl(storeB, 1, "");

    doReturn(new Pair<>(true, storeAVersion)).when(internalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeA,
            "",
            VERSION_ID_UNSET,
            3,
            3,
            true,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            0,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);
    doReturn(new Pair<>(true, storeBVersion)).when(internalAdmin)
        .addVersionAndTopicOnly(
            clusterName,
            storeB,
            "",
            VERSION_ID_UNSET,
            3,
            3,
            true,
            false,
            Version.PushType.BATCH,
            null,
            null,
            Optional.empty(),
            -1,
            0,
            Optional.empty(),
            false,
            null,
            -1,
            DEFAULT_RT_VERSION_NUMBER,
            -1);
    doReturn(new Exception("test")).when(internalAdmin).getLastExceptionForStore(clusterName, storeA);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeA);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeB);
    doReturn(0).when(store).getLargestUsedRTVersionNumber();
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(
        AdminTopicMetadataAccessor.generateMetadataMap(
            Optional.of(1L),
            Optional.of(-1L),
            Optional.of(1L),
            Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    try {
      parentAdmin.incrementVersionIdempotent(clusterName, storeA, "", 3, 3);
      fail("Admin operations to a store with existing exception should be blocked");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("due to existing exception"));
    }
    // store B should still be able to process admin operations.
    assertEquals(
        parentAdmin.incrementVersionIdempotent(clusterName, storeB, "", 3, 3),
        storeBVersion,
        "Unexpected new version returned");

    doReturn(null).when(internalAdmin).getLastExceptionForStore(clusterName, storeA);
    // Store A is unblocked and should be able to process admin operations now.
    assertEquals(
        parentAdmin.incrementVersionIdempotent(clusterName, storeA, "", 3, 3),
        storeAVersion,
        "Unexpected new version returned");
    verify(zkClient, times(3)).readData(zkMetadataNodePath, null);
  }

  /**
   * all ACL related api should throw exception as the autorizerservice is not enabled here.
   */
  @Test
  public void testAclException() {
    String storeName = "test-store-authorizer";
    assertThrows(
        VeniceUnsupportedOperationException.class,
        () -> parentAdmin.updateAclForStore(clusterName, storeName, ""));
    assertThrows(VeniceUnsupportedOperationException.class, () -> parentAdmin.getAclForStore(clusterName, storeName));
    assertThrows(
        VeniceUnsupportedOperationException.class,
        () -> parentAdmin.deleteAclForStore(clusterName, storeName));
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHybridAndIncrementalUpdateStoreCommands(boolean aaEnabled) {
    String storeName = Utils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(20000).setHybridRewindSeconds(60));

    verify(zkClient, times(2)).readData(zkMetadataNodePath, null);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.UPDATE_STORE.getValue());

    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertEquals(updateStore.hybridStoreConfig.offsetLagThresholdToGoOnline, 20000);
    assertEquals(updateStore.hybridStoreConfig.rewindTimeInSeconds, 60);

    store.setActiveActiveReplicationEnabled(aaEnabled);
    store.setHybridStoreConfig(new HybridStoreConfigImpl(60, 20000, 0, BufferReplayPolicy.REWIND_FROM_EOP));
    // Incremental push can be enabled on a hybrid store, default inc push policy is inc push to RT now
    if (aaEnabled) {
      parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));
      // veniceWriter.put will be called again for the second update store command
      verify(veniceWriter, times(2)).put(
          keyCaptor.capture(),
          valueCaptor.capture(),
          schemaCaptor.capture(),
          any(),
          any(),
          anyLong(),
          any(),
          any(),
          any(),
          any());
    } else {
      assertThrows(
          () -> parentAdmin
              .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true)));
    }
  }

  @Test
  public void testSetVersionShouldFailOnParentController() {
    try {
      parentAdmin.setStoreCurrentVersion(clusterName, "any_store", 1);
      fail("Set version should not be allowed on parent controllers.");
    } catch (VeniceUnsupportedOperationException e) {
      // Expected
    } catch (Throwable e) {
      fail("SetVersion command on parent controller should fail with VeniceUnsupportedOperationException");
    }
  }

  @Test
  public void testSendAdminMessageAcquiresClusterReadLock() {
    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
    parentAdmin.initStorageCluster(clusterName);
    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    // To support the store update during store creation.
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    parentAdmin.createStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
    doReturn(clusterLockManager).when(resources).getClusterLockManager();
    verify(clusterLockManager, times(2)).createClusterReadLock();
    verify(zkClient, times(3)).readData(zkMetadataNodePath, null);
  }

  @Test
  public void testDataRecoveryAPIs() {
    final String storeName = "test";
    final String owner = "test";
    final int numOfPartition = 5;
    final int replicationFactor = 3;
    final String kafkaTopic = "test_v1";

    OfflinePushStatus status = new OfflinePushStatus(
        kafkaTopic,
        numOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    LocalDateTime now = LocalDateTime.now();

    for (int i = 0; i < numOfPartition; i++) {
      PartitionStatus partition = new PartitionStatus(i);
      for (int j = 0; j < replicationFactor; j++) {
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.STARTED);
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.COMPLETED);
      }
      status.setPartitionStatus(partition);
    }

    status.getStatusHistory().add(new StatusSnapshot(ExecutionStatus.STARTED, now.toString()));
    status.getStatusHistory().add(new StatusSnapshot(ExecutionStatus.COMPLETED, now.plusHours(1).toString()));
    doReturn(status).when(internalAdmin).retrievePushStatus(anyString(), any());

    Store s = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    s.addVersion(new VersionImpl(s.getName(), 1, "pushJobId"));
    s.setCurrentVersion(1);
    when(internalAdmin.getRegionPushDetails(anyString(), anyString(), anyBoolean())).thenCallRealMethod();
    doReturn(s).when(internalAdmin).getStore(anyString(), anyString());

    RegionPushDetails details = internalAdmin.getRegionPushDetails(clusterName, storeName, true);
    assertEquals(details.getPushEndTimestamp(), now.plusHours(1).toString());
    assertEquals(details.getVersions().size(), 1);
    assertEquals(details.getCurrentVersion().intValue(), 1);
    assertEquals(details.getPartitionDetails().size(), numOfPartition);
    for (int i = 0; i < numOfPartition; i++) {
      assertEquals(details.getPartitionDetails().get(i).getReplicaDetails().size(), replicationFactor);
    }

    doReturn(null).when(internalAdmin).getStore(anyString(), anyString());
    assertThrows(VeniceNoStoreException.class, () -> internalAdmin.getRegionPushDetails(clusterName, storeName, true));
  }

  @Test
  public void testTargetedRegionValidation() {
    try {
      HelixVeniceClusterResources clusterResources = internalAdmin.getHelixVeniceClusterResources(clusterName);
      doReturn(clusterResources).when(internalAdmin).getHelixVeniceClusterResources(clusterName);
      parentAdmin.incrementVersionIdempotent(
          clusterName,
          "test",
          "test",
          1,
          1,
          Version.PushType.BATCH,
          false,
          false,
          null,
          null,
          null,
          -1,
          null,
          false,
          "invalidRegion",
          -1,
          -1);
      fail("Test should fail, but doesn't");
    } catch (VeniceException e) {
      assertEquals(
          e.getMessage(),
          "One of the targeted region invalidRegion is not a valid region in cluster test-cluster");
    }
  }

  @Test
  public void testGetFinalReturnStatus() {
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    Set<String> childRegions = new HashSet<>();
    childRegions.add("region1");
    childRegions.add("region2");
    childRegions.add("region3");
    ExecutionStatus finalStatus;

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.COMPLETED);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.COMPLETED);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.PROGRESS);
    statuses.put("region3", ExecutionStatus.COMPLETED);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.PROGRESS);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.COMPLETED);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.ERROR);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.UNKNOWN);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 1, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.UNKNOWN);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.UNKNOWN);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.UNKNOWN);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 2, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.PROGRESS);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_OTHER);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    statuses.clear();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_OTHER);
    finalStatus = VeniceParentHelixAdmin.getFinalReturnStatus(statuses, childRegions, 0, new StringBuilder());
    assertEquals(finalStatus, ExecutionStatus.DVC_INGESTION_ERROR_OTHER);
  }

  @Test
  public void testUpdateAdminOperationProtocolVersion() {
    String clusterName = "test-cluster";
    Long adminProtocolVersion = 10L;
    VeniceParentHelixAdmin veniceParentHelixAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin veniceHelixAdmin = mock(VeniceHelixAdmin.class);
    when(veniceParentHelixAdmin.getVeniceHelixAdmin()).thenReturn(veniceHelixAdmin);
    doCallRealMethod().when(veniceParentHelixAdmin)
        .updateAdminOperationProtocolVersion(clusterName, adminProtocolVersion);
    doCallRealMethod().when(veniceHelixAdmin).updateAdminOperationProtocolVersion(clusterName, adminProtocolVersion);
    AdminConsumerService adminConsumerService = mock(AdminConsumerService.class);
    when(veniceHelixAdmin.getAdminConsumerService(clusterName)).thenReturn(adminConsumerService);

    veniceParentHelixAdmin.updateAdminOperationProtocolVersion(clusterName, adminProtocolVersion);
    verify(adminConsumerService, times(1)).updateAdminOperationProtocolVersion(clusterName, adminProtocolVersion);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Roll forward failed without any future version")
  public void testRollForwardNoFutureVersions() {
    VeniceParentHelixAdmin adminSpy = spy(parentAdmin);
    doNothing().when(adminSpy).acquireAdminMessageLock(clusterName, storeName);
    doNothing().when(adminSpy).releaseAdminMessageLock(clusterName, storeName);
    doReturn(Collections.emptyMap()).when(adminSpy).getFutureVersionsForMultiColos(clusterName, storeName);
    adminSpy.rollForwardToFutureVersion(clusterName, storeName, "");
  }

  @Test
  public void testRollForwardSuccess() {
    VeniceParentHelixAdmin adminSpy = spy(parentAdmin);
    doNothing().when(adminSpy).acquireAdminMessageLock(clusterName, storeName);
    doNothing().when(adminSpy).releaseAdminMessageLock(clusterName, storeName);

    Map<String, String> future = Collections.singletonMap("r1", "5");
    doReturn(future).when(adminSpy).getFutureVersionsForMultiColos(clusterName, storeName);

    doNothing().when(adminSpy)
        .sendAdminMessageAndWaitForConsumed(eq(clusterName), eq(storeName), any(AdminOperation.class));
    doReturn(ConcurrentPushDetectionStrategy.TOPIC_BASED_ONLY).when(config).getConcurrentPushDetectionStrategy();
    doReturn(true).when(adminSpy).truncateKafkaTopic(Version.composeKafkaTopic(storeName, 5));

    Map<String, Integer> after = Collections.singletonMap("r1", 5);
    doReturn(after).when(adminSpy).getCurrentVersionsForMultiColos(clusterName, storeName);
    Version version = mock(Version.class);
    doReturn(true).when(version).isVersionSwapDeferred();
    doReturn(version).when(store).getVersion(5);
    doReturn(store).when(adminSpy).getStore(anyString(), anyString());

    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      ControllerResponse response = new ControllerResponse();
      doReturn(response).when(entry.getValue()).rollForwardToFutureVersion(any(), any(), anyInt());
    }
    adminSpy.rollForwardToFutureVersion(clusterName, storeName, "r1");

    verify(adminSpy).truncateKafkaTopic(Version.composeKafkaTopic(storeName, 5));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Roll forward failed in the following regions.*")
  public void testRollForwardPartialFailure() {
    VeniceParentHelixAdmin adminSpy = spy(parentAdmin);
    doNothing().when(adminSpy).acquireAdminMessageLock(clusterName, storeName);
    doNothing().when(adminSpy).releaseAdminMessageLock(clusterName, storeName);

    Map<String, String> future = new HashMap<>();
    future.put("r1", "5");
    future.put("r2", "5");
    doReturn(future).when(adminSpy).getFutureVersionsForMultiColos(clusterName, storeName);

    doNothing().when(adminSpy)
        .sendAdminMessageAndWaitForConsumed(eq(clusterName), eq(storeName), any(AdminOperation.class));

    doReturn(ConcurrentPushDetectionStrategy.TOPIC_BASED_ONLY).when(config).getConcurrentPushDetectionStrategy();
    doReturn(true).when(adminSpy).truncateKafkaTopic(anyString());

    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      ControllerResponse response = new ControllerResponse();
      response.setError("test error");
      doReturn(response).when(entry.getValue()).rollForwardToFutureVersion(any(), any(), anyInt());
    }
    adminSpy.rollForwardToFutureVersion(clusterName, storeName, null);
  }

  @Test
  public void testUpdateStoreETLConfig() {
    String storeName = Utils.getUniqueString("testUpdatedStoreETLConfigs");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));
    String etlUserProxyAccount = "test";
    parentAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setRegularVersionETLEnabled(true).setEtledProxyUserAccount(etlUserProxyAccount));
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.UPDATE_STORE.getValue());

    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    ETLStoreConfigRecord etlStoreConfigRecord = updateStore.getETLStoreConfig();
    Assert.assertNotNull(etlStoreConfigRecord);
    Assert.assertTrue(etlStoreConfigRecord.regularVersionETLEnabled);
    Assert.assertEquals(etlStoreConfigRecord.etledUserProxyAccount.toString(), etlUserProxyAccount);
    Assert.assertFalse(etlStoreConfigRecord.futureVersionETLEnabled);
    Assert.assertEquals(etlStoreConfigRecord.etlStrategy, VeniceETLStrategy.EXTERNAL_SERVICE.getValue());
  }

  @Test
  public void testUpdateStoreFlinkVeniceViewsEnable() {
    String storeName = Utils.getUniqueString("testUpdateStoreFlinkVeniceViewsEnable");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setFlinkVeniceViewsEnabled(true));

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.UPDATE_STORE.getValue());

    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    assertTrue(updateStore.flinkVeniceViewsEnabled);
  }

  private Store setupForStoreViewConfigUpdateTest(String storeName) {
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    store.setActiveActiveReplicationEnabled(true);
    store.setChunkingEnabled(true);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(
            AdminTopicMetadataAccessor.generateMetadataMap(
                Optional.of(1L),
                Optional.of(-1L),
                Optional.of(1L),
                Optional.of(LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)));

    parentAdmin.initStorageCluster(clusterName);
    return store;
  }

  private AdminOperation verifyAndGetSingleAdminOperation() {
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter, times(1)).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    return adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
  }
}
