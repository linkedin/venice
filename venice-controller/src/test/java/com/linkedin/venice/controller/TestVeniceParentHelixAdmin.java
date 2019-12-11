package com.linkedin.venice.controller;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.ZkAdminTopicMetadataAccessor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.ParentHelixOfflinePushAccessor;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;

import org.apache.avro.Schema;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;


public class TestVeniceParentHelixAdmin {
  private static final Logger logger = Logger.getLogger(TestVeniceParentHelixAdmin.class);
  private static final int TIMEOUT_IN_MS = 60 * Time.MS_PER_SECOND;
  private static int KAFKA_REPLICA_FACTOR = 3;
  private static final String PUSH_JOB_STATUS_STORE_NAME = "push-job-status-store";
  private static final String PUSH_JOB_DETAILS_STORE_NAME = VeniceSystemStoreUtils.getPushJobDetailsStoreName();

  private final String clusterName = "test-cluster";
  private final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final String zkMetadataNodePath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(clusterName);
  private final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;
  private final TopicPartition topicPartition = new TopicPartition(topicName, partitionId);
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private TopicManager topicManager;
  private VeniceHelixAdmin internalAdmin;
  private VeniceControllerConfig config;
  private ZkClient zkClient;
  private VeniceWriter veniceWriter;
  private VeniceParentHelixAdmin parentAdmin;
  private VeniceHelixResources resources;
  private Store store;
  private ParentHelixOfflinePushAccessor accessor;

  @BeforeMethod
  public void setupTestCase() {
    topicManager = mock(TopicManager.class);
    doReturn(new HashSet<String>(Arrays.asList(topicName))).when(topicManager).listTopics();
    doReturn(true).when(topicManager).containsTopic(topicName);

    internalAdmin = mock(VeniceHelixAdmin.class);
    doReturn(topicManager).when(internalAdmin).getTopicManager();

    zkClient = mock(ZkClient.class);
    doReturn(zkClient).when(internalAdmin).getZkClient();
    doReturn(new HelixAdapterSerializer()).when(internalAdmin).getAdapterSerializer();

    ExecutionIdAccessor executionIdAccessor = mock(ExecutionIdAccessor.class);
    doReturn(executionIdAccessor).when(internalAdmin).getExecutionIdAccessor();
    doReturn(0L).when(executionIdAccessor).getLastSucceededExecutionId(any());

    store = mock(Store.class);
    doReturn(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION).when(store).getOffLinePushStrategy();

    HelixReadWriteStoreRepository storeRepository = mock(HelixReadWriteStoreRepository.class);
    doReturn(store).when(storeRepository).getStore(any());

    config = mockConfig(clusterName);
    doReturn(1).when(config).getParentControllerWaitingTimeForConsumptionMs();

    resources = mockResources(config, clusterName);
    doReturn(storeRepository).when(resources).getMetadataRepository();

    parentAdmin = new VeniceParentHelixAdmin(internalAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
    parentAdmin.getAdminCommandExecutionTracker(clusterName)
        .get()
        .getFabricToControllerClientsMap()
        .put("test-fabric", mock(ControllerClient.class));

    accessor = mock(ParentHelixOfflinePushAccessor.class);
    parentAdmin.setOfflinePushAccessor(accessor);

    // Need to bypass VeniceWriter initialization
    veniceWriter = mock(VeniceWriter.class);
    parentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
  }

  @DataProvider(name = "isControllerSslEnabled")
  public static Object[][] isControllerSslEnabled() {
    return new Object[][]{{false}, {true}};
  }

  private VeniceControllerConfig mockConfig(String clusterName) {
    VeniceControllerConfig config = mock(VeniceControllerConfig.class);
    doReturn(clusterName).when(config).getClusterName();
    doReturn(KAFKA_REPLICA_FACTOR).when(config).getKafkaReplicationFactor();
    doReturn(10000).when(config).getParentControllerWaitingTimeForConsumptionMs();
    doReturn("fake_kafka_bootstrap_servers").when(config).getKafkaBootstrapServers();
    doReturn(clusterName).when(config).getPushJobStatusStoreClusterName();
    doReturn(PUSH_JOB_STATUS_STORE_NAME).when(config).getPushJobStatusStoreName();
    doReturn(true).when(config).isParticipantMessageStoreEnabled();
    Map<String, String> childClusterMap = new HashMap<>();
    childClusterMap.put("test-colo", "localhost");
    doReturn(childClusterMap).when(config).getChildClusterMap();
    return config;
  }

  private VeniceHelixResources mockResources(VeniceControllerConfig config, String clusterName) {
    VeniceHelixResources resources = mock(VeniceHelixResources.class);
    doReturn(config).when(resources).getConfig();
    doReturn(resources).when(internalAdmin).getVeniceHelixResource(any());
    return resources;
  }

  @Test
  public void testStartWithTopicExists() {
    parentAdmin.start(clusterName);
    verify(internalAdmin).getTopicManager();
    verify(topicManager, never()).createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC,
        KAFKA_REPLICA_FACTOR);
  }

  @Test
  public void testStartWhenTopicNotExists() {
    doReturn(false).when(topicManager).containsTopic(topicName);
    parentAdmin.start(clusterName);
    verify(internalAdmin).getTopicManager();
    verify(topicManager).createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, KAFKA_REPLICA_FACTOR);
  }

  /**
   * Partially stubbed class to verify async setup behavior.
   */
  private static class AsyncSetupMockVeniceParentHelixAdmin extends VeniceParentHelixAdmin {
    private Map<String, Store> systemStores = new VeniceConcurrentHashMap<>();

    public AsyncSetupMockVeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerConfig config) {
      super(veniceHelixAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
    }

    public boolean isAsyncSetupRunning(String clusterName) {
      return asyncSetupEnabledMap.get(clusterName);
    }

    @Override
    public void addStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
      if (systemStores.containsKey(storeName)) {
        // no op
        return;
      }
      Store newStore = new Store(storeName, owner, System.currentTimeMillis(), PersistenceType.IN_MEMORY,
          RoutingStrategy.HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
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
    public void updateStore(String clusterName,
        String storeName,
        Optional<String> owner,
        Optional<Boolean> readability,
        Optional<Boolean> writeability,
        Optional<Integer> partitionCount,
        Optional<Long> storageQuotaInByte,
        Optional<Boolean> hybridStoreOverheadBypass,
        Optional<Long> readQuotaInCU,
        Optional<Integer> currentVersion,
        Optional<Integer> largestUsedVersionNumber,
        Optional<Long> hybridRewindSeconds,
        Optional<Long> hybridOffsetLagThreshold,
        Optional<Boolean> accessControlled,
        Optional<CompressionStrategy> compressionStrategy,
        Optional<Boolean> clientDecompressionEnabled,
        Optional<Boolean> chunkingEnabled,
        Optional<Boolean> singleGetRouterCacheEnabled,
        Optional<Boolean> batchGetRouterCacheEnabled,
        Optional<Integer> batchGetLimit,
        Optional<Integer> numVersionsToPreserve,
        Optional<Boolean> incrementalPushEnabled,
        Optional<Boolean> storeMigration,
        Optional<Boolean> writeComputationEnabled,
        Optional<Boolean> readComputationEnabled,
        Optional<Integer> bootstrapToOnlineTimeoutInHours,
        Optional<Boolean> leaderFollowerModelEnabled,
        Optional<BackupStrategy> backupStategy,
        Optional<Boolean> autoSchmePushJob,
        Optional<Boolean> autoSchmeAdmin,
        Optional<Boolean> hybridStoreDiskQuotaEnabled) {
      if (!systemStores.containsKey(storeName)) {
        throw new VeniceNoStoreException("Cannot update store " + storeName + " because it's missing.");
      }
      if (hybridRewindSeconds.isPresent() && hybridOffsetLagThreshold.isPresent()) {
        systemStores.get(storeName)
            .setHybridStoreConfig(new HybridStoreConfig(hybridRewindSeconds.get(), hybridOffsetLagThreshold.get()));
      }
    }

    @Override
    public Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId,
        int numberOfPartition, int replicationFactor) {
      if (!systemStores.containsKey(storeName)) {
        throw new VeniceNoStoreException("Cannot add version to store " + storeName + " because it's missing.");
      }
      Version version = new Version(storeName, 1, "test-id");
      List<Version> versions = new ArrayList<>();
      versions.add(version);
      systemStores.get(storeName).setVersions(versions);
      return version;
    }
  }

  @Test (timeOut = TIMEOUT_IN_MS)
  public void testAsyncSetupForPushStatusStore() {
    String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
    doReturn(true).when(internalAdmin).isMasterController(clusterName);
    doReturn(Version.composeRealTimeTopic(PUSH_JOB_STATUS_STORE_NAME)).when(internalAdmin)
        .getRealTimeTopic(clusterName, PUSH_JOB_STATUS_STORE_NAME);
    doReturn(Version.composeRealTimeTopic(PUSH_JOB_DETAILS_STORE_NAME)).when(internalAdmin)
        .getRealTimeTopic(clusterName, PUSH_JOB_DETAILS_STORE_NAME);
    doReturn(Version.composeRealTimeTopic(participantStoreName)).when(internalAdmin)
        .getRealTimeTopic(clusterName, participantStoreName);
    AsyncSetupMockVeniceParentHelixAdmin mockVeniceParentHelixAdmin =
        new AsyncSetupMockVeniceParentHelixAdmin(internalAdmin, config);
    mockVeniceParentHelixAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    mockVeniceParentHelixAdmin.setTimer(new MockTime());
    try {
      mockVeniceParentHelixAdmin.start(clusterName);
      String[] systemStoreNames = {PUSH_JOB_STATUS_STORE_NAME, PUSH_JOB_DETAILS_STORE_NAME, participantStoreName};
      for (String systemStore : systemStoreNames) {
        TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> {
          Store s = mockVeniceParentHelixAdmin.getStore(clusterName, systemStore);
          return s != null && !s.getVersions().isEmpty();
        });
        Store verifyStore = mockVeniceParentHelixAdmin.getStore(clusterName, systemStore);
        Assert.assertEquals(verifyStore.getName(), systemStore, "Unexpected store name");
        Assert.assertTrue(verifyStore.isHybrid(), "Store should be configured to be hybrid");
        Assert.assertEquals(verifyStore.getVersions().size(), 1 , "Store should have one version");
      }
    } finally {
      mockVeniceParentHelixAdmin.stop(clusterName);
    }
    Assert.assertFalse(mockVeniceParentHelixAdmin.isAsyncSetupRunning(clusterName), "Async setup should be stopped");
  }

  @Test
  public void testAddStore() {
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    verify(internalAdmin)
        .checkPreConditionForAddStore(clusterName, storeName, keySchemaStr, valueSchemaStr);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.STORE_CREATION.getValue());

    StoreCreation storeCreationMessage = (StoreCreation) adminMessage.payloadUnion;
    Assert.assertEquals(storeCreationMessage.clusterName.toString(), clusterName);
    Assert.assertEquals(storeCreationMessage.storeName.toString(), storeName);
    Assert.assertEquals(storeCreationMessage.owner.toString(), owner);
    Assert.assertEquals(storeCreationMessage.keySchema.definition.toString(), keySchemaStr);
    Assert.assertEquals(storeCreationMessage.valueSchema.definition.toString(), valueSchemaStr);
  }

  @Test
  public void testAddStoreForMultiCluster() {
    String secondCluster = "testAddStoreForMultiCluster";
    VeniceControllerConfig configForSecondCluster = mockConfig(secondCluster);
    mockResources(configForSecondCluster, secondCluster);
    Map<String, VeniceControllerConfig> configMap = new HashMap<>();
    configMap.put(clusterName, config);
    configMap.put(secondCluster, configForSecondCluster);
    parentAdmin = new VeniceParentHelixAdmin(internalAdmin, new VeniceControllerMultiClusterConfig(configMap));
    Map<String, VeniceWriter> writerMap = new HashMap<>();
    for(String cluster:configMap.keySet()) {
      parentAdmin.getAdminCommandExecutionTracker(cluster).get().getFabricToControllerClientsMap()
          .put("test-fabric", mock(ControllerClient.class));
      VeniceWriter veniceWriter = mock(VeniceWriter.class);
      // Need to bypass VeniceWriter initialization
      parentAdmin.setVeniceWriterForCluster(cluster, veniceWriter);
      writerMap.put(cluster,veniceWriter);
      parentAdmin.start(cluster);
    }

    for(String cluster:configMap.keySet()){
      String adminTopic =  AdminTopicUtils.getTopicNameFromClusterName(cluster);
      String metadataPath = ZkAdminTopicMetadataAccessor.getAdminTopicMetadataNodePath(cluster);

      VeniceWriter veniceWriter = writerMap.get(cluster);

      // Return offset -1 before writing any data into topic.
      when(zkClient.readData(metadataPath, null))
          .thenReturn(null);

      String storeName = "test-store-"+cluster;
      String owner = "test-owner-"+cluster;
      String keySchemaStr = "\"string\"";
      String valueSchemaStr = "\"string\"";
      when(veniceWriter.put(any(),any(),anyInt())).then(invocation -> {
        // Once we send message to topic through venice writer, return offset 1
        when(zkClient.readData(metadataPath, null))
            .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));
        Future future = mock(Future.class);
        doReturn(new RecordMetadata(new TopicPartition(adminTopic, partitionId), 0, 1, -1, -1L, -1, -1))
            .when(future).get();
        return future;
      });

      parentAdmin.addStore(cluster, storeName, owner, keySchemaStr, valueSchemaStr);

      verify(internalAdmin)
          .checkPreConditionForAddStore(cluster, storeName, keySchemaStr, valueSchemaStr);
      verify(veniceWriter)
          .put(any(), any(), anyInt());
      verify(zkClient, times(1))
          .readData(metadataPath, null);
      ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
      verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
      byte[] keyBytes = keyCaptor.getValue();
      byte[] valueBytes = valueCaptor.getValue();
      int schemaId = schemaCaptor.getValue();
      Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      Assert.assertEquals(keyBytes.length, 0);
      AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
      Assert.assertEquals(adminMessage.operationType, AdminMessageType.STORE_CREATION.getValue());
      StoreCreation storeCreationMessage = (StoreCreation) adminMessage.payloadUnion;
      Assert.assertEquals(storeCreationMessage.clusterName.toString(), cluster);
      Assert.assertEquals(storeCreationMessage.storeName.toString(), storeName);
      Assert.assertEquals(storeCreationMessage.owner.toString(), owner);
      Assert.assertEquals(storeCreationMessage.keySchema.definition.toString(), keySchemaStr);
      Assert.assertEquals(storeCreationMessage.valueSchema.definition.toString(), valueSchemaStr);
    }

  }

  @Test
  public void testAddStoreWhenExists() {
    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    doThrow(new VeniceStoreAlreadyExistsException(storeName, clusterName))
        .when(internalAdmin).checkPreConditionForAddStore(clusterName, storeName, keySchemaStr, valueSchemaStr);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null);
    parentAdmin.start(clusterName);

    Assert.assertThrows(VeniceStoreAlreadyExistsException.class, () -> parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr));
  }

  @Test
  public void testAddStoreWhenLastExceptionIsNotNull() {
    String storeName = "test-store";
    when(internalAdmin.getLastExceptionForStore(clusterName, storeName))
        .thenReturn(null)
        .thenReturn(new VeniceException("mock exception"));
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.start(clusterName);
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    // Add store again now with an existing exception
    Assert.assertThrows(VeniceException.class,
        () -> parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr));
  }

  @Test
  public void testAddValueSchema() {
    String storeName = "test-store";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin)
        .getStore(clusterName, storeName);

    int valueSchemaId = 10;
    String valueSchemaStr = "\"string\"";
    doReturn(valueSchemaId).when(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);
    doReturn(valueSchemaId).when(internalAdmin)
        .getValueSchemaId(clusterName, storeName, valueSchemaStr);

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);
    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);

    verify(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.VALUE_SCHEMA_CREATION.getValue());

    ValueSchemaCreation valueSchemaCreationMessage = (ValueSchemaCreation) adminMessage.payloadUnion;
    Assert.assertEquals(valueSchemaCreationMessage.clusterName.toString(), clusterName);
    Assert.assertEquals(valueSchemaCreationMessage.storeName.toString(), storeName);
    Assert.assertEquals(valueSchemaCreationMessage.schema.definition.toString(), valueSchemaStr);
    Assert.assertEquals(valueSchemaCreationMessage.schemaId, valueSchemaId);
  }

  @Test
  public void testAddDerivedSchema() {
    String storeName = "test-store";
    String derivedSchemaStr = "\"string\"";
    int valueSchemaId = 10;
    int derivedSchemaId = 1;

    doReturn(derivedSchemaId).when(internalAdmin)
        .checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    doReturn(new Pair<>(valueSchemaId, derivedSchemaId))
        .when(internalAdmin).getDerivedSchemaId(clusterName, storeName, derivedSchemaStr);

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);
    parentAdmin.addDerivedSchema(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(any(), valueCaptor.capture(), schemaCaptor.capture());

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueCaptor.getValue(), schemaCaptor.getValue());
    DerivedSchemaCreation derivedSchemaCreation = (DerivedSchemaCreation) adminMessage.payloadUnion;

    Assert.assertEquals(derivedSchemaCreation.clusterName.toString(), clusterName);
    Assert.assertEquals(derivedSchemaCreation.storeName.toString(), storeName);
    Assert.assertEquals(derivedSchemaCreation.schema.definition.toString(), derivedSchemaStr);
    Assert.assertEquals(derivedSchemaCreation.valueSchemaId, valueSchemaId);
    Assert.assertEquals(derivedSchemaCreation.derivedSchemaId, derivedSchemaId);
  }

  @Test
  public void testDisableStoreRead() {
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));


    String storeName = "test-store";
    parentAdmin.start(clusterName);
    parentAdmin.setStoreReadability(clusterName, storeName, false);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.DISABLE_STORE_READ.getValue());

    DisableStoreRead disableStoreRead = (DisableStoreRead) adminMessage.payloadUnion;
    Assert.assertEquals(disableStoreRead.clusterName.toString(), clusterName);
    Assert.assertEquals(disableStoreRead.storeName.toString(), storeName);
  }

  @Test
  public void testDisableStoreWrite() {
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    String storeName = "test-store";
    parentAdmin.start(clusterName);
    parentAdmin.setStoreWriteability(clusterName, storeName, false);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.DISABLE_STORE_WRITE.getValue());

    PauseStore pauseStore = (PauseStore) adminMessage.payloadUnion;
    Assert.assertEquals(pauseStore.clusterName.toString(), clusterName);
    Assert.assertEquals(pauseStore.storeName.toString(), storeName);
  }

  @Test
  public void testDisableStoreWriteWhenStoreDoesNotExist() {
    String storeName = "test-store";
    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin).checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);
    Assert.assertThrows(VeniceNoStoreException.class, () -> parentAdmin.setStoreWriteability(clusterName, storeName, false));
  }

  @Test
  public void testEnableStoreRead() {
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    String storeName = "test-store";
    parentAdmin.start(clusterName);
    parentAdmin.setStoreReadability(clusterName, storeName, true);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.ENABLE_STORE_READ.getValue());

    EnableStoreRead enableStoreRead = (EnableStoreRead) adminMessage.payloadUnion;
    Assert.assertEquals(enableStoreRead.clusterName.toString(), clusterName);
    Assert.assertEquals(enableStoreRead.storeName.toString(), storeName);
  }

  @Test
  public void testEnableStoreWrite() {
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    String storeName = "test-store";
    parentAdmin.start(clusterName);
    parentAdmin.setStoreWriteability(clusterName, storeName, true);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.ENABLE_STORE_WRITE.getValue());

    ResumeStore resumeStore = (ResumeStore) adminMessage.payloadUnion;
    Assert.assertEquals(resumeStore.clusterName.toString(), clusterName);
    Assert.assertEquals(resumeStore.storeName.toString(), storeName);
  }

  @Test
  public void testKillOfflinePushJob() {
    String kafkaTopic = "test_store_v1";
    doReturn(new HashSet<>(Arrays.asList(kafkaTopic)))
        .when(topicManager).listTopics();

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));
    Store store = mock(Store.class);
    doReturn(store)
        .when(internalAdmin).getStore(clusterName, Version.parseStoreFromKafkaTopicName(kafkaTopic));

    parentAdmin.start(clusterName);
    parentAdmin.killOfflinePush(clusterName, kafkaTopic);

    verify(internalAdmin)
        .checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
    verify(internalAdmin)
        .truncateKafkaTopic(kafkaTopic);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.KILL_OFFLINE_PUSH_JOB.getValue());

    KillOfflinePushJob killJob = (KillOfflinePushJob) adminMessage.payloadUnion;
    Assert.assertEquals(killJob.clusterName.toString(), clusterName);
    Assert.assertEquals(killJob.kafkaTopic.toString(), kafkaTopic);
  }

  @Test
  public void testIdempotentIncrementVersionWhenNoPreviousTopics() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    doReturn(new Version(storeName, 1, pushJobId)).when(internalAdmin)
        .addVersionOnly(clusterName, storeName, pushJobId, 1, 1, false, false, Version.PushType.BATCH);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1);
    verify(internalAdmin).addVersionOnly(clusterName, storeName, pushJobId, 1, 1, false, false, Version.PushType.BATCH);
  }

  /**
   * This class is used to assist unit test for {@link VeniceParentHelixAdmin#incrementVersionIdempotent(String, String, String, int, int)}
   * to mock various offline job status.
   */
  private static class PartialMockVeniceParentHelixAdmin extends VeniceParentHelixAdmin {
    private ExecutionStatus offlineJobStatus = ExecutionStatus.NOT_CREATED;

    public PartialMockVeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerConfig config) {
      super(veniceHelixAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
    }

    public void setOfflineJobStatus(ExecutionStatus executionStatus) {
      this.offlineJobStatus = executionStatus;
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
      return new OfflinePushStatusInfo(offlineJobStatus);
    }
  }

  @Test
  public void testIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsStillRunning() {
    String storeName = TestUtils.getUniqueString("test_store");
    String previousKafkaTopic = storeName + "_v1";
    String unknownTopic = "1unknown_topic_v1";
    doReturn(new HashSet<>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager).listTopics();

    Store store = new Store(storeName, "test_owner", 1, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    String pushJobId = "test_push_id";
    String pushJobId2 = "test_push_id2";
    store.addVersion(new Version(storeName, 1, pushJobId));
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.PROGRESS);

    Assert.assertThrows(VeniceException.class, () -> partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId2, 1, 1));
  }

  /**
   * Idempotent increment version should work because existing topic uses the same push ID as the request
   */
  @Test
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsNotDoneForSamePushId() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    String previousKafkaTopic = storeName + "_v1";
    doReturn(new HashSet<>(Arrays.asList(previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Version version = new Version(storeName, 1, pushJobId);
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(version).when(internalAdmin)
        .addVersionOnly(clusterName, storeName, pushJobId, 1, 1, false, false, Version.PushType.BATCH);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));
    Version newVersion =
        partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1,
            Version.PushType.BATCH, false, false);
    verify(internalAdmin).addVersionOnly(clusterName, storeName, pushJobId, 1, 1, false, false, Version.PushType.BATCH);
    Assert.assertEquals(newVersion, version);
  }

  /**
   * Idempotent increment version should work because existing topic is truncated
   */
  @Test
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistButTruncated() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    String previousKafkaTopic = storeName + "_v1";
    doReturn(new HashSet<>(Arrays.asList(previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Version version = new Version(storeName, 1, pushJobId + "_different");
    store.addVersion(version);
    doReturn(true).when(internalAdmin).isTopicTruncated(previousKafkaTopic);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(new Version(storeName, 1, pushJobId)).when(internalAdmin)
        .addVersionOnly(clusterName, storeName, pushJobId, 1, 1, false, false, Version.PushType.BATCH);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, Version.PushType.BATCH, false, false);
    verify(internalAdmin).addVersionOnly(clusterName, storeName, pushJobId, 1, 1, false, false, Version.PushType.BATCH);
  }

  /**
   * Idempotent increment version should NOT work because existing topic uses different push ID than the request
   */
  @Test
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsNotDoneForDifferentPushId() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    String previousKafkaTopic = storeName + "_v1";
    doReturn(new HashSet<>(Arrays.asList(previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Version version = new Version(storeName, 1, Version.guidBasedDummyPushId());
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    try {
      partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1);
    } catch (VeniceException e){
      Assert.assertTrue(e.getMessage().contains(pushJobId), "Exception for topic exists when increment version should contain requested pushId");
    }
  }

  @Test
  public void testStoreVersionCleanUpWithFewerVersions() {
    String storeName = "test_store";
    Store testStore = new Store(storeName, "test_owner", -1, PersistenceType.BDB,
            RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
    testStore.addVersion(new Version(storeName, 1));
    testStore.addVersion(new Version(storeName, 2));
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(testStore).when(storeRepo).getStore(storeName);
    doReturn(storeRepo).when(resources)
            .getMetadataRepository();
    parentAdmin.cleanupHistoricalVersions(clusterName, storeName);
    verify(storeRepo).getStore(storeName);
    verify(storeRepo, never()).updateStore(any());
  }

  @Test
  public void testStoreVersionCleanUpWithMoreVersions() {
    String storeName = "test_store";
    Store testStore = new Store(storeName, "test_owner", -1, PersistenceType.BDB,
            RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
    for (int i = 1; i <= 10; ++i) {
      testStore.addVersion(new Version(storeName, i));
    }
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(testStore).when(storeRepo).getStore(storeName);
    doReturn(storeRepo).when(resources)
            .getMetadataRepository();
    parentAdmin.cleanupHistoricalVersions(clusterName, storeName);
    verify(storeRepo).getStore(storeName);
    ArgumentCaptor<Store> storeCaptor = ArgumentCaptor.forClass(Store.class);
    verify(storeRepo).updateStore(storeCaptor.capture());
    Store capturedStore = storeCaptor.getValue();
    Assert.assertEquals(capturedStore.getVersions().size(), VeniceParentHelixAdmin.STORE_VERSION_RETENTION_COUNT);
    for (int i = 1; i <= 5; ++i) {
      Assert.assertFalse(capturedStore.containsVersion(i));
    }
    for (int i = 6; i <= 10; ++i) {
      Assert.assertTrue(capturedStore.containsVersion(i));
    }
  }
  //get a map of mock client that can return vairable execution status
  Map<ExecutionStatus, ControllerClient> getMockJobStatusQueryClient() {
    Map<ExecutionStatus, ControllerClient> clientMap = new HashMap<>();
    for (ExecutionStatus status : ExecutionStatus.values()){
      JobStatusQueryResponse response = new JobStatusQueryResponse();
      response.setStatus(status.toString());
      ControllerClient statusClient = mock(ControllerClient.class);
      doReturn(response).when(statusClient).queryJobStatus(anyString(), any());
      clientMap.put(status, statusClient);
    }

    return clientMap;
  }

  private Schema generateSchema(boolean addFieldWithDefaultValue) {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n";
    if (addFieldWithDefaultValue) {
      schemaStr += "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
    } else {
      schemaStr +=   "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";
    }
    schemaStr +=
        "           ]\n" +
            "        }\n" +
            "      }\n" +
            " ]\n" +
            "}";
    return Schema.parse(schemaStr);
  }

  private Schema generateSuperSetSchema() {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"},\n" +
        "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";

    schemaStr +=
        "           ]\n" +
            "        }\n" +
            "      }\n" +
            " ]\n" +
            "}";
    return Schema.parse(schemaStr);
  }

  private Schema generateSuperSetSchemaNewField() {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"},\n" +
        "{\"name\": \"favorite_company\", \"type\": \"string\", \"default\": \"linkedin\"},\n" +
        "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";

    schemaStr +=
        "           ]\n" +
            "        }\n" +
            "      }\n" +
            " ]\n" +
            "}";
    return Schema.parse(schemaStr);
  }

  @Test(dataProvider = "isControllerSslEnabled")
  public void testSuperSetSchemaGen(boolean isControllerSslEnabled) throws IOException {
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper childControllerWrapper =
        ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, isControllerSslEnabled);
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper controllerWrapper =
        ServiceFactory.getVeniceParentController(clusterName, parentZk.getAddress(), kafkaBrokerWrapper,
            new VeniceControllerWrapper[]{childControllerWrapper}, isControllerSslEnabled);

    String controllerUrl = isControllerSslEnabled ? controllerWrapper.getSecureControllerUrl() : controllerWrapper.getControllerUrl();
    Optional<SSLFactory> sslFactory = isControllerSslEnabled ? Optional.of(SslUtils.getVeniceLocalSslFactory()) : Optional.empty();

    // Adding store
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchema = generateSchema(false);

    ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl, sslFactory);
    controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    params.setAutoSupersetSchemaEnabledFromReadComputeStore(true);
    params.setAutoSchemaPushJobEnabled(true);
    controllerClient.updateStore(storeName, params);

    valueSchema = generateSchema(true);
    controllerClient.addValueSchema(storeName, valueSchema.toString());

    MultiSchemaResponse schemaResponse = controllerClient.getAllValueSchema(storeName);

    Assert.assertEquals(schemaResponse.getSchemas().length,3);
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().getLatestSuperSetValueSchemaId() != -1);

    valueSchema = generateSuperSetSchemaNewField();
    controllerClient.addValueSchema(storeName, valueSchema.toString());

    schemaResponse = controllerClient.getAllValueSchema(storeName);
    Assert.assertEquals(schemaResponse.getSchemas().length,4);

    controllerWrapper.close();
    childControllerWrapper.close();
    kafkaBrokerWrapper.close();
  }

  @Test(dataProvider = "isControllerSslEnabled")
  public void testSuperSetSchemaGenWithSameUpcomingSchema(boolean isControllerSslEnabled) throws IOException {
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper childControllerWrapper =
        ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, isControllerSslEnabled);
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper controllerWrapper =
        ServiceFactory.getVeniceParentController(clusterName, parentZk.getAddress(), kafkaBrokerWrapper,
            new VeniceControllerWrapper[]{childControllerWrapper}, isControllerSslEnabled);

    String controllerUrl = isControllerSslEnabled ? controllerWrapper.getSecureControllerUrl() : controllerWrapper.getControllerUrl();
    Optional<SSLFactory> sslFactory = isControllerSslEnabled ? Optional.of(SslUtils.getVeniceLocalSslFactory()) : Optional.empty();

    // Adding store
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchema = generateSchema(false);

    ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl, sslFactory);
    controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    params.setAutoSupersetSchemaEnabledFromReadComputeStore(true);
    params.setAutoSchemaPushJobEnabled(true);
    controllerClient.updateStore(storeName, params);

    valueSchema = generateSuperSetSchema();
    controllerClient.addValueSchema(storeName, valueSchema.toString());

    MultiSchemaResponse schemaResponse = controllerClient.getAllValueSchema(storeName);

    Assert.assertEquals(schemaResponse.getSchemas().length,2);
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().getLatestSuperSetValueSchemaId() == -1);

    controllerWrapper.close();
    childControllerWrapper.close();
    kafkaBrokerWrapper.close();
  }

  @Test
  public void testAddValueSchemaDocUpdate() throws IOException {
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper childControllerWrapper =
        ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, false);
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper controllerWrapper =
        ServiceFactory.getVeniceParentController(clusterName, parentZk.getAddress(), kafkaBrokerWrapper,
            new VeniceControllerWrapper[]{childControllerWrapper}, false);

    String controllerUrl = controllerWrapper.getControllerUrl();
    Optional<SSLFactory> sslFactory =  Optional.empty();

    // Adding store
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"double\"}]}";
    String schemaStrDoc = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field updated\"},{\"name\":\"id1\",\"type\":\"double\"}]}";

    Schema valueSchema = Schema.parse(schemaStr);

    ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl, sslFactory);
    controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());

    valueSchema = Schema.parse(schemaStrDoc);
    controllerClient.addValueSchema(storeName, valueSchema.toString());

    MultiSchemaResponse schemaResponse = controllerClient.getAllValueSchema(storeName);

    Assert.assertEquals(schemaResponse.getSchemas().length,2);

    controllerWrapper.close();
    childControllerWrapper.close();
    kafkaBrokerWrapper.close();
  }

  @Test
  public void testGetIncrementalPushVersion() {
   Version incrementalPushVersion = new Version("testStore", 1);
   Assert.assertEquals(parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.COMPLETED), incrementalPushVersion);

   try {
     parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.STARTED);
     Assert.fail();
   } catch (VeniceException e) {}

   try {
     parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.ERROR);
     Assert.fail();
   } catch (VeniceException e) {}

   doReturn(true).when(internalAdmin).isTopicTruncated(incrementalPushVersion.kafkaTopicName());
   try {
     parentAdmin.getIncrementalPushVersion(incrementalPushVersion, ExecutionStatus.COMPLETED);
     Assert.fail();
   } catch (VeniceException e) {}
  }

  @Test
  public void testGetExecutionStatus(){
    Map<ExecutionStatus, ControllerClient> clientMap = getMockJobStatusQueryClient();
    TopicManager topicManager = mock(TopicManager.class);

    JobStatusQueryResponse failResponse = new JobStatusQueryResponse();
    failResponse.setError("error");
    ControllerClient failClient = mock(ControllerClient.class);
    doReturn(failResponse).when(failClient).queryJobStatus(anyString(), any());
    clientMap.put(null, failClient);

    // Verify clients work as expected
    for (ExecutionStatus status : ExecutionStatus.values()) {
      Assert.assertEquals(clientMap.get(status).queryJobStatus("topic", Optional.empty())
          .getStatus(), status.toString());
    }
    Assert.assertTrue(clientMap.get(null).queryJobStatus("topic", Optional.empty())
        .isError());

    Map<String, ControllerClient> completeMap = new HashMap<>();
    completeMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    completeMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    completeMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    Set<String> topicList = new HashSet<>(
        Arrays.asList(
            "topic1_v1",
            "topic2_v1",
            "topic3_v1",
            "topic4_v1",
            "topic5_v1",
            "topic6_v1",
            "topic7_v1",
            "topic8_v1",
            "topic9_v1"
        )
    );
    doReturn(topicList).when(topicManager).listTopics();
    Store store = mock(Store.class);
    doReturn(false).when(store).isIncrementalPushEnabled();
    doReturn(Optional.empty()).when(store).getVersion(anyInt());
    doReturn(store).when(internalAdmin).getStore(anyString(), anyString());

    Admin.OfflinePushStatusInfo
        offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic1_v1", completeMap, topicManager);
    Map<String, String> extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.COMPLETED);
    verify(internalAdmin, timeout(TIMEOUT_IN_MS)).truncateKafkaTopic("topic1_v1");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());

    completeMap.put("cluster-slow", clientMap.get(ExecutionStatus.NOT_CREATED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic2_v1", completeMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NOT_CREATED);  // Do we want this to be Progress?  limitation of ordering used in aggregation code
    verify(internalAdmin, never()).truncateKafkaTopic("topic2_v1");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster-slow"), ExecutionStatus.NOT_CREATED.toString());

    Map<String, ControllerClient> progressMap = new HashMap<>();
    progressMap.put("cluster", clientMap.get(ExecutionStatus.NOT_CREATED));
    progressMap.put("cluster3", clientMap.get(ExecutionStatus.NOT_CREATED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic3_v1", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NOT_CREATED);
    verify(internalAdmin, never()).truncateKafkaTopic("topic3_v1");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.NOT_CREATED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.NOT_CREATED.toString());

    progressMap.put("cluster5", clientMap.get(ExecutionStatus.NEW));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic4_v1", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NEW);
    verify(internalAdmin, never()).truncateKafkaTopic("topic4_v1");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.NOT_CREATED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.NOT_CREATED.toString());
    Assert.assertEquals(extraInfo.get("cluster5"), ExecutionStatus.NEW.toString());

    progressMap.put("cluster7", clientMap.get(ExecutionStatus.PROGRESS));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic5_v1", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic5_v1");;
    Assert.assertEquals(extraInfo.get("cluster7"), ExecutionStatus.PROGRESS.toString());

    progressMap.put("cluster9", clientMap.get(ExecutionStatus.STARTED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic6_v1", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic6_v1");
    Assert.assertEquals(extraInfo.get("cluster9"), ExecutionStatus.STARTED.toString());

    progressMap.put("cluster11", clientMap.get(ExecutionStatus.END_OF_PUSH_RECEIVED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic7_v1", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic7_v1");
    Assert.assertEquals(extraInfo.get("cluster11"), ExecutionStatus.END_OF_PUSH_RECEIVED.toString());

    progressMap.put("cluster13", clientMap.get(ExecutionStatus.COMPLETED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("IGNORED", "topic8_v1", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(internalAdmin, never()).truncateKafkaTopic("topic8_v1");
    Assert.assertEquals(extraInfo.get("cluster13"), ExecutionStatus.COMPLETED.toString());

    // 1 unreachable data center is UNKNOWN; it keeps trying until timeout
    Map<String, ControllerClient> failCompleteMap = new HashMap<>();
    failCompleteMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("failcluster", clientMap.get(null));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic8_v1", failCompleteMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.UNKNOWN);
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("failcluster"), ExecutionStatus.UNKNOWN.toString());

    Map<String, ControllerClient> errorMap = new HashMap<>();
    errorMap.put("cluster-err", clientMap.get(ExecutionStatus.ERROR));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic10_v1", errorMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    verify(internalAdmin, timeout(TIMEOUT_IN_MS)).truncateKafkaTopic("topic10_v1");
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(extraInfo.get("cluster-err"), ExecutionStatus.ERROR.toString());

    errorMap.put("cluster-complete", clientMap.get(ExecutionStatus.COMPLETED));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic11_v1", errorMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    verify(internalAdmin, timeout(TIMEOUT_IN_MS)).truncateKafkaTopic("topic11_v1");
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(extraInfo.get("cluster-complete"), ExecutionStatus.COMPLETED.toString());

    // Test whether errored topics will be truncated or not when 'maxErroredTopicNumToKeep' is > 0.
    parentAdmin.setMaxErroredTopicNumToKeep(2);
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic12_v1", errorMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    verify(internalAdmin, never()).truncateKafkaTopic("topic12_v1");
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    // Reset
    parentAdmin.setMaxErroredTopicNumToKeep(0);

    errorMap.put("cluster-new", clientMap.get(ExecutionStatus.NEW));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic13_v1", errorMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NEW); // Do we want this to be Progress?  limitation of ordering used in aggregation code
    Assert.assertEquals(extraInfo.get("cluster-new"), ExecutionStatus.NEW.toString());
  }

  @Test
  public void testGetProgress() {
    JobStatusQueryResponse tenResponse = new JobStatusQueryResponse();
    Map<String, Long> tenPerTaskProgress = new HashMap<>();
    tenPerTaskProgress.put("task1", 10L);
    tenPerTaskProgress.put("task2", 10L);
    tenResponse.setPerTaskProgress(tenPerTaskProgress);
    ControllerClient tenStatusClient = mock(ControllerClient.class);
    doReturn(tenResponse).when(tenStatusClient).queryJobStatus(anyString());

    JobStatusQueryResponse failResponse = new JobStatusQueryResponse();
    failResponse.setError("error2");
    ControllerClient failClient = mock(ControllerClient.class);
    doReturn(failResponse).when(failClient).queryJobStatus(anyString());

    // Clients work as expected
    JobStatusQueryResponse status = tenStatusClient.queryJobStatus("topic");
    Map<String,Long> perTask = status.getPerTaskProgress();
    Assert.assertEquals(perTask.get("task1"), new Long(10L));
    Assert.assertTrue(failClient.queryJobStatus("topic").isError());

    // Test logic
    Map<String, ControllerClient> tenMap = new HashMap<>();
    tenMap.put("cluster1", tenStatusClient);
    tenMap.put("cluster2", tenStatusClient);
    tenMap.put("cluster3", failClient);
    Map<String, Long> tenProgress = VeniceParentHelixAdmin.getOfflineJobProgress("cluster", "topic", tenMap);
    Assert.assertEquals(tenProgress.values().size(), 4); // nothing from fail client
    Assert.assertEquals(tenProgress.get("cluster1_task1"), new Long(10L));
    Assert.assertEquals(tenProgress.get("cluster2_task2"), new Long(10L));
  }

  @Test
  public void testUpdateStore() {
    String storeName = TestUtils.getUniqueString("testUpdateStore");
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName,storeName);

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));

    verify(zkClient, times(1)).readData(zkMetadataNodePath, null);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.UPDATE_STORE.getValue());

    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertEquals(updateStore.incrementalPushEnabled, true);

    long readQuota = 100L;
    boolean readability = true;
    boolean accessControlled = true;
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setEnableReads(readability)
        .setIncrementalPushEnabled(false)
        .setPartitionCount(64)
        .setReadQuotaInCU(readQuota)
        .setAccessControlled(accessControlled)
        .setCompressionStrategy(CompressionStrategy.GZIP)
        .setHybridRewindSeconds(135l)
        .setHybridOffsetLagThreshold(2000)
        .setBootstrapToOnlineTimeoutInHours(48));

    verify(veniceWriter, times(2)).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
    valueBytes = valueCaptor.getValue();
    schemaId = schemaCaptor.getValue();
    adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertEquals(updateStore.clusterName.toString(), clusterName);
    Assert.assertEquals(updateStore.storeName.toString(), storeName);
    Assert.assertEquals(updateStore.readQuotaInCU, readQuota,
        "New read quota should be written into kafka message.");
    Assert.assertEquals(updateStore.enableReads, readability,
        "New read readability should be written into kafka message.");
    Assert.assertEquals(updateStore.currentVersion, AdminConsumptionTask.IGNORED_CURRENT_VERSION,
        "As we don't pass any current version into updateStore, a magic version number should be used to prevent current version being overrided in prod colo.");
    Assert.assertNotNull(updateStore.hybridStoreConfig, "Hybrid store config should result in something not null in the avro object");
    Assert.assertEquals(updateStore.hybridStoreConfig.rewindTimeInSeconds, 135L);
    Assert.assertEquals(updateStore.hybridStoreConfig.offsetLagThresholdToGoOnline, 2000L);
    Assert.assertEquals(updateStore.accessControlled, accessControlled);
    Assert.assertEquals(updateStore.bootstrapToOnlineTimeoutInHours, 48);

    // Disable Access Control
    accessControlled = false;
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setAccessControlled(accessControlled));

    verify(veniceWriter, times(3)).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
    valueBytes = valueCaptor.getValue();
    schemaId = schemaCaptor.getValue();
    adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertEquals(updateStore.accessControlled, accessControlled);
  }

  @Test
  public void testDeleteStore() {
    String storeName = "test-testReCreateStore";
    String owner = "unittest";
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(eq(clusterName), eq(storeName));
    doReturn(store).when(internalAdmin).checkPreConditionForDeletion(eq(clusterName), eq(storeName));

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);
    parentAdmin.deleteStore(clusterName, storeName, 0);

    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(1))
        .readData(zkMetadataNodePath, null);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.DELETE_STORE.getValue());

    DeleteStore deleteStore = (DeleteStore) adminMessage.payloadUnion;
    Assert.assertEquals(deleteStore.clusterName.toString(), clusterName);
    Assert.assertEquals(deleteStore.storeName.toString(), storeName);
    Assert.assertEquals(deleteStore.largestUsedVersionNumber, 0);
  }

  @Test
  public void testGetCurrentVersionForMultiColos() {
    int coloCount = 4;
    Map<String, ControllerClient> controllerClientMap = prepareForCurrentVersionTest(coloCount);
    Map<String, Integer> result = parentAdmin.getCurrentVersionForMultiColos(clusterName, "test", controllerClientMap);
    Assert.assertEquals(result.size(), coloCount, "Should return the current versions for all colos.");
    for (int i = 0; i < coloCount; i++) {
      Assert.assertEquals(result.get("colo" + i).intValue(), i);
    }
  }

  @Test
  public void testGetCurrentVersionForMultiColosWithError() {
    int coloCount = 4;
    Map<String, ControllerClient> controllerClientMap = prepareForCurrentVersionTest(coloCount - 1);
    ControllerClient errorClient = mock(ControllerClient.class);
    StoreResponse errorResponse = new StoreResponse();
    errorResponse.setError("Error getting store for testing.");
    doReturn(errorResponse).when(errorClient).getStore(anyString());
    controllerClientMap.put("colo4", errorClient);

    Map<String, Integer> result = parentAdmin.getCurrentVersionForMultiColos(clusterName, "test", controllerClientMap);
    Assert.assertEquals(result.size(), coloCount, "Should return the current versions for all colos.");
    for (int i = 0; i < coloCount - 1; i++) {
      Assert.assertEquals(result.get("colo" + i).intValue(), i);
    }
    Assert.assertEquals(result.get("colo4").intValue(), AdminConsumptionTask.IGNORED_CURRENT_VERSION,
        "Met an error while querying a current version from a colo, should return -1.");
  }

  private Map<String, ControllerClient> prepareForCurrentVersionTest(int coloCount){
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();

    for (int i = 0; i < coloCount; i++) {
      ControllerClient client = mock(ControllerClient.class);
      StoreResponse storeResponse = new StoreResponse();
      Store s = TestUtils.createTestStore("s" + i, "test", System.currentTimeMillis());
      s.setCurrentVersion(i);
      storeResponse.setStore(StoreInfo.fromStore(s));
      doReturn(storeResponse).when(client).getStore(anyString());
      controllerClientMap.put("colo" + i, client);
    }
    return controllerClientMap;
  }

  @Test
  public void testGetKafkaTopicsByAge() {
    String storeName = TestUtils.getUniqueString("test-store");
    List<String> versionTopics = parentAdmin.getKafkaTopicsByAge(storeName);
    Assert.assertTrue(versionTopics.isEmpty());

    Set<String> topicList = new HashSet<>();
    topicList.add(storeName + "_v1");
    topicList.add(storeName + "_v2");
    topicList.add(storeName + "_v3");
    doReturn(topicList).when(topicManager).listTopics();
    versionTopics = parentAdmin.getKafkaTopicsByAge(storeName);
    Assert.assertFalse(versionTopics.isEmpty());
    String latestTopic = versionTopics.get(0);
    Assert.assertEquals(latestTopic, storeName + "_v3");
    Assert.assertTrue(topicList.containsAll(versionTopics));
    Assert.assertTrue(versionTopics.containsAll(topicList));
  }

  @Test
  public void testGetTopicForCurrentPushJob() {
    String storeName = TestUtils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(new ArrayList<String>()).when(mockParentAdmin).getKafkaTopicsByAge(any());
    doCallRealMethod().when(mockParentAdmin).getTopicForCurrentPushJob(clusterName, storeName, false);

    Store store = new Store(storeName, "test_owner", 1, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    store.addVersion(new Version(storeName, 1, "test_push_id"));
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Assert.assertFalse(mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false).isPresent());

    String latestTopic = storeName + "_v1";
    doReturn(Arrays.asList(latestTopic)).when(mockParentAdmin).getKafkaTopicsByAge(storeName);

    doReturn(topicManager).when(mockParentAdmin).getTopicManager();

    // When there is a deprecated topic
    doReturn(true).when(mockParentAdmin).isTopicTruncated(latestTopic);
    Assert.assertFalse(mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false).isPresent());
    verify(mockParentAdmin, never()).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is terminal
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED))
        .when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    doReturn(false).when(mockParentAdmin).isTopicTruncated(latestTopic);
    Assert.assertFalse(mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false).isPresent());
    verify(mockParentAdmin).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is not terminal
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS))
        .when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    Optional<String> currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false);
    Assert.assertTrue(currentPush.isPresent());
    Assert.assertEquals(currentPush.get(), latestTopic);
    verify(mockParentAdmin, times(2)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is 'UNKNOWN' in some colo,
    // but overall status is 'COMPLETED'
    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("cluster1", ExecutionStatus.UNKNOWN.toString());
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED, extraInfo))
        .when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    doCallRealMethod().when(mockParentAdmin).setTimer(any());
    mockParentAdmin.setTimer(new MockTime());
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false);
    Assert.assertFalse(currentPush.isPresent());
    verify(mockParentAdmin, times(7)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is 'UNKNOWN' in some colo,
    // but overall status is 'PROGRESS'
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS, extraInfo))
        .when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false);
    Assert.assertTrue(currentPush.isPresent());
    Assert.assertEquals(currentPush.get(), latestTopic);
    verify(mockParentAdmin, times(12)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic and the job status is 'UNKNOWN' in some colo for the first time,
    // but overall status is 'PROGRESS'
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS, extraInfo))
        .when(mockParentAdmin)
        .getOffLinePushStatus(clusterName, latestTopic);
    when(mockParentAdmin.getOffLinePushStatus(clusterName, latestTopic))
        .thenReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS, extraInfo))
        .thenReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.PROGRESS));
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false);
    Assert.assertTrue(currentPush.isPresent());
    Assert.assertEquals(currentPush.get(), latestTopic);
    verify(mockParentAdmin, times(14)).getOffLinePushStatus(clusterName, latestTopic);

    // When there is a regular topic, but there is no corresponding version
    store.deleteVersion(1);
    currentPush = mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false);
    Assert.assertFalse(currentPush.isPresent());
    verify(mockParentAdmin).killOfflinePush(clusterName, latestTopic);
  }

  @Test
  public void testTruncateTopicsBasedOnMaxErroredTopicNumToKeep() {
    String storeName = TestUtils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    List<String> topics = new ArrayList<>();
    topics.add(storeName + "_v1");
    topics.add(storeName + "_v10");
    topics.add(storeName + "_v8");
    topics.add(storeName + "_v5");
    topics.add(storeName + "_v7");
    doReturn(topics).when(mockParentAdmin).existingTopicsForStore(storeName);
    // isTopicTruncated will return false for other topics
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName + "_v8");
    doCallRealMethod().when(mockParentAdmin).truncateTopicsBasedOnMaxErroredTopicNumToKeep(any());
    doCallRealMethod().when(mockParentAdmin).setMaxErroredTopicNumToKeep(anyInt());
    mockParentAdmin.setMaxErroredTopicNumToKeep(2);
    mockParentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(topics);
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
    String storeName1 = TestUtils.getUniqueString("test-store");
    List<String> topics1 = new ArrayList<>();
    topics1.add(storeName1 + "_v1");
    topics1.add(storeName1 + "_v10");
    topics1.add(storeName1 + "_v8");
    topics1.add(storeName1 + "_v5");
    topics1.add(storeName1 + "_v7");
    doReturn(topics1).when(mockParentAdmin).existingTopicsForStore(storeName1);
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "_v10");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "_v7");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "_v8");
    doCallRealMethod().when(mockParentAdmin).truncateTopicsBasedOnMaxErroredTopicNumToKeep(any());
    mockParentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(topics1);
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
    List<String> topics = Arrays.asList(storeName + "_v1", storeName + "_v2", storeName + "_v3");
    doReturn(new HashSet(topics)).when(topicManager).listTopics();

    parentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(topics);
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v1");
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v2");
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v3");
  }

  @Test
  public void testAdminCanKillLingeringVersion() {
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    long startTime = System.currentTimeMillis();
    MockTime mockTime = new MockTime(startTime);
    partialMockParentAdmin.setTimer(mockTime);
    mockTime.addMilliseconds(30 * Time.MS_PER_HOUR);
    String storeName = "test_store";
    String existingTopicName = storeName + "_v1";
    Store store = mock(Store.class);
    Version version = new Version(storeName, 1, startTime, "test-push");
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.STARTED);
    String newPushJobId = "new-test-push";
    Version newVersion = new Version(storeName, 2, mockTime.getMilliseconds(), newPushJobId);

    doReturn(24).when(store).getBootstrapToOnlineTimeoutInHours();
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(Optional.of(version)).when(store).getVersion(1);
    doReturn(new HashSet<>(Arrays.asList(topicName, existingTopicName))).when(topicManager).listTopics();
    doReturn(newVersion).when(internalAdmin).addVersionOnly(clusterName, storeName, newPushJobId,
        3, 3, false, true, Version.PushType.BATCH);

    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    partialMockParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    Assert.assertEquals(partialMockParentAdmin
        .incrementVersionIdempotent(clusterName, storeName, newPushJobId, 3, 3, Version.PushType.BATCH, false, true),
        newVersion, "Unexpected new version returned by incrementVersionIdempotent");
  }

  @Test
  public void testAdminMessageIsolation() {
    String storeA = "test_store_A";
    String storeB = "test_store_B";
    Version storeAVersion = new Version(storeA, 1, "");
    Version storeBVersion = new Version(storeB, 1, "");

    doReturn(storeAVersion)
        .when(internalAdmin)
        .addVersionOnly(clusterName, storeA, "", 3, 3, false, false, Version.PushType.BATCH);
    doReturn(storeBVersion)
        .when(internalAdmin)
        .addVersionOnly(clusterName, storeB, "", 3, 3, false, false, Version.PushType.BATCH);
    doReturn(new Exception("test"))
        .when(internalAdmin)
        .getLastExceptionForStore(clusterName, storeA);
    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    try {
      parentAdmin.incrementVersionIdempotent(clusterName, storeA, "", 3, 3);
      Assert.fail("Admin operations to a store with existing exception should be blocked");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("due to existing exception"));
    }
    // store B should still be able to process admin operations.
    Assert.assertEquals(parentAdmin.incrementVersionIdempotent(clusterName, storeB, "", 3, 3),
        storeBVersion, "Unexpected new version returned");

    doReturn(null)
        .when(internalAdmin)
        .getLastExceptionForStore(clusterName, storeA);
    // Store A is unblocked and should be able to process admin operations now.
    Assert.assertEquals(parentAdmin.incrementVersionIdempotent(clusterName, storeA, "", 3, 3),
        storeAVersion, "Unexpected new version returned");
  }
}
