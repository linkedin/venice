package com.linkedin.venice.controller;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
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
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.ParentHelixOfflinePushAccessor;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.migration.MigrationPushStrategy;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;


public class TestVeniceParentHelixAdmin {
  private static final int TIMEOUT_IN_MS = 40 * Time.MS_PER_SECOND;
  private static int KAFKA_REPLICA_FACTOR = 3;
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
  public void init() {
    topicManager = mock(TopicManager.class);
    doReturn(new HashSet<String>(Arrays.asList(topicName)))
        .when(topicManager)
        .listTopics();
    doReturn(true).when(topicManager).containsTopic(topicName);
    internalAdmin = mock(VeniceHelixAdmin.class);
    doReturn(topicManager).when(internalAdmin).getTopicManager();
    zkClient = mock(ZkClient.class);

    doReturn(zkClient).when(internalAdmin).getZkClient();
    doReturn(new HelixAdapterSerializer()).when(internalAdmin).getAdapterSerializer();

    ExecutionIdAccessor executionIdAccessor = Mockito.mock(ExecutionIdAccessor.class);
    doReturn(executionIdAccessor).when(internalAdmin).getExecutionIdAccessor();
    doReturn(0L).when(executionIdAccessor).getLastSucceededExecutionId(any());

    config = mockConfig(clusterName);

    accessor = Mockito.mock(ParentHelixOfflinePushAccessor.class);
    resources = mockResources(config, clusterName);

    store = mock(Store.class);
    doReturn(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION).when(store).getOffLinePushStrategy();
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(store).when(storeRepo).getStore(any());
    // Please override this default mock implementation if you need special store repo logic for your test
    doReturn(storeRepo).when(resources).getMetadataRepository();

    parentAdmin = new VeniceParentHelixAdmin(internalAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
    parentAdmin.setOfflinePushAccessor(accessor);
    parentAdmin.getAdminCommandExecutionTracker(clusterName)
        .get()
        .getFabricToControllerClientsMap()
        .put("test-fabric", Mockito.mock(ControllerClient.class));
    veniceWriter = mock(VeniceWriter.class);
    // Need to bypass VeniceWriter initialization
    parentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
  }

  private VeniceControllerConfig mockConfig(String clusterName) {
    VeniceControllerConfig config = mock(VeniceControllerConfig.class);
    doReturn(clusterName).when(config).getClusterName();
    doReturn(KAFKA_REPLICA_FACTOR).when(config).getKafkaReplicationFactor();
    doReturn(10000).when(config).getParentControllerWaitingTimeForConsumptionMs();
    doReturn("fake_kafka_bootstrap_servers").when(config).getKafkaBootstrapServers();
    doReturn(true).when(config).isAddVersionViaAdminProtocolEnabled();
    doReturn(false).when(config).isAddVersionViaTopicMonitorEnabled();
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

  private class AsyncSetupMockVeniceParentHelixAdmin extends VeniceParentHelixAdmin {
    private Store store;
    private String pushJobStatusStoreName;

    public AsyncSetupMockVeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerConfig config,
        Store store, String pushJobStatusStoreName) {
      super(veniceHelixAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
      this.store = store;
      this.pushJobStatusStoreName = pushJobStatusStoreName;
    }

    public boolean isAsyncSetupRunning(String clusterName) {
      return asyncSetupEnabledMap.get(clusterName);
    }

    @Override
    public void addStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
      doReturn(store).when(internalAdmin).getStore(clusterName, pushJobStatusStoreName);
    }

    @Override
    public void updateStore(String clusterName,
        String storeName,
        Optional<String> owner,
        Optional<Boolean> readability,
        Optional<Boolean> writeability,
        Optional<Integer> partitionCount,
        Optional<Long> storageQuotaInByte,
        Optional<Long> readQuotaInCU,
        Optional<Integer> currentVersion,
        Optional<Integer> largestUsedVersionNumber,
        Optional<Long> hybridRewindSeconds,
        Optional<Long> hybridOffsetLagThreshold,
        Optional<Boolean> accessControlled,
        Optional<CompressionStrategy> compressionStrategy,
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
        Optional<BackupStrategy> backupStategy) {
      if (hybridOffsetLagThreshold.isPresent() && hybridRewindSeconds.isPresent()) {
        doReturn(true).when(store).isHybrid();
      }
    }

    @Override
    public Version incrementVersionIdempotent(String clusterName,
        String storeName,
        String pushJobId,
        int numberOfPartition,
        int replicationFactor,
        boolean offlinePush) {
      List versions = new ArrayList();
      versions.add(new Version("push-job-status-store", 1, "test-id"));
      doReturn(versions).when(store).getVersions();
      return new Version("push-job-status-store", 1, "test-id");
    }

  }

  @Test (timeOut = TIMEOUT_IN_MS)
  public void testAsyncSetupForPushStatusStore() {
    String pushJobStatusStoreName = "push-job-status-store";
    Store store = mock(Store.class);
    doReturn(false).when(store).isHybrid();
    doReturn(Collections.emptyList()).when(store).getVersions();
    doReturn(true).when(internalAdmin).isMasterController(clusterName);
    doReturn(clusterName).when(config).getPushJobStatusStoreClusterName();
    doReturn(pushJobStatusStoreName).when(config).getPushJobStatusStoreName();
    doReturn(Version.composeRealTimeTopic(pushJobStatusStoreName)).when(internalAdmin)
        .getRealTimeTopic(clusterName, pushJobStatusStoreName);
    AsyncSetupMockVeniceParentHelixAdmin mockVeniceParentHelixAdmin =
        new AsyncSetupMockVeniceParentHelixAdmin(internalAdmin, config, store, pushJobStatusStoreName);
    mockVeniceParentHelixAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    mockVeniceParentHelixAdmin.setTimer(new MockTime());
    try {
      mockVeniceParentHelixAdmin.start(clusterName);
      TestUtils.waitForNonDeterministicCompletion(1, TimeUnit.SECONDS, () -> !store.getVersions().isEmpty());
      verify(internalAdmin, times(4)).getStore(clusterName, pushJobStatusStoreName);
      verify(store, times(2)).isHybrid();
      verify(store, atLeast(2)).getVersions();
    } finally {
      mockVeniceParentHelixAdmin.stop(clusterName);
    }
    Assert.assertEquals(mockVeniceParentHelixAdmin.isAsyncSetupRunning(clusterName), false,
        "Async setup should be stopped");
  }

  @Test
  public void testAddStore() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    verify(internalAdmin)
    .checkPreConditionForAddStore(clusterName, storeName, keySchemaStr, valueSchemaStr);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
  public void testAddStoreForMultiCluster()
      throws ExecutionException, InterruptedException {
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
          .put("test-fabric", Mockito.mock(ControllerClient.class));
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
        doReturn(new RecordMetadata(new TopicPartition(adminTopic, partitionId), 0, 1, -1, -1, -1, -1))
            .when(future).get();
        return future;
      });

      parentAdmin.addStore(cluster, storeName, owner, keySchemaStr, valueSchemaStr);

      verify(internalAdmin)
          .checkPreConditionForAddStore(cluster, storeName, keySchemaStr, valueSchemaStr);
      verify(veniceWriter)
          .put(any(), any(), anyInt());
      verify(zkClient, times(3))
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

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".* already exists.*")
  public void testAddStoreWhenExists() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null);

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    doThrow(new VeniceException("Store: " + storeName + " already exists. ..."))
        .when(internalAdmin)
        .checkPreConditionForAddStore(clusterName, storeName, keySchemaStr, valueSchemaStr);

    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
  }

  // This test forces a timeout in the admin consumption task which takes 10 seconds.
  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*waiting for admin consumption to catch up.*")
  public void testAddStoreWhenLastOffsetHasntBeenConsumed() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    Future future = mock(Future.class);
    doReturn(new VeniceException("mock exception"))
        .when(internalAdmin)
        .getLastException(clusterName);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(0, 0))
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1))
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(0, 0));

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    // Add store again with smaller consumed offset
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
  }

  @Test
  public void testAddValueSchema() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";
    String valueSchemaStr = "\"string\"";
    int valueSchemaId = 10;
    doReturn(valueSchemaId).when(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);
    doReturn(valueSchemaId).when(internalAdmin)
        .getValueSchemaId(clusterName, storeName, valueSchemaStr);

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);

    verify(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
  public void testDisableStoreRead()
      throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.setStoreReadability(clusterName, storeName, false);
    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
  public void testDisableStoreWrite() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.setStoreWriteability(clusterName, storeName, false);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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

  @Test (expectedExceptions = VeniceNoStoreException.class)
  public void testDisableStoreWriteWhenStoreDoesntExist() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    when(internalAdmin.checkPreConditionForUpdateStoreMetadata(clusterName, storeName))
        .thenThrow(new VeniceNoStoreException(storeName));

    parentAdmin.setStoreWriteability(clusterName, storeName, false);
  }

  @Test
  public void testEnableStoreRead() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.setStoreReadability(clusterName, storeName, true);
    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
  public void testEnableStoreWrite() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.setStoreWriteability(clusterName, storeName, true);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
  public void testKillOfflinePushJob() throws ExecutionException, InterruptedException {
    String kafkaTopic = "test_store_v1";
    parentAdmin.start(clusterName);

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    doReturn(new HashSet<>(Arrays.asList(kafkaTopic)))
        .when(topicManager).listTopics();

    parentAdmin.killOfflinePush(clusterName, kafkaTopic);

    verify(internalAdmin)
        .checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
    verify(internalAdmin)
        .truncateKafkaTopic(kafkaTopic);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
        .incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false, false, false);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true);
    verify(internalAdmin).incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false, false, false);
    // TODO remove or re-enable based on our decision to kill or revive push job properties upload feature
    /*verify(accessor).createOfflinePushStatus(eq(clusterName), any());
    verify(accessor).getAllPushNames(eq(clusterName));*/
  }

  /**
   * This class is used to assist unit test for {@link VeniceParentHelixAdmin#incrementVersionIdempotent(String, String, String, int, int, boolean)}
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

    @Override
    protected void sendAddVersionAdminMessage(String clusterName, String storeName, String pushJobId, int versionNum,
        int numberOfPartitions) {
      // No op, add version admin message does not need to be sent for unit test.
    }
  }

  @Test (expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*must be terminated before another push can be started.")
  public void testIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsStillRunning() {
    String storeName = TestUtils.getUniqueString("test_store");
    String previousKafkaTopic = storeName + "_v1";
    String unknownTopic = "1unknown_topic_v1";
    doReturn(new HashSet<>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "test_owner", 1, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    String pushJobId = "test_push_id";
    String pushJobId2 = "test_push_id2";
    store.addVersion(new Version(storeName, 1, pushJobId));
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.PROGRESS);
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId2, 1, 1, true);
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
        .incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false, false, false);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    Version newVersion =
        partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true, false, false, false);
    verify(internalAdmin).incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false, false, false);
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
        .incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false, false, false);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true, false, false, false);
    verify(internalAdmin).incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false, false, false);
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
      partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true);
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

  @Test
  public void testEnd2End() throws IOException {
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper childControllerWrapper =
        ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper);
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper controllerWrapper =
        ServiceFactory.getVeniceParentController(clusterName, parentZk.getAddress(), kafkaBrokerWrapper,
            new VeniceControllerWrapper[]{childControllerWrapper}, false);

    String controllerUrl = controllerWrapper.getControllerUrl();
    String childControllerUrl = childControllerWrapper.getControllerUrl();
    // Adding store
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr = "\"string\"";

    ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl);
    ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl);
    controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
    StoreResponse response = controllerClient.getStore(storeName);
    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getStore().getName(), storeName);

    // Adding key schema
    SchemaResponse keySchemaResponse = controllerClient.getKeySchema(storeName);
    Assert.assertEquals(keySchemaResponse.getSchemaStr(), keySchemaStr);

    // Adding value schema
    controllerClient.addValueSchema(storeName, valueSchemaStr);
    MultiSchemaResponse valueSchemaResponse = controllerClient.getAllValueSchema(storeName);
    MultiSchemaResponse.Schema[] schemas = valueSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 1);
    Assert.assertEquals(schemas[0].getId(), 1);
    Assert.assertEquals(schemas[0].getSchemaStr(), valueSchemaStr);

    // Disable store write
    controllerClient.enableStoreWrites(storeName, false);
    StoreResponse storeResponse = ControllerClient.getStore(controllerUrl, clusterName, storeName);
    Assert.assertFalse(storeResponse.getStore().isEnableStoreWrites());

    // Enable store write
    controllerClient.enableStoreWrites(storeName, true);
    storeResponse = ControllerClient.getStore(controllerUrl, clusterName, storeName);
    Assert.assertTrue(storeResponse.getStore().isEnableStoreWrites());

    // Add version
    VersionCreationResponse versionCreationResponse = controllerClient.requestTopicForWrites(storeName, 10 * 1024 * 1024,
        ControllerApiConstants.PushType.BATCH, Version.guidBasedDummyPushId(), false, true);
    Assert.assertFalse(versionCreationResponse.isError());

    // Set up migration push strategy
    String voldemortStoreName = TestUtils.getUniqueString("voldemort_store_name");
    String pushStrategy = MigrationPushStrategy.RunBnPAndH2VWaitForBothStrategy.name();
    controllerClient.setMigrationPushStrategy(voldemortStoreName, pushStrategy);
    // Retrieve migration push strategy
    MigrationPushStrategyResponse pushStrategyResponse = controllerClient.getMigrationPushStrategies();
    Assert.assertEquals(pushStrategyResponse.getStrategies().get(voldemortStoreName), pushStrategy);

    // Update backup strategy
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBackupStrategy(BackupStrategy.DELETE_ON_NEW_PUSH_START));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertEquals(storeResponse.getStore().getBackupStrategy(), BackupStrategy.DELETE_ON_NEW_PUSH_START);
    // Update chunking
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setChunkingEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isChunkingEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isChunkingEnabled());
    });

    // Update singleGetRouterCacheEnabled
    Assert.assertFalse(storeResponse.getStore().isSingleGetRouterCacheEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setSingleGetRouterCacheEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isSingleGetRouterCacheEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isSingleGetRouterCacheEnabled());
    });

    // Update batchGetRouterCacheEnabled
    Assert.assertFalse(storeResponse.getStore().isBatchGetRouterCacheEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetRouterCacheEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isBatchGetRouterCacheEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isBatchGetRouterCacheEnabled());
    });

    // Update batchGetLimit
    Assert.assertEquals(storeResponse.getStore().getBatchGetLimit(), -1);
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertEquals(storeResponse.getStore().getBatchGetLimit(), 100);
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertEquals(childStoreResponse.getStore().getBatchGetLimit(), 100);
    });

    // Disable router cache to test hybrid store
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setSingleGetRouterCacheEnabled(false)
        .setBatchGetRouterCacheEnabled(false));
    // Config the store to be hybrid
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(100).setHybridOffsetLagThreshold(100));
    // This command should not fail
    ControllerResponse controllerResponse = controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setAccessControlled(true));
    Assert.assertFalse(controllerResponse.isError());

    // Update writeComputationEnabled
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertFalse(storeResponse.getStore().isWriteComputationEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isWriteComputationEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isWriteComputationEnabled());
    });

    // Update computationEnabled
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertFalse(storeResponse.getStore().isReadComputationEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setReadComputationEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isReadComputationEnabled());
    // Update bootstrapToOnlineTimeout
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertEquals(storeResponse.getStore().getBootstrapToOnlineTimeoutInHours(), 24,
        "Default bootstrapToOnlineTimeoutInHours should be 24");
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBootstrapToOnlineTimeoutInHours(48));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertEquals(storeResponse.getStore().getBootstrapToOnlineTimeoutInHours(), 48);
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isReadComputationEnabled());
    });

    // Update store leader follower state model
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertFalse(storeResponse.getStore().isLeaderFollowerModelEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setLeaderFollowerModel(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isLeaderFollowerModelEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isLeaderFollowerModelEnabled());
    });

    controllerWrapper.close();
    childControllerWrapper.close();
    kafkaBrokerWrapper.close();
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

  // throttle for topic creation may no longer be needed.
  @Test@Ignore
  public void testCreateTopicConcurrently()
      throws InterruptedException, ExecutionException {
    long timeWindowMs = 1000;
    String storeName = "testCreateTopicConcurrently";
    Time timer = new MockTime();
    doReturn(timeWindowMs).when(config).getTopicCreationThrottlingTimeWindowMs();
    VeniceParentHelixAdmin throttledParentAdmin = new VeniceParentHelixAdmin(internalAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
    throttledParentAdmin.setTimer(timer);
    throttledParentAdmin.getAdminCommandExecutionTracker(clusterName)
        .get()
        .getFabricToControllerClientsMap()
        .put("test-fabric", Mockito.mock(ControllerClient.class));
    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());
    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    throttledParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    throttledParentAdmin.start(clusterName);
    throttledParentAdmin.addStore(clusterName, storeName, "test", "\"string\"", "\"string\"");

    // Create 3 thread to increment version concurrently, we expected admin will sleep a while to avoid creating topic
    // too frequently.
    int threadCount = 3;
    Thread[] threads = new Thread[threadCount];
    for(int i =0;i<threadCount;i++){
      threads[i] = new Thread(() -> {
        throttledParentAdmin.incrementVersionIdempotent(clusterName,storeName, Version.guidBasedDummyPushId(),
            1,1, true);
      });
    }
    long start = timer.getMilliseconds();
    for(Thread thread:threads){
      thread.start();
      thread.join();
    }
    Assert.assertTrue(timer.getMilliseconds() - start == 3000l,
        "We created 3 topic, at least cost 3s to prevent creating topic too frequently.");
  }

  @Test
  public void testUpdateStore()
      throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = TestUtils.getUniqueString("testUpdateStore");

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));
    long readQuota = 100l;
    boolean readability = true;
    boolean accessControlled = true;
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName,storeName);

    //test incremental push
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));

    verify(zkClient, times(3)).readData(zkMetadataNodePath, null);
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
  public void testDeleteStore()
      throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);
    String storeName = "test-testReCreateStore";
    String owner = "unittest";
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    Mockito.doReturn(store).when(internalAdmin).getStore(eq(clusterName), eq(storeName));
    Mockito.doReturn(store).when(internalAdmin).checkPreConditionForDeletion(eq(clusterName), eq(storeName));
    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.deleteStore(clusterName, storeName, 0);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(3))
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
    ControllerClient errorClient = Mockito.mock(ControllerClient.class);
    StoreResponse errorResponse = new StoreResponse();
    errorResponse.setError("Error getting store for testing.");
    Mockito.doReturn(errorResponse).when(errorClient).getStore(Mockito.anyString());
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
      ControllerClient client = Mockito.mock(ControllerClient.class);
      StoreResponse storeResponse = new StoreResponse();
      Store s = TestUtils.createTestStore("s" + i, "test", System.currentTimeMillis());
      s.setCurrentVersion(i);
      storeResponse.setStore(StoreInfo.fromStore(s));
      Mockito.doReturn(storeResponse).when(client).getStore(Mockito.anyString());
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

  // Ignored until we decide if we are keeping the push job properties upload feature.
  @Test@Ignore
  public void testCreateOfflinePushStatus() throws ExecutionException, InterruptedException {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    doReturn(new Version(storeName, 1)).when(internalAdmin)
        .addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false, false , false, false);
    List<String> names = new ArrayList<>();
    int count = VeniceParentHelixAdmin.MAX_PUSH_STATUS_PER_STORE_TO_KEEP+1;
    for(int i=0;i<count;i++){
      names.add(storeName+"_v"+i);
    }
    doReturn(names).when(accessor).getAllPushNames(clusterName);
    parentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true);
    verify(accessor, atLeastOnce()).createOfflinePushStatus(eq(clusterName), any());
    // Collect the old offline push status.
    verify(accessor).deleteOfflinePushStatus(clusterName, storeName+"_v0");
  }

  @Test
  public void testUpdatePushProperties(){
    String storeName = TestUtils.getUniqueString("testUpdatePushProperties");
    OfflinePushStatus status = new OfflinePushStatus(storeName+"_v1", 1, 1,OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    doReturn(status).when(accessor).getOfflinePushStatus(clusterName, storeName+"_v1");
    Map<String, String> map = new HashMap<>();
    map.put("test","test");
    parentAdmin.updatePushProperties(clusterName, storeName, 1, map);
    OfflinePushStatus status1 = status.clonePushStatus();
    status1.setPushProperties(map);
    verify(accessor).updateOfflinePushStatus(eq(clusterName), eq(status1));
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
  public void testAdminCanKillLingeringVersion() throws ExecutionException, InterruptedException {
    boolean expectedExceptionThrown = false;
    long startTime = System.currentTimeMillis();
    MockTime mockTime = new MockTime(startTime);
    parentAdmin.setTimer(mockTime);
    mockTime.addMilliseconds(30 * Time.MS_PER_HOUR);
    String storeName = "test_store";
    String existingTopicName = storeName + "_v1";
    Store store = mock(Store.class);
    Version version = new Version(storeName, 1, startTime, "test-push");
    String newPushJobId = "new-test-push";
    Version newVersion = new Version(storeName, 2, mockTime.getMilliseconds(), newPushJobId);

    doReturn(24).when(store).getBootstrapToOnlineTimeoutInHours();
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(Optional.of(version)).when(store).getVersion(1);
    doReturn(new HashSet<>(Arrays.asList(topicName, existingTopicName))).when(topicManager).listTopics();
    doReturn(newVersion).when(internalAdmin).incrementVersionIdempotent(clusterName, storeName, newPushJobId,
        3, 3, false, false, false);

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.incrementVersionIdempotent(clusterName, storeName, newPushJobId, 3, 3, true, false, false);
  }
}
