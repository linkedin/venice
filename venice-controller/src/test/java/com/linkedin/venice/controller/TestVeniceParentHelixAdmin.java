package com.linkedin.venice.controller;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
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
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
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
import com.linkedin.venice.meta.*;
import com.linkedin.venice.migration.MigrationPushStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.Matchers.anyString;


public class TestVeniceParentHelixAdmin {
  private static int TIMEOUT_IN_MS = 40 * Time.MS_PER_SECOND;
  private static int KAFKA_REPLICA_FACTOR = 3;
  private final String clusterName = "test-cluster";
  private final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final String zkOffsetNodePath = AdminOffsetManager.getAdminTopicOffsetNodePathForCluster(clusterName);
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
    doReturn(topicManager)
        .when(internalAdmin)
        .getTopicManager();
    zkClient = mock(ZkClient.class);

    doReturn(zkClient)
        .when(internalAdmin)
        .getZkClient();
    doReturn(new HelixAdapterSerializer())
        .when(internalAdmin)
        .getAdapterSerializer();

    ExecutionIdAccessor executionIdAccessor = Mockito.mock(ExecutionIdAccessor.class);
    doReturn(executionIdAccessor)
        .when(internalAdmin)
        .getExecutionIdAccessor();
    doReturn(0L)
        .when(executionIdAccessor)
        .getLastSucceedExecutionId(any());

    config = mockConfig(clusterName);

    accessor = Mockito.mock(ParentHelixOfflinePushAccessor.class);
    resources = mockResources(config, clusterName);

    store = mock(Store.class);
    doReturn(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION).when(store).getOffLinePushStrategy();
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(store).when(storeRepo).getStore(any());
    // Please override this default mock implementation if you need special store repo logic for your test
    doReturn(storeRepo).when(resources)
        .getMetadataRepository();

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

  private VeniceControllerConfig mockConfig(String clusterName){
    VeniceControllerConfig config = mock(VeniceControllerConfig.class);
    doReturn(clusterName).when(config).getClusterName();
    doReturn(KAFKA_REPLICA_FACTOR).when(config)
        .getKafkaReplicaFactor();
    doReturn(10000).when(config)
        .getParentControllerWaitingTimeForConsumptionMs();
    doReturn("fake_kafka_bootstrap_servers").when(config)
        .getKafkaBootstrapServers();
    return config;
  }

  private VeniceHelixResources mockResources(VeniceControllerConfig config, String clusterName){
    VeniceHelixResources resources = mock(VeniceHelixResources.class);
    doReturn(config).when(resources)
        .getConfig();
    doReturn(resources).when(internalAdmin)
        .getVeniceHelixResource(any());
    return resources;
  }

  @Test
  public void testStartWithTopicExists() {
    parentAdmin.start(clusterName);

    verify(internalAdmin)
        .getTopicManager();
    verify(topicManager, never())
        .createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, KAFKA_REPLICA_FACTOR);
  }

  @Test
  public void testStartWhenTopicNotExists() {
    doReturn(false)
        .when(topicManager)
        .containsTopic(topicName);
    parentAdmin.start(clusterName);
    verify(internalAdmin)
        .getTopicManager();
    verify(topicManager)
        .createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, KAFKA_REPLICA_FACTOR);
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
    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(null)
        .thenReturn(TestUtils.getOffsetRecord(1));

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    verify(internalAdmin)
    .checkPreConditionForAddStore(clusterName, storeName, keySchemaStr, valueSchemaStr);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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
      String offsetPath = AdminOffsetManager.getAdminTopicOffsetNodePathForCluster(cluster);

      VeniceWriter veniceWriter = writerMap.get(cluster);

      // Return offset -1 before writing any data into topic.
      when(zkClient.readData(offsetPath, null))
          .thenReturn(null);

      String storeName = "test-store-"+cluster;
      String owner = "test-owner-"+cluster;
      String keySchemaStr = "\"string\"";
      String valueSchemaStr = "\"string\"";
      when(veniceWriter.put(any(),any(),anyInt())).then(invocation -> {
        // Once we send message to topic through venice writer, return offset 1
        when(zkClient.readData(offsetPath, null))
            .thenReturn(TestUtils.getOffsetRecord(1));
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
      verify(zkClient, times(2))
          .readData(offsetPath, null);
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

    when(zkClient.readData(zkOffsetNodePath, null))
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(TestUtils.getOffsetRecord(0))
        .thenReturn(TestUtils.getOffsetRecord(1))
        .thenReturn(TestUtils.getOffsetRecord(0));

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
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
    doReturn(valueSchemaId).when(internalAdmin)
        .getValueSchemaId(clusterName, storeName, valueSchemaStr);

    Future future = mock(Future.class);
    doReturn(new RecordMetadata(topicPartition, 0, 1, -1, -1, -1, -1))
        .when(future).get();
    doReturn(future)
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));


    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr);

    verify(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

    parentAdmin.setStoreReadability(clusterName, storeName, false);
    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

    parentAdmin.setStoreWriteability(clusterName, storeName, false);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

    parentAdmin.setStoreReadability(clusterName, storeName, true);
    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

    parentAdmin.setStoreWriteability(clusterName, storeName, true);

    verify(internalAdmin)
        .checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

    doReturn(new HashSet<>(Arrays.asList(kafkaTopic)))
        .when(topicManager).listTopics();

    parentAdmin.killOfflinePush(clusterName, kafkaTopic);

    verify(internalAdmin)
        .checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
    verify(internalAdmin)
        .truncateKafkaTopic(kafkaTopic);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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
  public void testIncrementVersionWhenNoPreviousTopics() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    doReturn(new Version(storeName, 1)).when(internalAdmin)
        .addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
    parentAdmin.incrementVersion(clusterName, storeName, pushJobId, 1, 1);
    verify(internalAdmin).addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
    verify(accessor).createOfflinePushStatus(eq(clusterName), any());
    verify(accessor).getAllPushNames(eq(clusterName));
  }

  /**
   * This class is used to assist unit test for {@link VeniceParentHelixAdmin#incrementVersion(String, String, int, int)}
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

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*exists for store.*")
  public void testIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsStillRunning() {
    String storeName = TestUtils.getUniqueString("test_store");
    String previousKafkaTopic = storeName + "_v1";
    String unknownTopic = "1unknown_topic";
    doReturn(new HashSet<>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "test_owner", 1, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    store.addVersion(new Version(storeName, 1, "test_push_id"));
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.PROGRESS);
    partialMockParentAdmin.incrementVersion(clusterName, storeName, 1, 1);
  }

  @Test
  public void testIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsAlreadyDone() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    String previousKafkaTopic = storeName + "_v1";
    String unknownTopic = "1unknown_topic";
    doReturn(new HashSet<>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "test_owner", 1, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    store.addVersion(new Version(storeName, 1, "test_push_id"));
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.COMPLETED);
    doReturn(new Version(storeName, 1)).when(internalAdmin)
        .addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
    partialMockParentAdmin.incrementVersion(clusterName, storeName, pushJobId, 1, 1);
    verify(internalAdmin).addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
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
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true, false);
    verify(internalAdmin).incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false);
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
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true, false);
    verify(internalAdmin).incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false, false);
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

  /**
   * End-to-end test
   *
   * TODO: Fix this test. It's flaky.
   */
  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
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
    controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
    MultiStoreResponse response = ControllerClient.listStores(controllerUrl, clusterName);
    String[] stores = response.getStores();
    Assert.assertEquals(stores.length, 1);
    Assert.assertEquals(stores[0], storeName);

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
    VersionCreationResponse versionCreationResponse = ControllerClient.createNewStoreVersion(controllerUrl,
        clusterName, storeName, 10 * 1024 * 1024);
    Assert.assertFalse(versionCreationResponse.isError());

    // Set up migration push strategy
    String voldemortStoreName = TestUtils.getUniqueString("voldemort_store_name");
    String pushStrategy = MigrationPushStrategy.RunBnPAndH2VWaitForBothStrategy.name();
    controllerClient.setMigrationPushStrategy(voldemortStoreName, pushStrategy);
    // Retrieve migration push strategy
    MigrationPushStrategyResponse pushStrategyResponse = controllerClient.getMigrationPushStrategies();
    Assert.assertEquals(pushStrategyResponse.getStrategies().get(voldemortStoreName), pushStrategy);

    // Update chunking
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setChunkingEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isChunkingEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = controllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isChunkingEnabled());
    });

    // Update singleGetRouterCacheEnabled
    Assert.assertFalse(storeResponse.getStore().isSingleGetRouterCacheEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setSingleGetRouterCacheEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isSingleGetRouterCacheEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = controllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isSingleGetRouterCacheEnabled());
    });

    // Update batchGetRouterCacheEnabled
    Assert.assertFalse(storeResponse.getStore().isBatchGetRouterCacheEnabled());
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetRouterCacheEnabled(true));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().isBatchGetRouterCacheEnabled());
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = controllerClient.getStore(storeName);
      Assert.assertTrue(childStoreResponse.getStore().isBatchGetRouterCacheEnabled());
    });

    // Update batchGetLimit
    Assert.assertEquals(storeResponse.getStore().getBatchGetLimit(), -1);
    controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    storeResponse = controllerClient.getStore(storeName);
    Assert.assertEquals(storeResponse.getStore().getBatchGetLimit(), 100);
    // Child controller will be updated asynchronously
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT_IN_MS, TimeUnit.MILLISECONDS, () -> {
      StoreResponse childStoreResponse = controllerClient.getStore(storeName);
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
      doReturn(response).when(statusClient).queryJobStatusWithRetry(anyString(), anyInt(), any());
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
    doReturn(failResponse).when(failClient).queryJobStatusWithRetry(anyString(), anyInt(), any());
    clientMap.put(null, failClient);

    // Verify clients work as expected
    for (ExecutionStatus status : ExecutionStatus.values()) {
      Assert.assertEquals(clientMap.get(status).queryJobStatusWithRetry("topic", 1, Optional.empty())
          .getStatus(), status.toString());
    }
    Assert.assertTrue(clientMap.get(null).queryJobStatusWithRetry("topic", 1, Optional.empty())
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

    // 1 in 4 failures is ERROR
    Map<String, ControllerClient> failCompleteMap = new HashMap<>();
    failCompleteMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("failcluster", clientMap.get(null));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic8_v1", failCompleteMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    verify(internalAdmin, timeout(TIMEOUT_IN_MS)).truncateKafkaTopic("topic8_v1");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("failcluster"), ExecutionStatus.UNKNOWN.toString());

    // 3 in 6 failures is PROGRESS (so it keeps trying)
    failCompleteMap.put("failcluster2", clientMap.get(null));
    failCompleteMap.put("failcluster3", clientMap.get(null));
    offlineJobStatus = parentAdmin.getOffLineJobStatus("mycluster", "topic9_v1", failCompleteMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    Assert.assertEquals(extraInfo.get("failcluster2"), ExecutionStatus.UNKNOWN.toString());
    Assert.assertEquals(extraInfo.get("failcluster3"), ExecutionStatus.UNKNOWN.toString());

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
  public void testMayThrottleTopicCreation() {
    long timeWindowMs = 1000;
    doReturn(timeWindowMs).when(config).getTopicCreationThrottlingTimeWindowMs();
    VeniceParentHelixAdmin throttledParentAdmin = new VeniceParentHelixAdmin(internalAdmin, TestUtils.getMultiClusterConfigFromOneCluster(config));
    long start = System.currentTimeMillis();
    MockTime timer = new MockTime(start);
    throttledParentAdmin.mayThrottleTopicCreation(timer);
    Assert.assertTrue(timer.getMilliseconds() - start == 1000l, "First time to create topic, must sleep 1s.");
    throttledParentAdmin.mayThrottleTopicCreation(timer);
    Assert.assertTrue(timer.getMilliseconds() - start == 2000l, "Create topic too frequently, must sleep another 1s before creating the next topic.");
  }

  @Test
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
    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(null)
        .thenReturn(TestUtils.getOffsetRecord(1));

    throttledParentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
    throttledParentAdmin.start(clusterName);
    throttledParentAdmin.addStore(clusterName, storeName, "test", "\"string\"", "\"string\"");

    // Create 3 thread to increment version concurrently, we expected admin will sleep a while to avoid creating topic
    // too frequently.
    int threadCount = 3;
    Thread[] threads = new Thread[threadCount];
    for(int i =0;i<threadCount;i++){
      threads[i] = new Thread(() -> {
        throttledParentAdmin.incrementVersion(clusterName,storeName,1,1);
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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));
    long readQuota = 100l;
    boolean readability = true;
    boolean accessControlled = true;
    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName,storeName);

    //test incremental push
    parentAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));

    verify(zkClient, times(2)).readData(zkOffsetNodePath, null);
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
        .setHybridOffsetLagThreshold(2000));

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

    when(zkClient.readData(zkOffsetNodePath, null))
        .thenReturn(new OffsetRecord())
        .thenReturn(TestUtils.getOffsetRecord(1));

    parentAdmin.deleteStore(clusterName, storeName, 0);
    verify(veniceWriter)
        .put(any(), any(), anyInt());
    verify(zkClient, times(2))
        .readData(zkOffsetNodePath, null);
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
  public void testGetLargestKafkaTopic() {
    String storeName = TestUtils.getUniqueString("test-store");
    Optional<String> latestTopic = parentAdmin.getLatestKafkaTopic(storeName);
    Assert.assertFalse(latestTopic.isPresent());

    Set<String> topicList = new HashSet<>();
    topicList.add(storeName + "_v1");
    topicList.add(storeName + "_v2");
    topicList.add(storeName + "_v3");
    doReturn(topicList).when(topicManager).listTopics();
    latestTopic = parentAdmin.getLatestKafkaTopic(storeName);
    Assert.assertTrue(latestTopic.isPresent());
    Assert.assertEquals(latestTopic.get(), storeName + "_v3");
  }

  @Test
  public void testGetTopicForCurrentPushJob() {
    String storeName = TestUtils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(Optional.empty()).when(mockParentAdmin).getLatestKafkaTopic(any());
    doCallRealMethod().when(mockParentAdmin).getTopicForCurrentPushJob(clusterName, storeName, false);

    Store store = new Store(storeName, "test_owner", 1, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    store.addVersion(new Version(storeName, 1, "test_push_id"));
    doReturn(store).when(mockParentAdmin).getStore(clusterName, storeName);

    Assert.assertFalse(mockParentAdmin.getTopicForCurrentPushJob(clusterName, storeName, false).isPresent());

    String latestTopic = storeName + "_v1";
    doReturn(Optional.of(latestTopic)).when(mockParentAdmin).getLatestKafkaTopic(storeName);

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

  @Test (expectedExceptions = VeniceException.class)
  public void testIncrementVersionWithParallelPush() {
    String storeName = TestUtils.getUniqueString("test-store");
    VeniceParentHelixAdmin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(Optional.of(storeName + "v1")).when(mockParentAdmin).getTopicForCurrentPushJob(clusterName, storeName);
    doCallRealMethod().when(mockParentAdmin).incrementVersion(any(), any(), any(), anyInt(), anyInt());
    mockParentAdmin.incrementVersion(clusterName, storeName, "", 64, 3);
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
    mockParentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(storeName);

    verify(mockParentAdmin).truncateKafkaTopic(storeName + "_v1");
    verify(mockParentAdmin).truncateKafkaTopic(storeName + "_v5");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName + "_v7");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName + "_v8");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName + "_v10");

    // Test with more truncated topics
    String storeName1 = TestUtils.getUniqueString("test-store");
    List<String> topics1 = new ArrayList<>();
    topics.add(storeName1 + "_v1");
    topics.add(storeName1 + "_v10");
    topics.add(storeName1 + "_v8");
    topics.add(storeName1 + "_v5");
    topics.add(storeName1 + "_v7");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "v10");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "v7");
    doReturn(true).when(mockParentAdmin).isTopicTruncated(storeName1 + "v8");
    mockParentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(storeName1);
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v1");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v5");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v7");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v8");
    verify(mockParentAdmin, never()).truncateKafkaTopic(storeName1 + "_v10");
  }

  @Test
  public void testCreateOfflinePushStatus(){
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    doReturn(new Version(storeName, 1)).when(internalAdmin)
        .addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
    List<String> names = new ArrayList<>();
    int count = VeniceParentHelixAdmin.MAX_PUSH_STATUS_PER_STORE_TO_KEEP+1;
    for(int i=0;i<count;i++){
      names.add(storeName+"_v"+i);
    }
    doReturn(names).when(accessor).getAllPushNames(clusterName);
    parentAdmin.incrementVersion(clusterName, storeName, pushJobId, 1, 1);
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

  /*@Test
  public void testAdminCanCleanupLeakingTopics() {
    String storeName = "test_store";
    doReturn(new HashSet(Arrays.asList(storeName + "_v1", storeName + "_v2", storeName + "_v3"))).when(topicManager)
        .listTopics();

    parentAdmin.truncateTopicsBasedOnMaxErroredTopicNumToKeep(storeName);
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v1");
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v2");
    verify(internalAdmin).truncateKafkaTopic(storeName + "_v3");
  }*/
}
