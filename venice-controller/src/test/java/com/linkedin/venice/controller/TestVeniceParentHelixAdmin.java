package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
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
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Map;

import java.util.Optional;
import org.apache.commons.cli.Option;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.tools.ant.taskdefs.Apt;
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

    config = mock(VeniceControllerConfig.class);
    doReturn(KAFKA_REPLICA_FACTOR).when(config)
        .getKafkaReplicaFactor();
    doReturn(10000).when(config)
        .getParentControllerWaitingTimeForConsumptionMs();
    doReturn("fake_kafka_bootstrap_servers").when(config)
        .getKafkaBootstrapServers();

    resources = mock(VeniceHelixResources.class);
    doReturn(config).when(resources)
        .getConfig();
    doReturn(resources).when(internalAdmin)
        .getVeniceHelixResource(clusterName);

    Store mockStore = mock(Store.class);
    HelixReadWriteStoreRepository storeRepo = mock(HelixReadWriteStoreRepository.class);
    doReturn(mockStore).when(storeRepo).getStore(any());
    // Please override this default mock implementation if you need special store repo logic for your test
    doReturn(storeRepo).when(resources)
        .getMetadataRepository();

    parentAdmin = new VeniceParentHelixAdmin(internalAdmin, config);
    parentAdmin.getAdminCommandExecutionTracker()
        .get()
        .getFabricToControllerClientsMap()
        .put("test-fabric", Mockito.mock(ControllerClient.class));
    veniceWriter = mock(VeniceWriter.class);
    // Need to bypass VeniceWriter initialization
    parentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.STORE_CREATION.ordinal());
    StoreCreation storeCreationMessage = (StoreCreation) adminMessage.payloadUnion;
    Assert.assertEquals(storeCreationMessage.clusterName.toString(), clusterName);
    Assert.assertEquals(storeCreationMessage.storeName.toString(), storeName);
    Assert.assertEquals(storeCreationMessage.owner.toString(), owner);
    Assert.assertEquals(storeCreationMessage.keySchema.definition.toString(), keySchemaStr);
    Assert.assertEquals(storeCreationMessage.valueSchema.definition.toString(), valueSchemaStr);
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.VALUE_SCHEMA_CREATION.ordinal());
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.DIABLE_STORE_READ.ordinal());
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.DISABLE_STORE_WRITE.ordinal());
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.ENABLE_STORE_READ.ordinal());
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.ENABLE_STORE_WRITE.ordinal());
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

    doReturn(new HashSet<String>(Arrays.asList(kafkaTopic)))
        .when(topicManager).listTopics();

    parentAdmin.killOfflinePush(clusterName, kafkaTopic);

    verify(internalAdmin)
        .checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
    verify(topicManager)
        .syncDeleteTopic(kafkaTopic);
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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.KILL_OFFLINE_PUSH_JOB.ordinal());
    KillOfflinePushJob killJob = (KillOfflinePushJob) adminMessage.payloadUnion;
    Assert.assertEquals(killJob.clusterName.toString(), clusterName);
    Assert.assertEquals(killJob.kafkaTopic.toString(), kafkaTopic);
  }

  @Test
  public void testIncrementVersionWhenNoPreviousTopics() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    parentAdmin.incrementVersion(clusterName, storeName, pushJobId, 1, 1);
    verify(internalAdmin).addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
  }

  /**
   * This class is used to assist unit test for {@link VeniceParentHelixAdmin#incrementVersion(String, String, int, int)}
   * to mock various offline job status.
   */
  private static class PartialMockVeniceParentHelixAdmin extends VeniceParentHelixAdmin {
    private ExecutionStatus offlineJobStatus = ExecutionStatus.NOT_CREATED;

    public PartialMockVeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerConfig config) {
      super(veniceHelixAdmin, config);
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
    doReturn(new HashSet<String>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
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
    doReturn(new HashSet<String>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.COMPLETED);
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
    doReturn(new HashSet<String>(Arrays.asList(previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    Store store = new Store(storeName, "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    Version version = new Version(storeName, 1, pushJobId);
    store.addVersion(version);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    PartialMockVeniceParentHelixAdmin partialMockParentAdmin = new PartialMockVeniceParentHelixAdmin(internalAdmin, config);
    partialMockParentAdmin.setOfflineJobStatus(ExecutionStatus.NEW);
    partialMockParentAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, true);
    verify(internalAdmin).incrementVersionIdempotent(clusterName, storeName, pushJobId, 1, 1, false);
  }

  /**
   * Idempotent increment version should NOT work because existing topic uses different push ID than the request
   */
  @Test
  public void testIdempotentIncrementVersionWhenPreviousTopicsExistAndOfflineJobIsNotDoneForDifferentPushId() {
    String storeName = TestUtils.getUniqueString("test_store");
    String pushJobId = TestUtils.getUniqueString("push_job_id");
    String previousKafkaTopic = storeName + "_v1";
    doReturn(new HashSet<String>(Arrays.asList(previousKafkaTopic)))
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
            childControllerWrapper);

    String controllerUrl = controllerWrapper.getControllerUrl();
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

    controllerWrapper.close();
    kafkaBrokerWrapper.close();
  }


  @Test
  public void testGetExecutionStatus(){
    Map<ExecutionStatus, ControllerClient> clientMap = new HashMap<>();
    for (ExecutionStatus status : ExecutionStatus.values()){
      JobStatusQueryResponse response = new JobStatusQueryResponse();
      response.setStatus(status.toString());
      ControllerClient statusClient = mock(ControllerClient.class);
      doReturn(response).when(statusClient).queryJobStatus(anyString());
      clientMap.put(status, statusClient);
    }
    TopicManager topicManager = mock(TopicManager.class);

    JobStatusQueryResponse failResponse = new JobStatusQueryResponse();
    failResponse.setError("error");
    ControllerClient failClient = mock(ControllerClient.class);
    doReturn(failResponse).when(failClient).queryJobStatus(anyString());
    clientMap.put(null, failClient);

    // Verify clients work as expected
    for (ExecutionStatus status : ExecutionStatus.values()) {
      Assert.assertEquals(clientMap.get(status).queryJobStatus("topic").getStatus(), status.toString());
    }
    Assert.assertTrue(clientMap.get(null).queryJobStatus("topic").isError());

    Map<String, ControllerClient> completeMap = new HashMap<>();
    completeMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    completeMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    completeMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    Set<String> topicList = new HashSet<>(
        Arrays.asList(
            "topic1",
            "topic2",
            "topic3",
            "topic4",
            "topic5",
            "topic6",
            "topic7",
            "topic8",
            "topic9"
        )
    );
    doReturn(topicList).when(topicManager).listTopics();

    Admin.OfflinePushStatusInfo
        offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic1", completeMap, topicManager);
    Map<String, String> extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.COMPLETED);
    verify(topicManager, timeout(TIMEOUT_IN_MS)).syncDeleteTopic("topic1");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());

    completeMap.put("cluster-slow", clientMap.get(ExecutionStatus.NOT_CREATED));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic2", completeMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NOT_CREATED);  // Do we want this to be Progress?  limitation of ordering used in aggregation code
    verify(topicManager, never()).syncDeleteTopic("topic2");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster-slow"), ExecutionStatus.NOT_CREATED.toString());

    Map<String, ControllerClient> progressMap = new HashMap<>();
    progressMap.put("cluster", clientMap.get(ExecutionStatus.NOT_CREATED));
    progressMap.put("cluster3", clientMap.get(ExecutionStatus.NOT_CREATED));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic3", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NOT_CREATED);
    verify(topicManager, never()).syncDeleteTopic("topic3");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.NOT_CREATED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.NOT_CREATED.toString());

    progressMap.put("cluster5", clientMap.get(ExecutionStatus.NEW));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic4", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.NEW);
    verify(topicManager, never()).syncDeleteTopic("topic4");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.NOT_CREATED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.NOT_CREATED.toString());
    Assert.assertEquals(extraInfo.get("cluster5"), ExecutionStatus.NEW.toString());

    progressMap.put("cluster7", clientMap.get(ExecutionStatus.PROGRESS));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic5", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(topicManager, never()).syncDeleteTopic("topic5");;
    Assert.assertEquals(extraInfo.get("cluster7"), ExecutionStatus.PROGRESS.toString());

    progressMap.put("cluster9", clientMap.get(ExecutionStatus.STARTED));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic6", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(topicManager, never()).syncDeleteTopic("topic6");
    Assert.assertEquals(extraInfo.get("cluster9"), ExecutionStatus.STARTED.toString());

    progressMap.put("cluster11", clientMap.get(ExecutionStatus.COMPLETED));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic7", progressMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    verify(topicManager, never()).syncDeleteTopic("topic7");
    Assert.assertEquals(extraInfo.get("cluster11"), ExecutionStatus.COMPLETED.toString());

    // 1 in 4 failures is ERROR
    Map<String, ControllerClient> failCompleteMap = new HashMap<>();
    failCompleteMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("failcluster", clientMap.get(null));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic8", failCompleteMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    verify(topicManager, timeout(TIMEOUT_IN_MS)).syncDeleteTopic("topic8");
    Assert.assertEquals(extraInfo.get("cluster"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster2"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("cluster3"), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(extraInfo.get("failcluster"), ExecutionStatus.UNKNOWN.toString());

    // 3 in 6 failures is PROGRESS (so it keeps trying)
    failCompleteMap.put("failcluster2", clientMap.get(null));
    failCompleteMap.put("failcluster3", clientMap.get(null));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", failCompleteMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.PROGRESS);
    Assert.assertEquals(extraInfo.get("failcluster2"), ExecutionStatus.UNKNOWN.toString());
    Assert.assertEquals(extraInfo.get("failcluster3"), ExecutionStatus.UNKNOWN.toString());

    Map<String, ControllerClient> errorMap = new HashMap<>();
    errorMap.put("cluster-err", clientMap.get(ExecutionStatus.ERROR));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", errorMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(extraInfo.get("cluster-err"), ExecutionStatus.ERROR.toString());

    errorMap.put("cluster-complete", clientMap.get(ExecutionStatus.COMPLETED));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", errorMap, topicManager);
    extraInfo = offlineJobStatus.getExtraInfo();
    Assert.assertEquals(offlineJobStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(extraInfo.get("cluster-complete"), ExecutionStatus.COMPLETED.toString());

    errorMap.put("cluster-new", clientMap.get(ExecutionStatus.NEW));
    offlineJobStatus = VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", errorMap, topicManager);
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
    VeniceParentHelixAdmin throttledParentAdmin = new VeniceParentHelixAdmin(internalAdmin, config);
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
    VeniceParentHelixAdmin throttledParentAdmin = new VeniceParentHelixAdmin(internalAdmin, config);
    throttledParentAdmin.setTimer(timer);
    throttledParentAdmin.getAdminCommandExecutionTracker()
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

    Store store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName,storeName);
    Optional<Integer> partitionCount = Optional.of(64);
    Optional<Boolean> readability = Optional.of(true);
    Optional<Boolean> writebility = Optional.empty();
    Optional<String> owner = Optional.empty();
    Optional<Long> readQuota = Optional.of(100l);
    Optional<Long> storageQuota = Optional.empty();
    parentAdmin.updateStore(clusterName, storeName, owner, readability, writebility, partitionCount, storageQuota,
        readQuota, Optional.empty(), Optional.of(135L), Optional.of(2000L));

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
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.UPDATE_STORE.ordinal());
    UpdateStore updateStore = (UpdateStore) adminMessage.payloadUnion;
    Assert.assertEquals(updateStore.clusterName.toString(), clusterName);
    Assert.assertEquals(updateStore.storeName.toString(), storeName);
    Assert.assertEquals(updateStore.readQuotaInCU, readQuota.get().longValue(),
        "New read quota should be written into kafka message.");
    Assert.assertEquals(updateStore.enableReads, readability.get().booleanValue(),
        "New read readability should be written into kafka message.");
    Assert.assertEquals(updateStore.currentVersion, AdminConsumptionTask.IGNORED_CURRENT_VERSION,
        "As we don't pass any current version into updateStore, a magic version number should be used to prevent current version being overrided in prod colo.");
    Assert.assertNotNull(updateStore.hybridStoreConfig, "Hybrid store config should result in something not null in the avro object");
    Assert.assertEquals(updateStore.hybridStoreConfig.rewindTimeInSeconds, 135L);
    Assert.assertEquals(updateStore.hybridStoreConfig.offsetLagThresholdToGoOnline, 2000L);
  }
}
