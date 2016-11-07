package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
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
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentCaptor;
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
import static org.mockito.Mockito.doReturn;


public class TestVeniceParentHelixAdmin {
  private static int TIMEOUT_IN_MS = 10 * Time.MS_PER_SECOND;
  private static int KAFKA_REPLICA_FACTOR = 3;
  private final String clusterName = "test-cluster";
  private final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;
  private final TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private TopicManager topicManager;
  private VeniceHelixAdmin internalAdmin;
  private KafkaConsumerWrapper consumer;
  private VeniceWriter veniceWriter;
  private VeniceParentHelixAdmin parentAdmin;

  @BeforeMethod
  public void init() {
    topicManager = Mockito.mock(TopicManager.class);
    Mockito.doReturn(new HashSet<String>(Arrays.asList(topicName)))
        .when(topicManager)
        .listTopics();
    internalAdmin = Mockito.mock(VeniceHelixAdmin.class);
    Mockito.doReturn(topicManager)
        .when(internalAdmin)
        .getTopicManager();

    VeniceControllerConfig config = Mockito.mock(VeniceControllerConfig.class);
    Mockito.doReturn(KAFKA_REPLICA_FACTOR).when(config)
        .getKafkaReplicaFactor();
    Mockito.doReturn(3).when(config)
        .getParentControllerWaitingTimeForConsumptionMs();
    Mockito.doReturn("fake_kafka_bootstrap_servers").when(config)
        .getKafkaBootstrapServers();

    VeniceHelixResources resources = Mockito.mock(VeniceHelixResources.class);
    Mockito.doReturn(config).when(resources)
        .getConfig();
    Mockito.doReturn(resources).when(internalAdmin)
        .getVeniceHelixResource(clusterName);

    consumer = Mockito.mock(KafkaConsumerWrapper.class);

    VeniceConsumerFactory consumerFactory = Mockito.mock(VeniceConsumerFactory.class);
    doReturn(consumer).when(consumerFactory).getConsumer(Mockito.any());

    parentAdmin = new VeniceParentHelixAdmin(internalAdmin, config, consumerFactory);
    veniceWriter = Mockito.mock(VeniceWriter.class);
    // Need to bypass VeniceWriter initialization
    parentAdmin.setVeniceWriterForCluster(clusterName, veniceWriter);
  }

  @Test
  public void testStartWithTopicExists() {
    parentAdmin.start(clusterName);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .getTopicManager();
    Mockito.verify(topicManager, Mockito.never())
        .createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, KAFKA_REPLICA_FACTOR);
  }

  @Test
  public void testStartWhenTopicNotExists() {
    Mockito.doReturn(new HashSet<String>())
        .when(topicManager)
        .listTopics();
    parentAdmin.start(clusterName);
    Mockito.verify(internalAdmin, Mockito.times(1))
        .getTopicManager();
    Mockito.verify(topicManager, Mockito.times(1))
        .createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, KAFKA_REPLICA_FACTOR);
  }

  @Test
  public void testAddStore() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET))
        .thenReturn(TestUtils.getOffsetAndMetadata(1));

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    Mockito.verify(internalAdmin, Mockito.times(1))
    .checkPreConditionForAddStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(consumer, Mockito.times(2))
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
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

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET));

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    Mockito.doThrow(new VeniceException("Store: " + storeName + " already exists. ..."))
        .when(internalAdmin)
        .checkPreConditionForAddStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Some operation is going on.*")
  public void testAddStoreWhenLastOffsetHasntBeenConsumed() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new VeniceException("mock exception"))
        .when(internalAdmin)
        .getLastException(clusterName);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(0))
        .thenReturn(TestUtils.getOffsetAndMetadata(1))
        .thenReturn(TestUtils.getOffsetAndMetadata(0));

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
    Mockito.doReturn(valueSchemaId).when(internalAdmin)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
    Mockito.doReturn(valueSchemaId).when(internalAdmin)
        .getValueSchemaId(clusterName, storeName, valueSchemaStr);

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET))
        .thenReturn(TestUtils.getOffsetAndMetadata(1));

    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(consumer, Mockito.times(2))
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
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
  public void testPauseStore() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET))
        .thenReturn(TestUtils.getOffsetAndMetadata(1));

    parentAdmin.pauseStore(clusterName, storeName);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, true);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(consumer, Mockito.times(2))
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);
    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.PAUSE_STORE.ordinal());
    PauseStore pauseStore = (PauseStore) adminMessage.payloadUnion;

    Assert.assertEquals(pauseStore.clusterName.toString(), clusterName);
    Assert.assertEquals(pauseStore.storeName.toString(), storeName);
  }

  @Test (expectedExceptions = VeniceNoStoreException.class)
  public void testPauseStoreWhenStoreDoesntExist() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET))
        .thenReturn(TestUtils.getOffsetAndMetadata(1));

    Mockito.when(internalAdmin.checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, true))
        .thenThrow(new VeniceNoStoreException(storeName));

    parentAdmin.pauseStore(clusterName, storeName);
  }

  @Test
  public void testResumeStore() throws ExecutionException, InterruptedException {
    parentAdmin.start(clusterName);

    String storeName = "test-store";

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET))
        .thenReturn(TestUtils.getOffsetAndMetadata(1));

    parentAdmin.resumeStore(clusterName, storeName);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, false);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(consumer, Mockito.times(2))
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    Assert.assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Assert.assertEquals(keyBytes.length, 0);
    AdminOperation adminMessage = adminOperationSerializer.deserialize(valueBytes, schemaId);
    Assert.assertEquals(adminMessage.operationType, AdminMessageType.RESUME_STORE.ordinal());
    ResumeStore resumeStore = (ResumeStore) adminMessage.payloadUnion;
    Assert.assertEquals(resumeStore.clusterName.toString(), clusterName);
    Assert.assertEquals(resumeStore.storeName.toString(), storeName);
  }

  @Test
  public void testKillOfflinePushJob() throws ExecutionException, InterruptedException {
    String kafkaTopic = "test_store_v1";
    parentAdmin.start(clusterName);

    Future future = Mockito.mock(Future.class);
    Mockito.doReturn(new RecordMetadata(topicPartition, 0, 1, -1))
        .when(future).get();
    Mockito.doReturn(future)
        .when(veniceWriter)
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());

    Mockito.when(consumer.committed(topicName, partitionId))
        .thenReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET))
        .thenReturn(TestUtils.getOffsetAndMetadata(1));

    Mockito.doReturn(new HashSet<String>(Arrays.asList(kafkaTopic)))
        .when(topicManager).listTopics();

    parentAdmin.killOfflineJob(clusterName, kafkaTopic);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForKillOfflineJob(clusterName, kafkaTopic);
    Mockito.verify(topicManager, Mockito.times(1))
        .deleteTopic(kafkaTopic);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(consumer, Mockito.times(2))
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    Mockito.verify(veniceWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaCaptor.capture());
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
    String storeName = "test_store";
    parentAdmin.incrementVersion(clusterName, storeName, 1, 1);
    Mockito.verify(internalAdmin).addVersion(clusterName, storeName, VeniceHelixAdmin.VERSION_ID_UNSET, 1, 1, false);
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*exists for store.*")
  public void testIncrementVersionWhenPreviousTopicsExist() {
    String storeName = "test_store";
    String previousKafkaTopic = "test_store_v1";
    String unknownTopic = "1unknown_topic";
    Mockito.doReturn(new HashSet<String>(Arrays.asList(unknownTopic, previousKafkaTopic)))
        .when(topicManager)
        .listTopics();
    parentAdmin.incrementVersion(clusterName, storeName, 1, 1);
  }

  /**
   * End-to-end test
   *
   * TODO: Fix this test, it's flaky.
   */
  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void testEnd2End() throws IOException {
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceParentController(clusterName, kafkaBrokerWrapper);

    String controllerUrl = controllerWrapper.getControllerUrl();
    // Adding store
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr = "\"string\"";
    ControllerClient.createNewStore(controllerUrl, clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
    MultiStoreResponse response = ControllerClient.queryStoreList(controllerUrl, clusterName);
    String[] stores = response.getStores();
    Assert.assertEquals(stores.length, 1);
    Assert.assertEquals(stores[0], storeName);

    // Adding key schema
    SchemaResponse keySchemaResponse = ControllerClient.getKeySchema(controllerUrl, clusterName, storeName);
    Assert.assertEquals(keySchemaResponse.getSchemaStr(), keySchemaStr);

    // Adding value schema
    ControllerClient.addValueSchema(controllerUrl, clusterName, storeName, valueSchemaStr);
    MultiSchemaResponse valueSchemaResponse = ControllerClient.getAllValueSchema(controllerUrl, clusterName, storeName);
    MultiSchemaResponse.Schema[] schemas = valueSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 1);
    Assert.assertEquals(schemas[0].getId(), 1);
    Assert.assertEquals(schemas[0].getSchemaStr(), valueSchemaStr);

    // Pause store
    ControllerClient.setPauseStatus(controllerUrl, clusterName, storeName, true);
    StoreResponse storeResponse = ControllerClient.getStore(controllerUrl, clusterName, storeName);
    Assert.assertTrue(storeResponse.getStore().isPaused());

    // Resume store
    ControllerClient.setPauseStatus(controllerUrl, clusterName, storeName, false);
    storeResponse = ControllerClient.getStore(controllerUrl, clusterName, storeName);
    Assert.assertFalse(storeResponse.getStore().isPaused());

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
      ControllerClient statusClient = Mockito.mock(ControllerClient.class);
      doReturn(response).when(statusClient).queryJobStatus(anyString(), anyString());
      clientMap.put(status, statusClient);
    }
    TopicManager topicManager = Mockito.mock(TopicManager.class);

    JobStatusQueryResponse failResponse = new JobStatusQueryResponse();
    failResponse.setError("error");
    ControllerClient failClient = Mockito.mock(ControllerClient.class);
    doReturn(failResponse).when(failClient).queryJobStatus(anyString(), anyString());
    clientMap.put(null, failClient);

    // Verify clients work as expected
    for (ExecutionStatus status : ExecutionStatus.values()) {
      Assert.assertEquals(clientMap.get(status).queryJobStatus("cluster", "topic").getStatus(), status.toString());
    }
    Assert.assertTrue(clientMap.get(null).queryJobStatus("cluster", "topic").isError());

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

    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic1", completeMap, topicManager),
        ExecutionStatus.COMPLETED);
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).times(1)).deleteTopic("topic1");

    completeMap.put("cluster-slow", clientMap.get(ExecutionStatus.NOT_CREATED));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic2", completeMap, topicManager),
        ExecutionStatus.NOT_CREATED);  // Do we want this to be Progress?  limitation of ordering used in aggregation code
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).never()).deleteTopic("topic2");


    Map<String, ControllerClient> progressMap = new HashMap<>();
    progressMap.put("cluster", clientMap.get(ExecutionStatus.NOT_CREATED));
    progressMap.put("cluster3", clientMap.get(ExecutionStatus.NOT_CREATED));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic3", progressMap, topicManager),
        ExecutionStatus.NOT_CREATED);
    Mockito.verify(topicManager,Mockito.timeout(TIMEOUT_IN_MS).never()).deleteTopic("topic3");

    progressMap.put("cluster5", clientMap.get(ExecutionStatus.NEW));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic4", progressMap, topicManager),
        ExecutionStatus.NEW);
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).never()).deleteTopic("topic4");

    progressMap.put("cluster7", clientMap.get(ExecutionStatus.PROGRESS));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic5", progressMap, topicManager),
        ExecutionStatus.PROGRESS);
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).never()).deleteTopic("topic5");;

    progressMap.put("cluster9", clientMap.get(ExecutionStatus.STARTED));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic6", progressMap, topicManager),
        ExecutionStatus.PROGRESS);
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).never()).deleteTopic("topic6");

    progressMap.put("cluster11", clientMap.get(ExecutionStatus.COMPLETED));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic7", progressMap, topicManager),
        ExecutionStatus.PROGRESS);
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).never()).deleteTopic("topic7");

    // 1 in 4 failures is ERROR
    Map<String, ControllerClient> failCompleteMap = new HashMap<>();
    failCompleteMap.put("cluster", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster2", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("cluster3", clientMap.get(ExecutionStatus.COMPLETED));
    failCompleteMap.put("failcluster", clientMap.get(null));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "topic8", failCompleteMap, topicManager),
        ExecutionStatus.ERROR);
    Mockito.verify(topicManager, Mockito.timeout(TIMEOUT_IN_MS).times(1)).deleteTopic("topic8");

    // 3 in 6 failures is PROGRESS (so it keeps trying)
    failCompleteMap.put("failcluster2", clientMap.get(null));
    failCompleteMap.put("failcluster3", clientMap.get(null));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", failCompleteMap, topicManager),
        ExecutionStatus.PROGRESS);

    Map<String, ControllerClient> errorMap = new HashMap<>();
    errorMap.put("cluster-err", clientMap.get(ExecutionStatus.ERROR));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", errorMap, topicManager),
        ExecutionStatus.ERROR);
    errorMap.put("cluster-complete", clientMap.get(ExecutionStatus.COMPLETED));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", errorMap, topicManager),
        ExecutionStatus.ERROR);
    errorMap.put("cluster-new", clientMap.get(ExecutionStatus.NEW));
    Assert.assertEquals(VeniceParentHelixAdmin.getOffLineJobStatus("mycluster", "atopic", errorMap, topicManager),
        ExecutionStatus.NEW); // Do we want this to be Progress?  limitation of ordering used in aggregation code
  }

  @Test
  public void testGetProgress() {
    JobStatusQueryResponse tenResponse = new JobStatusQueryResponse();
    Map<String, Long> tenPerTaskProgress = new HashMap<>();
    tenPerTaskProgress.put("task1", 10L);
    tenPerTaskProgress.put("task2", 10L);
    tenResponse.setPerTaskProgress(tenPerTaskProgress);
    ControllerClient tenStatusClient = Mockito.mock(ControllerClient.class);
    doReturn(tenResponse).when(tenStatusClient).queryJobStatus(anyString(), anyString());

    JobStatusQueryResponse failResponse = new JobStatusQueryResponse();
    failResponse.setError("error2");
    ControllerClient failClient = Mockito.mock(ControllerClient.class);
    doReturn(failResponse).when(failClient).queryJobStatus(anyString(), anyString());

    // Clients work as expected
    JobStatusQueryResponse status = tenStatusClient.queryJobStatus("cluster", "topic");
    Map<String,Long> perTask = status.getPerTaskProgress();
    Assert.assertEquals(perTask.get("task1"), new Long(10L));
    Assert.assertTrue(failClient.queryJobStatus("cluster", "topic").isError());

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
}
