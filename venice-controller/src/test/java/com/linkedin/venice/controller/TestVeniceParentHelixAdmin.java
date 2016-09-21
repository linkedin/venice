package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.writer.VeniceWriter;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class TestVeniceParentHelixAdmin {
  private static int KAFKA_REPLICA_FACTOR = 3;
  private final String clusterName = "test-cluster";
  private final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;
  private final TopicPartition topicPartition = new TopicPartition(topicName, partitionId);

  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private TopicManager topicManager;
  private VeniceHelixAdmin internalAdmin;
  private OffsetManager offsetManager;
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

    VeniceHelixResources resources = Mockito.mock(VeniceHelixResources.class);
    Mockito.doReturn(config).when(resources)
        .getConfig();
    Mockito.doReturn(resources).when(internalAdmin)
        .getVeniceHelixResource(clusterName);

    offsetManager = Mockito.mock(OffsetManager.class);


    parentAdmin = new VeniceParentHelixAdmin(internalAdmin, offsetManager, config);
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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1))
        .thenReturn(new OffsetRecord(1));

    String storeName = "test-store";
    String owner = "test-owner";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.addStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);

    Mockito.verify(internalAdmin, Mockito.times(1))
    .checkPreConditionForAddStore(clusterName, storeName, owner, keySchemaStr, valueSchemaStr);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(offsetManager, Mockito.times(2))
        .getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1));

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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1))
        .thenReturn(new OffsetRecord(1))
        .thenReturn(new OffsetRecord(0));

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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1))
        .thenReturn(new OffsetRecord(1));

    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(offsetManager, Mockito.times(2))
        .getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1))
        .thenReturn(new OffsetRecord(1));

    parentAdmin.pauseStore(clusterName, storeName);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, true);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(offsetManager, Mockito.times(2))
        .getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1))
        .thenReturn(new OffsetRecord(1));
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

    Mockito.when(offsetManager.getLastOffset(topicName, partitionId))
        .thenReturn(new OffsetRecord(-1))
        .thenReturn(new OffsetRecord(1));

    parentAdmin.resumeStore(clusterName, storeName);

    Mockito.verify(internalAdmin, Mockito.times(1))
        .checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, false);
    Mockito.verify(veniceWriter, Mockito.times(1))
        .put(Mockito.any(), Mockito.any(), Mockito.anyInt());
    Mockito.verify(offsetManager, Mockito.times(2))
        .getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
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

  /**
   * End-to-end test
   */
  @Test
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

    controllerWrapper.close();
    kafkaBrokerWrapper.close();
  }
}
