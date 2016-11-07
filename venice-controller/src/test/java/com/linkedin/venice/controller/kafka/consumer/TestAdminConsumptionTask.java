package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.ControllerStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.DeepCopyOffsetManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestAdminConsumptionTask {
  private static final int TIMEOUT = 10000;

  private final String clusterName = "test-cluster";
  private final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final KafkaKey emptyKey = new KafkaKey(MessageType.PUT, new byte[0]);
  private final VeniceConsumerFactory consumerFactory = Mockito.mock(VeniceConsumerFactory.class);
  private final String kafkaBootstrapServers = "fake_kafka_servers";

  private TopicPartition topicPartition = new TopicPartition(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

  // Objects will be used by each test method
  private KafkaConsumerWrapper consumer;
  private Admin admin;
  private ExecutorService executor;

  @BeforeClass
  public void initControllerStats(){
    MetricsRepository mockMetrics = TehutiUtils.getMetricsRepository(TestUtils.getUniqueString("controller-stats"));
    ControllerStats.init(mockMetrics);
  }

  private void initTaskRelatedArgs(ConsumerRecord ... recordList) {
    executor = Executors.newCachedThreadPool();
    consumer = Mockito.mock(KafkaConsumerWrapper.class);
    Mockito.doReturn(consumer).when(consumerFactory)
        .getConsumer(Mockito.any());
    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> partitionRecordMap = new HashMap<>();
    partitionRecordMap.put(topicPartition, Arrays.asList(recordList));
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = new ConsumerRecords<>(partitionRecordMap);
    Mockito.doReturn(consumerRecords).when(consumer).poll(Mockito.anyLong());
    Mockito.when(consumer.poll(Mockito.anyLong())).thenAnswer(new Answer<ConsumerRecords>() {
      @Override
      public ConsumerRecords answer(InvocationOnMock invocation) {
        Utils.sleep(AdminConsumptionTask.READ_CYCLE_DELAY_MS / 10);
        return consumerRecords;
      }
    });

    Mockito.doReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET)).when(consumer)
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    admin = Mockito.mock(Admin.class);
    // By default, current controller is the master controller
    Mockito.doReturn(true).when(admin).isMasterController(clusterName);
    TopicManager topicManager = Mockito.mock(TopicManager.class);
    // By default, topic has already been created
    Mockito.doReturn(new HashSet<String>(Arrays.asList(topicName))).when(topicManager).listTopics();
    Mockito.doReturn(topicManager).when(admin).getTopicManager();
  }

  private AdminConsumptionTask getAdminConsumptionTask(boolean isParent) {
    return getAdminConsumptionTask(TimeUnit.HOURS.toMinutes(1), isParent);
  }

  private AdminConsumptionTask getAdminConsumptionTask(long failureRetryTimeout, boolean isParent) {
    return new AdminConsumptionTask(clusterName, consumerFactory, kafkaBootstrapServers, admin, failureRetryTimeout, isParent);
  }

  static class OffsetAndMetadataMatcher extends ArgumentMatcher<OffsetAndMetadata> {
    private final OffsetAndMetadata expected;
    public OffsetAndMetadataMatcher(OffsetAndMetadata expected) {
      this.expected = expected;
    }
    @Override
    public boolean matches(Object argument) {
      return ((OffsetAndMetadata)argument).offset() == expected.offset();
    }
  }

  static OffsetAndMetadata offsetAndMetadataEq(OffsetAndMetadata expected) {
    return argThat(new OffsetAndMetadataMatcher(expected));
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenNotMasterController() throws IOException, InterruptedException {
    initTaskRelatedArgs();
    // Update admin to be a slave controller
    Mockito.doReturn(false).when(admin).isMasterController(clusterName);

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).never())
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenTopicNotExist() throws InterruptedException, IOException {
    initTaskRelatedArgs();
    TopicManager topicManager = Mockito.mock(TopicManager.class);
    Mockito.doReturn(new HashSet<String>()).when(topicManager).listTopics();
    Mockito.doReturn(topicManager).when(admin).getTopicManager();

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).never())
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRun() throws InterruptedException, IOException {
    String storeName = "test_store";
    String kafkaTopic = storeName + "_v1";
    String owner = "test_owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    initTaskRelatedArgs(
        getStoreCreationMessage(clusterName, storeName, owner, 1, keySchema, valueSchema),
        getKillOfflinePushJobMessage(clusterName, kafkaTopic, 2)
    );
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));

    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).killOfflineJob(clusterName, kafkaTopic);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenStoreCreationGotExceptionForTheFirstTime() throws InterruptedException, IOException {
    String storeName = TestUtils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    initTaskRelatedArgs(getStoreCreationMessage(clusterName, storeName, owner, 1, keySchema, valueSchema));
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);
    Mockito.doThrow(new VeniceException("Mock store creation exception"))
        .doNothing()
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(2)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testSkipMessageAfterTimeout () throws IOException, InterruptedException {
    String storeName = TestUtils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    initTaskRelatedArgs(getStoreCreationMessage(clusterName, storeName, owner, 1, keySchema, valueSchema));
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);
    Mockito.doThrow(new VeniceException("Mock store creation exception"))
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    long timeoutMinutes = 0L;
    AdminConsumptionTask task = getAdminConsumptionTask(timeoutMinutes, false);
    executor.submit(task);
    // admin throws errors, so record offset means we skipped the message
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithSameOffset() throws InterruptedException, IOException {
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    ConsumerRecord storeCreationRecord = getStoreCreationMessage(clusterName, storeName, owner, 1, keySchema, valueSchema);
    initTaskRelatedArgs(storeCreationRecord, storeCreationRecord);
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithDifferentOffset() throws InterruptedException, IOException {
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    ConsumerRecord storeCreationRecord1 = getStoreCreationMessage(clusterName, storeName, owner, 1, keySchema, valueSchema);
    ConsumerRecord storeCreationRecord2 = getStoreCreationMessage(clusterName, storeName, owner, 2, keySchema, valueSchema);
    initTaskRelatedArgs(storeCreationRecord1, storeCreationRecord2);
    // The store doesn't exist for the first time, and exist for the second time
    when(admin.hasStore(clusterName, storeName))
        .thenReturn(false)
        .thenReturn(true);

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithBiggerStartingOffset() throws InterruptedException, IOException {
    String storeName1 = "test_store1";
    String storeName2 = "test_stoer2";
    String owner = "test_owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    initTaskRelatedArgs(
        getStoreCreationMessage(clusterName, storeName1, owner, 1, keySchema, valueSchema),
        getStoreCreationMessage(clusterName, storeName2, owner, 2, keySchema, valueSchema)
    );
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName1);
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName2);
    Mockito.doReturn(TestUtils.getOffsetAndMetadata(1)).when(consumer)
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    AdminConsumptionTask task = getAdminConsumptionTask(false);
    executor.submit(task);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());

    Mockito.verify(admin, never()).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce()).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
    Mockito.verify(consumer, never()).commitSync(
        eq(topicName),
        eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
        offsetAndMetadataEq(new OffsetAndMetadata(1)));
  }


  @Test (timeOut = TIMEOUT)
  public void testParentControllerSkipKillOfflinePushJobMessage() throws InterruptedException, IOException {
    String storeName = "test_store";
    String kafkaTopic = storeName + "_v1";
    initTaskRelatedArgs(
        getKillOfflinePushJobMessage(clusterName, kafkaTopic, 2)
    );

    AdminConsumptionTask task = getAdminConsumptionTask(true);
    executor.submit(task);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(consumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).never()).killOfflineJob(clusterName, kafkaTopic);
  }

  private ConsumerRecord getStoreCreationMessage(String clusterName, String storeName, String owner, long offset, String keySchema, String valueSchema) {
    AdminOperationSerializer serializer = new AdminOperationSerializer();
    StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
    storeCreation.clusterName = clusterName;
    storeCreation.storeName = storeName;
    storeCreation.owner = owner;
    storeCreation.keySchema = new SchemaMeta();
    storeCreation.keySchema.definition = keySchema;
    storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.ordinal();
    storeCreation.valueSchema = new SchemaMeta();
    storeCreation.valueSchema.definition = valueSchema;
    storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.ordinal();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.STORE_CREATION.ordinal();
    adminMessage.payloadUnion = storeCreation;
    byte[] payload = serializer.serialize(adminMessage);
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(payload);
    put.schemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
    KafkaMessageEnvelope message = new KafkaMessageEnvelope();
    message.messageType = MessageType.PUT.ordinal();
    message.payloadUnion = put;

    return new ConsumerRecord(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offset, -1, TimestampType.NO_TIMESTAMP_TYPE, emptyKey, message);
  }

  private ConsumerRecord getKillOfflinePushJobMessage(String clusterName, String kafkaTopic, long offset) {
    AdminOperationSerializer serializer = new AdminOperationSerializer();
    KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
    killJob.clusterName = clusterName;
    killJob.kafkaTopic = kafkaTopic;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.ordinal();
    adminMessage.payloadUnion = killJob;
    byte[] payload = serializer.serialize(adminMessage);
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(payload);
    put.schemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
    KafkaMessageEnvelope message = new KafkaMessageEnvelope();
    message.messageType = MessageType.PUT.ordinal();
    message.payloadUnion = put;

    return new ConsumerRecord(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offset, -1, TimestampType.NO_TIMESTAMP_TYPE, emptyKey, message);
  }
}