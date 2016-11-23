package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.ConfigKeys;
import com.google.common.collect.Sets;
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
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.consumer.poll.AbstractPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.ArbitraryOrderingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.CompositePollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.FilteringPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
public class TestAdminConsumptionTask {
  private static final int TIMEOUT = 10000;

  private String clusterName;
  private String topicName;
  private final byte[] emptyKeyBytes = new byte[]{'a'};
  private final VeniceConsumerFactory consumerFactory = Mockito.mock(VeniceConsumerFactory.class);
  private final String kafkaBootstrapServers = "fake_kafka_servers";
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private final String storeName = TestUtils.getUniqueString("test_store");
  private final String storeTopicName = storeName + "v1";
  private final String owner = "test_owner";
  private final String keySchema = "\"string\"";
  private final String valueSchema = "\"string\"";

  // Objects will be used by each test method
  private KafkaConsumerWrapper mockKafkaConsumer;
  private Admin admin;
  private ExecutorService executor;
  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;

  @BeforeClass
  public void initControllerStats(){
    MetricsRepository mockMetrics = TehutiUtils.getMetricsRepository(TestUtils.getUniqueString("controller-stats"));
    ControllerStats.init(mockMetrics);
  }

  @BeforeMethod
  public void methodSetup() {
    clusterName = TestUtils.getUniqueString("test-cluster");
    topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    executor = Executors.newCachedThreadPool();
    inMemoryKafkaBroker = new InMemoryKafkaBroker();
    inMemoryKafkaBroker.createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC);
    veniceWriter = getVeniceWriter(inMemoryKafkaBroker);

    mockKafkaConsumer = mock(KafkaConsumerWrapper.class);

    Mockito.doReturn(TestUtils.getOffsetAndMetadata(OffsetRecord.LOWEST_OFFSET)).when(mockKafkaConsumer)
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    admin = Mockito.mock(Admin.class);
    // By default, current controller is the master controller
    Mockito.doReturn(true).when(admin).isMasterController(clusterName);
    TopicManager topicManager = Mockito.mock(TopicManager.class);
    // By default, topic has already been created
    Mockito.doReturn(new HashSet<>(Arrays.asList(topicName))).when(topicManager).listTopics();
    Mockito.doReturn(topicManager).when(admin).getTopicManager();
    Mockito.doReturn(true).when(topicManager).containsTopic(topicName);
  }

  private VeniceWriter getVeniceWriter(InMemoryKafkaBroker inMemoryKafkaBroker) {
    Properties props = new Properties();
    props.put(VeniceWriter.CHECK_SUM_TYPE, CheckSumType.NONE.name());
    return new VeniceWriter(new VeniceProperties(props),
        topicName,
        new DefaultSerializer(),
        new DefaultSerializer(),
        new SimplePartitioner(),
        SystemTime.INSTANCE,
        () -> new MockInMemoryProducer(inMemoryKafkaBroker));
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy, boolean isParent) {
    return getAdminConsumptionTask(pollStrategy, TimeUnit.HOURS.toMinutes(1), isParent);
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy, long failureRetryTimeout, boolean isParent) {
    MockInMemoryConsumer inMemoryKafkaConsumer = new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);
    Mockito.doReturn(inMemoryKafkaConsumer)
        .when(consumerFactory)
        .getConsumer(Mockito.any());

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

  private Pair<TopicPartition, OffsetRecord> getTopicPartitionOffsetPair(RecordMetadata recordMetadata) {
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(recordMetadata.offset());
    return new Pair<>(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), offsetRecord);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenNotMasterController() throws IOException, InterruptedException {
    // Update admin to be a slave controller
    Mockito.doReturn(false).when(admin).isMasterController(clusterName);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).never())
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenTopicNotExist() throws InterruptedException, IOException {
    TopicManager topicManager = Mockito.mock(TopicManager.class);
    Mockito.doReturn(new HashSet<String>()).when(topicManager).listTopics();
    Mockito.doReturn(topicManager).when(admin).getTopicManager();

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).never())
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRun() throws InterruptedException, IOException {
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);

    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));

    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).killOfflineJob(clusterName, storeTopicName);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenStoreCreationGotExceptionForTheFirstTime() throws InterruptedException, IOException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);
    Mockito.doThrow(new VeniceException("Mock store creation exception"))
        .doNothing()
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(2)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testSkipMessageAfterTimeout () throws IOException, InterruptedException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);
    Mockito.doThrow(new VeniceException("Mock store creation exception"))
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);
    long timeoutMinutes = 0L;
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(),  timeoutMinutes, false);
    executor.submit(task);
    // admin throws errors, so record offset means we skipped the message
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testSkipMessageCommand () throws IOException, InterruptedException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);
    Mockito.doThrow(new VeniceException("Mock store creation exception"))
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    long timeoutMs = TimeUnit.MINUTES.toMillis(10); // dont timeout
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), timeoutMs, false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, ()->{
      try {
        task.skipMessageWithOffset(1L); // won't accept skip command until task has failed on this offset.
      } catch (VeniceException e) {
        return false;
      }
    return true;
    });

    // admin throws errors, so record offset means we skipped the message
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT  * 2)
  public void testSkipMessageEndToEnd() throws ExecutionException, InterruptedException {
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
    TopicManager topicManager = new TopicManager(kafka.getZkAddress());
    String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    topicManager.createTopic(adminTopic, 1, 1);
    VeniceControllerWrapper controller = ServiceFactory.getVeniceController(clusterName, kafka);

    Properties props = new Properties();
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    props.put(VeniceWriter.CHECK_SUM_TYPE, CheckSumType.NONE.name());
    VeniceProperties veniceWriterProperties = new VeniceProperties(props);
    VeniceWriter<byte[], byte[]> writer = new VeniceWriter<>(veniceWriterProperties, adminTopic, new DefaultSerializer(), new DefaultSerializer());

    byte[] message = getStoreCreationMessage(clusterName, "store-that-fails", owner, "invalid_key_schema", valueSchema); // This name is special
    long badOffset = writer.put(new byte[0], message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get().offset();

    byte[] goodMessage = getStoreCreationMessage(clusterName, "good-store", owner, keySchema, valueSchema);
    writer.put(new byte[0], goodMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    Thread.sleep(5000); // Non deterministic, but whatever.  This should never fail.
    Assert.assertFalse(controller.getVeniceAdmin().hasStore(clusterName, "good-store"));

    new ControllerClient(clusterName, controller.getControllerUrl()).skipAdminMessage(clusterName, Long.toString(badOffset));
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(controller.getVeniceAdmin().hasStore(clusterName, "good-store"));
    });
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithSameOffset() throws Exception {
    RecordMetadata killJobMetadata = (RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());
    Queue<Pair<TopicPartition, OffsetRecord>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(killJobMetadata));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));
    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    Utils.sleep(1000); // TODO: find a better to wait for AdminConsumptionTask consume the last message.
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  private OffsetRecord getOffsetRecordByOffsetAndSeqNum(long offset, int seqNum) {
    PartitionState partitionState = new PartitionState();
    partitionState.endOfPush = false;
    partitionState.offset = offset;
    partitionState.lastUpdate = System.currentTimeMillis();
    partitionState.producerStates = new HashMap<>();

    ProducerPartitionState ppState = new ProducerPartitionState();
    ppState.segmentNumber = 0;
    ppState.segmentStatus = SegmentStatus.IN_PROGRESS.ordinal();
    ppState.messageSequenceNumber = seqNum;
    ppState.messageTimestamp = System.currentTimeMillis();
    ppState.checksumType = CheckSumType.NONE.ordinal();
    ppState.checksumState = ByteBuffer.allocate(0);
    ppState.aggregates = new HashMap<>();
    ppState.debugInfo = new HashMap<>();

    partitionState.producerStates.put(
        GuidUtils.getCharSequenceFromGuid(veniceWriter.getProducerGUID()),
        ppState);

    return new OffsetRecord(partitionState);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenRestart() throws Exception {
    // Mock previous-persisted offset
    int firstAdminMessageOffset = 1;
    int firstAdminMessageSeqNum = 1;
    OffsetRecord offsetRecord = getOffsetRecordByOffsetAndSeqNum(firstAdminMessageOffset, firstAdminMessageSeqNum);
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offsetRecord.getOffset(), ByteUtils.toHexString(offsetRecord.toBytes()));
    Mockito.doReturn(offsetAndMetadata).when(mockKafkaConsumer)
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    RecordMetadata killJobMetadata = (RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();

    Queue<Pair<TopicPartition, OffsetRecord>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(killJobMetadata));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
    // Kill message is before persisted offset
    Mockito.verify(admin, Mockito.never()).killOfflineJob(clusterName, storeTopicName);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithMissingMessages() throws Exception {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeName3 = "test_store3";
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offsetToSkip = ((RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get())
        .offset();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName3, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(offsetToSkip - 1);
    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(false),
        Sets.newHashSet(
            new Pair(
                new TopicPartition(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
                offsetRecord
            )
        )
    );

    // The stores don't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName1);
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName2);
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName3);

    // Mock stats object
    ControllerStats stats = Mockito.mock(ControllerStats.class);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    task.setControllerStats(stats);
    executor.submit(task);
    Mockito.verify(stats, Mockito.timeout(TIMEOUT).atLeastOnce())
        .recordAdminTopicDIVErrorReportCount();
    task.close();
    executor.shutdownNow();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).never()).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).never()).addStore(clusterName, storeName3, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithDifferentOffset() throws InterruptedException, IOException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist for the first time, and exist for the second time
    when(admin.hasStore(clusterName, storeName))
        .thenReturn(false)
        .thenReturn(true);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).times(1)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithBiggerStartingOffset() throws InterruptedException, IOException {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // This scenario mostly happens when master controller fails over
    veniceWriter = getVeniceWriter(inMemoryKafkaBroker);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName1);
    Mockito.doReturn(false).when(admin).hasStore(clusterName, storeName2);
    Mockito.doReturn(TestUtils.getOffsetAndMetadata(1)).when(mockKafkaConsumer)
        .committed(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(2)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());

    Mockito.verify(admin, never()).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce()).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
    Mockito.verify(mockKafkaConsumer, never()).commitSync(
        eq(topicName),
        eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
        offsetAndMetadataEq(new OffsetAndMetadata(1)));
  }

  @Test (timeOut = TIMEOUT)
  public void testParentControllerSkipKillOfflinePushJobMessage() throws InterruptedException, IOException {
    veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce())
        .commitSync(eq(topicName),
            eq(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetAndMetadataEq(new OffsetAndMetadata(1)));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    Mockito.verify(admin, Mockito.timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .subscribe(Mockito.any(), Mockito.anyInt(), Mockito.any());
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1))
        .unSubscribe(Mockito.any(), Mockito.anyInt());
    Mockito.verify(admin, Mockito.timeout(TIMEOUT).never()).killOfflineJob(clusterName, storeTopicName);
  }

  private byte[] getStoreCreationMessage(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
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
    return adminOperationSerializer.serialize(adminMessage);
  }

  private byte[] getKillOfflinePushJobMessage(String clusterName, String kafkaTopic) {
    KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
    killJob.clusterName = clusterName;
    killJob.kafkaTopic = kafkaTopic;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.ordinal();
    adminMessage.payloadUnion = killJob;
    return adminOperationSerializer.serialize(adminMessage);
  }
}