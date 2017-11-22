package com.linkedin.venice.controller.kafka.consumer;

import com.google.common.collect.Sets;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
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
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.offsets.DeepCopyOffsetManager;
import com.linkedin.venice.offsets.InMemoryOffsetManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.DefaultSerializer;
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
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Slow test class, given fast priority
 */
@Test(priority = -5)
public class TestAdminConsumptionTask {
  private static final int TIMEOUT = 10000;

  private String clusterName;
  private String topicName;
  private final byte[] emptyKeyBytes = new byte[]{'a'};
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private final String storeName = TestUtils.getUniqueString("test_store");
  private final String storeTopicName = storeName + "v1";
  private final String owner = "test_owner";
  private final String keySchema = "\"string\"";
  private final String valueSchema = "\"string\"";

  // Objects will be used by each test method
  private KafkaConsumerWrapper mockKafkaConsumer;
  private Admin admin;
  private OffsetManager offsetManager;
  private ExecutorService executor;
  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;
  private ExecutionIdAccessor executionIdAccessor;

  @BeforeMethod
  public void methodSetup() {
    clusterName = TestUtils.getUniqueString("test-cluster");
    topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    executor = Executors.newCachedThreadPool();
    inMemoryKafkaBroker = new InMemoryKafkaBroker();
    inMemoryKafkaBroker.createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC);
    veniceWriter = getVeniceWriter(inMemoryKafkaBroker);
    executionIdAccessor = Mockito.mock(ExecutionIdAccessor.class);

    mockKafkaConsumer = mock(KafkaConsumerWrapper.class);

    admin = mock(Admin.class);
    // By default, current controller is the master controller
    doReturn(true).when(admin).isMasterController(clusterName);

    offsetManager = new InMemoryOffsetManager();

    TopicManager topicManager = mock(TopicManager.class);
    // By default, topic has already been created
    doReturn(new HashSet<>(Arrays.asList(topicName))).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();
    doReturn(true).when(topicManager).containsTopic(topicName);
  }

  @AfterMethod
  public void cleanUp() {
    executor.shutdownNow();
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
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);

    return getAdminConsumptionTask(pollStrategy, failureRetryTimeout, isParent, stats);
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy,
                                                       long failureRetryTimeout,
                                                       boolean isParent,
                                                       AdminConsumptionStats stats) {
    MockInMemoryConsumer inMemoryKafkaConsumer = new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);
    DeepCopyOffsetManager deepCopyOffsetManager = new DeepCopyOffsetManager(offsetManager);

    return new AdminConsumptionTask(clusterName, inMemoryKafkaConsumer, admin, deepCopyOffsetManager,
        executionIdAccessor, failureRetryTimeout, isParent, stats, 1000);
  }

  private Pair<TopicPartition, OffsetRecord> getTopicPartitionOffsetPair(RecordMetadata recordMetadata) {
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(recordMetadata.offset());
    return new Pair<>(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), offsetRecord);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenNotMasterController() throws IOException, InterruptedException {
    // Update admin to be a slave controller
    doReturn(false).when(admin).isMasterController(clusterName);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, never())
        .subscribe(any(), anyInt(), any());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenTopicNotExist() throws InterruptedException, IOException {
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(new HashSet<String>()).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, never())
        .subscribe(any(), anyInt(), any());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testRun() throws InterruptedException, IOException {
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);

    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 0),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(2));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, timeout(TIMEOUT)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
    verify(admin, timeout(TIMEOUT)).killOfflinePush(clusterName, storeTopicName);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWhenStoreCreationGotExceptionForTheFirstTime() throws InterruptedException, IOException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception"))
        .doNothing()
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(1));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, timeout(TIMEOUT).times(2)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testSkipMessageAfterTimeout () throws IOException, InterruptedException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception"))
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);
    long timeoutMinutes = 0L;
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(),  timeoutMinutes, false);
    executor.submit(task);
    // admin throws errors, so record offset means we skipped the message
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(1));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test (timeOut = TIMEOUT)
  public void testSkipMessageCommand() throws IOException, InterruptedException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception"))
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
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(1));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  /**
   * This test is flaky on slower hardware, with a short timeout ):
   */
  @Test (timeOut = TIMEOUT * 6)
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

    new ControllerClient(clusterName, controller.getControllerUrl()).skipAdminMessage(Long.toString(badOffset));
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT * 3, TimeUnit.MILLISECONDS, () -> {
      Assert.assertTrue(controller.getVeniceAdmin().hasStore(clusterName, "good-store"));
    });
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithSameOffset() throws Exception {
    RecordMetadata killJobMetadata = (RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 0),
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

    doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(2));
    });

    Utils.sleep(1000); // TODO: find a better to wait for AdminConsumptionTask consume the last message.
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, timeout(TIMEOUT)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  private OffsetRecord getOffsetRecordByOffsetAndSeqNum(long offset, int seqNum) {
    PartitionState partitionState = new PartitionState();
    partitionState.endOfPush = false;
    partitionState.offset = offset;
    partitionState.lastUpdate = System.currentTimeMillis();
    partitionState.producerStates = new HashMap<>();

    ProducerPartitionState ppState = new ProducerPartitionState();
    ppState.segmentNumber = 0;
    ppState.segmentStatus = SegmentStatus.IN_PROGRESS.getValue();
    ppState.messageSequenceNumber = seqNum;
    ppState.messageTimestamp = System.currentTimeMillis();
    ppState.checksumType = CheckSumType.NONE.getValue();
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
    offsetManager.put(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offsetRecord);

    RecordMetadata killJobMetadata = (RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 0),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();

    Queue<Pair<TopicPartition, OffsetRecord>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(killJobMetadata));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(2));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, timeout(TIMEOUT)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
    // Kill message is before persisted offset
    verify(admin, never()).killOfflinePush(clusterName, storeTopicName);
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
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    doReturn(false).when(admin).hasStore(clusterName, storeName3);

    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, 1, false, stats);
    executor.submit(task);
    verify(stats, timeout(TIMEOUT).atLeastOnce())
        .recordAdminTopicDIVErrorReportCount();
    task.close();
    executor.shutdownNow();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, timeout(TIMEOUT)).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    verify(admin, never()).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
    verify(admin, never()).addStore(clusterName, storeName3, owner, keySchema, valueSchema);
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

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(2));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, timeout(TIMEOUT)).addStore(clusterName, storeName, owner, keySchema, valueSchema);
  }

  @Test (timeOut = 2 * TIMEOUT)
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
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    offsetManager.put(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, TestUtils.getOffsetRecord(1));

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(3));
    });
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());

    verify(admin, never()).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    verify(admin, atLeastOnce()).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
  }

  @Test (timeOut = TIMEOUT)
  public void testParentControllerSkipKillOfflinePushJobMessage() throws InterruptedException, IOException {
    veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 0),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(1));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());
    verify(admin, never()).killOfflinePush(clusterName, storeTopicName);
  }

  @Test
  public void testGetLastSucceedExecutionId()
      throws Exception {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);
    for (long executionId = 1; executionId <= 3; executionId++) {
      veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, executionId),
          AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      final long offset = executionId;
      TestUtils.waitForNonDeterministicCompletion(TIMEOUT, TimeUnit.MILLISECONDS,
          () -> offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID)
              .equals(TestUtils.getOffsetRecord(offset)));

      Assert.assertEquals(task.getLastSucceedExecutionId(), executionId,
          "After consumption succeed, the last succeed execution id should be updated.");
    }

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSetStore() throws Exception{
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    String newOwner = owner+"updated";
    int partitionNumber = 100;
    int currentVersion = 100;
    boolean enableReads = false;
    boolean enableWrites = true;
    boolean accessControlled = true;
    UpdateStore setStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    setStore.clusterName = clusterName;
    setStore.storeName = storeName;
    setStore.owner = newOwner;
    setStore.partitionNum = partitionNumber;
    setStore.currentVersion = currentVersion;
    setStore.enableReads = enableReads;
    setStore.enableWrites = enableWrites;
    setStore.accessControlled = accessControlled;

    HybridStoreConfigRecord hybridConfig = new HybridStoreConfigRecord();
    hybridConfig.rewindTimeInSeconds = 123L;
    hybridConfig.offsetLagThresholdToGoOnline = 1000L;
    setStore.hybridStoreConfig = hybridConfig;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = setStore;
    byte[] message = adminOperationSerializer.serialize(adminMessage);


    veniceWriter.put(emptyKeyBytes, message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(2));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .updateStore(eq(clusterName), eq(storeName), any(), any(), any(), any(), any(), any(), any(),
            eq(Optional.of(123L)), eq(Optional.of(1000L)), eq(Optional.of(accessControlled)), any(), any());
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
    adminMessage.operationType = AdminMessageType.STORE_CREATION.getValue();
    adminMessage.payloadUnion = storeCreation;
    return adminOperationSerializer.serialize(adminMessage);
  }

  private byte[] getKillOfflinePushJobMessage(String clusterName, String kafkaTopic, long executionId) {
    KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
    killJob.clusterName = clusterName;
    killJob.kafkaTopic = kafkaTopic;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.getValue();
    adminMessage.payloadUnion = killJob;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage);
  }
}