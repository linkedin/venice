package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.admin.InMemoryExecutionIdAccessor;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
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
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.DeepCopyOffsetManager;
import com.linkedin.venice.offsets.InMemoryOffsetManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
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
import com.linkedin.venice.utils.Time;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
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
  private final String storeTopicName = storeName + "_v1";
  private final String owner = "test_owner";
  private final String keySchema = "\"string\"";
  private final String valueSchema = "\"string\"";

  // Objects will be used by each test method
  private KafkaConsumerWrapper mockKafkaConsumer;
  private VeniceHelixAdmin admin;
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
    executionIdAccessor = new InMemoryExecutionIdAccessor();

    mockKafkaConsumer = mock(KafkaConsumerWrapper.class);

    admin = mock(VeniceHelixAdmin.class);
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
    return new TestVeniceWriter(new VeniceProperties(props),
        topicName,
        new DefaultSerializer(),
        new DefaultSerializer(),
        new SimplePartitioner(),
        SystemTime.INSTANCE,
        () -> new MockInMemoryProducer(inMemoryKafkaBroker));
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy, boolean isParent) {
    return getAdminConsumptionTask(pollStrategy, isParent, 10000);
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy, boolean isParent,
      long adminConsumptionCycleTimeoutMs) {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);

    return getAdminConsumptionTask(pollStrategy, isParent, stats, adminConsumptionCycleTimeoutMs);
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy, boolean isParent,
      AdminConsumptionStats stats, long adminConsumptionCycleTimeoutMs) {
    MockInMemoryConsumer inMemoryKafkaConsumer =
        new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);
    DeepCopyOffsetManager deepCopyOffsetManager = new DeepCopyOffsetManager(offsetManager);

    return new AdminConsumptionTask(clusterName, inMemoryKafkaConsumer, admin, deepCopyOffsetManager,
        executionIdAccessor, isParent, stats, 1, adminConsumptionCycleTimeoutMs, 1);
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

  /**
   * In a parent controller, when the topic doesn't exist, it should be automatically created.
   */
  @Test (timeOut = TIMEOUT)
  public void testRunWhenTopicDoesNotExistInParent() throws InterruptedException, IOException {
    testRunWhenTopicDoesNotExist(true);
  }

  /**
   * In a non-parent controller, when the topic doesn't exist, it should NOT be automatically created.
   *
   * N.B.: This behavior is somewhat flawed. In reality, we would want multi-colo child controllers
   *       to be the only ones which do not auto-create topics, not all non-parent controllers.
   *       The current behavior means that a single-colo deployment requires outside intervention
   *       to work properly, and only multi-colo deployments auto-create the topic(s) they need.
   */
  @Test (timeOut = TIMEOUT)
  public void testRunWhenTopicDoesNotExistInNonParent() throws InterruptedException, IOException {
    testRunWhenTopicDoesNotExist(false);
  }

  private void testRunWhenTopicDoesNotExist(boolean isParent) throws InterruptedException, IOException {
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(new HashSet<String>()).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), isParent);
    executor.submit(task);
    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    if (isParent) {
      verify(topicManager, timeout(TIMEOUT))
          .createTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName), 1, 1, true, false, Optional.of(0));
      verify(mockKafkaConsumer, timeout(TIMEOUT))
          .subscribe(any(), anyInt(), any());
    } else {
      verify(topicManager, never())
          .createTopic(anyString(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
      verify(mockKafkaConsumer, never())
          .subscribe(any(), anyInt(), any());
    }
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }


  @Test (timeOut = TIMEOUT)
  public void testRun() throws InterruptedException, IOException {
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);

    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2),
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
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception"))
        .doNothing()
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            TestUtils.getOffsetRecord(1)));

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
  public void testConsumeFailedStats() throws IOException, InterruptedException {
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    doThrow(new VeniceException("Mock store creation exception"))
        .doNothing()
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);
    AdminConsumptionStats mockStats = mock(AdminConsumptionStats.class);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, mockStats, 10000);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(task.getFailingOffset(), 1L);
    });

    verify(mockStats, timeout(100).atLeastOnce()).setAdminConsumptionFailedOffset(1);
    verify(mockStats, timeout(100).atLeastOnce()).recordPendingAdminMessagesCount(2D);
    verify(mockStats, timeout(100).atLeastOnce()).recordStoresWithPendingAdminMessagesCount(1D);
    verify(mockStats, timeout(100).atLeastOnce()).recordAdminConsumptionCycleDurationMs(anyDouble());

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
    Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(-1L));
  }

  @Test (timeOut = TIMEOUT)
  public void testSkipMessageCommand() throws IOException, InterruptedException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception"))
        .when(admin)
        .addStore(clusterName, storeName, owner, keySchema, valueSchema);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
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
    TopicManager topicManager = new TopicManager(kafka.getZkAddress(), TestUtils.getVeniceConsumerFactory(kafka.getAddress()));
    String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    topicManager.createTopic(adminTopic, 1, 1, true);
    VeniceControllerWrapper controller = ServiceFactory.getVeniceController(clusterName, kafka);
    String storeName = "test-store";

    VeniceWriter<byte[], byte[]> writer = TestUtils.getVeniceTestWriterFactory(kafka.getAddress()).getBasicVeniceWriter(adminTopic);

    byte[] message = getStoreCreationMessage(clusterName, storeName, owner, "invalid_key_schema", valueSchema, 1); // This name is special
    long badOffset =
        writer.put(new byte[0], message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get().offset();

    byte[] goodMessage = getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2);
    writer.put(new byte[0], goodMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    Thread.sleep(5000); // Non deterministic, but whatever.  This should never fail.
    Assert.assertFalse(controller.getVeniceAdmin().hasStore(clusterName, storeName));

    new ControllerClient(clusterName, controller.getControllerUrl()).skipAdminMessage(Long.toString(badOffset), false);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT * 3, TimeUnit.MILLISECONDS, () -> {
      Assert.assertTrue(controller.getVeniceAdmin().hasStore(clusterName, storeName));
    });
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithSameOffset() throws Exception {
    RecordMetadata killJobMetadata = (RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2),
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
    partitionState.databaseInfo = new HashMap<>();

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
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get();

    Queue<Pair<TopicPartition, OffsetRecord>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(killJobMetadata));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            TestUtils.getOffsetRecord(2)));

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
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offsetToSkip = ((RecordMetadata) veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION).get())
        .offset();
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName3, owner, keySchema, valueSchema, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(offsetToSkip - 1);
    Set<Pair<TopicPartition, OffsetRecord>> set = new HashSet<>();
    set.add(new Pair(
        new TopicPartition(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
        offsetRecord
    ));
    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(false), set);

    // The stores don't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    doReturn(false).when(admin).hasStore(clusterName, storeName3);

    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy,false, stats, 10000);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            TestUtils.getOffsetRecord(1)));
    Assert.assertEquals(task.getFailingOffset(), 3L);
    task.skipMessageWithOffset(3L);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            TestUtils.getOffsetRecord(3)));
    task.close();
    executor.shutdownNow();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
    verify(stats, atLeastOnce()).recordAdminTopicDIVErrorReportCount();
    verify(admin, atLeastOnce()).isMasterController(clusterName);
    verify(mockKafkaConsumer, times(1)).subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, times(1)).unSubscribe(any(), anyInt());
    verify(admin, times(1)).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    verify(admin, never()).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
    verify(admin, never()).addStore(clusterName, storeName3, owner, keySchema, valueSchema);
    Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), 1L);
  }

  @Test (timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithDifferentOffset() throws InterruptedException, IOException {
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
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
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // This scenario mostly happens when master controller fails over
    veniceWriter = getVeniceWriter(inMemoryKafkaBroker);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
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
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 1),
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

      Assert.assertEquals(task.getLastSucceededExecutionId(), executionId,
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
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    String newOwner = owner+"updated";
    int partitionNumber = 100;
    int currentVersion = 100;
    boolean enableReads = false;
    boolean enableWrites = true;
    boolean accessControlled = true;
    boolean storeMigration = true;
    boolean writeComputationEnabled = true;
    boolean computationEnabled = true;
    int bootstrapToOnlineTimeoutInHours = 48;
    UpdateStore setStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    setStore.clusterName = clusterName;
    setStore.storeName = storeName;
    setStore.owner = newOwner;
    setStore.partitionNum = partitionNumber;
    setStore.currentVersion = currentVersion;
    setStore.enableReads = enableReads;
    setStore.enableWrites = enableWrites;
    setStore.accessControlled = accessControlled;
    setStore.incrementalPushEnabled = true;
    setStore.isMigrating = storeMigration;
    setStore.writeComputationEnabled = writeComputationEnabled;
    setStore.readComputationEnabled = computationEnabled;
    setStore.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours;

    HybridStoreConfigRecord hybridConfig = new HybridStoreConfigRecord();
    hybridConfig.rewindTimeInSeconds = 123L;
    hybridConfig.offsetLagThresholdToGoOnline = 1000L;
    setStore.hybridStoreConfig = hybridConfig;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = setStore;
    adminMessage.executionId = 2;
    byte[] message = adminOperationSerializer.serialize(adminMessage);


    veniceWriter.put(emptyKeyBytes, message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), TestUtils.getOffsetRecord(2));
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).updateStore(eq(clusterName), eq(storeName), any(), any(), any(),
        any(), any(), any(), any(), any(), eq(Optional.of(123L)), eq(Optional.of(1000L)),
        eq(Optional.of(accessControlled)), any(), any(), any(), any(), any(), any(), eq(Optional.of(true)),
        eq(Optional.of(storeMigration)), eq(Optional.of(writeComputationEnabled)), eq(Optional.of(computationEnabled)),
        eq(Optional.of(bootstrapToOnlineTimeoutInHours)), any(), any());

  }

  @Test (timeOut = TIMEOUT)
  public void testStoreIsolation() throws Exception {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeTopicName1 = storeName1 + "_v1";
    String storeTopicName2 = storeName2 + "_v1";
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName1, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName2, 4),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // The store doesn't exist
    when(admin.hasStore(clusterName, storeName1)).thenReturn(false);
    when(admin.hasStore(clusterName, storeName2)).thenReturn(false);

    doThrow(new VeniceException("Mock store creation exception"))
        .when(admin).addStore(clusterName, storeName1, owner, keySchema, valueSchema);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), 1L));
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName2, -1L).longValue(), 4L));
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName2, -1L).longValue(), 4L);
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName1, -1L).longValue(), -1L);
    Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
        TestUtils.getOffsetRecord(-1L));
    Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), -1L);

    // skip the blocking message
    task.skipMessageWithOffset(1);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            TestUtils.getOffsetRecord(4)));

    Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), 4L);
    Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName1, -1L).longValue(), 3L);
    Assert.assertEquals(task.getFailingOffset(), -1L);
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce())
        .isMasterController(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .subscribe(any(), anyInt(), any());
    verify(mockKafkaConsumer, timeout(TIMEOUT))
        .unSubscribe(any(), anyInt());

    verify(admin, atLeastOnce()).addStore(clusterName, storeName1, owner, keySchema, valueSchema);
    verify(admin, times(1)).addStore(clusterName, storeName2, owner, keySchema, valueSchema);
  }

  @Test
  public void testResubscribe() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    // Let the admin consumption task process some admin messages
    veniceWriter.put(emptyKeyBytes, getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 2L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), 2L));
    // Mimic a transfer of mastership
    doReturn(false).when(admin).isMasterController(clusterName);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getLastSucceededExecutionId(), -1L));
    // Mimic the behavior where another controller has processed some admin messages
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 3L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Future<RecordMetadata> future = veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 4L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).offset();
    OffsetRecord offsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ProducerPartitionState partitionState = offsetRecord.getProducerPartitionState(veniceWriter.getProducerGUID());
    partitionState.messageSequenceNumber = partitionState.messageSequenceNumber + 2;
    offsetRecord.setProducerPartitionState(veniceWriter.getProducerGUID(), partitionState);
    offsetRecord.setOffset(offset);
    offsetManager.put(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offsetRecord);
    executionIdAccessor.updateLastSucceededExecutionId(clusterName, 4L);
    executionIdAccessor.updateLastSucceededExecutionIdMap(clusterName, storeName, 4L);
    // Resubscribe to the admin topic and make sure it can still process new admin messages
    doReturn(true).when(admin).isMasterController(clusterName);
    // Let the admin task to stay idle for at least one cycle
    Utils.sleep(2000);
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 5L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), 5L));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSkipDIV() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    // Let the admin consumption task process some admin messages
    veniceWriter.put(emptyKeyBytes, getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 2L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), 2L));
    // Manipulate the consumption task to force a DIV error
    doReturn(false).when(admin).isMasterController(clusterName);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getLastSucceededExecutionId(), -1L));
    OffsetRecord offsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    ProducerPartitionState partitionState = offsetRecord.getProducerPartitionState(veniceWriter.getProducerGUID());
    int sequenceNumber = partitionState.messageSequenceNumber;
    partitionState.messageSequenceNumber = sequenceNumber - 1;
    offsetRecord.setProducerPartitionState(veniceWriter.getProducerGUID(), partitionState);
    offsetManager.put(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offsetRecord);
    doReturn(true).when(admin).isMasterController(clusterName);
    // New admin messages should fail with DIV error
    Future<RecordMetadata> future = veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 3L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).offset();
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), offset));
    // Skip the DIV check, make sure the sequence number is updated and new admin messages can also be processed
    task.skipMessageDIVWithOffset(offset);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID)
            .getProducerPartitionState(veniceWriter.getProducerGUID()).messageSequenceNumber, sequenceNumber + 1));
    veniceWriter.put(emptyKeyBytes, getKillOfflinePushJobMessage(clusterName, storeTopicName, 4L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionId(clusterName).longValue(), 4L));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testRetriableConsumptionException()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, stats, 10000);
    executor.submit(task);
    String mockPushJobId = "mock push job id";
    int versionNumber = 1;
    int numberOfPartitions = 1;
    doThrow(new VeniceOperationAgainstKafkaTimedOut("Mocking kafka topic creation timeout"))
        .when(admin)
        .addVersionAndStartIngestion(clusterName, storeName, mockPushJobId, versionNumber, numberOfPartitions);
    Future<RecordMetadata> future = veniceWriter.put(emptyKeyBytes,
        getAddVersionMessage(clusterName, storeName, mockPushJobId, versionNumber, numberOfPartitions, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).offset();
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), offset));
    // The add version message is expected to fail with retriable VeniceOperationAgainstKafkaTimeOut exception and the
    // corresponding code path for handling retriable exceptions should have been executed.
    verify(stats, atLeastOnce()).recordFailedRetriableAdminConsumption();
    verify(stats, never()).recordFailedAdminConsumption();
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  private byte[] getStoreCreationMessage(String clusterName, String storeName, String owner, String keySchema, String valueSchema, long executionId) {
    StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
    storeCreation.clusterName = clusterName;
    storeCreation.storeName = storeName;
    storeCreation.owner = owner;
    storeCreation.keySchema = new SchemaMeta();
    storeCreation.keySchema.definition = keySchema;
    storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.valueSchema = new SchemaMeta();
    storeCreation.valueSchema.definition = valueSchema;
    storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.STORE_CREATION.getValue();
    adminMessage.payloadUnion = storeCreation;
    adminMessage.executionId = executionId;
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

  private byte[] getAddVersionMessage(String clusterName, String storeName, String pushJobId, int versionNum,
      int numberOfPartitions, long executionId) {
    AddVersion addVersion = (AddVersion) AdminMessageType.ADD_VERSION.getNewInstance();
    addVersion.clusterName = clusterName;
    addVersion.storeName = storeName;
    addVersion.pushJobId = pushJobId;
    addVersion.versionNum = versionNum;
    addVersion.numberOfPartitions = numberOfPartitions;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.ADD_VERSION.getValue();
    adminMessage.payloadUnion = addVersion;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage);
  }

  /**
   * Used for test only. As the CTOR of VeniceWriter is protected, so the only way to create an instance is extend it
   * and create an instance of this sub-class.
   */
  private static class TestVeniceWriter<K,V> extends VeniceWriter{

    protected TestVeniceWriter(VeniceProperties props, String topicName, VeniceKafkaSerializer keySerializer,
        VeniceKafkaSerializer valueSerializer, VenicePartitioner partitioner, Time time, Supplier supplier) {
      super(props, topicName, keySerializer, valueSerializer, partitioner, time, supplier);
    }
  }
}