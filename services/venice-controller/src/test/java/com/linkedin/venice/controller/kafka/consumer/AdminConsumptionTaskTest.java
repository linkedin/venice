package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_CONTROLLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_READS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_WRITES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETLED_PROXY_USER_ACCOUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_CLASS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_COMPUTATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TIME_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_COMPUTATION_ENABLED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.admin.InMemoryAdminTopicMetadataAccessor;
import com.linkedin.venice.admin.InMemoryExecutionIdAccessor;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ETLStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PartitionerConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.InMemoryOffsetManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.consumer.poll.AbstractPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.ArbitraryOrderingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.CompositePollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.FilteringPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.PubSubTopicPartitionOffset;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
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
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Slow test class, given fast priority
 */
@Test(priority = -5)
public class AdminConsumptionTaskTest {
  private static final int TIMEOUT = 10000;

  private String clusterName;
  private String topicName;
  private static final byte[] emptyKeyBytes = new byte[] { 'a' };
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  private static final String storeName = Utils.getUniqueString("test_store");
  private static final String storeTopicName = storeName + "_v1";
  private static final String owner = "test_owner";
  private static final String keySchema = "\"string\"";
  private static final String valueSchema = "\"string\"";

  // Objects will be used by each test method
  private PubSubConsumerAdapter mockKafkaConsumer;
  private VeniceHelixAdmin admin;
  private OffsetManager offsetManager;
  private AdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private ExecutorService executor;
  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;
  private ExecutionIdAccessor executionIdAccessor;

  @BeforeMethod
  public void methodSetup() {
    clusterName = Utils.getUniqueString("test-cluster");
    topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    executor = Executors.newCachedThreadPool();
    inMemoryKafkaBroker = new InMemoryKafkaBroker("local");
    inMemoryKafkaBroker.createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC);
    veniceWriter = getVeniceWriter(inMemoryKafkaBroker);
    executionIdAccessor = new InMemoryExecutionIdAccessor();

    mockKafkaConsumer = mock(PubSubConsumerAdapter.class);

    admin = mock(VeniceHelixAdmin.class);
    // By default, current controller is the leader controller
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);
    doReturn(true).when(admin).isAdminTopicConsumptionEnabled(clusterName);

    offsetManager = new InMemoryOffsetManager();
    adminTopicMetadataAccessor = new InMemoryAdminTopicMetadataAccessor();

    TopicManager topicManager = mock(TopicManager.class);
    // By default, topic has already been created
    doReturn(new HashSet<>(Arrays.asList(pubSubTopic))).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();
    doReturn(true).when(topicManager).containsTopicAndAllPartitionsAreOnline(pubSubTopic);
  }

  @AfterMethod
  public void cleanUp() throws InterruptedException {
    TestUtils.shutdownExecutor(executor);
    veniceWriter.close();
  }

  private VeniceWriter getVeniceWriter(InMemoryKafkaBroker inMemoryKafkaBroker) {
    Properties props = new Properties();
    props.put(VeniceWriter.CHECK_SUM_TYPE, CheckSumType.NONE.name());
    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(topicName).setKeySerializer(new DefaultSerializer())
            .setValueSerializer(new DefaultSerializer())
            .setWriteComputeSerializer(new DefaultSerializer())
            .setPartitioner(new SimplePartitioner())
            .setTime(SystemTime.INSTANCE)
            .build();
    return new VeniceWriter(
        veniceWriterOptions,
        new VeniceProperties(props),
        new MockInMemoryProducerAdapter(inMemoryKafkaBroker));
  }

  private AdminConsumptionTask getAdminConsumptionTask(PollStrategy pollStrategy, boolean isParent) {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    return getAdminConsumptionTask(pollStrategy, isParent, stats, 10000, false, null, 3);
  }

  private AdminConsumptionTask getAdminConsumptionTask(
      PollStrategy pollStrategy,
      boolean isParent,
      AdminConsumptionStats stats,
      long adminConsumptionCycleTimeoutMs) {
    return getAdminConsumptionTask(pollStrategy, isParent, stats, adminConsumptionCycleTimeoutMs, false, null, 3);
  }

  private AdminConsumptionTask getAdminConsumptionTask(
      PollStrategy pollStrategy,
      boolean isParent,
      AdminConsumptionStats stats,
      long adminConsumptionCycleTimeoutMs,
      boolean remoteConsumptionEnabled,
      String remoteKafkaServerUrl,
      int maxWorkerThreadPoolSize) {
    MockInMemoryConsumer inMemoryKafkaConsumer =
        new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

    return new AdminConsumptionTask(
        clusterName,
        inMemoryKafkaConsumer,
        remoteConsumptionEnabled,
        Optional.ofNullable(remoteKafkaServerUrl),
        admin,
        adminTopicMetadataAccessor,
        executionIdAccessor,
        isParent,
        stats,
        1,
        Optional.empty(),
        adminConsumptionCycleTimeoutMs,
        maxWorkerThreadPoolSize,
        pubSubTopicRepository,
        "dc-0");
  }

  private PubSubTopicPartitionOffset getTopicPartitionOffsetPair(PubSubProduceResult produceResult) {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(produceResult.getTopic()),
        produceResult.getPartition());
    return new PubSubTopicPartitionOffset(pubSubTopicPartition, produceResult.getOffset());
  }

  private long getLastOffset(String clusterName) {
    return AdminTopicMetadataAccessor.getOffsets(adminTopicMetadataAccessor.getMetadata(clusterName)).getFirst();
  }

  private long getLastExecutionId(String clusterName) {
    return AdminTopicMetadataAccessor.getExecutionId(adminTopicMetadataAccessor.getMetadata(clusterName));
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWhenNotLeaderController() throws IOException, InterruptedException {
    // Update admin to be a follower controller
    doReturn(false).when(admin).isLeaderControllerFor(clusterName);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, never()).subscribe(any(), anyLong());
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  /**
   * In a parent controller, when the topic doesn't exist, it should be automatically created.
   */
  @Test(timeOut = TIMEOUT)
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
  @Test(timeOut = TIMEOUT)
  public void testRunWhenTopicDoesNotExistInNonParent() throws InterruptedException, IOException {
    testRunWhenTopicDoesNotExist(false);
  }

  private void testRunWhenTopicDoesNotExist(boolean isParent) throws InterruptedException, IOException {
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(new HashSet<String>()).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), isParent);
    executor.submit(task);
    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
    if (isParent) {
      verify(topicManager, timeout(TIMEOUT)).createTopic(pubSubTopic, 1, 1, true, false, Optional.empty());
      verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    } else {
      verify(topicManager, never()).createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any());
      verify(mockKafkaConsumer, never()).subscribe(any(), anyLong());
    }
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(timeOut = TIMEOUT)
  public void testRun() throws InterruptedException, IOException {
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);

    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 2L);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());
    verify(admin, timeout(TIMEOUT)).createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
    verify(admin, timeout(TIMEOUT)).killOfflinePush(clusterName, storeTopicName, false);
  }

  @Test(timeOut = TIMEOUT)
  public void testRecordConsumptionLag() throws InterruptedException, IOException {
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(admin).getTopicManager("remote.pubsub");
    doReturn(new HashMap<>()).when(mockKafkaConsumer).poll(anyLong());

    AdminConsumptionTask task =
        Mockito.spy(getAdminConsumptionTask(new RandomPollStrategy(), true, stats, 0, true, "remote.pubsub", 3));
    doReturn(-1L).when(task).getConsumptionLagUpdateIntervalInMs();
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> verify(task, times(1)).recordConsumptionLag());

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWhenStoreCreationGotExceptionForTheFirstTime() throws InterruptedException, IOException {
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception")).doNothing()
        .when(admin)
        .createStore(clusterName, storeName, owner, keySchema, valueSchema, false);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 1L));

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());
    verify(admin, timeout(TIMEOUT).times(2)).createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
  }

  @Test(timeOut = TIMEOUT)
  public void testDelegateExceptionSetsFailingOffset() throws ExecutionException, InterruptedException, IOException {
    long failingOffset =
        ((PubSubProduceResult) veniceWriter
            .put(
                emptyKeyBytes,
                getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
                AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get()).getOffset();
    AdminConsumptionStats mockStats = mock(AdminConsumptionStats.class);
    doThrow(StringIndexOutOfBoundsException.class).when(mockStats).recordAdminMessageDelegateLatency(anyDouble());
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, mockStats, 10000);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(task.getFailingOffset(), failingOffset);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(timeOut = TIMEOUT)
  public void testConsumeFailedStats() throws IOException, InterruptedException {
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    doThrow(new VeniceException("Mock store creation exception")).doNothing()
        .when(admin)
        .createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
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
    Assert.assertEquals(getLastOffset(clusterName), -1L);
  }

  @Test(timeOut = TIMEOUT)
  public void testSkipMessageCommand() throws IOException, InterruptedException {
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName);
    doThrow(new VeniceException("Mock store creation exception")).when(admin)
        .createStore(clusterName, storeName, owner, keySchema, valueSchema, false);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      try {
        task.skipMessageWithOffset(1L); // won't accept skip command until task has failed on this offset.
      } catch (VeniceException e) {
        return false;
      }
      return true;
    });

    // admin throws errors, so record offset means we skipped the message
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 1L);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithSameOffset() throws Exception {
    PubSubProduceResult killJobMetadata =
        (PubSubProduceResult) veniceWriter
            .put(
                emptyKeyBytes,
                getKillOfflinePushJobMessage(clusterName, storeTopicName, 1),
                AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get();
    veniceWriter
        .put(
            emptyKeyBytes,
            getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2),
            AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
        .get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());
    Queue<PubSubTopicPartitionOffset> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(killJobMetadata));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));
    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 2L);
    });

    Utils.sleep(1000); // TODO: find a better to wait for AdminConsumptionTask consume the last message.
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());
    verify(admin, timeout(TIMEOUT)).createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
  }

  private OffsetRecord getOffsetRecordByOffsetAndSeqNum(long offset, int seqNum) {
    PartitionState partitionState = new PartitionState();
    partitionState.endOfPush = false;
    partitionState.offset = offset;
    partitionState.lastUpdate = System.currentTimeMillis();
    partitionState.producerStates = new HashMap<>();
    partitionState.databaseInfo = new HashMap<>();
    partitionState.upstreamOffsetMap = new VeniceConcurrentHashMap<>();
    ProducerPartitionState ppState = new ProducerPartitionState();
    ppState.segmentNumber = 0;
    ppState.segmentStatus = SegmentStatus.IN_PROGRESS.getValue();
    ppState.messageSequenceNumber = seqNum;
    ppState.messageTimestamp = System.currentTimeMillis();
    ppState.checksumType = CheckSumType.NONE.getValue();
    ppState.checksumState = ByteBuffer.allocate(0);
    ppState.aggregates = new HashMap<>();
    ppState.debugInfo = new HashMap<>();

    partitionState.producerStates.put(GuidUtils.getCharSequenceFromGuid(veniceWriter.getProducerGUID()), ppState);

    return new OffsetRecord(partitionState, AvroProtocolDefinition.PARTITION_STATE.getSerializer());
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWhenRestart() throws Exception {
    // Mock previous-persisted offset
    int firstAdminMessageOffset = 1;
    int firstAdminMessageSeqNum = 1;
    OffsetRecord offsetRecord = getOffsetRecordByOffsetAndSeqNum(firstAdminMessageOffset, firstAdminMessageSeqNum);
    offsetManager.put(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offsetRecord);

    PubSubProduceResult killJobMetadata =
        (PubSubProduceResult) veniceWriter
            .put(
                emptyKeyBytes,
                getKillOfflinePushJobMessage(clusterName, storeTopicName, 1),
                AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get();
    veniceWriter
        .put(
            emptyKeyBytes,
            getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2),
            AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
        .get();

    Queue<PubSubTopicPartitionOffset> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(killJobMetadata));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    doReturn(false).when(admin).hasStore(clusterName, storeName);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 2L));

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());
    verify(admin, timeout(TIMEOUT)).createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
    // Kill message is before persisted offset
    verify(admin, never()).killOfflinePush(clusterName, storeTopicName, false);
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWithMissingMessages() throws Exception {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeName3 = "test_store3";
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offsetToSkip =
        ((PubSubProduceResult) veniceWriter
            .put(
                emptyKeyBytes,
                getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
                AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get()).getOffset();
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName3, owner, keySchema, valueSchema, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    Set<PubSubTopicPartitionOffset> set = new HashSet<>();
    set.add(
        new PubSubTopicPartitionOffset(
            new PubSubTopicPartitionImpl(
                pubSubTopicRepository.getTopic(topicName),
                AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            offsetToSkip - 1));
    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(false), set);

    // The stores don't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    doReturn(false).when(admin).hasStore(clusterName, storeName3);

    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);

    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false, stats, 10000);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 1L));
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), 3L));
    task.skipMessageWithOffset(3L);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 3L));
    task.close();
    executor.shutdownNow();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
    verify(stats, atLeastOnce()).recordAdminTopicDIVErrorReportCount();
    verify(admin, atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, times(1)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, times(1)).unSubscribe(any());
    verify(admin, times(1)).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);
    verify(admin, never()).createStore(clusterName, storeName2, owner, keySchema, valueSchema, false);
    verify(admin, never()).createStore(clusterName, storeName3, owner, keySchema, valueSchema, false);
    Assert.assertEquals(getLastExecutionId(clusterName), 1L);
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWithFalsePositiveMissingMessages() throws Exception {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeName3 = "test_store3";
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // Write a message with a skipped execution id but valid producer metadata.
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName3, owner, keySchema, valueSchema, 4),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The stores don't exist yet.
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    doReturn(false).when(admin).hasStore(clusterName, storeName3);
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, stats, TIMEOUT);
    executor.submit(task);
    // The missing data based on execution id should be ignored and everything should be processed accordingly.
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 3L));
    Assert.assertEquals(task.getFailingOffset(), -1);
    task.close();
    verify(stats, never()).recordAdminTopicDIVErrorReportCount();
    verify(admin, atLeastOnce()).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);
    verify(admin, atLeastOnce()).createStore(clusterName, storeName2, owner, keySchema, valueSchema, false);
    verify(admin, atLeastOnce()).createStore(clusterName, storeName3, owner, keySchema, valueSchema, false);
    Assert.assertEquals(getLastExecutionId(clusterName), 4L);
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWithFalsePositiveMissingMessagesWhenFirstBecomeLeaderController() throws Exception {
    String storeName0 = "test_store0";
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeName3 = "test_store3";

    VeniceWriter oldVeniceWriter = getVeniceWriter(inMemoryKafkaBroker);
    Future<PubSubProduceResult> metadataForStoreName0Future = oldVeniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName0, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    adminTopicMetadataAccessor.updateMetadata(
        clusterName,
        AdminTopicMetadataAccessor.generateMetadataMap(metadataForStoreName0Future.get().getOffset(), -1, 1));

    // Write a message with a skipped execution id but a different producer metadata.
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // Write a message with a skipped execution id but valid producer metadata.
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 4),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName3, owner, keySchema, valueSchema, 5),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The stores don't exist yet.
    doReturn(false).when(admin).hasStore(clusterName, storeName0);
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    doReturn(false).when(admin).hasStore(clusterName, storeName3);
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, stats, TIMEOUT);
    executor.submit(task);
    // The missing data based on execution id should be ignored and everything should be processed accordingly.
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 5L));
    Assert.assertEquals(task.getFailingOffset(), -1);
    task.close();
    verify(stats, never()).recordAdminTopicDIVErrorReportCount();
    verify(admin, atLeastOnce()).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);
    verify(admin, never()).createStore(clusterName, storeName0, owner, keySchema, valueSchema, false);
    verify(admin, atLeastOnce()).createStore(clusterName, storeName2, owner, keySchema, valueSchema, false);
    verify(admin, atLeastOnce()).createStore(clusterName, storeName3, owner, keySchema, valueSchema, false);
    Assert.assertEquals(getLastExecutionId(clusterName), 5L);
  }

  @Test(timeOut = TIMEOUT)
  public void testRunWithDuplicateMessagesWithDifferentOffset() throws InterruptedException, IOException {
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist for the first time, and exist for the second time
    when(admin.hasStore(clusterName, storeName)).thenReturn(false).thenReturn(true);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 2L);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());
    verify(admin, timeout(TIMEOUT)).createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
  }

  @Test(timeOut = 2 * TIMEOUT)
  public void testRunWithBiggerStartingOffset() throws InterruptedException, IOException {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // This scenario mostly happens when leader controller fails over
    veniceWriter = getVeniceWriter(inMemoryKafkaBroker);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // The store doesn't exist
    doReturn(false).when(admin).hasStore(clusterName, storeName1);
    doReturn(false).when(admin).hasStore(clusterName, storeName2);
    Map<String, Long> newMetadata = AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1);
    adminTopicMetadataAccessor.updateMetadata(clusterName, newMetadata);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 3L);
    });
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());

    verify(admin, never()).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);
    verify(admin, atLeastOnce()).createStore(clusterName, storeName2, owner, keySchema, valueSchema, false);
  }

  @Test(timeOut = TIMEOUT)
  public void testParentControllerSkipKillOfflinePushJobMessage() throws InterruptedException, IOException {
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 1L);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());
    verify(admin, never()).killOfflinePush(clusterName, storeTopicName, false);
  }

  @Test
  public void testGetLastSucceedExecutionId() throws Exception {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);
    for (long i = 1; i <= 3; i++) {
      veniceWriter.put(
          emptyKeyBytes,
          getKillOfflinePushJobMessage(clusterName, storeTopicName, i),
          AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      final long executionId = i;
      TestUtils.waitForNonDeterministicCompletion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Map<String, Long> metaData = adminTopicMetadataAccessor.getMetadata(clusterName);
        return AdminTopicMetadataAccessor.getOffsets(metaData).getFirst() == executionId
            && AdminTopicMetadataAccessor.getExecutionId(metaData) == executionId;
      });

      Assert.assertEquals(
          (long) task.getLastSucceededExecutionId(),
          executionId,
          "After consumption succeed, the last succeed execution id should be updated.");
    }

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSetStore(boolean replicateAllConfigs) throws Exception {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    String newOwner = owner + "updated";
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
    hybridConfig.producerTimestampLagThresholdToGoOnlineInSeconds = 300L;
    hybridConfig.dataReplicationPolicy = DataReplicationPolicy.AGGREGATE.getValue();
    setStore.hybridStoreConfig = hybridConfig;

    ETLStoreConfigRecord etlStoreConfig = new ETLStoreConfigRecord();
    etlStoreConfig.etledUserProxyAccount = "";
    setStore.ETLStoreConfig = etlStoreConfig;
    PartitionerConfigRecord partitionerConfig = new PartitionerConfigRecord();
    partitionerConfig.amplificationFactor = 10;
    partitionerConfig.partitionerParams = new HashMap<>();
    partitionerConfig.partitionerClass = "dummyClassName";
    setStore.partitionerConfig = partitionerConfig;

    if (replicateAllConfigs) {
      setStore.replicateAllConfigs = true;
      setStore.updatedConfigsList = Collections.emptyList();
    } else {
      setStore.replicateAllConfigs = false;
      setStore.updatedConfigsList = new LinkedList<>();
      setStore.updatedConfigsList.add(OWNER);
      setStore.updatedConfigsList.add(PARTITION_COUNT);
      setStore.updatedConfigsList.add(VERSION);
      setStore.updatedConfigsList.add(ENABLE_READS);
      setStore.updatedConfigsList.add(ENABLE_WRITES);
      setStore.updatedConfigsList.add(ACCESS_CONTROLLED);
      setStore.updatedConfigsList.add(INCREMENTAL_PUSH_ENABLED);
      setStore.updatedConfigsList.add(STORE_MIGRATION);
      setStore.updatedConfigsList.add(WRITE_COMPUTATION_ENABLED);
      setStore.updatedConfigsList.add(READ_COMPUTATION_ENABLED);
      setStore.updatedConfigsList.add(BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);
      setStore.updatedConfigsList.add(REWIND_TIME_IN_SECONDS);
      setStore.updatedConfigsList.add(OFFSET_LAG_TO_GO_ONLINE);
      setStore.updatedConfigsList.add(TIME_LAG_TO_GO_ONLINE);
      setStore.updatedConfigsList.add(ETLED_PROXY_USER_ACCOUNT);
      setStore.updatedConfigsList.add(AMPLIFICATION_FACTOR);
      setStore.updatedConfigsList.add(PARTITIONER_CLASS);
      setStore.updatedConfigsList.add(PARTITIONER_PARAMS);
    }

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = setStore;
    adminMessage.executionId = 2;
    byte[] message = adminOperationSerializer.serialize(adminMessage);

    veniceWriter.put(emptyKeyBytes, message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 2L);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    // Not checking if contents of Optional are present or not since if they're not present, exception will be raised
    // and it would fail the test.
    verify(admin, timeout(TIMEOUT).atLeastOnce()).updateStore(
        eq(clusterName),
        eq(storeName),
        argThat(
            updateStoreQueryParams -> updateStoreQueryParams.getHybridRewindSeconds().get() == 123L
                && updateStoreQueryParams.getHybridOffsetLagThreshold().get() == 1000L
                && updateStoreQueryParams.getAccessControlled().get() == accessControlled
                && updateStoreQueryParams.getStoreMigration().get() == storeMigration
                && updateStoreQueryParams.getWriteComputationEnabled().get() == writeComputationEnabled
                && updateStoreQueryParams.getReadComputationEnabled().get() == computationEnabled
                && updateStoreQueryParams.getBootstrapToOnlineTimeoutInHours().get() == bootstrapToOnlineTimeoutInHours
                && updateStoreQueryParams.getIncrementalPushEnabled().get() // Incremental push must be enabled.
        ));
  }

  @Test(timeOut = TIMEOUT)
  public void testStoreIsolation() throws Exception {
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeTopicName1 = storeName1 + "_v1";
    String storeTopicName2 = storeName2 + "_v1";
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName1, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName2, 4),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // The store doesn't exist
    when(admin.hasStore(clusterName, storeName1)).thenReturn(false);
    when(admin.hasStore(clusterName, storeName2)).thenReturn(false);

    doThrow(new VeniceException("Mock store creation exception")).when(admin)
        .createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);

    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), 1L));
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(
            executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName2, -1L).longValue(),
            4L));
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName2, -1L).longValue(),
        4L);
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName1, -1L).longValue(),
        -1L);
    Assert.assertEquals(getLastOffset(clusterName), -1L);
    Assert.assertEquals(getLastExecutionId(clusterName), -1L);

    // skip the blocking message
    task.skipMessageWithOffset(1);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 4L));

    Assert.assertEquals(getLastExecutionId(clusterName), 4L);
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName1, -1L).longValue(),
        3L);
    Assert.assertEquals(task.getFailingOffset(), -1L);
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());

    verify(admin, atLeastOnce()).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);
    verify(admin, times(1)).createStore(clusterName, storeName2, owner, keySchema, valueSchema, false);
  }

  @Test
  public void testResubscribe() throws IOException, InterruptedException, TimeoutException, ExecutionException {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    // Let the admin consumption task process some admin messages
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 2L));
    // Mimic a transfer of leadership
    doReturn(false).when(admin).isLeaderControllerFor(clusterName);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals((long) task.getLastSucceededExecutionId(), -1));
    // Mimic the behavior where another controller has processed some admin messages
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 3L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Future<PubSubProduceResult> future = veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 4L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).getOffset();
    Map<String, Long> newMetadata = AdminTopicMetadataAccessor.generateMetadataMap(offset, -1, 4L);
    adminTopicMetadataAccessor.updateMetadata(clusterName, newMetadata);
    executionIdAccessor.updateLastSucceededExecutionIdMap(clusterName, storeName, 4L);
    // Resubscribe to the admin topic and make sure it can still process new admin messages
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);
    // Let the admin task to stay idle for at least one cycle
    Utils.sleep(2000);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 5L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 5L));
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(timeOut = 2 * TIMEOUT)
  public void testResubscribeWithFailedAdminMessages() throws ExecutionException, InterruptedException, IOException {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    String mockPushJobId = "test";
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long failingOffset =
        ((PubSubProduceResult) veniceWriter
            .put(
                emptyKeyBytes,
                getAddVersionMessage(clusterName, storeName, mockPushJobId, 1, 1, 2L),
                AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get()).getOffset();
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 3L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    doThrow(new VeniceException("Mocked add version exception")).when(admin)
        .addVersionAndStartIngestion(
            clusterName,
            storeName,
            mockPushJobId,
            1,
            1,
            Version.PushType.BATCH,
            null,
            -1,
            1,
            false,
            0);
    // isLeaderController() is called once every consumption cycle (1000ms) and for every message processed in
    // AdminExecutionTask.
    // Provide a sufficient number of true -> false -> true to mimic a transfer of leaderShip and resubscribed behavior
    // while stuck on a failing message.
    when(admin.isLeaderControllerFor(clusterName)).thenReturn(true, true, true, true, true, false, false, false, true);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), failingOffset));
    // Failing offset should be set to -1 once the consumption task unsubscribed.
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), -1));
    // The consumption task will eventually resubscribe and should be stuck on the same admin message.
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), failingOffset));
    task.close();
    executor.shutdown();
    ;
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testMigrateFromOffsetManagerToMetadataAccessor()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    // Populate the topic with message and mimic the behavior where some message were processed when offset manager was
    // in use.
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Future<PubSubProduceResult> future = veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 3L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    final long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).getOffset();
    OffsetRecord offsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    offsetRecord.setCheckpointLocalVersionTopicOffset(offset);
    offsetManager.put(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, offsetRecord);
    executionIdAccessor.updateLastSucceededExecutionIdMap(clusterName, storeName, 3L);
    executionIdAccessor.updateLastSucceededExecutionId(clusterName, 3L);
    executor.submit(task);
    // Verify the move to the new scheme with metadata accessor
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), offset));
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 3L));
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 4L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    future = veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 5L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    final long latestOffset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).getOffset();
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 5L));
    Assert.assertEquals(executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).get(storeName).longValue(), 5L);
    Assert.assertEquals(getLastOffset(clusterName), latestOffset);
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test(timeOut = TIMEOUT)
  public void testSkipDIV() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    // Let the admin consumption task process some admin messages
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 2L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 3L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // New admin messages should fail with DIV error
    Future<PubSubProduceResult> future = veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 4L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).getOffset();
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName, 5L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    // We need to actually create a gap in producer metadata too in order to craft a valid DIV exception.
    Set<PubSubTopicPartitionOffset> set = new HashSet<>();
    set.add(
        new PubSubTopicPartitionOffset(
            new PubSubTopicPartitionImpl(
                pubSubTopicRepository.getTopic(topicName),
                AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID),
            2L));
    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(false), set);
    AdminConsumptionTask task = getAdminConsumptionTask(pollStrategy, false);
    executor.submit(task);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 2L));
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), offset));
    // Skip the DIV check, make sure the sequence number is updated and new admin messages can also be processed
    task.skipMessageDIVWithOffset(offset);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 5L));
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
    doThrow(new PubSubOpTimeoutException("Mocking kafka topic creation timeout")).when(admin)
        .addVersionAndStartIngestion(
            clusterName,
            storeName,
            mockPushJobId,
            versionNumber,
            numberOfPartitions,
            Version.PushType.BATCH,
            null,
            -1,
            1,
            false,
            0);
    Future<PubSubProduceResult> future = veniceWriter.put(
        emptyKeyBytes,
        getAddVersionMessage(clusterName, storeName, mockPushJobId, versionNumber, numberOfPartitions, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long offset = future.get(TIMEOUT, TimeUnit.MILLISECONDS).getOffset();
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(task.getFailingOffset(), offset));
    // The add version message is expected to fail with retriable VeniceOperationAgainstKafkaTimeOut exception and the
    // corresponding code path for handling retriable exceptions should have been executed.
    verify(stats, atLeastOnce()).recordFailedRetriableAdminConsumption();
    verify(stats, never()).recordFailedAdminConsumption();
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testWithMessageRewind() throws IOException, InterruptedException {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, stats, 10000);
    executor.submit(task);
    String topicName = Version.composeKafkaTopic(storeName, 1);
    byte[][] messages = new byte[10][];
    int i = 0;
    for (int id = 1; id < 11; id++) {
      messages[i] = getKillOfflinePushJobMessage(clusterName, topicName, id);
      veniceWriter.put(emptyKeyBytes, messages[i], AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      i++;
    }
    // Mimic rewind.
    for (i = 4; i < 10; i++) {
      veniceWriter.put(emptyKeyBytes, messages[i], AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    }
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastExecutionId(clusterName), 10L));
    // Duplicate messages from the rewind should be skipped.
    verify(admin, times(10)).killOfflinePush(clusterName, topicName, false);
    verify(stats, never()).recordFailedAdminConsumption();
    verify(stats, never()).recordAdminTopicDIVErrorReportCount();
    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testAddingDerivedSchema() throws Exception {
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), true);
    executor.submit(task);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    DerivedSchemaCreation derivedSchemaCreation =
        (DerivedSchemaCreation) AdminMessageType.DERIVED_SCHEMA_CREATION.getNewInstance();
    derivedSchemaCreation.clusterName = clusterName;
    derivedSchemaCreation.storeName = storeName;
    SchemaMeta schemaMeta = new SchemaMeta();
    schemaMeta.definition = valueSchema;
    schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    derivedSchemaCreation.schema = schemaMeta;
    derivedSchemaCreation.valueSchemaId = 1;
    derivedSchemaCreation.derivedSchemaId = 2;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.DERIVED_SCHEMA_CREATION.getValue();
    adminMessage.payloadUnion = derivedSchemaCreation;
    adminMessage.executionId = 2;
    byte[] message = adminOperationSerializer.serialize(adminMessage);

    veniceWriter.put(emptyKeyBytes, message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(getLastOffset(clusterName), 2L);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).addDerivedSchema(
        eq(clusterName),
        eq(storeName),
        eq(derivedSchemaCreation.valueSchemaId),
        eq(derivedSchemaCreation.derivedSchemaId),
        eq(schemaMeta.definition.toString()));
  }

  @Test
  public void testAddVersionMsgHandlingForTargetedRegionPush() throws Exception {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false, stats, 10000);
    executor.submit(task);
    String mockPushJobId = "mock push job id";
    int versionNumber = 1;
    int numberOfPartitions = 1;

    // Sending 3 messages.
    // dc-0 is the default region for the testing suite so the message for "dc-1" and "dc-2" should be ignored.
    veniceWriter.put(
        emptyKeyBytes,
        getAddVersionMessage(clusterName, storeName, mockPushJobId, versionNumber, numberOfPartitions, 1L, "dc-1"),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    veniceWriter.put(
        emptyKeyBytes,
        getAddVersionMessage(clusterName, storeName, mockPushJobId, versionNumber, numberOfPartitions, 2L, "dc-2"),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    veniceWriter.put(
        emptyKeyBytes,
        getAddVersionMessage(clusterName, storeName, mockPushJobId, versionNumber, numberOfPartitions, 3L, "dc-0"),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      verify(admin, times(1)).addVersionAndStartIngestion(
          clusterName,
          storeName,
          mockPushJobId,
          versionNumber,
          numberOfPartitions,
          Version.PushType.BATCH,
          null,
          -1,
          1,
          false,
          0);
    });

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  private byte[] getStoreCreationMessage(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      long executionId) {
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

  private byte[] getAddVersionMessage(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNum,
      int numberOfPartitions,
      long executionId) {
    return getAddVersionMessage(clusterName, storeName, pushJobId, versionNum, numberOfPartitions, executionId, null);
  }

  private byte[] getAddVersionMessage(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNum,
      int numberOfPartitions,
      long executionId,
      String targetedRegions) {
    AddVersion addVersion = (AddVersion) AdminMessageType.ADD_VERSION.getNewInstance();
    addVersion.clusterName = clusterName;
    addVersion.storeName = storeName;
    addVersion.pushJobId = pushJobId;
    addVersion.versionNum = versionNum;
    addVersion.numberOfPartitions = numberOfPartitions;
    addVersion.rewindTimeInSecondsOverride = -1;
    addVersion.timestampMetadataVersionId = 1;
    if (targetedRegions != null) {
      addVersion.targetedRegions = new ArrayList<>(RegionUtils.parseRegionsFilterList(targetedRegions));
    }

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.ADD_VERSION.getValue();
    adminMessage.payloadUnion = addVersion;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Admin topic remote consumption is enabled but no config found for the source Kafka bootstrap server url")
  public void testRemoteConsumptionEnabledButRemoteBootstrapUrlsAreMissing() {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    getAdminConsumptionTask(new RandomPollStrategy(), true, stats, 0, true, null, 3);
  }

  @Test
  public void testRemoteConsumptionEnabledAndRemoteBootstrapUrlsAreGiven() {
    AdminConsumptionStats stats = mock(AdminConsumptionStats.class);
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(admin).getTopicManager("remote.pubsub");
    AdminConsumptionTask task = getAdminConsumptionTask(null, true, stats, 0, true, "remote.pubsub", 3);
    Assert.assertEquals(task.getSourceKafkaClusterTopicManager(), topicManager);
  }

  @Test(timeOut = TIMEOUT)
  public void testLongRunningBadTask() throws Exception {
    // This test will fail when the AdminConsumptionTask maxWorkerThreadPoolSize is 1
    String storeName1 = "test_store1";
    String storeName2 = "test_store2";
    String storeTopicName1 = storeName1 + "_v1";
    String storeTopicName2 = storeName2 + "_v1";
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName1, owner, keySchema, valueSchema, 1),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName2, owner, keySchema, valueSchema, 2),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName1, 3),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    veniceWriter.put(
        emptyKeyBytes,
        getKillOfflinePushJobMessage(clusterName, storeTopicName2, 4),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // The store doesn't exist
    when(admin.hasStore(clusterName, storeName1)).thenReturn(false);
    when(admin.hasStore(clusterName, storeName2)).thenReturn(false);

    // Delay by more than the cycle time. This will cause this thread to be interrupted.
    // The task will be retried but will not succeed
    doAnswer(AdditionalAnswers.answersWithDelay(2000, invocation -> {
      return null;
    })).when(admin).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);

    AdminConsumptionTask task = getAdminConsumptionTask(
        new RandomPollStrategy(),
        false,
        mock(AdminConsumptionStats.class),
        1000,
        false,
        null,
        3);

    executor.submit(task);

    // Make sure that the "good" store tasks make progress while the "bad" store task is stuck
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(
            executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName2, -1L).longValue(),
            4L));

    Assert.assertEquals(getLastOffset(clusterName), -1L);
    Assert.assertEquals(getLastExecutionId(clusterName), -1L);
    Assert.assertEquals(task.getFailingOffset(), 1L);
    Assert.assertEquals(task.getLastSucceededExecutionId().longValue(), -1L);
    Assert.assertNull(task.getLastSucceededExecutionId(storeName1));
    Assert.assertEquals(task.getLastSucceededExecutionId(storeName2).longValue(), 4L);

    // Once we skip the failing message , the store should recover

    task.skipMessageWithOffset(1);
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> Assert.assertEquals(getLastOffset(clusterName), 4L));

    Assert.assertEquals(getLastExecutionId(clusterName), 4L);
    Assert.assertEquals(
        executionIdAccessor.getLastSucceededExecutionIdMap(clusterName).getOrDefault(storeName1, -1L).longValue(),
        3L);
    Assert.assertEquals(task.getFailingOffset(), -1L);

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);

    verify(admin, timeout(TIMEOUT).atLeastOnce()).isLeaderControllerFor(clusterName);
    verify(mockKafkaConsumer, timeout(TIMEOUT)).subscribe(any(), anyLong());
    verify(mockKafkaConsumer, timeout(TIMEOUT)).unSubscribe(any());

    verify(admin, atLeastOnce()).createStore(clusterName, storeName1, owner, keySchema, valueSchema, false);
    verify(admin, times(1)).createStore(clusterName, storeName2, owner, keySchema, valueSchema, false);

  }

  @Test(timeOut = TIMEOUT)
  public void testSystemStoreMessageOrder() throws InterruptedException, IOException {
    doThrow(new VeniceException("Prevent store creation")).when(admin)
        .createStore(clusterName, storeName, owner, keySchema, valueSchema, false);
    AdminConsumptionTask task = getAdminConsumptionTask(new RandomPollStrategy(), false);
    executor.submit(task);
    String sysStorePushId = "empty_push";
    int sysStoreVersion = 1;
    int sysStorePartition = 1;
    veniceWriter.put(
        emptyKeyBytes,
        getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 1L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    String systemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
    veniceWriter.put(
        emptyKeyBytes,
        getAddVersionMessage(clusterName, systemStoreName, sysStorePushId, sysStoreVersion, sysStorePartition, 2L),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // Give the consumption task at least two cycles to retry the failing user store creation message. The corresponding
    // system store message should remain blocked/unprocessed.
    TestUtils.waitForNonDeterministicAssertion(
        TIMEOUT,
        TimeUnit.MILLISECONDS,
        () -> verify(admin, times(2)).createStore(clusterName, storeName, owner, keySchema, valueSchema, false));

    verify(admin, never()).addVersionAndStartIngestion(
        clusterName,
        systemStoreName,
        sysStorePushId,
        sysStoreVersion,
        sysStorePartition,
        Version.PushType.BATCH,
        null,
        -1,
        1,
        false,
        0);

    task.close();
    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }
}
