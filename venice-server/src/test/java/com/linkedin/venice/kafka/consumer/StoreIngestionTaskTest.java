package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.PartitionPushStatusNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.DeepCopyStorageMetadataService;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.consumer.poll.AbstractPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.ArbitraryOrderingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.BlockingObserverPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.CompositePollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.DuplicatingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.FilteringPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducer;
import com.linkedin.venice.unit.kafka.producer.TransformingProducer;
import com.linkedin.venice.unit.matchers.ExceptionClassAndCauseClassMatcher;
import com.linkedin.venice.unit.matchers.ExceptionClassMatcher;
import com.linkedin.venice.unit.matchers.LongEqualOrGreaterThanMatcher;
import com.linkedin.venice.unit.matchers.NonEmptyStringMatcher;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 *
 * Be ware that most of the test cases in this suite depend on {@link StoreIngestionTaskTest#TEST_TIMEOUT}
 * Adjust it based on environment if timeout failure occurs.
 */
@Test(singleThreaded = true)
public class StoreIngestionTaskTest {

  private static final Logger logger = Logger.getLogger(StoreIngestionTaskTest.class);

  private static final long TEST_TIMEOUT;
  private static final int RUN_TEST_FUNCTION_TIMEOUT = 10;
  private static final long READ_CYCLE_DELAY_MS = 5;
  private static final long EMPTY_POLL_SLEEP_MS = 0;

  static {
    StoreIngestionTask.SCHEMA_POLLING_DELAY_MS = 100;
    IngestionNotificationDispatcher.PROGRESS_REPORT_INTERVAL = -1; // Report all the time.
    // Report progress/throttling for every message
    StoreIngestionTask.OFFSET_REPORTING_INTERVAL = 1;
    TEST_TIMEOUT = 500 * READ_CYCLE_DELAY_MS;
  }

  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;
  private StorageEngineRepository mockStorageEngineRepository;
  private VeniceNotifier mockNotifier, mockPartitionStatusNotifier;
  private List<Object[]> mockNotifierProgress;
  private List<Object[]> mockNotifierEOPReveived;
  private List<Object[]> mockNotifierCompleted;
  private List<Object[]> mockNotifierError;
  private StorageMetadataService mockStorageMetadataService;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private EventThrottler mockBandwidthThrottler;
  private EventThrottler mockRecordsThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private ReadOnlyStoreRepository mockMetadataRepo;
  /** N.B.: This mock can be used to verify() calls, but not to return arbitrary things. */
  private KafkaConsumerWrapper mockKafkaConsumer;
  private TopicManager mockTopicManager;
  private AggStoreIngestionStats mockStoreIngestionStats;
  private AggVersionedDIVStats mockVersionedDIVStats;
  private AggVersionedStorageIngestionStats mockVersionedStorageIngestionStats;
  private StoreIngestionTask storeIngestionTaskUnderTest;
  private ExecutorService taskPollingService;
  private StoreBufferService storeBufferService;
  private BooleanSupplier isCurrentVersion;
  private Optional<HybridStoreConfig> hybridStoreConfig;
  private long databaseSyncBytesIntervalForTransactionalMode = 1;
  private long databaseSyncBytesIntervalForDeferredWriteMode = 2;

  private static final String storeNameWithoutVersionInfo = "TestTopic";
  private static final String topic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);

  private static final int PARTITION_COUNT = 10;
  private static final Set<Integer> ALL_PARTITIONS = new HashSet<>();
  static { for (int partition = 0; partition < PARTITION_COUNT; partition++) { ALL_PARTITIONS.add(partition); } }
  private static final int PARTITION_FOO = 1;
  private static final int PARTITION_BAR = 2;
  private static final int SCHEMA_ID = -1;
  private static final int EXISTING_SCHEMA_ID = 1;
  private static final int NON_EXISTING_SCHEMA_ID = 2;

  private static final byte[] putKeyFoo = getRandomKey(PARTITION_FOO);
  private static final byte[] putKeyFoo2 = getRandomKey(PARTITION_FOO);
  private static final byte[] putKeyBar = getRandomKey(PARTITION_BAR);
  private static final byte[] putValue = "TestValuePut".getBytes(StandardCharsets.UTF_8);
  private static final byte[] putValueToCorrupt = "Please corrupt me!".getBytes(StandardCharsets.UTF_8);
  private static final byte[] deleteKeyFoo = getRandomKey(PARTITION_FOO);

  private static byte[] getRandomKey(Integer partition) {
    String randomString = getUniqueString("KeyForPartition" + partition);
    return ByteBuffer.allocate(randomString.length() + 1)
        .put(partition.byteValue())
        .put(randomString.getBytes())
        .array();
  }

  @BeforeClass(alwaysRun = true)
  public void suiteSetUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);
    storeBufferService = new StoreBufferService(3, 10000, 1000);
    storeBufferService.start();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    taskPollingService.shutdownNow();
    storeBufferService.stop();
  }

  @BeforeMethod(alwaysRun = true)
  public void methodSetUp() throws Exception {
    inMemoryKafkaBroker = new InMemoryKafkaBroker();
    inMemoryKafkaBroker.createTopic(topic, PARTITION_COUNT);
    veniceWriter = getVeniceWriter(() -> new MockInMemoryProducer(inMemoryKafkaBroker));
    mockStorageEngineRepository = mock(StorageEngineRepository.class);

    mockNotifier = mock(LogNotifier.class);
    mockNotifierProgress = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierProgress.add(args);
      return null;
    }).when(mockNotifier).progress(anyString(), anyInt(), anyLong());
    mockNotifierEOPReveived = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierEOPReveived.add(args);
      return null;
    }).when(mockNotifier).endOfPushReceived(anyString(), anyInt(), anyLong());
    mockNotifierCompleted = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierCompleted.add(args);
      return null;
    }).when(mockNotifier).completed(anyString(), anyInt(), anyLong());
    mockNotifierError = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierError.add(args);
      return null;
    }).when(mockNotifier).error(anyString(), anyInt(), anyString(), any());

    mockPartitionStatusNotifier = mock(PartitionPushStatusNotifier.class);

    mockAbstractStorageEngine = mock(AbstractStorageEngine.class);
    mockStorageMetadataService = mock(StorageMetadataService.class);

    mockBandwidthThrottler = mock(EventThrottler.class);
    mockRecordsThrottler = mock(EventThrottler.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockKafkaConsumer = mock(KafkaConsumerWrapper.class);
    mockTopicManager = mock(TopicManager.class);
    mockStoreIngestionStats = mock(AggStoreIngestionStats.class);
    mockVersionedDIVStats = mock(AggVersionedDIVStats.class);
    mockVersionedStorageIngestionStats = mock(AggVersionedStorageIngestionStats.class);
    isCurrentVersion = () -> false;
    hybridStoreConfig = Optional.<HybridStoreConfig>empty();
  }

  private VeniceWriter getVeniceWriter(Supplier<KafkaProducerWrapper> producerSupplier) {
    return new TestVeniceWriter(new VeniceProperties(new Properties()), topic, new DefaultSerializer(),
        new DefaultSerializer(), new DefaultSerializer(), new SimplePartitioner(), SystemTime.INSTANCE, producerSupplier);
  }

  private VeniceWriter getCorruptedVeniceWriter(byte[] valueToCorrupt) {
    return getVeniceWriter(() -> new CorruptedKafkaProducer(new MockInMemoryProducer(inMemoryKafkaBroker), valueToCorrupt));
  }

  class CorruptedKafkaProducer extends TransformingProducer {
    public CorruptedKafkaProducer(KafkaProducerWrapper baseProducer, byte[] valueToCorrupt) {
      super(baseProducer, (topicName, key, value, partition) -> {
        KafkaMessageEnvelope transformedMessageEnvelope = value;

        if (MessageType.valueOf(transformedMessageEnvelope) == MessageType.PUT) {
          Put put = (Put) transformedMessageEnvelope.payloadUnion;
          if (put.putValue.array() == valueToCorrupt) {
            put.putValue = ByteBuffer.wrap("CORRUPT_VALUE".getBytes());
            transformedMessageEnvelope.payloadUnion = put;
          }
        }

        return new TransformingProducer.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
      });
    }
  }

  private long getOffset(Future<RecordMetadata> recordMetadataFuture)
      throws ExecutionException, InterruptedException {
    return recordMetadataFuture.get().offset();
  }

  private void runTest(Set<Integer> partitions, Runnable assertions, boolean isLeaderFollowerModelEnabled) throws Exception {
    runTest(partitions, () -> {}, assertions, isLeaderFollowerModelEnabled);
  }

  private void runHybridTest(Set<Integer> partitions, Runnable assertions, boolean isLeaderFollowerModelEnabled) throws Exception {
    runTest(new RandomPollStrategy(), partitions, () -> {}, assertions,
        Optional.of(new HybridStoreConfig(100,100)),
        Optional.empty(), isLeaderFollowerModelEnabled);
  }

  private void runTest(Set<Integer> partitions,
                       Runnable beforeStartingConsumption,
                       Runnable assertions,
                       boolean isLeaderFollowerModelEnabled) throws Exception {
    runTest(new RandomPollStrategy(), partitions, beforeStartingConsumption, assertions, isLeaderFollowerModelEnabled);
  }

  private void runTest(PollStrategy pollStrategy, Set<Integer> partitions, Runnable beforeStartingConsumption,
                       Runnable assertions, boolean isLeaderFollowerModelEnabled) throws Exception {
    runTest(pollStrategy, partitions, beforeStartingConsumption, assertions, this.hybridStoreConfig, Optional.empty(), isLeaderFollowerModelEnabled);
  }

  private void runTest(PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      Optional<DiskUsage> diskUsageForTest,
      boolean isLeaderFollowerModelEnabled) throws Exception{
    runTest(pollStrategy, partitions, beforeStartingConsumption, assertions, hybridStoreConfig, false, diskUsageForTest, isLeaderFollowerModelEnabled);
  }

  private void runTest(PollStrategy pollStrategy,
                       Set<Integer> partitions,
                       Runnable beforeStartingConsumption,
                       Runnable assertions,
                       Optional<HybridStoreConfig> hybridStoreConfig,
                       boolean incrementalPushEnabled,
                       Optional<DiskUsage> diskUsageForTest,
                       boolean isLeaderFollowerModelEnabled) throws Exception {
    MockInMemoryConsumer inMemoryKafkaConsumer = new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);
    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryKafkaBroker.getKafkaBootstrapServer());
    KafkaClientFactory mockFactory = mock(KafkaClientFactory.class);
    doReturn(inMemoryKafkaConsumer).when(mockFactory).getConsumer(any());
    VeniceWriterFactory mockWriterFactory = mock(VeniceWriterFactory.class);
    doReturn(null).when(mockWriterFactory).createBasicVeniceWriter(any());
    StorageMetadataService offsetManager;
    logger.info("mockStorageMetadataService: " + mockStorageMetadataService.getClass().getName());
    if (mockStorageMetadataService.getClass() != InMemoryStorageMetadataService.class) {
      for (int partition : partitions) {
        doReturn(new OffsetRecord()).when(mockStorageMetadataService).getLastOffset(topic, partition);
        if(hybridStoreConfig.isPresent()){
          doReturn(Optional.of(new StoreVersionState())).when(mockStorageMetadataService).getStoreVersionState(topic);
        }else {
          doReturn(Optional.empty()).when(mockStorageMetadataService).getStoreVersionState(topic);
        }
      }
    }
    offsetManager = new DeepCopyStorageMetadataService(mockStorageMetadataService);
    Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>(Arrays.asList(mockNotifier,
        new LogNotifier(), mockPartitionStatusNotifier));
    DiskUsage diskUsage;
    if (diskUsageForTest.isPresent()){
      diskUsage = diskUsageForTest.get();
    } else {
      diskUsage = mock(DiskUsage.class);
      doReturn(false).when(diskUsage).isDiskFull(anyLong());
    }
    VeniceStoreConfig storeConfig = mock(VeniceStoreConfig.class);
    doReturn(topic).when(storeConfig).getStoreName();
    doReturn(0).when(storeConfig).getTopicOffsetCheckIntervalMs();
    doReturn(READ_CYCLE_DELAY_MS).when(storeConfig).getKafkaReadCycleDelayMs();
    doReturn(EMPTY_POLL_SLEEP_MS).when(storeConfig).getKafkaEmptyPollSleepMs();
    doReturn(databaseSyncBytesIntervalForTransactionalMode).when(storeConfig).getDatabaseSyncBytesIntervalForTransactionalMode();
    doReturn(databaseSyncBytesIntervalForDeferredWriteMode).when(storeConfig).getDatabaseSyncBytesIntervalForDeferredWriteMode();
    doReturn(false).when(storeConfig).isReadOnlyForBatchOnlyStoreEnabled();


    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(500l).when(serverConfig).getServerPromotionToLeaderReplicaDelayMs();
    doReturn(inMemoryKafkaBroker.getKafkaBootstrapServer()).when(serverConfig).getKafkaBootstrapServers();
    doReturn(false).when(serverConfig).isHybridQuotaEnabled();
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();

    EventThrottler mockUnorderedBandwidthThrottler = mock(EventThrottler.class);
    EventThrottler mockUnorderedRecordsThrottler = mock(EventThrottler.class);
    StoreIngestionTaskFactory ingestionTaskFactory = StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(mockWriterFactory)
        .setKafkaClientFactory(mockFactory)
        .setStorageEngineRepository(mockStorageEngineRepository)
        .setStorageMetadataService(offsetManager)
        .setNotifiersQueue(notifiers)
        .setBandwidthThrottler(mockBandwidthThrottler)
        .setRecordsThrottler(mockRecordsThrottler)
        .setUnorderedBandwidthThrottler(mockUnorderedBandwidthThrottler)
        .setUnorderedRecordsThrottler(mockUnorderedRecordsThrottler)
        .setSchemaRepository(mockSchemaRepo)
        .setMetadataRepository(mockMetadataRepo)
        .setTopicManager(mockTopicManager)
        .setStoreIngestionStats(mockStoreIngestionStats)
        .setVersionedDIVStats(mockVersionedDIVStats)
        .setVersionedStorageIngestionStats(mockVersionedStorageIngestionStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(serverConfig)
        .setDiskUsage(diskUsage)
        .build();
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(isLeaderFollowerModelEnabled, kafkaProps,
        isCurrentVersion, hybridStoreConfig, incrementalPushEnabled, storeConfig, true,
        false, "", PARTITION_FOO);
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository).getLocalStorageEngine(topic);

    Future testSubscribeTaskFuture = null;
    try {
      for (int partition: partitions) {
        storeIngestionTaskUnderTest.subscribePartition(topic, partition);
      }

      beforeStartingConsumption.run();

      // MockKafkaConsumer is prepared. Schedule for polling.
      testSubscribeTaskFuture = taskPollingService.submit(storeIngestionTaskUnderTest);

      assertions.run();

    } finally {
      storeIngestionTaskUnderTest.close();
      if (testSubscribeTaskFuture != null) {
        testSubscribeTaskFuture.get(RUN_TEST_FUNCTION_TIMEOUT, TimeUnit.SECONDS);
      }
    }
  }

  private Pair<TopicPartition, Long> getTopicPartitionOffsetPair(RecordMetadata recordMetadata) {
    return new Pair<>(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), recordMetadata.offset());
  }

  private <STUFF> Set<STUFF> getSet(STUFF... stuffs) {
    Set<STUFF> set = new HashSet<>();
    for (STUFF stuff: stuffs) {
      set.add(stuff);
    }
    return set;
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   * 1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   * 2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#delete.
   * 3. A VeniceMessage with a Kafka offset that was already processed is ignored.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessing(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata deleteMetadata = (RecordMetadata) veniceWriter.delete(deleteKeyFoo, null).get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());

    // We re-deliver the old put out of order, so we can make sure it's ignored.
    Queue<Pair<TopicPartition, Long>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));

    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    runTest(pollStrategy, getSet(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).delete(PARTITION_FOO, deleteKeyFoo);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.offset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testMissingMessagesForTopicWithLogCompactionEnabled(boolean isLeaderFollowerModelEnabled) throws Exception {
    // enable log compaction
    when(mockTopicManager.isTopicCompactionEnabled(topic)).thenReturn(true);

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    RecordMetadata putMetadata1 = (RecordMetadata) veniceWriter.put(putKeyFoo, putValueToCorrupt, SCHEMA_ID).get();
    RecordMetadata putMetadata2 = (RecordMetadata) veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata putMetadata3 = (RecordMetadata) veniceWriter.put(putKeyFoo2, putValueToCorrupt, SCHEMA_ID).get();
    RecordMetadata putMetadata4 = (RecordMetadata) veniceWriter.put(putKeyFoo2, putValue, SCHEMA_ID).get();

    // We will only deliver the unique entries after compaction: putMetadata2 and putMetadata4
    Queue<Pair<TopicPartition, Long>> pollDeliveryOrder = new LinkedList<>();
    /**
     * The reason to put "putMetadata1" and "putMetadata3" in the deliveryOrder queue is that {@link AbstractPollStrategy#poll(InMemoryKafkaBroker, Map, long)}
     * is always trying to return the next message.
     * If it is not expected, we need to change it.
     */
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata1));
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata3));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    runTest(pollStrategy, getSet(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo2, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForLastMessage = getOffsetRecord(putMetadata4.offset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForLastMessage);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithExistingSchemaId(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = getOffsetRecord(fooLastOffset);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).put(topic, PARTITION_FOO, expected);
    }, isLeaderFollowerModelEnabled);
  }

  /**
   * Test the situation where records arrive faster than the schemas.
   * In this case, Venice would keep polling schemaRepo until schemas arrive.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    long existingSchemaOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    when(mockSchemaRepo.hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID))
        .thenReturn(false, false, true);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify that after retrying 3 times, record with 'NON_EXISTING_SCHEMA_ID' was put into BDB.
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT).atLeast(3)).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).
        put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(NON_EXISTING_SCHEMA_ID, putValue).serialize()));

      //Verify that the following record is consumed well.
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
        .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));

      OffsetRecord expected = getOffsetRecord(existingSchemaOffset);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).put(topic, PARTITION_FOO, expected);
    }, isLeaderFollowerModelEnabled);
  }

  /**
   * Test the situation where records' schemas never arrive. In the case, the StoreIngestionTask will keep being blocked.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithNonExistingSchemaId(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);

    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      //StoreIngestionTask#checkValueSchemaAvail will keep polling for 'NON_EXISTING_SCHEMA_ID'. It blocks the
      //#putConsumerRecord and will not enter drainer queue
      verify(mockSchemaRepo, after(TEST_TIMEOUT).never()).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockSchemaRepo, atLeastOnce()).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, never()).put(eq(PARTITION_FOO), any(), any(byte[].class));

      // Only two records(start_of_segment, start_of_push) offset were able to be recorded before 'NON_EXISTING_SCHEMA_ID' blocks #putConsumerRecord
      verify(mockStorageMetadataService, atMost(2)).put(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReportStartWhenRestarting(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    final long STARTING_OFFSET = 2;
    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      doReturn(getOffsetRecord(STARTING_OFFSET)).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt());
    }, () -> {
      // Verify STARTED is NOT reported when offset is 0
      verify(mockNotifier, never()).started(topic, PARTITION_BAR);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testNotifier(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      /**
       * Considering that the {@link VeniceWriter} will send an {@link ControlMessageType#END_OF_PUSH},
       * we need to add 1 to last data message offset.
       */
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset + 1);
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR, barLastOffset  + 1);
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO,
          fooLastOffset + 1);
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR,
          barLastOffset  + 1);

      /**
       * It seems Mockito will mess up the verification if there are two functions with the same name:
       * {@link StorageMetadataService#put(String, StoreVersionState)}
       * {@link StorageMetadataService#put(String, int, OffsetRecord)}
       *
       * So if the first function gets invoked, Mockito will try to match the second function this test wanted,
       * which is specified by the following statements, which will cause 'argument mismatch error'.
       * For now, to bypass this issue, this test will try to wait the async consumption to be done before
       * any {@link StorageMetadataService#put} function verification by verifying {@link VeniceNotifier#completed(String, int, long)}
       * first.
       */
      verify(mockStorageMetadataService)
          .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooLastOffset + 1, true)));
      verify(mockStorageMetadataService)
          .put(eq(topic), eq(PARTITION_BAR), eq(getOffsetRecord(barLastOffset + 1, true)));
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barLastOffset);
      verify(mockPartitionStatusNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockPartitionStatusNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockPartitionStatusNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockPartitionStatusNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barLastOffset);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartition(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(true).when(mockStore).isHybrid();
    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    StoragePartitionConfig storagePartitionConfigFoo = new StoragePartitionConfig(topic, PARTITION_FOO);
    storagePartitionConfigFoo.setWriteOnlyConfig(false);
    StoragePartitionConfig storagePartitionConfigBar = new StoragePartitionConfig(topic, PARTITION_BAR);

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_BAR);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).preparePartitionForReading(PARTITION_FOO);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartitionValidateIngestionSuccess(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(false).when(mockStore).isHybrid();
    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    mockAbstractStorageEngine.addStoragePartition(PARTITION_FOO);
    AbstractStoragePartition mockPartition = mock(AbstractStoragePartition.class);
    doReturn(mockPartition).when(mockAbstractStorageEngine).getPartitionOrThrow(PARTITION_FOO);
    doReturn(true).when(mockPartition).validateBatchIngestion();
    StoragePartitionConfig storagePartitionConfigFoo = new StoragePartitionConfig(topic, PARTITION_FOO);

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_FOO);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartitionWriteOnly(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);

    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(true).when(mockStore).isHybrid();
    mockAbstractStorageEngine.addStoragePartition(PARTITION_FOO);

    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    StoragePartitionConfig storagePartitionConfigFoo = new StoragePartitionConfig(topic, PARTITION_FOO);
    storagePartitionConfigFoo.setWriteOnlyConfig(true);
    StoragePartitionConfig storagePartitionConfigBar = new StoragePartitionConfig(topic, PARTITION_BAR);

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_FOO);
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_BAR);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testResetPartition(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).times(2))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testResetPartitionAfterUnsubscription(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
      doThrow(new UnsubscribedTopicPartitionException(topic, PARTITION_FOO))
          .when(mockKafkaConsumer).resetOffset(topic, PARTITION_FOO);
      // Reset should be able to handle the scenario, when the topic partition has been unsubscribed.
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);
      verify(mockKafkaConsumer, timeout(TEST_TIMEOUT)).unSubscribe(topic, PARTITION_FOO);
      // StoreIngestionTask won't invoke consumer.resetOffset() if it already unsubscribe from that topic/partition
      verify(mockKafkaConsumer, timeout(TEST_TIMEOUT).times(0)).resetOffset(topic, PARTITION_FOO);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).clearOffset(topic, PARTITION_FOO);
    }, isLeaderFollowerModelEnabled);
  }

  /**
   * In this test, partition FOO will complete successfully, but partition BAR will be missing a record.
   *
   * The {@link VeniceNotifier} should see the completion and error reported for the appropriate partitions.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDetectionOfMissingRecord(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToSkip = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(),
        getSet(new Pair(new TopicPartition(topic, PARTITION_BAR), barOffsetToSkip)));

    runTest(pollStrategy, getSet(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(MissingDataException.class)));

      // After we verified that completed() and error() are called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    }, isLeaderFollowerModelEnabled);
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record. The
   * {@link VeniceNotifier} should see the completion for both partitions.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSkippingOfDuplicateRecord(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToDupe = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new DuplicatingPollStrategy(new RandomPollStrategy(),
        getSet(new Pair(new TopicPartition(topic, PARTITION_BAR), barOffsetToDupe)));

    runTest(pollStrategy, getSet(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe);
      verify(mockNotifier, after(TEST_TIMEOUT).never()).endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe + 1);

      // After we verified that completed() is called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testThrottling(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.delete(deleteKeyFoo, null);

    runTest(new RandomPollStrategy(1), getSet(PARTITION_FOO), () -> {}, () -> {
      // START_OF_SEGMENT, START_OF_PUSH, PUT, DELETE
      verify(mockBandwidthThrottler, timeout(TEST_TIMEOUT).times(4)).maybeThrottle(anyDouble());
      verify(mockRecordsThrottler, timeout(TEST_TIMEOUT).times(4)).maybeThrottle(1);
    }, isLeaderFollowerModelEnabled);
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreIngestionTask} to fail fast. The
   * message in {@link #PARTITION_FOO} will receive a bad message type, whereas the message in {@link #PARTITION_BAR}
   * will receive a bad control message type.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBadMessageTypesFailFast(boolean isLeaderFollowerModelEnabled) throws Exception {
    int badMessageTypeId = 99; // Venice got 99 problems, but a bad message type ain't one.

    // Dear future maintainer,
    //
    // I sincerely hope Venice never ends up with 99 distinct message types, but I'll
    // make sure anyway. If you end up breaking this check, holla at me, I'm curious
    // to know how you managed to end up in this mess ;)
    //
    // - @felixgv on the twitters

    try {
      MessageType.valueOf(badMessageTypeId);
      fail("The message type " + badMessageTypeId + " is valid. "
          + "This test needs to be updated in order to send an invalid message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    try {
      ControlMessageType.valueOf(badMessageTypeId);
      fail("The control message type " + badMessageTypeId + " is valid. "
          + "This test needs to be updated in order to send an invalid control message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    veniceWriter = getVeniceWriter(() -> new TransformingProducer(new MockInMemoryProducer(inMemoryKafkaBroker),
        (topicName, key, value, partition) -> {
          KafkaMessageEnvelope transformedMessageEnvelope = value;

          switch (partition) {
            case PARTITION_FOO:
              transformedMessageEnvelope.messageType = badMessageTypeId;
              break;
            case PARTITION_BAR:
              if (MessageType.valueOf(transformedMessageEnvelope) == MessageType.CONTROL_MESSAGE) {
                ControlMessage transformedControlMessage = (ControlMessage) transformedMessageEnvelope.payloadUnion;
                if (ControlMessageType.valueOf(transformedControlMessage) == ControlMessageType.START_OF_SEGMENT) {
                  transformedControlMessage.controlMessageType = badMessageTypeId;
                  transformedMessageEnvelope.payloadUnion = transformedControlMessage;
                }
              }
              break;
          }

          return new TransformingProducer.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
        }));
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);

    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
          eq(topic), eq(PARTITION_FOO), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(VeniceException.class)));

      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(VeniceException.class)));
    }, isLeaderFollowerModelEnabled);
  }

  /**
   * In this test, {@link #PARTITION_BAR} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a good one. We expect the Notifier to not report any errors after the
   * EOP.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCorruptMessagesDoNotFailFastAfterEOP(boolean isLeaderFollowerModelEnabled) throws Exception {
    VeniceWriter veniceWriterForDataDuringPush = getVeniceWriter(() -> new MockInMemoryProducer(inMemoryKafkaBroker));
    VeniceWriter veniceWriterForDataAfterPush = getCorruptedVeniceWriter(putValueToCorrupt);

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long lastOffsetBeforeEOP = getOffset(veniceWriterForDataDuringPush.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForDataDuringPush.close();
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // We simulate the version swap...
    isCurrentVersion = () -> true;

    // After the end of push, we simulate a nearline writer, which somehow pushes corrupt data.
    getOffset(veniceWriterForDataAfterPush.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    long lastOffset = getOffset(veniceWriterForDataAfterPush.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForDataAfterPush.close();

    logger.info("lastOffsetBeforeEOP: " + lastOffsetBeforeEOP + ", lastOffset: " + lastOffset);

    try {
      runTest(getSet(PARTITION_BAR), () -> {

        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args : mockNotifierProgress) {
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) >= lastOffsetBeforeEOP) {
              return true;
            }
          }
          return false;
        });

        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args : mockNotifierCompleted) {
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) > lastOffsetBeforeEOP) {
              return true;
            }
          }
          return false;
        });

        for (Object[] args : mockNotifierError) {
          Assert.assertFalse(
              args[0].equals(topic)
                  && args[1].equals(PARTITION_BAR)
                  && ((String)args[2]).length() > 0
          && args[3] instanceof CorruptDataException);
        }

      }, isLeaderFollowerModelEnabled);
    } catch (VerifyError e) {
      StringBuilder msg = new StringBuilder();
      ClassLoader cl = ClassLoader.getSystemClassLoader();
      URL[] urls = ((URLClassLoader)cl).getURLs();
      msg.append("VerifyError, possibly from junit or mockito version conflict. \nPrinting junit on classpath:\n");
      Arrays.asList(urls).stream().filter(url -> url.getFile().contains("junit")).forEach(url -> msg.append(url + "\n"));
      msg.append("Printing mockito on classpath:\n");
      Arrays.asList(urls).stream().filter(url -> url.getFile().contains("mockito")).forEach(url -> msg.append(url + "\n"));
      throw new VeniceException(msg.toString(), e);
    }
  }

  /**
   * In this test, the {@link #PARTITION_FOO} will receive a well-formed message, while the {@link #PARTITION_BAR} will
   * receive a corrupt message. We expect the Notifier to report as such.
   * <p>
   * N.B.: There was an edge case where this test was flaky. The edge case is now fixed, but the invocationCount of 100
   * should ensure that if this test is ever made flaky again, it will be detected right away. The skipFailedInvocations
   * annotation parameter makes the test skip any invocation after the first failure.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, invocationCount = 100, skipFailedInvocations = true)
  public void testCorruptMessagesFailFast(boolean isLeaderFollowerModelEnabled) throws Exception {
    VeniceWriter veniceWriterForData = getCorruptedVeniceWriter(putValueToCorrupt);

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    getOffset(veniceWriterForData.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    veniceWriterForData.close();
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT))
          .completed(eq(topic), eq(PARTITION_FOO), LongEqualOrGreaterThanMatcher.get(fooLastOffset));

      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(CorruptDataException.class)));

      /**
       * Let's make sure that {@link PARTITION_BAR} never sends a completion notification.
       *
       * This used to be flaky, because the {@link StoreIngestionTask} occasionally sent
       * a completion notification for partitions where it had already detected an error.
       * Now, the {@link StoreIngestionTask} keeps track of the partitions that had error
       * and avoids sending completion notifications for those. The high invocationCount on
       * this test is to detect this edge case.
       */
      verify(mockNotifier, never()).completed(eq(topic), eq(PARTITION_BAR), anyLong());
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSubscribeCompletedPartition(boolean isLeaderFollowerModelEnabled) throws Exception {
    final int offset = 100;
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    runTest(getSet(PARTITION_FOO),
        () -> doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO),
        () -> {
          verify(mockNotifier, timeout(TEST_TIMEOUT)).completed(topic, PARTITION_FOO, offset);
        },
        isLeaderFollowerModelEnabled
    );
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testUnsubscribeConsumption(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT)).started(topic, PARTITION_FOO);
      //Start of push has already been consumed. Stop consumption
      storeIngestionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKillConsumption(boolean isLeaderFollowerModelEnabled) throws Exception {
    final Thread writingThread = new Thread(() -> {
      while (true) {
        veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
        veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
        try {
          TimeUnit.MILLISECONDS.sleep(READ_CYCLE_DELAY_MS);
        } catch (InterruptedException e) {
          break;
        }
      }
    });

    try {
      runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
        veniceWriter.broadcastStartOfPush(new HashMap<>());
        writingThread.start();
      }, () -> {
        verify(mockNotifier, timeout(TEST_TIMEOUT)).started(topic, PARTITION_FOO);
        verify(mockNotifier, timeout(TEST_TIMEOUT)).started(topic, PARTITION_BAR);

        //Start of push has already been consumed. Stop consumption
        storeIngestionTaskUnderTest.kill();
        // task should report an error to notifier that it's killed.
        verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
            eq(topic), eq(PARTITION_FOO), argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassAndCauseClassMatcher(VeniceException.class, InterruptedException.class)));
        verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
            eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassAndCauseClassMatcher(VeniceException.class, InterruptedException.class)));

        waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
            () -> storeIngestionTaskUnderTest.isRunning() == false);
      }, isLeaderFollowerModelEnabled);
    } finally {
      writingThread.interrupt();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKillActionPriority(boolean isLeaderFollowerModelEnabled) throws Exception {
    runTest(getSet(PARTITION_FOO), () -> {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
      // Add a reset consumer action
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);
      // Add a kill consumer action in higher priority than subscribe and reset.
      storeIngestionTaskUnderTest.kill();
    }, () -> {
      // verify subscribe has not been processed. Because consumption task should process kill action at first
      verify(mockStorageMetadataService, after(TEST_TIMEOUT).never()).getLastOffset(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.getConsumer() != null);
      Collection<KafkaConsumerWrapper> consumers = (storeIngestionTaskUnderTest.getConsumer());
      /**
       * Consumers are constructed lazily; if the store ingestion task is killed before it tries to subscribe to any
       * topics, there is no consumer.
       */
      assertEquals(consumers.size(), 0,
          "subscribe should not be processed in this consumer.");

      // Verify offset has not been processed. Because consumption task should process kill action at first.
      // offSetManager.clearOffset should only be invoked one time during clean up after killing this task.
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).clearOffset(topic, PARTITION_FOO);

      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    }, isLeaderFollowerModelEnabled);
  }

  private byte[] getNumberedKey(int number) {
    return ByteBuffer.allocate(putKeyFoo.length + Integer.BYTES).put(putKeyFoo).putInt(number).array();
  }
  private byte[] getNumberedValue(int number) {
    return ByteBuffer.allocate(putValue.length + Integer.BYTES).put(putValue).putInt(number).array();
  }

  @DataProvider(name = "sortedInput_isLeaderFollowerModelEnabled")
  public static Object[][] sortedInputAndStateTransitionModel() {
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @Test(dataProvider = "sortedInput_isLeaderFollowerModelEnabled", invocationCount = 3, skipFailedInvocations = true)
  public void testDataValidationCheckPointing(boolean sortedInput, boolean isLeaderFollowerModelEnabled) throws Exception {
    final Map<Integer, Long> maxOffsetPerPartition = new HashMap<>();
    final Map<Pair<Integer, ByteArray>, ByteArray> pushedRecords = new HashMap<>();
    final int totalNumberOfMessages = 1000;
    final int totalNumberOfConsumptionRestarts = 10;

    veniceWriter.broadcastStartOfPush(sortedInput, new HashMap<>());
    for (int i = 0; i < totalNumberOfMessages; i++) {
      byte[] key = getNumberedKey(i);
      byte[] value = getNumberedValue(i);

      RecordMetadata recordMetadata = (RecordMetadata) veniceWriter.put(key, value, SCHEMA_ID).get();

      maxOffsetPerPartition.put(recordMetadata.partition(), recordMetadata.offset());
      pushedRecords.put(new Pair(recordMetadata.partition(), new ByteArray(key)), new ByteArray(value));
    }
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Basic sanity checks
    assertEquals(pushedRecords.size(), totalNumberOfMessages, "We did not produce as many unique records as we expected!");
    assertFalse(maxOffsetPerPartition.isEmpty(), "There should be at least one partition getting anything published into it!");

    Set<Integer> relevantPartitions = getSet(PARTITION_FOO);

    // This doesn't really need to be atomic, but it does need to be final, and int/Integer cannot be mutated.
    final AtomicInteger messagesConsumedSoFar = new AtomicInteger(0);

    PollStrategy pollStrategy = new BlockingObserverPollStrategy(
        new RandomPollStrategy(false),
        topicPartitionOffsetRecordPair -> {
          if (null == topicPartitionOffsetRecordPair || null == topicPartitionOffsetRecordPair.getSecond()) {
            logger.info("Received null OffsetRecord!");
          } else if (messagesConsumedSoFar.incrementAndGet() % (totalNumberOfMessages / totalNumberOfConsumptionRestarts) == 0) {
            logger.info("Restarting consumer after consuming " + messagesConsumedSoFar.get() + " messages so far.");
            relevantPartitions.stream().forEach(partition ->
                storeIngestionTaskUnderTest.unSubscribePartition(topic, partition));
            relevantPartitions.stream().forEach(partition ->
                storeIngestionTaskUnderTest.subscribePartition(topic, partition));
          } else {
            logger.info("TopicPartition: " + topicPartitionOffsetRecordPair.getFirst() +
                ", Offset: " + topicPartitionOffsetRecordPair.getSecond());
          }
        }
    );

    runTest(pollStrategy, relevantPartitions, () -> {}, () -> {
      // Verify that all partitions reported success.
      maxOffsetPerPartition.entrySet().stream().filter(entry -> relevantPartitions.contains(entry.getKey())).forEach(entry -> {
            int partition = entry.getKey();
            long offset = entry.getValue();
            logger.info("Verifying completed was called for partition " + partition + " and offset " + offset + " or greater.");
            verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(eq(topic), eq(partition), LongEqualOrGreaterThanMatcher.get(offset));
          }
      );

      // After this, all asynchronous processing should be finished, so there's no need for time outs anymore.

      // Verify that no partitions reported errors.
      relevantPartitions.stream().forEach(partition ->
          verify(mockNotifier, never()).error(eq(topic), eq(partition), anyString(), any()));

      // Verify that we really unsubscribed and re-subscribed.
      relevantPartitions.stream().forEach(partition -> {
        verify(mockKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts + 1)).subscribe(eq(topic), eq(partition), anyLong());
        verify(mockKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts)).unSubscribe(topic, partition);

        if (sortedInput) {
          // Check database mode switches from deferred-write to transactional after EOP control message
          StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, partition);
          deferredWritePartitionConfig.setDeferredWrite(true);
          // SOP control message and restart
          verify(mockAbstractStorageEngine, atLeast(1))
              .beginBatchWrite(eq(deferredWritePartitionConfig), any());
          StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, partition);
          // only happen after EOP control message
          verify(mockAbstractStorageEngine, times(1))
              .endBatchWrite(transactionalPartitionConfig);
        }
      });

      // Verify that the storage engine got hit with every record.
      verify(mockAbstractStorageEngine, atLeast(pushedRecords.size())).put(anyInt(), any(), any(ByteBuffer.class));

      // Verify that every record hit the storage engine
      pushedRecords.entrySet().stream().forEach(entry -> {
        int partition = entry.getKey().getFirst();
        byte[] key = entry.getKey().getSecond().get();
        byte[] value = ValueRecord.create(SCHEMA_ID, entry.getValue().get()).serialize();
        verify(mockAbstractStorageEngine, atLeastOnce()).put(partition, key, ByteBuffer.wrap(value));
      });
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKillAfterPartitionIsCompleted(boolean isLeaderFollowerModelEnabled)
      throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockNotifier, after(TEST_TIMEOUT).never()).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);

      storeIngestionTaskUnderTest.kill();
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testNeverReportProgressBeforeStart(boolean isLeaderFollowerModelEnabled)
      throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    // Read one message for each poll.
    runTest(new RandomPollStrategy(1), getSet(PARTITION_FOO), () -> {}, () -> {
      verify(mockNotifier, after(TEST_TIMEOUT).never()).progress(topic, PARTITION_FOO, 0);
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      // The current behavior is only to sync offset/report progress after processing a pre-configured amount
      // of messages in bytes, since control message is being counted as 0 bytes (no data persisted in disk),
      // then no progress will be reported during start, but only for processed messages.
      verify(mockNotifier, after(TEST_TIMEOUT).never()).progress(any(), anyInt(), anyInt());
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testOffsetPersistent(boolean isLeaderFollowerModelEnabled)
      throws Exception {
    // Do not persist every message.
    List<Long> offsets = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      offsets.add(5l);
    }
    databaseSyncBytesIntervalForTransactionalMode = 1000;
    try {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.broadcastEndOfPush(new HashMap<>());
      veniceWriter.broadcastStartOfBufferReplay(offsets,"t", topic, new HashMap<>());
      /**
       * Persist for every control message except START_OF_SEGMENT and END_OF_SEGMENT:
       * START_OF_PUSH, END_OF_PUSH, START_OF_BUFFER_REPLAY
       */
      runHybridTest(
          getSet(PARTITION_FOO),
          () -> {
            verify(mockStorageMetadataService, timeout(TEST_TIMEOUT).times(3)).put(eq(topic), eq(PARTITION_FOO), any());
          },
          isLeaderFollowerModelEnabled);
    }finally {
      databaseSyncBytesIntervalForTransactionalMode = 1;
    }

  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithSortedInput(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(true, new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata deleteMetadata = (RecordMetadata) veniceWriter.delete(deleteKeyFoo, null).get();
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).delete(PARTITION_FOO, deleteKeyFoo);

      // Verify it commits the offset to Offset Manager after receiving EOP control message
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.offset() + 1, true);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
      // Deferred write is not going to commit offset for every message, but will commit offset for every control message
      // The following verification is for START_OF_PUSH control message
      verify(mockStorageMetadataService, times(1))
          .put(topic, PARTITION_FOO, getOffsetRecord(putMetadata.offset() - 1));
      // Check database mode switches from deferred-write to transactional after EOP control message
      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);
      verify(mockAbstractStorageEngine, times(1))
          .beginBatchWrite(eq(deferredWritePartitionConfig), any());
      StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, times(1))
          .endBatchWrite(transactionalPartitionConfig);
    }, isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDelayedTransitionToOnlineInHybridMode(boolean isLeaderFollowerModelEnabled) throws Exception {
    final long MESSAGES_BEFORE_EOP = 100;
    final long MESSAGES_AFTER_EOP = 100;

    // This does not entirely make sense, but we can do better when we have a real integration test which includes buffer replay
    // TODO: We now have a real integration test which includes buffer replay, maybe fix up this test?
    final long TOTAL_MESSAGES_PER_PARTITION = (MESSAGES_BEFORE_EOP + MESSAGES_AFTER_EOP) / ALL_PARTITIONS.size();
    when(mockTopicManager.getLatestOffset(anyString(), anyInt())).thenReturn(TOTAL_MESSAGES_PER_PARTITION);

    mockStorageMetadataService = new InMemoryStorageMetadataService();
    hybridStoreConfig = Optional.of(new HybridStoreConfig(10, 20));
    runTest(ALL_PARTITIONS, () -> {
          veniceWriter.broadcastStartOfPush(new HashMap<>());
          for (int i = 0; i < MESSAGES_BEFORE_EOP; i++) {
            try {
              veniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID).get();
            } catch (InterruptedException | ExecutionException e) {
              throw new VeniceException(e);
            }
          }
          veniceWriter.broadcastEndOfPush(new HashMap<>());

        }, () -> {
          verify(mockNotifier, timeout(TEST_TIMEOUT).atLeast(ALL_PARTITIONS.size())).started(eq(topic), anyInt());
          verify(mockNotifier, never()).completed(anyString(), anyInt(), anyLong());

          if (isLeaderFollowerModelEnabled) {
            veniceWriter.broadcastTopicSwitch(Arrays.asList(inMemoryKafkaBroker.getKafkaBootstrapServer()),
                Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
                System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
                new HashMap<>());
          } else {
            List<Long> offsets = new ArrayList<>();
            for (int i = 0; i < PARTITION_COUNT; i++) {
              offsets.add(5l);
            }
            veniceWriter.broadcastStartOfBufferReplay(offsets, "source K cluster", "source K topic", new HashMap<>());
          }

          for (int i = 0; i < MESSAGES_AFTER_EOP; i++) {
            try {
              veniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID).get();
            } catch (InterruptedException | ExecutionException e) {
              throw new VeniceException(e);
            }
          }

          verify(mockNotifier, timeout(TEST_TIMEOUT).atLeast(ALL_PARTITIONS.size())).completed(anyString(), anyInt(), anyLong());
        },
        isLeaderFollowerModelEnabled
    );
  }

  /**
   * This test writes a message to Kafka then creates a StoreIngestionTask (and StoreBufferDrainer)  It also passes a DiskUsage
   * object to the StoreIngestionTask that always reports disk full.  This means when the StoreBufferDrainer tries to persist
   * the record, it will receive a disk full error.  This test checks for that disk full error on the Notifier object.
   * @throws Exception
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void StoreIngestionTaskRespectsDiskUsage(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    DiskUsage diskFullUsage = mock(DiskUsage.class);
    doReturn(true).when(diskFullUsage).isDiskFull(anyLong());
    doReturn("mock disk full disk usage").when(diskFullUsage).getDiskStatus();
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(new RandomPollStrategy(), getSet(PARTITION_FOO), () -> {
    }, () -> {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {

        // If the partition already got EndOfPushReceived, then all errors will be suppressed and not reported.
        // The speed for a partition to get EOP is non-deterministic, adds the if check here to make this test not flaky.
        if (mockNotifierEOPReveived.isEmpty()) {
          Assert.assertFalse(mockNotifierError.isEmpty(), "Disk Usage should have triggered an ingestion error");
          String errorMessages = mockNotifierError.stream().map(o -> ((Exception) o[3]).getCause().getMessage()) //elements in object array are 0:store name (String), 1: partition (int), 2: message (String), 3: cause (Exception)
              .collect(Collectors.joining());
          Assert.assertTrue(errorMessages.contains("Disk is full"),
              "Expected disk full error, found following error messages: " + errorMessages);
        }
      });
    }, Optional.empty(), Optional.of(diskFullUsage), isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIncrementalPush(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    String version = String.valueOf(System.currentTimeMillis());
    veniceWriter.broadcastStartOfIncrementalPush(version, new HashMap<>());
    long fooNewOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfIncrementalPush(version, new HashMap<>());
    //Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg, StartOfSeg, StartOfIncrementalPush
    //data, EndOfIncrementalPush

    runTest(new RandomPollStrategy(), getSet(PARTITION_FOO), () -> {}, () -> {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        //sync the offset when receiving EndOfPush
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT).atLeastOnce())
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooOffset + 1, true)));
        //sync the offset when receiving StartOfIncrementalPush and EndOfIncrementalPush
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT).atLeastOnce())
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNewOffset - 1, true, version)));
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT).atLeastOnce())
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNewOffset + 1, true, version)));

        verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);

        //since notifier reporting happens before offset update, it actually reports previous offsets
        verify(mockNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
        verify(mockNotifier, atLeastOnce()).endOfIncrementalPushReceived(topic, PARTITION_FOO, fooNewOffset, version);
      });
    }, Optional.empty(), true, Optional.empty(), isLeaderFollowerModelEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReportErrorWithEmptyPcsMap(boolean isLeaderFollowerModelEnabled) throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    doThrow(new VeniceException("fake exception")).when(mockVersionedStorageIngestionStats).resetIngestionTaskErroredGauge(anyString(), anyInt());

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
    }, isLeaderFollowerModelEnabled);
  }

  private static class TestVeniceWriter<K,V> extends VeniceWriter{

    protected TestVeniceWriter(VeniceProperties props, String topicName, VeniceKafkaSerializer keySerializer,
        VeniceKafkaSerializer valueSerializer, VeniceKafkaSerializer updateSerializer, VenicePartitioner partitioner,
        Time time, Supplier supplier) {
      super(props, topicName, keySerializer, valueSerializer, updateSerializer, partitioner, time, supplier);
    }
  }

}
