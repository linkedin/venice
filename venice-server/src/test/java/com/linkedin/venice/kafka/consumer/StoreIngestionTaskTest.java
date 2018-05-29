package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.DeepCopyStorageMetadataService;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.store.AbstractStorageEngine;
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
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
    StoreIngestionTask.POLLING_SCHEMA_DELAY_MS = 100;
    IngestionNotificationDispatcher.PROGRESS_REPORT_INTERVAL = -1; // Report all the time.
    // Report progress/throttling for every message
    StoreIngestionTask.OFFSET_THROTTLE_INTERVAL = 1;
    TEST_TIMEOUT = 500 * READ_CYCLE_DELAY_MS;
  }

  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;
  private StoreRepository mockStoreRepository;
  private VeniceNotifier mockNotifier;
  private List<Object[]> mockNotifierProgress;
  private List<Object[]> mockNotifierCompleted;
  private List<Object[]> mockNotifierError;
  private StorageMetadataService mockStorageMetadataService;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private EventThrottler mockBandWidthThrottler;
  private EventThrottler mockRecordsThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;
  /** N.B.: This mock can be used to verify() calls, but not to return arbitrary things. */
  private KafkaConsumerWrapper mockKafkaConsumer;
  private TopicManager mockTopicManager;
  private AggStoreIngestionStats mockStoreIngestionStats;
  private AggVersionedDIVStats mockVersionedDIVStats;
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

  @BeforeClass
  public void suiteSetUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);
    storeBufferService = new StoreBufferService(3, 10000, 1000);
    storeBufferService.start();
  }

  @AfterClass
  public void tearDown() throws Exception {
    taskPollingService.shutdown();
    storeBufferService.stop();
  }

  @BeforeMethod
  public void methodSetUp() throws Exception {
    inMemoryKafkaBroker = new InMemoryKafkaBroker();
    inMemoryKafkaBroker.createTopic(topic, PARTITION_COUNT);
    veniceWriter = getVeniceWriter(() -> new MockInMemoryProducer(inMemoryKafkaBroker));
    mockStoreRepository = mock(StoreRepository.class);

    mockNotifier = mock(LogNotifier.class);
    mockNotifierProgress = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierProgress.add(args);
      return null;
    }).when(mockNotifier).progress(anyString(), anyInt(), anyLong());
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

    mockAbstractStorageEngine = mock(AbstractStorageEngine.class);
    mockStorageMetadataService = mock(StorageMetadataService.class);

    mockBandWidthThrottler = mock(EventThrottler.class);
    mockRecordsThrottler = mock(EventThrottler.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockKafkaConsumer = mock(KafkaConsumerWrapper.class);
    mockTopicManager = mock(TopicManager.class);
    mockStoreIngestionStats = mock(AggStoreIngestionStats.class);
    mockVersionedDIVStats = mock(AggVersionedDIVStats.class);
    isCurrentVersion = () -> false;
    hybridStoreConfig = Optional.<HybridStoreConfig>empty();
  }

  private VeniceWriter getVeniceWriter(Supplier<KafkaProducerWrapper> producerSupplier) {
    return new TestVeniceWriter(new VeniceProperties(new Properties()), topic, new DefaultSerializer(),
        new DefaultSerializer(), new SimplePartitioner(), SystemTime.INSTANCE, producerSupplier);
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

  private void runTest(Set<Integer> partitions, Runnable assertions) throws Exception {
    runTest(partitions, () -> {}, assertions);
  }

  private void runHybridTest(Set<Integer> partitions, Runnable assertions) throws Exception {
    runTest(new RandomPollStrategy(), partitions, () -> {}, assertions,
        Optional.of(new HybridStoreConfig(100,100)),
        Optional.empty());
  }

  private void runTest(Set<Integer> partitions,
                       Runnable beforeStartingConsumption,
                       Runnable assertions) throws Exception {
    runTest(new RandomPollStrategy(), partitions, beforeStartingConsumption, assertions);
  }

  private void runTest(PollStrategy pollStrategy, Set<Integer> partitions, Runnable beforeStartingConsumption,
                       Runnable assertions) throws Exception {
    runTest(pollStrategy, partitions, beforeStartingConsumption, assertions, this.hybridStoreConfig, Optional.empty());
  }

  private void runTest(PollStrategy pollStrategy,
                       Set<Integer> partitions,
                       Runnable beforeStartingConsumption,
                       Runnable assertions,
                       Optional<HybridStoreConfig> hybridStoreConfig,
                       Optional<DiskUsage> diskUsageForTest) throws Exception {
    MockInMemoryConsumer inMemoryKafkaConsumer = new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);
    Properties kafkaProps = new Properties();
    VeniceConsumerFactory mockFactory = mock(VeniceConsumerFactory.class);
    doReturn(inMemoryKafkaConsumer).when(mockFactory).getConsumer(any());
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
    Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>(Arrays.asList(mockNotifier, new LogNotifier()));
    DiskUsage diskUsage;
    if (diskUsageForTest.isPresent()){
      diskUsage = diskUsageForTest.get();
    } else {
      diskUsage = mock(DiskUsage.class);
      doReturn(false).when(diskUsage).isDiskFull(anyLong());
    }
    storeIngestionTaskUnderTest =
        new StoreIngestionTask(mockFactory, kafkaProps, mockStoreRepository, offsetManager, notifiers,
            mockBandWidthThrottler, mockRecordsThrottler, topic, mockSchemaRepo, mockTopicManager,
            mockStoreIngestionStats, mockVersionedDIVStats, storeBufferService, isCurrentVersion, hybridStoreConfig, 0,
            READ_CYCLE_DELAY_MS, EMPTY_POLL_SLEEP_MS, databaseSyncBytesIntervalForTransactionalMode, databaseSyncBytesIntervalForDeferredWriteMode, diskUsage);
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStoreRepository).getLocalStorageEngine(topic);



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

  private Pair<TopicPartition, OffsetRecord> getTopicPartitionOffsetPair(RecordMetadata recordMetadata) {
    OffsetRecord offsetRecord = getOffsetRecord(recordMetadata.offset());
    return new Pair<>(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), offsetRecord);
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
  @Test
  public void testVeniceMessagesProcessing() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata deleteMetadata = (RecordMetadata) veniceWriter.delete(deleteKeyFoo).get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());

    // We re-deliver the old put out of order, so we can make sure it's ignored.
    Queue<Pair<TopicPartition, OffsetRecord>> pollDeliveryOrder = new LinkedList<>();
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
    });
  }

  @Test
  public void testVeniceMessagesProcessingWithExistingSchemaId() throws Exception {
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
    });
  }

  /**
   * Test the situation where records arrive faster than the schemas.
   * In this case, Venice would keep polling schemaRepo until schemas arrive.
   */
  @Test
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId() throws Exception {
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
    });
  }

  /**
   * Test the situation where records' schemas never arrive. In the case, the StoreIngestionTask will keep being blocked.
   */
  @Test
  public void testVeniceMessagesProcessingWithNonExistingSchemaId() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);

    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      //StoreIngestionTask#checkValueSchemaAvail will keep polling for 'NON_EXISTING_SCHEMA_ID'. It blocks the thread
      //so that the next record would never be put into BDB.
      verify(mockSchemaRepo, after(TEST_TIMEOUT).never()).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockSchemaRepo, atLeastOnce()).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, never()).put(eq(PARTITION_FOO), any(), any(byte[].class));

      // Only two records(start_of_segment, start_of_push) offset were able to be recorded before thread being blocked
      verify(mockStorageMetadataService, atMost(2)).put(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));
    });
  }

  @Test
  public void testReportStartWhenRestarting() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    final long STARTING_OFFSET = 2;
    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      doReturn(getOffsetRecord(STARTING_OFFSET)).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt());
    }, () -> {
      // Verify RESTARTED is reported when offset is larger than 0 is invoked.
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).restarted(topic, PARTITION_FOO, STARTING_OFFSET);
      // Verify STARTED is NOT reported when offset is 0
      verify(mockNotifier, never()).started(topic, PARTITION_BAR);
    });
  }

  @Test
  public void testNotifier() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      /**
       * Considering that the {@link VeniceWriter} will send an {@link ControlMessageType#END_OF_PUSH},
       * we need to add 1 to last data message offset.
       */
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR, barLastOffset);
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
    });
  }

  @Test
  public void testResetPartition() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).times(2))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
    });
  }

  @Test
  public void testResetPartitionAfterUnsubscription() throws Exception {
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
      verify(mockKafkaConsumer, timeout(TEST_TIMEOUT)).resetOffset(topic, PARTITION_FOO);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).clearOffset(topic, PARTITION_FOO);
    });
  }

  /**
   * In this test, partition FOO will complete successfully, but partition BAR will be missing a record.
   *
   * The {@link VeniceNotifier} should see the completion and error reported for the appropriate partitions.
   */
  @Test
  public void testDetectionOfMissingRecord() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToSkip = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(),
        getSet(new Pair(new TopicPartition(topic, PARTITION_BAR), getOffsetRecord(barOffsetToSkip))));

    runTest(pollStrategy, getSet(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(MissingDataException.class)));

      // After we verified that completed() and error() are called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    });
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record. The
   * {@link VeniceNotifier} should see the completion for both partitions.
   *
   * TODO: Add a metric to track duplicate records, and verify that it gets tracked properly.
   */
  @Test
  public void testSkippingOfDuplicateRecord() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToDupe = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    OffsetRecord offsetRecord = getOffsetRecord(barOffsetToDupe);
    PollStrategy pollStrategy = new DuplicatingPollStrategy(new RandomPollStrategy(),
        getSet(new Pair(new TopicPartition(topic, PARTITION_BAR), offsetRecord)));

    runTest(pollStrategy, getSet(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR, barOffsetToDupe);
      verify(mockNotifier, after(TEST_TIMEOUT).never()).completed(topic, PARTITION_BAR, barOffsetToDupe + 1);

      // After we verified that completed() is called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    });
  }

  @Test
  public void testThrottling() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.delete(deleteKeyFoo);

    runTest(new RandomPollStrategy(1), getSet(PARTITION_FOO), () -> {}, () -> {
      verify(mockBandWidthThrottler, timeout(TEST_TIMEOUT).atLeastOnce()).maybeThrottle(putKeyFoo.length + putValue.length);
      verify(mockBandWidthThrottler, timeout(TEST_TIMEOUT).atLeastOnce()).maybeThrottle(deleteKeyFoo.length);
      verify(mockRecordsThrottler, timeout(TEST_TIMEOUT).atLeast(2)).maybeThrottle(1);
    });
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreIngestionTask} to fail fast. The
   * message in {@link #PARTITION_FOO} will receive a bad message type, whereas the message in {@link #PARTITION_BAR}
   * will receive a bad control message type.
   */
  @Test
  public void testBadMessageTypesFailFast() throws Exception {
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
    });
  }

  /**
   * In this test, {@link #PARTITION_BAR} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a good one. We expect the Notifier to not report any errors after the
   * EOP.
   */
  @Test
  public void testCorruptMessagesDoNotFailFastAfterEOP() throws Exception {
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
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) >= lastOffsetBeforeEOP) {
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

      });
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
  @Test(invocationCount = 100, skipFailedInvocations = true)
  public void testCorruptMessagesFailFast() throws Exception {
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
    });
  }

  @Test
  public void testSubscribeCompletedPartition() throws Exception {
    final int offset = 100;
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    runTest(getSet(PARTITION_FOO),
        () -> doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO),
        () -> {
          verify(mockNotifier, timeout(TEST_TIMEOUT)).restarted(topic, PARTITION_FOO, offset);
          verify(mockNotifier, timeout(TEST_TIMEOUT)).completed(topic, PARTITION_FOO, offset);
        }
    );
  }

  @Test
  public void testUnsubscribeConsumption() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT)).started(topic, PARTITION_FOO);
      //Start of push has already been consumed. Stop consumption
      storeIngestionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    });
  }

  @Test
  public void testKillConsumption() throws Exception {
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
      });
    } finally {
      writingThread.interrupt();
    }
  }

  @Test
  public void testKillActionPriority() throws Exception {
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
      MockInMemoryConsumer mockConsumer = (MockInMemoryConsumer)(storeIngestionTaskUnderTest.getConsumer());
      assertEquals(mockConsumer.getOffsets().size(), 0,
          "subscribe should not be processed in this consumer.");

      // Verify offset has not been processed. Because consumption task should process kill action at first.
      // offSetManager.clearOffset should only be invoked one time during clean up after killing this task.
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT)).clearOffset(topic, PARTITION_FOO);
      assertEquals(mockConsumer.getOffsets().size(), 0,
          "reset should not be processed in this consumer.");

      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    });
  }

  private byte[] getNumberedKey(int number) {
    return ByteBuffer.allocate(putKeyFoo.length + Integer.BYTES).put(putKeyFoo).putInt(number).array();
  }
  private byte[] getNumberedValue(int number) {
    return ByteBuffer.allocate(putValue.length + Integer.BYTES).put(putValue).putInt(number).array();
  }

  @DataProvider(name = "sortedInput")
  public static Object[][] sortedInput() {
    List<Boolean[]> returnList = new ArrayList<>();
    returnList.add(new Boolean[]{true});
    returnList.add(new Boolean[]{false});
    Boolean[][] valuesToReturn = new Boolean[returnList.size()][1];
    return returnList.toArray(valuesToReturn);
  }

  @Test(dataProvider = "sortedInput", invocationCount = 3, skipFailedInvocations = true)
  public void testDataValidationCheckPointing(boolean sortedInput) throws Exception {
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
                ", OffsetRecord: " + topicPartitionOffsetRecordPair.getSecond());
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
        verify(mockKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts + 1)).subscribe(eq(topic), eq(partition), any());
        verify(mockKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts)).unSubscribe(topic, partition);

        if (sortedInput) {
          // Check database mode switches from deferred-write to transactional after EOP control message
          StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, partition);
          deferredWritePartitionConfig.setDeferredWrite(true);
          // SOP control message and restart
          verify(mockAbstractStorageEngine, atLeast(totalNumberOfConsumptionRestarts + 1))
              .beginBatchWrite(eq(deferredWritePartitionConfig), any());
          StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, partition);
          // only happen after EOP control message
          verify(mockAbstractStorageEngine, times(1))
              .endBatchWrite(transactionalPartitionConfig);
        }
      });

      // Verify that the storage engine got hit with every record.
      verify(mockAbstractStorageEngine, atLeast(pushedRecords.size())).put(any(), any(), any(ByteBuffer.class));

      // Verify that every record hit the storage engine
      pushedRecords.entrySet().stream().forEach(entry -> {
        int partition = entry.getKey().getFirst();
        byte[] key = entry.getKey().getSecond().get();
        byte[] value = ValueRecord.create(SCHEMA_ID, entry.getValue().get()).serialize();
        verify(mockAbstractStorageEngine, atLeastOnce()).put(partition, key, ByteBuffer.wrap(value));
      });
    });
  }

  @Test
  public void testKillAfterPartitionIsCompleted()
      throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockNotifier, after(TEST_TIMEOUT).never()).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);

      storeIngestionTaskUnderTest.kill();
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
    });
  }

  @Test
  public void testNeverReportProgressBeforeStart()
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
    });
  }

  @Test
  public void testOffsetPersistent()
      throws Exception {
    // Do not persist every message.
    databaseSyncBytesIntervalForTransactionalMode = 1000;
    List<Long> offsets = new ArrayList<>();
    offsets.add(5l);
    try {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.broadcastEndOfPush(new HashMap<>());
      veniceWriter.broadcastStartOfBufferReplay(offsets, "t", topic, new HashMap<>());
      // Should persist twice. One for end of push and another one for start of buffer replay
      runHybridTest(getSet(PARTITION_FOO), () -> {
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT).times(2)).put(eq(topic), eq(PARTITION_FOO), any());
      });
    }finally {
      databaseSyncBytesIntervalForTransactionalMode = 1;
    }

  }

  @Test
  public void testVeniceMessagesProcessingWithSortedInput() throws Exception {
    veniceWriter.broadcastStartOfPush(true, new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata deleteMetadata = (RecordMetadata) veniceWriter.delete(deleteKeyFoo).get();
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
      // Deferred write is not going to commit offset for every message
      verify(mockStorageMetadataService, never())
          .put(topic, PARTITION_FOO, getOffsetRecord(putMetadata.offset() - 1));
      // Check database mode switches from deferred-write to transactional after EOP control message
      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);
      verify(mockAbstractStorageEngine, times(1))
          .beginBatchWrite(eq(deferredWritePartitionConfig), any());
      StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, times(1))
          .endBatchWrite(transactionalPartitionConfig);
    });
  }

  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void testDelayedTransitionToOnlineInHybridMode() throws Exception {
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

          veniceWriter.broadcastStartOfBufferReplay(
              Arrays.asList(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L),
              "source K cluster",
              "source K topic",
              new HashMap<>());

          for (int i = 0; i < MESSAGES_AFTER_EOP; i++) {
            try {
              veniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID).get();
            } catch (InterruptedException | ExecutionException e) {
              throw new VeniceException(e);
            }
          }

          verify(mockNotifier, timeout(TEST_TIMEOUT).atLeast(ALL_PARTITIONS.size())).completed(anyString(), anyInt(), anyLong());
        }
    );
  }

  /**
   * This test writes a message to Kafka then creates a StoreIngestionTask (and StoreBufferDrainer)  It also passes a DiskUsage
   * object to the StoreIngestionTask that always reports disk full.  This means when the StoreBufferDrainer tries to persist
   * the record, it will receive a disk full error.  This test checks for that disk full error on the Notifier object.
   * @throws Exception
   */
  @Test
  public void StoreIngestionTaskRespectsDiskUsage() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    veniceWriter.broadcastEndOfPush(new HashMap<>());
    DiskUsage diskFullUsage = mock(DiskUsage.class);
    doReturn(true).when(diskFullUsage).isDiskFull(anyLong());
    doReturn("mock disk full disk usage").when(diskFullUsage).getDiskStatus();

    runTest(new RandomPollStrategy(), getSet(PARTITION_FOO), ()->{}, ()->{
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Assert.assertFalse(mockNotifierError.isEmpty(), "Disk Usage should have triggered an ingestion error");
        String errorMessages = mockNotifierError.stream().map(o -> ((Exception) o[3]).getCause().getMessage()) //elements in object array are 0:store name (String), 1: partition (int), 2: message (String), 3: cause (Exception)
            .collect(Collectors.joining());
        Assert.assertTrue(errorMessages.contains("Disk is full"),
            "Expected disk full error, found following error messages: " + errorMessages);
      });
    }, Optional.empty(), Optional.of(diskFullUsage));
  }

  private static class TestVeniceWriter<K,V> extends VeniceWriter{

    protected TestVeniceWriter(VeniceProperties props, String topicName, VeniceKafkaSerializer keySerializer,
        VeniceKafkaSerializer valueSerializer, VenicePartitioner partitioner, Time time, Supplier supplier) {
      super(props, topicName, keySerializer, valueSerializer, partitioner, time, supplier);
    }
  }

}
