package com.linkedin.venice.kafka.consumer;

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
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.DeepCopyOffsetManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.ServerAggStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
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
import com.linkedin.venice.unit.matchers.LongGreaterThanMatcher;
import com.linkedin.venice.unit.matchers.NonEmptyStringMatcher;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
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
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 */
public class StoreConsumptionTaskTest {

  private static final Logger LOGGER = Logger.getLogger(StoreConsumptionTaskTest.class);

  private static final int TEST_TIMEOUT;

  static {
    StoreConsumptionTask.READ_CYCLE_DELAY_MS = 5;
    StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS = 100;
    TEST_TIMEOUT = 500 * StoreConsumptionTask.READ_CYCLE_DELAY_MS;

    // TODO Hack, after trying to configure the log4j with log4j.properties
    // at multiple places ( test root directory, test resources)
    // log4j does not produce any log with no appender error message
    // So configuring it in the code as a hack now.
//    Logger rootLogger = Logger.getRootLogger();
//    rootLogger.setLevel(org.apache.log4j.Level.INFO);
//    rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
  }

  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;
  private StoreRepository mockStoreRepository;
  private VeniceNotifier mockNotifier;
  private OffsetManager mockOffSetManager;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private EventThrottler mockThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;
  /** N.B.: This mock can be used to verify() calls, but not to return arbitrary things. */
  private KafkaConsumerWrapper mockKafkaConsumer;
  private TopicManager mockTopicManager;
  private  KafkaConsumerPerStoreService mockKafKaConsumerPerStoreService;

  private StoreConsumptionTask storeConsumptionTaskUnderTest;

  private ExecutorService taskPollingService;

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

    mockKafKaConsumerPerStoreService = mock(KafkaConsumerPerStoreService.class);
    ServerAggStats.init(new MetricsRepository(), mockKafKaConsumerPerStoreService);
  }

  @AfterClass
  public void tearDown() throws Exception {
    taskPollingService.shutdown();
    ServerAggStats.getInstance().close();
  }

  @BeforeMethod
  public void methodSetUp() throws Exception {
    inMemoryKafkaBroker = new InMemoryKafkaBroker();
    inMemoryKafkaBroker.createTopic(topic, PARTITION_COUNT);
    veniceWriter = getVeniceWriter(() -> new MockInMemoryProducer(inMemoryKafkaBroker));
    mockStoreRepository = mock(StoreRepository.class);
    mockNotifier = mock(LogNotifier.class);
    mockAbstractStorageEngine = mock(AbstractStorageEngine.class);
    mockOffSetManager = mock(OffsetManager.class);
    mockThrottler = mock(EventThrottler.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockKafkaConsumer = mock(KafkaConsumerWrapper.class);
    mockTopicManager = mock(TopicManager.class);
  }

  private VeniceWriter getVeniceWriter(Supplier<KafkaProducerWrapper> producerSupplier) {
    return new VeniceWriter(new VeniceProperties(new Properties()), topic, new DefaultSerializer(),
        new DefaultSerializer(), new SimplePartitioner(), SystemTime.INSTANCE, producerSupplier);
  }

  private long getOffset(Future<RecordMetadata> recordMetadataFuture)
      throws ExecutionException, InterruptedException {
    return recordMetadataFuture.get().offset();
  }

  private void runTest(Set<Integer> partitions, Runnable assertions) throws Exception {
    runTest(partitions, () -> {}, assertions);
  }

  private void runTest(Set<Integer> partitions,
                       Runnable beforeStartingConsumption,
                       Runnable assertions) throws Exception {
    runTest(new RandomPollStrategy(), partitions, beforeStartingConsumption, assertions);
  }

  private void runTest(PollStrategy pollStrategy,
                       Set<Integer> partitions,
                       Runnable beforeStartingConsumption,
                       Runnable assertions) throws Exception {
    MockInMemoryConsumer inMemoryKafkaConsumer = new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy, mockKafkaConsumer);
    Properties kafkaProps = new Properties();
    VeniceConsumerFactory mockFactory = mock(VeniceConsumerFactory.class);
    doReturn(inMemoryKafkaConsumer).when(mockFactory).getConsumer(kafkaProps);
    for (int partition : partitions) {
      doReturn(new OffsetRecord()).when(mockOffSetManager).getLastOffset(topic, partition);
    }
    Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>(Arrays.asList(mockNotifier, new LogNotifier()));
    OffsetManager offsetManager = new DeepCopyOffsetManager(mockOffSetManager);
    storeConsumptionTaskUnderTest = new StoreConsumptionTask(
        mockFactory, kafkaProps, mockStoreRepository, offsetManager, notifiers, mockThrottler, topic, mockSchemaRepo, mockTopicManager);
    doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

    Future testSubscribeTaskFuture = null;
    try {
      for (int partition: partitions) {
        storeConsumptionTaskUnderTest.subscribePartition(topic, partition);
      }

      beforeStartingConsumption.run();

      // MockKafkaConsumer is prepared. Schedule for polling.
      testSubscribeTaskFuture = taskPollingService.submit(storeConsumptionTaskUnderTest);

      assertions.run();

    } finally {
      storeConsumptionTaskUnderTest.close();
      if (testSubscribeTaskFuture != null) {
        testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
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
      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).put(eq(PARTITION_FOO), any(), any());
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(SCHEMA_ID, putValue).serialize());

      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).delete(eq(PARTITION_FOO), any());
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).delete(PARTITION_FOO, deleteKeyFoo);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.offset());

      verify(mockOffSetManager, timeout(TEST_TIMEOUT))
          .recordOffset(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
    });
  }

  @Test
  public void testVeniceMessagesProcessingWithExistingSchemaId() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(getSet(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT)).put(eq(PARTITION_FOO), any(), any());
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize());
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = getOffsetRecord(fooLastOffset);

      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).recordOffset(topic, PARTITION_FOO, expected);
    });
  }

  /**
   * TODO: Fix this test. It fails when running individually, but succeeds when the whole suite runs...
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
      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).never()).put(eq(PARTITION_FOO), any(), any());
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).never()).put(eq(PARTITION_FOO), eq(putKeyFoo), any());
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT)).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT).never())
          .hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify no offset commit to Offset Manager
      verify(mockOffSetManager, timeout(TEST_TIMEOUT).never())
          .recordOffset(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));

      verify(mockAbstractStorageEngine, timeout(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(NON_EXISTING_SCHEMA_ID, putValue).serialize());

      verify(mockAbstractStorageEngine, timeout(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize());

      OffsetRecord expected = getOffsetRecord(existingSchemaOffset);
      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).recordOffset(topic, PARTITION_FOO, expected);
    });
  }

  /**
   * TODO: Fix this test. It fails when running individually, but succeeds when the whole suite runs...
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
      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).never()).put(eq(PARTITION_FOO), any(), any());
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).never()).put(PARTITION_FOO, putKeyFoo, putValue);
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT).atLeastOnce())
          .hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT).never())
          .hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify no offset commit to Offset Manager
      verify(mockOffSetManager, timeout(TEST_TIMEOUT).never())
          .recordOffset(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));
    });
  }

  @Test
  public void testReportStartWhenRestarting() throws Exception {
    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      doReturn(getOffsetRecord(1)).when(mockOffSetManager).getLastOffset(topic, PARTITION_FOO);
    }, () -> {
      // Verify STARTED is reported when offset is larger than 0 is invoked.
      verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).restarted(topic, PARTITION_FOO, 1);
      // Verify STARTED is NOT reported when offset is 0
      verify(mockNotifier, timeout(TEST_TIMEOUT).never()).restarted(topic, PARTITION_BAR, 1);
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
       * Considering that the {@link VeniceWriter} will send an {@link ControlMessageType#END_OF_PUSH} and
       * an {@link ControlMessageType#END_OF_SEGMENT}, we need to add 2 to last data message offset.
       */
      verify(mockOffSetManager, timeout(TEST_TIMEOUT).atLeastOnce())
          .recordOffset(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooLastOffset + 2, true)));
      verify(mockOffSetManager, timeout(TEST_TIMEOUT).atLeastOnce())
          .recordOffset(eq(topic), eq(PARTITION_BAR), eq(getOffsetRecord(barLastOffset + 2, true)));

      // After we verified that recordOffset() is called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockNotifier, atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      verify(mockNotifier, atLeastOnce()).completed(topic, PARTITION_BAR, barLastOffset);
    });
  }

  @Test
  public void testResetPartition() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(SCHEMA_ID, putValue).serialize());

      storeConsumptionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT).times(2))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(SCHEMA_ID, putValue).serialize());
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
      verify(mockNotifier, timeout(TEST_TIMEOUT).never()).completed(topic, PARTITION_BAR, barOffsetToDupe + 1);

      // After we verified that completed() is called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    });
  }

  /**
   * TODO: Fix this test. It fails when running individually, but succeeds when the whole suite runs...
   */
  @Test
  public void testThrottling() throws Exception {
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.delete(deleteKeyFoo);

    runTest(getSet(PARTITION_FOO), () -> {
      verify(mockThrottler, timeout(TEST_TIMEOUT)).maybeThrottle(putKeyFoo.length + putValue.length);
      verify(mockThrottler, timeout(TEST_TIMEOUT)).maybeThrottle(deleteKeyFoo.length);
    });
  }

  /**
   * In this test, we validate {@link StoreConsumptionTask#getOffsetLag()}. This method is for monitoring the Kafka
   * offset consumption rate. It pulls the most recent offsets from {@link TopicManager} and then compares it with
   * local offset cache {@link StoreConsumptionTask#partitionToOffsetMap}
   */
  @Test
  public void testOffsetLag() throws Exception{
    //The offset will be 4 and there will be 5 kafka messages generated for PARTITION_FOO and PARTITION_BAR
    //start_of_segment, start_of_push, putKeyFoo/putValue, end_of_push, and end_of_segment
    veniceWriter.broadcastStartOfPush(new HashMap<>());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    Map<Integer, Long> latestOffsetsMap = new HashMap<>();
    latestOffsetsMap.put(PARTITION_FOO, 5l);
    latestOffsetsMap.put(PARTITION_BAR, 6l);
    //put an arbitrary partition which the consumer doesn't subscribe to
    latestOffsetsMap.put(3, 3l);

    doReturn(latestOffsetsMap).when(mockTopicManager).getLatestOffsets(topic);

    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeConsumptionTaskUnderTest.getOffsetLag() == 3);
      verify(mockTopicManager, timeout(TEST_TIMEOUT).atLeastOnce()).getLatestOffsets(topic);
    });
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreConsumptionTask} to fail fast. The
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
   * In this test, the {@link #PARTITION_FOO} will receive a well-formed message, while the {@link #PARTITION_BAR} will
   * receive a corrupt message. We expect the Notifier to report as such.
   * <p>
   * N.B.: There was an edge case where this test was flaky. The edge case is now fixed, but the invocationCount of 100
   * should ensure that if this test is ever made flaky again, it will be detected right away. The skipFailedInvocations
   * annotation parameter makes the test skip any invocation after the first failure.
   */
  @Test(invocationCount = 100, skipFailedInvocations = true)
  public void testCorruptMessagesFailFast() throws Exception {
    VeniceWriter veniceWriterForData = getVeniceWriter(
        () -> new TransformingProducer(new MockInMemoryProducer(inMemoryKafkaBroker),
            (topicName, key, value, partition) -> {
              KafkaMessageEnvelope transformedMessageEnvelope = value;

              if (partition == PARTITION_BAR && MessageType.valueOf(transformedMessageEnvelope) == MessageType.PUT) {
                Put put = (Put) transformedMessageEnvelope.payloadUnion;
                put.putValue = ByteBuffer.wrap("CORRUPT_VALUE".getBytes());
                transformedMessageEnvelope.payloadUnion = put;
              }

              return new TransformingProducer.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
            }));

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    getOffset(veniceWriterForData.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForData.close();
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(getSet(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockNotifier, timeout(TEST_TIMEOUT))
          .completed(eq(topic), eq(PARTITION_FOO), longThat(new LongGreaterThanMatcher(fooLastOffset)));

      /**
       * Let's make sure that {@link PARTITION_BAR} never sends a completion notification.
       *
       * This used to be flaky, because the {@link StoreConsumptionTask} occasionally sent
       * a completion notification for partitions where it had already detected an error.
       * Now, the {@link StoreConsumptionTask} keeps track of the partitions that had error
       * and avoids sending completion notifications for those. The high invocationCount on
       * this test is to detect this edge case.
       */
      verify(mockNotifier, timeout(TEST_TIMEOUT).never()).completed(eq(topic), eq(PARTITION_BAR), anyLong());

      verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(CorruptDataException.class)));
    });
  }

  @Test
  public void testSubscribeCompletedPartition() throws Exception {
    final int offset = 100;
    runTest(getSet(PARTITION_FOO),
        () -> doReturn(getOffsetRecord(offset, true)).when(mockOffSetManager).getLastOffset(topic, PARTITION_FOO),
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
      storeConsumptionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeConsumptionTaskUnderTest.isRunning() == false);
    });
  }

  @Test
  public void testKillConsumption() throws Exception {
    final Thread writingThread = new Thread(() -> {
      while (true) {
        veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
        veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
        try {
          TimeUnit.MILLISECONDS.sleep(StoreConsumptionTask.READ_CYCLE_DELAY_MS);
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

        //Start of push has alread been consumed. Stop consuption
        storeConsumptionTaskUnderTest.kill();
        // task should report an error to notifier that it's killed.
        verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
            eq(topic), eq(PARTITION_FOO), argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassAndCauseClassMatcher(VeniceException.class, InterruptedException.class)));
        verify(mockNotifier, timeout(TEST_TIMEOUT)).error(
            eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassAndCauseClassMatcher(VeniceException.class, InterruptedException.class)));

        waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
            () -> storeConsumptionTaskUnderTest.isRunning() == false);
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
      storeConsumptionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);
      // Add a kill consumer action in higher priority than subscribe and reset.
      storeConsumptionTaskUnderTest.kill();
    }, () -> {
      // verify subscribe has not been processed. Because consumption task should process kill action at frist
      verify(mockOffSetManager, timeout(TEST_TIMEOUT).never()).getLastOffset(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeConsumptionTaskUnderTest.getConsumer() != null);
      assertEquals(((MockInMemoryConsumer) storeConsumptionTaskUnderTest.getConsumer()).getOffsets().size(), 0,
          "subscribe should not be processed in this consumer.");

      // Verify offset has not been processed. Because consumption task should process kill action at first.
      // offSetManager.clearOffset should only be invoked one time during clean up after killing this task.
      verify(mockOffSetManager, timeout(TEST_TIMEOUT)).clearOffset(topic, PARTITION_FOO);
      assertEquals(((MockInMemoryConsumer) storeConsumptionTaskUnderTest.getConsumer()).getOffsets().size(), 0,
          "reset should not be processed in this consumer.");

      waitForNonDeterministicCompletion(TEST_TIMEOUT, TimeUnit.MILLISECONDS,
          () -> storeConsumptionTaskUnderTest.isRunning() == false);
    });
  }

  @Test
  public void testDataValidationCheckPointing() throws Exception {
    final Map<Integer, Long> maxOffsetPerPartition = new HashMap<>();
    final Map<Pair<Integer, ByteArray>, ByteArray> pushedRecords = new HashMap<>();
    final int totalNumberOfMessages = 1000;
    final int totalNumberOfConsumptionRestarts = 10;

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    for (int i = 0; i < totalNumberOfMessages; i++) {
      byte[] key = ByteBuffer.allocate(putKeyFoo.length + Integer.BYTES).put(putKeyFoo).putInt(i).array();
      byte[] value = ByteBuffer.allocate(putValue.length + Integer.BYTES).put(putValue).putInt(i).array();

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
            LOGGER.info("Received null OffsetRecord!");
          } else if (messagesConsumedSoFar.incrementAndGet() % (totalNumberOfMessages / totalNumberOfConsumptionRestarts) == 0) {
            LOGGER.info("Restarting consumer after consuming " + messagesConsumedSoFar.get() + " messages so far.");
            relevantPartitions.stream().forEach(partition ->
                storeConsumptionTaskUnderTest.unSubscribePartition(topic, partition));
            relevantPartitions.stream().forEach(partition ->
                storeConsumptionTaskUnderTest.subscribePartition(topic, partition));
          } else {
            LOGGER.info("TopicPartition: " + topicPartitionOffsetRecordPair.getFirst() +
                ", OffsetRecord: " + topicPartitionOffsetRecordPair.getSecond());
          }
        }
    );

    runTest(pollStrategy, relevantPartitions, () -> {}, () -> {
      // Verify that all partitions reported success.
      maxOffsetPerPartition.entrySet().stream().filter(entry -> relevantPartitions.contains(entry.getKey())).forEach(entry ->
          verify(mockNotifier, timeout(TEST_TIMEOUT).atLeastOnce()).completed(eq(topic), eq(entry.getKey()), eq(entry.getValue())));

      // After this, all asynchronous processing should be finished, so there's no need for time outs anymore.

      // Verify that no partitions reported errors.
      relevantPartitions.stream().forEach(partition ->
          verify(mockNotifier, never()).error(eq(topic), eq(partition), anyString(), any()));

      // Verify that we really unsubscribed and re-subscribed.
      relevantPartitions.stream().forEach(partition -> {
        verify(mockKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts)).subscribe(eq(topic), eq(partition), any());
        verify(mockKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts)).unSubscribe(topic, partition);
      });

      // Verify that the storage engine got hit with every record.
      verify(mockAbstractStorageEngine, atLeast(pushedRecords.size())).put(any(), any(), any());

      // Verify that every record hit the storage engine
      pushedRecords.entrySet().stream().forEach(entry -> {
        int partition = entry.getKey().getFirst();
        byte[] key = entry.getKey().getSecond().get();
        byte[] value = ValueRecord.create(SCHEMA_ID, entry.getValue().get()).serialize();
        verify(mockAbstractStorageEngine, atLeastOnce()).put(partition, key, value);
      });
    });
  }
}
