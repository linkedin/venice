package com.linkedin.venice.kafka.consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.unit.kafka.consumer.poll.AbstractPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.ArbitraryOrderingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.CompositePollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.DuplicatingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.FilteringPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducer;
import com.linkedin.venice.unit.kafka.producer.TransformingProducer;
import com.linkedin.venice.unit.matchers.ExceptionClassMatcher;
import com.linkedin.venice.unit.matchers.LongGreaterThanMatcher;
import com.linkedin.venice.unit.matchers.NonEmptyStringMatcher;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;


/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 */
public class StoreConsumptionTaskTest {

  private static final int TEST_TIMEOUT;

  static {
    StoreConsumptionTask.READ_CYCLE_DELAY_MS = 5;
    StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS = 100;
    TEST_TIMEOUT = 500 * StoreConsumptionTask.READ_CYCLE_DELAY_MS;

    // TODO Hack, after trying to configure the log4j with log4j.properties
    // at multiple places ( test root directory, test resources)
    // log4j does not produce any log with no appender error message
    // So configuring it in the code as a hack now.
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(org.apache.log4j.Level.INFO);
    rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
  }

  private InMemoryKafkaBroker inMemoryKafkaBroker;
  private VeniceWriter veniceWriter;
  private StoreRepository mockStoreRepository;
  private VeniceNotifier mockNotifier;
  private OffsetManager mockOffSetManager;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private EventThrottler mockThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;

  private ExecutorService taskPollingService;

  private static final String storeNameWithoutVersionInfo = "TestTopic";
  private static final String topic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);

  private static final int PARTITION_COUNT = 10;
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
    String randomString = TestUtils.getUniqueString("KeyForPartition" + partition);
    return ByteBuffer.allocate(randomString.length() + 1)
        .put(partition.byteValue())
        .put(randomString.getBytes())
        .array();
  }

  @BeforeSuite
  public void suiteSetUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    taskPollingService.shutdown();
  }

  @BeforeMethod
  public void methodSetUp() throws Exception {
    inMemoryKafkaBroker = new InMemoryKafkaBroker();
    inMemoryKafkaBroker.createTopic(topic, 10);
    veniceWriter = getVeniceWriter(() -> new MockInMemoryProducer(inMemoryKafkaBroker));
    mockStoreRepository = Mockito.mock(StoreRepository.class);
    mockNotifier = Mockito.mock(LogNotifier.class);
    mockAbstractStorageEngine = Mockito.mock(AbstractStorageEngine.class);
    mockOffSetManager = Mockito.mock(OffsetManager.class);
    mockThrottler = Mockito.mock(EventThrottler.class);
    mockSchemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
  }

  private VeniceWriter getVeniceWriter(Supplier<KafkaProducerWrapper> producerSupplier) {
    return new VeniceWriter(
        new VeniceProperties(new Properties()),
        topic,
        new DefaultSerializer(),
        new DefaultSerializer(),
        new SimplePartitioner(),
        SystemTime.INSTANCE,
        producerSupplier
    );
  }

  private long getOffset(Future<RecordMetadata> recordMetadataFuture)
      throws ExecutionException, InterruptedException {
    return recordMetadataFuture.get().offset();
  }

  private StoreConsumptionTask getKafkaPerStoreConsumptionTask(int... partitions) throws Exception {
    return getKafkaPerStoreConsumptionTask(new RandomPollStrategy(), partitions);
  }

  private StoreConsumptionTask getKafkaPerStoreConsumptionTask(PollStrategy pollStrategy, int... partitions) {
    MockInMemoryConsumer mockKafkaConsumer = new MockInMemoryConsumer(inMemoryKafkaBroker, pollStrategy);
    Properties kafkaProps = new Properties();
    VeniceConsumerFactory mockFactory = Mockito.mock(VeniceConsumerFactory.class);
    Mockito.doReturn(mockKafkaConsumer).when(mockFactory).getConsumer(kafkaProps);
    for (int partition : partitions) {
      Mockito.doReturn(OffsetRecord.NON_EXISTENT_OFFSET).when(mockOffSetManager).getLastOffset(topic, partition);
    }

    Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>();
    notifiers.add(mockNotifier);
    notifiers.add(new LogNotifier());

    return new StoreConsumptionTask(
        mockFactory,
        kafkaProps,
        mockStoreRepository,
        mockOffSetManager,
        notifiers,
        mockThrottler,
        topic,
        mockSchemaRepo);
  }

  private Pair<TopicPartition, OffsetRecord> getTopicPartitionOffsetPair(RecordMetadata recordMetadata) {
    return new Pair<>(
        new TopicPartition(recordMetadata.topic(), recordMetadata.partition()),
        new OffsetRecord(recordMetadata.offset())
    );
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   * 1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   * 2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#delete.
   * 3. A VeniceMessage with a Kafka offset that was already processed is ignored.
   */
  @Test
  public void testVeniceMessagesProcessing() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    RecordMetadata putMetadata = (RecordMetadata) veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata deleteMetadata = (RecordMetadata) veniceWriter.delete(deleteKeyFoo).get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());

    // We re-deliver the old put out of order, so we can make sure it's ignored.
    Queue<Pair<TopicPartition, OffsetRecord>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));

    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(pollStrategy, PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1))
          .put(eq(PARTITION_FOO), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(SCHEMA_ID, putValue).serialize());

      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1)).delete(eq(PARTITION_FOO), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1)).delete(PARTITION_FOO, deleteKeyFoo);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = new OffsetRecord(deleteMetadata.offset());
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).recordOffset(topic, PARTITION_FOO, expected);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testVeniceMessagesProcessingWithExistingSchemaId() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);

      Mockito.doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1))
          .put(eq(PARTITION_FOO), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize());
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TEST_TIMEOUT).times(1))
          .hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = new OffsetRecord(fooLastOffset);
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).recordOffset(topic, PARTITION_FOO, expected);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    veniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    long existingSchemaOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);

      Mockito.when(mockSchemaRepo.hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID))
          .thenReturn(false, false, true);

      Mockito.doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).never())
          .put(eq(PARTITION_FOO), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).never())
          .put(eq(PARTITION_FOO), eq(putKeyFoo), any());
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TEST_TIMEOUT).times(1))
          .hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TEST_TIMEOUT).never())
          .hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify no offset commit to Offset Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).never()).recordOffset(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS).times(1))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(NON_EXISTING_SCHEMA_ID, putValue).serialize());

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS).times(1))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize());

      OffsetRecord expected = new OffsetRecord(existingSchemaOffset);
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).recordOffset(topic, PARTITION_FOO, expected);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testVeniceMessagesProcessingWithNonExistingSchemaId() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    veniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    veniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);

      Mockito.doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      Mockito.doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Utils.sleep(StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).times(1)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).never())
          .put(eq(PARTITION_FOO), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).never())
          .put(PARTITION_FOO, putKeyFoo, putValue);
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TEST_TIMEOUT).atLeastOnce())
          .hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TEST_TIMEOUT).never())
          .hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify no offset commit to Offset Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TEST_TIMEOUT).never()).recordOffset(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testNotifier() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(Maps.newHashMap());

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO,
          fooLastOffset);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR,
          barLastOffset);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testResetPartition() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(1))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(SCHEMA_ID, putValue).serialize());

      mockStoreConsumptionTask.resetPartitionConsumptionOffset(topic, PARTITION_FOO);

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TEST_TIMEOUT).times(2))
          .put(PARTITION_FOO, putKeyFoo, ValueRecord.create(SCHEMA_ID, putValue).serialize());

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  /**
   * In this test, partition FOO will complete successfully, but partition BAR will be missing a record.
   *
   * The {@link VeniceNotifier} should see the completion and error reported for the appropriate partitions.
   */
  @Test
  public void testDetectionOfMissingRecord() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToSkip = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    veniceWriter.broadcastEndOfPush(Maps.newHashMap());

    PollStrategy pollStrategy = new FilteringPollStrategy(
        new RandomPollStrategy(),
        Sets.newHashSet(new Pair(new TopicPartition(topic, PARTITION_BAR), new OffsetRecord(barOffsetToSkip)))
    );

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(pollStrategy, PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(MissingDataException.class)));

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record.
   * The {@link VeniceNotifier} should see the completion for both partitions.
   *
   * TODO: Add a metric to track duplicate records, and verify that it gets tracked properly.
   */
  @Test
  public void testSkippingOfDuplicateRecord() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    long fooLastOffset = getOffset(veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToDupe = getOffset(veniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriter.broadcastEndOfPush(Maps.newHashMap());

    PollStrategy pollStrategy = new DuplicatingPollStrategy(
        new RandomPollStrategy(),
        Sets.newHashSet(new Pair(new TopicPartition(topic, PARTITION_BAR), new OffsetRecord(barOffsetToDupe)))
    );

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(pollStrategy, PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR, barOffsetToDupe);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testThrottling() throws Exception {
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.delete(deleteKeyFoo);

    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Mockito.verify(mockThrottler, Mockito.timeout(TEST_TIMEOUT).times(1)).maybeThrottle(putKeyFoo.length + putValue.length);
      Mockito.verify(mockThrottler, Mockito.timeout(TEST_TIMEOUT).times(1)).maybeThrottle(deleteKeyFoo.length);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreConsumptionTask}
   * to fail fast. The message in {@link #PARTITION_FOO} will receive a bad message type, whereas
   * the message in {@link #PARTITION_BAR} will receive a bad control message type.
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
      Assert.fail("The message type " + badMessageTypeId + " is valid. "
          + "This test needs to be updated in order to send an invalid message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    try {
      ControlMessageType.valueOf(badMessageTypeId);
      Assert.fail("The control message type " + badMessageTypeId + " is valid. "
          + "This test needs to be updated in order to send an invalid control message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    veniceWriter = getVeniceWriter(() -> new TransformingProducer(
        new MockInMemoryProducer(inMemoryKafkaBroker),
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
        })
    );
    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    veniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    veniceWriter.put(putKeyBar, putValue, SCHEMA_ID);

    try (StoreConsumptionTask mockStoreConsumptionTask =
        getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);

      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).error(
          eq(topic),
          eq(PARTITION_FOO),
          argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(VeniceException.class)));

      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).atLeastOnce()).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(VeniceException.class)));

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  /**
   * In this test, the {@link #PARTITION_FOO} will receive a well-formed message, while the
   * {@link #PARTITION_BAR} will receive a corrupt message. We expect the Notifier to report
   * as such.
   *
   * N.B.: There was an edge case where this test was flaky. The edge case is now fixed, but
   * the invocationCount of 100 should ensure that if this test is ever made flaky again, it
   * will be detected right away.
   */
  @Test(invocationCount = 100)
  public void testCorruptMessagesFailFast() throws Exception {
    VeniceWriter veniceWriterForData = getVeniceWriter(() -> new TransformingProducer(
        new MockInMemoryProducer(inMemoryKafkaBroker),
        (topicName, key, value, partition) -> {
          KafkaMessageEnvelope transformedMessageEnvelope = value;

          if (partition == PARTITION_BAR &&
              MessageType.valueOf(transformedMessageEnvelope) == MessageType.PUT) {
            Put put = (Put) transformedMessageEnvelope.payloadUnion;
            put.putValue = ByteBuffer.wrap("CORRUPT_VALUE".getBytes());
            transformedMessageEnvelope.payloadUnion = put;
          }

          return new TransformingProducer.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
        })
    );

    veniceWriter.broadcastStartOfPush(Maps.newHashMap());
    long fooLastOffset = getOffset(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(veniceWriterForData.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForData.close();
    veniceWriter.broadcastEndOfPush(Maps.newHashMap());

    try (StoreConsumptionTask mockStoreConsumptionTask =
        getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).times(1)).completed(
          eq(topic),
          eq(PARTITION_FOO),
          longThat(new LongGreaterThanMatcher(fooLastOffset)));

      /**
       * Let's make sure that {@link PARTITION_BAR} never sends a completion notification.
       *
       * This used to be flaky, because the {@link StoreConsumptionTask} occasionally sent
       * a completion notification for partitions where it had already detected an error.
       * Now, the {@link StoreConsumptionTask} keeps track of the partitions that had error
       * and avoids sending completion notifications for those. The high invocationCount on
       * this test is to detect this edge case.
       */
      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).times(0)).completed(
          eq(topic),
          eq(PARTITION_BAR),
          anyLong());

      Mockito.verify(mockNotifier, Mockito.timeout(TEST_TIMEOUT).times(1)).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(CorruptDataException.class)));

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

}
