package com.linkedin.venice.kafka.consumer;

import com.google.common.collect.Maps;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.KafkaNotifier;
import com.linkedin.venice.notifier.LogNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;

/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 */
public class StoreConsumptionTaskTest {

  public static final int TIMEOUT;

  static {
    StoreConsumptionTask.READ_CYCLE_DELAY_MS = 500;
    StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS = 500;
    TIMEOUT = 5 * StoreConsumptionTask.READ_CYCLE_DELAY_MS;

    // TODO Hack, after trying to configure the log4j with log4j.properties
    // at multiple places ( test root directory, test resources)
    // log4j does not produce any log with no appender error message
    // So configuring it in the code as a hack now.
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(org.apache.log4j.Level.INFO);
    rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
  }

  private ApacheKafkaConsumer mockKafkaConsumer;
  private StoreRepository mockStoreRepository;
  private VeniceNotifier mockNotifier;
  private OffsetManager mockOffSetManager;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private EventThrottler mockThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;

  private ExecutorService taskPollingService;

  private final int nodeId = 0;
  private final String storeNameWithoutVersionInfo = "TestTopic";
  private final String topic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);

  private final int testPartition = 0;
  private final TopicPartition testTopicPartition = new TopicPartition(topic, testPartition);

  private final byte[] putKey = "TestKeyPut".getBytes(StandardCharsets.UTF_8);
  private final byte[] putValue = "TestValuePut".getBytes(StandardCharsets.UTF_8);
  private final byte[] deleteKey = "TestKeyDelete".getBytes(StandardCharsets.UTF_8);

  @BeforeSuite
  public void setUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    taskPollingService.shutdown();
  }

  private static final AtomicInteger mockProducerGuidGenerator = new AtomicInteger(0);

  /**
   * This class is used for unit testing the production of Kafka {@link ConsumerRecord} instances.
   * <p>
   * If there is a need for this functionality outside of this test class, then we can move it out.
   * <p>
   * There is an assumption that the producer is used for just a single topic (defined at
   * construction-time). This can be extended later on if need be.
   */
  private static class MockProducer {
    private final String topic;
    private final Time time;
    private final GUID producerGUID;

    /**
     * Maintains a mapping of partition to sequence number.
     */
    private final Map<Integer, Integer> sequenceNumbers = Maps.newHashMap();
    /**
     * Maintains a mapping of partition to segment number.
     */
    private final Map<Integer, Integer> segmentNumbers = Maps.newHashMap();

    MockProducer(String topic, Time time) {
      this.topic = topic;
      this.time = time;
      this.producerGUID = new GUID();
      producerGUID.bytes(ByteBuffer.allocate(16).putLong(mockProducerGuidGenerator.incrementAndGet()).array());
    }

    private synchronized int getNextSequenceNumber(int partition) {
      Integer currentSequenceNumber = sequenceNumbers.get(partition);
      if (currentSequenceNumber == null) {
        currentSequenceNumber = -1;
      }
      currentSequenceNumber++;
      sequenceNumbers.put(partition, currentSequenceNumber);
      return currentSequenceNumber;
    }

    private synchronized int getSegmentNumber(int partition) {
      Integer currentSegmentNumber = segmentNumbers.get(partition);
      if (currentSegmentNumber == null) {
        currentSegmentNumber = 0;
        segmentNumbers.put(partition, currentSegmentNumber);
      }
      return currentSegmentNumber;
    }

    private ProducerMetadata getProducerMetadata(int partition) {
      ProducerMetadata producerMetadata = new ProducerMetadata();
      producerMetadata.producerGUID = producerGUID;
      producerMetadata.messageSequenceNumber = getNextSequenceNumber(partition);
      producerMetadata.segmentNumber = getSegmentNumber(partition);
      producerMetadata.messageTimestamp = time.getMilliseconds();
      return producerMetadata;
    }

    private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> getConsumerRecord(MessageType type,
                                                                             int partition,
                                                                             long offset,
                                                                             byte[] key,
                                                                             Object kafkaValuePayload) {
      KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
      kafkaValue.messageType = type.getValue();
      kafkaValue.payloadUnion = kafkaValuePayload;
      kafkaValue.producerMetadata = getProducerMetadata(partition);

      return new ConsumerRecord<>(topic, partition, offset, 0, TimestampType.NO_TIMESTAMP_TYPE,
          new KafkaKey(type, key), kafkaValue);
    }

    public ConsumerRecord<KafkaKey, KafkaMessageEnvelope> getPutConsumerRecord(int partition,
                                                                               long offset,
                                                                               byte[] key,
                                                                               byte[] value,
                                                                               int schemaId) {
      Put put = new Put();
      put.schemaId = schemaId;
      put.putValue = ByteBuffer.wrap(value);
      return getConsumerRecord(MessageType.PUT, partition, offset, key, put);
    }

    public ConsumerRecord<KafkaKey, KafkaMessageEnvelope> getDeleteConsumerRecord(int partition,
                                                                                  long offset,
                                                                                  byte[] key) {
      return getConsumerRecord(MessageType.DELETE, partition, offset, key, new Delete());
    }

    public ConsumerRecord<KafkaKey, KafkaMessageEnvelope> getControlRecord(ControlMessageType controlMessageType,
                                                                           long offset,
                                                                           int partition) {
      com.linkedin.venice.kafka.protocol.ControlMessage controlMessage =
          (com.linkedin.venice.kafka.protocol.ControlMessage) MessageType.CONTROL_MESSAGE.getNewInstance();
      controlMessage.controlMessageType = controlMessageType.getValue();
      controlMessage.controlMessageUnion = controlMessageType.getNewInstance();
      controlMessage.debugInfo = new HashMap<>();

      if (controlMessageType == ControlMessageType.END_OF_SEGMENT) {
        synchronized (this) {
          segmentNumbers.put(partition, segmentNumbers.get(partition) + 1);
        }
      }

      return getConsumerRecord(MessageType.CONTROL_MESSAGE, partition, offset, new byte[]{}, controlMessage);
    }
  }

  private StoreConsumptionTask getKafkaPerStoreConsumptionTask(int... partitions) throws Exception {
    mockKafkaConsumer = Mockito.mock(ApacheKafkaConsumer.class);
    mockStoreRepository = Mockito.mock(StoreRepository.class);
    mockNotifier = Mockito.mock(KafkaNotifier.class);
    mockAbstractStorageEngine = Mockito.mock(AbstractStorageEngine.class);
    mockOffSetManager = Mockito.mock(OffsetManager.class);
    mockThrottler = Mockito.mock(EventThrottler.class);

    mockSchemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    VeniceConsumerFactory mockFactory = Mockito.mock(VeniceConsumerFactory.class);
    Properties kafkaProps = new Properties();
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
        nodeId,
        topic,
        mockSchemaRepo);
  }

  private ConsumerRecords createConsumerRecords(ConsumerRecord<KafkaKey, KafkaMessageEnvelope>... records) {
    Map<TopicPartition, List<ConsumerRecord>> mockPollResult = new HashMap<>();

    if (records.length > 0) {
      for (ConsumerRecord record : records) {
        TopicPartition topicPartition = new TopicPartition(topic, record.partition());
        if (mockPollResult.containsKey(topicPartition)) {
          mockPollResult.get(topicPartition).add(record);
        } else {
          List<ConsumerRecord> list = new ArrayList<>();
          list.add(record);
          mockPollResult.put(topicPartition, list);
        }
      }
    }

    return new ConsumerRecords(mockPollResult);
  }

  /**
   * Verifies that the KafkaTaskMessages are processed appropriately by invoking corresponding method on the
   * KafkaConsumer.
   */
  @Test
  public void testKafkaTaskMessagesProcessing() throws Exception {

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition)) {
      mockStoreConsumptionTask.subscribePartition(topic, testPartition);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      final long LAST_OFFSET= 15;
      final int schemaId = -1;
      ConsumerRecord testPutRecord = producer.getPutConsumerRecord(testPartition, 10, putKey, putValue, schemaId);
      ConsumerRecord testDeleteRecord = producer.getDeleteConsumerRecord(testPartition, LAST_OFFSET, deleteKey);
      ConsumerRecord ignorePutRecord = producer.getPutConsumerRecord(testPartition, 13, "Low-Offset-Ignored".getBytes(), "ignored-put".getBytes(), schemaId);
      ConsumerRecord ignoreDeleteRecord = producer.getDeleteConsumerRecord(testPartition, 15, "Equal-Offset-Ignored".getBytes());

      // Prepare the mockKafkaConsumer to send the test poll results.
      ConsumerRecords mockResult = createConsumerRecords(testPutRecord, testDeleteRecord, ignorePutRecord,
          ignoreDeleteRecord);
      Mockito.doReturn(mockResult).when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Set<TopicPartition> mockKafkaConsumerSubscriptions = new HashSet<>();
      mockKafkaConsumerSubscriptions.add(testTopicPartition);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(eq(testPartition), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(testPartition, putKey, ValueRecord.create(schemaId, putValue).serialize());

      // Verify the reset behavior
      mockStoreConsumptionTask.resetPartitionConsumptionOffset(topic, testPartition);
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).clearOffset(topic, testPartition);
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).resetOffset(topic, testPartition);

      // Verify unSubscribe behavior
      mockStoreConsumptionTask.unSubscribePartition(topic, testPartition);
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).unSubscribe(topic, testPartition);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   * 1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   * 2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#delete.
   */
  @Test
  public void testVeniceMessagesProcessing() throws Exception {
    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition)) {
      mockStoreConsumptionTask.subscribePartition(topic, testPartition);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      final long LAST_OFFSET= 15;
      final int schemaId = -1;
      ConsumerRecord testPutRecord = producer.getPutConsumerRecord(testPartition, 10, putKey, putValue, schemaId);
      ConsumerRecord testDeleteRecord = producer.getDeleteConsumerRecord(testPartition, LAST_OFFSET, deleteKey);
      ConsumerRecord ignorePutRecord = producer.getPutConsumerRecord(testPartition, 13, "Low-Offset-Ignored".getBytes(), "ignored-put".getBytes(), schemaId);
      ConsumerRecord ignoreDeleteRecord = producer.getDeleteConsumerRecord(testPartition, 15, "Equal-Offset-Ignored".getBytes());

      // Prepare the mockKafkaConsumer to send the test poll results.
      ConsumerRecords mockResult = createConsumerRecords(testPutRecord, testDeleteRecord, ignorePutRecord,
          ignoreDeleteRecord);
      Mockito.doReturn(mockResult).when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).getLastOffset(topic, testPartition);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(eq(testPartition), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(testPartition, putKey, ValueRecord.create(schemaId, putValue).serialize());

      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1)).delete(eq(testPartition), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1)).delete(testPartition, deleteKey);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = new OffsetRecord(LAST_OFFSET);
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).recordOffset(topic, testPartition, expected);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testVeniceMessagesProcessingWithExistingSchemaId() throws Exception {
    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition)) {
      mockStoreConsumptionTask.subscribePartition(topic, testPartition);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      final long LAST_OFFSET= 10;
      final int existingSchemaId = 1;
      Mockito.doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, existingSchemaId);

      ConsumerRecord testPutRecordWithExistingSchemaId = producer.getPutConsumerRecord(testPartition, LAST_OFFSET, putKey, putValue, existingSchemaId);

      // Prepare the mockKafkaConsumer to send the test poll results.
      ConsumerRecords mockResult = createConsumerRecords(testPutRecordWithExistingSchemaId);
      Mockito.doReturn(mockResult).when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).getLastOffset(topic, testPartition);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(eq(testPartition), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(testPartition, putKey, ValueRecord.create(existingSchemaId, putValue).serialize());
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TIMEOUT).times(1))
          .hasValueSchema(storeNameWithoutVersionInfo, existingSchemaId);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = new OffsetRecord(LAST_OFFSET);
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).recordOffset(topic, testPartition, expected);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId() throws Exception {
    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition)) {
      mockStoreConsumptionTask.subscribePartition(topic, testPartition);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      final long OFFSET_WITH_TEMP_NOT_AVAILABLE_SCHEMA = 10;
      final long OFFSET_WITH_VALID_SCHEMA = 11;
      final int existingSchemaId = 1;
      final int nonExistingSchemaId = 2;
      //Mockito.doReturn(false).doReturn(false).doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, nonExistingSchemaId);
      Mockito.when(mockSchemaRepo.hasValueSchema(storeNameWithoutVersionInfo, nonExistingSchemaId))
          .thenReturn(false, false, true);

      Mockito.doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, existingSchemaId);

      ConsumerRecord testPutRecordWithNonExistingSchemaId = producer.getPutConsumerRecord(testPartition, OFFSET_WITH_TEMP_NOT_AVAILABLE_SCHEMA, putKey, putValue, nonExistingSchemaId);
      ConsumerRecord testPutRecordWithExistingSchemaId = producer.getPutConsumerRecord(testPartition, OFFSET_WITH_VALID_SCHEMA, putKey, putValue, existingSchemaId);

      // Prepare the mockKafkaConsumer to send the test poll results.
      ConsumerRecords mockResult = createConsumerRecords(testPutRecordWithNonExistingSchemaId,
          testPutRecordWithExistingSchemaId);
      Mockito.doReturn(mockResult).when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).getLastOffset(topic, testPartition);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).never())
          .put(eq(testPartition), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).never())
          .put(eq(testPartition), eq(putKey), any());
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TIMEOUT).times(1))
          .hasValueSchema(storeNameWithoutVersionInfo, nonExistingSchemaId);
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TIMEOUT).never())
          .hasValueSchema(storeNameWithoutVersionInfo, existingSchemaId);

      // Verify no offset commit to Offset Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).never()).recordOffset(eq(topic), eq(testPartition), any(OffsetRecord.class));
      // Internally, StoreConsumptionTask will sleep POLLING_SCHEMA_DELAY_MS
      // to check for every round to check whether the value schema is available or not.
      // Since POLLING_SCHEMA_DELAY_MS might be bigger than the default TIMEOUT,
      // and the check will only get 'true' response in thrid retry,
      // so I would like to sleep for some time to make sure, the assertion does run after the third retry.
      Utils.sleep(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS);

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS).times(1))
          .put(testPartition, putKey, ValueRecord.create(nonExistingSchemaId, putValue).serialize());

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(2 * StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS).times(1))
          .put(testPartition, putKey, ValueRecord.create(existingSchemaId, putValue).serialize());

      OffsetRecord expected = new OffsetRecord(OFFSET_WITH_VALID_SCHEMA);
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).recordOffset(topic, testPartition, expected);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testVeniceMessagesProcessingWithNonExistingSchemaId() throws Exception {
    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition)) {
      mockStoreConsumptionTask.subscribePartition(topic, testPartition);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      final long OFFSET_WITH_INVALID_SCHEMA = 10;
      final long OFFSET_WITH_VALID_SCHEMA = 11;
      final int existingSchemaId = 1;
      final int nonExistingSchemaId = 2;
      Mockito.doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, nonExistingSchemaId);
      Mockito.doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, existingSchemaId);

      ConsumerRecord testPutRecordWithNonExistingSchemaId = producer.getPutConsumerRecord(testPartition, OFFSET_WITH_INVALID_SCHEMA, putKey, putValue, nonExistingSchemaId);
      ConsumerRecord testPutRecordWithExistingSchemaId = producer.getPutConsumerRecord(testPartition, OFFSET_WITH_VALID_SCHEMA, putKey, putValue, existingSchemaId);

      // Prepare the mockKafkaConsumer to send the test poll results.
      ConsumerRecords mockResult = createConsumerRecords(testPutRecordWithNonExistingSchemaId,
          testPutRecordWithExistingSchemaId);
      Mockito.doReturn(mockResult).when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Utils.sleep(StoreConsumptionTask.POLLING_SCHEMA_DELAY_MS);

      // Verify it retrieves the offset from the OffSet Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).getLastOffset(topic, testPartition);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).never())
          .put(eq(testPartition), any(), any());
      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).never())
          .put(testPartition, putKey, putValue);
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TIMEOUT).atLeastOnce())
          .hasValueSchema(storeNameWithoutVersionInfo, nonExistingSchemaId);
      Mockito.verify(mockSchemaRepo, Mockito.timeout(TIMEOUT).never())
          .hasValueSchema(storeNameWithoutVersionInfo, existingSchemaId);

      // Verify no offset commit to Offset Manager
      Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).never()).recordOffset(eq(topic), eq(testPartition), any(OffsetRecord.class));

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testNotifier() throws Exception {
    final int PARTITION_FOO = 1;
    final int PARTITION_BAR = 2;
    int currentOffset = 0;

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      ConsumerRecord fooStartRecord =
          producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentOffset++, PARTITION_FOO);
      int fooLastOffset = currentOffset++;
      ConsumerRecord fooPutRecord =
          producer.getPutConsumerRecord(PARTITION_FOO, fooLastOffset, putKey, putValue, -1);

      ConsumerRecords mockResult1 = createConsumerRecords(fooStartRecord, fooPutRecord);

      ConsumerRecord barStartRecord =
          producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentOffset++, PARTITION_BAR);
      int barLastOffset = currentOffset++;
      ConsumerRecord barPutRecord =
          producer.getPutConsumerRecord(PARTITION_BAR, barLastOffset, putKey, putValue, -1);

      ConsumerRecords mockResult2 = createConsumerRecords(barStartRecord, barPutRecord);

      ConsumerRecord fooEndRecord =
          producer.getControlRecord(ControlMessageType.END_OF_PUSH, currentOffset++, PARTITION_FOO);
      ConsumerRecords mockResult3 = createConsumerRecords(fooEndRecord);

      ConsumerRecord barEndRecord =
          producer.getControlRecord(ControlMessageType.END_OF_PUSH, currentOffset++, PARTITION_BAR);
      ConsumerRecords mockResult4 = createConsumerRecords(barEndRecord);

      // Tried breaking them into 4 separate lines with verify calls
      // But there was intermittent failures where the new return value was ignored
      // and it used only old return value. Chaining them in 4, did not have
      // intermittent failures on 200 repeated runs.
      Mockito.doReturn(mockResult1)
          .doReturn(mockResult2)
          .doReturn(mockResult3)
          .doReturn(mockResult4)
          .when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO,
          fooLastOffset);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR,
          barLastOffset);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testResetPartition() throws Exception {
    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition)) {
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      final int schemaId = -1;
      mockStoreConsumptionTask.subscribePartition(topic, testPartition);

      ConsumerRecord testPutRecord = producer.getPutConsumerRecord(testPartition, 10, putKey, putValue, schemaId);
      ConsumerRecords mockResult = createConsumerRecords(testPutRecord);

      Mockito.doReturn(mockResult)
          .when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
          .put(testPartition, putKey, ValueRecord.create(schemaId, putValue).serialize());

      mockStoreConsumptionTask.resetPartitionConsumptionOffset(topic, testPartition);

      Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(2))
          .put(testPartition, putKey, ValueRecord.create(schemaId, putValue).serialize());

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
    final int PARTITION_FOO = 0, PARTITION_BAR = 1;
    int currentFooKafkaOffset = 0,
        currentBarKafkaOffset = 0;

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      // FOO: Start of Push
      ConsumerRecord fooStartRecord =
          producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentFooKafkaOffset++, PARTITION_FOO);
      int fooLastOffset = currentFooKafkaOffset++;

      // FOO: Put record
      ConsumerRecord fooPutRecord =
          producer.getPutConsumerRecord(PARTITION_FOO, fooLastOffset, putKey, putValue, -1);

      ConsumerRecords mockResult1 = createConsumerRecords(fooStartRecord, fooPutRecord);

      // BAR: Start of Push
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> barStartRecord =
          producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentBarKafkaOffset++, PARTITION_BAR);
      int barLastOffset = currentBarKafkaOffset++;

      // BAR: Put record which we won't use, to create a gap in the sequence
      producer.getPutConsumerRecord(PARTITION_BAR, barLastOffset, putKey, putValue, -1);
      // BAR: Put record which we'll use
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> barPutRecord =
          producer.getPutConsumerRecord(PARTITION_BAR, barLastOffset, putKey, putValue, -1);

      ConsumerRecords mockResult2 = createConsumerRecords(barStartRecord, barPutRecord);

      // FOO: End of Push
      ConsumerRecord fooEndRecord =
          producer.getControlRecord(ControlMessageType.END_OF_PUSH, currentFooKafkaOffset++, PARTITION_FOO);
      ConsumerRecords mockResult3 = createConsumerRecords(fooEndRecord);

      // BAR: End of Push
      ConsumerRecord barEndRecord =
          producer.getControlRecord(ControlMessageType.END_OF_PUSH, currentBarKafkaOffset++, PARTITION_BAR);
      ConsumerRecords mockResult4 = createConsumerRecords(barEndRecord);

      Mockito.doReturn(mockResult1)
          .doReturn(mockResult2)
          .doReturn(mockResult3)
          .doReturn(mockResult4)
          .when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new ArgumentMatcher<String>() {
            @Override
            public boolean matches(Object argument) {
              return argument instanceof String && !((String) argument).isEmpty();
            }
          }),
          argThat(new ArgumentMatcher<Exception>() {
            @Override
            public boolean matches(Object argument) {
              return argument instanceof MissingDataException;
            }
          }));

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
    final int PARTITION_FOO = 1, PARTITION_BAR = 2;
    int currentFooKafkaOffset = 0,
        currentBarKafkaOffset = 0;

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_BAR);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      // FOO: Start of Push
      ConsumerRecord fooStartRecord =
          producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentFooKafkaOffset++, PARTITION_FOO);
      int fooLastOffset = currentFooKafkaOffset++;

      // FOO: Put record
      ConsumerRecord fooPutRecord =
          producer.getPutConsumerRecord(PARTITION_FOO, fooLastOffset, putKey, putValue, -1);

      ConsumerRecords mockResult1 = createConsumerRecords(fooStartRecord, fooPutRecord);

      // BAR: Start of Push
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> barStartRecord =
          producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentBarKafkaOffset++, PARTITION_BAR);

      // BAR: Put record which we'll use twice
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> barPutRecord =
          producer.getPutConsumerRecord(PARTITION_BAR, currentBarKafkaOffset++, putKey, putValue, -1);

      int barLastOffset = currentBarKafkaOffset++;

      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> duplicateBarPutRecord = new ConsumerRecord<>(
          barPutRecord.topic(),
          barPutRecord.partition(),
          barLastOffset, // The only difference with the previous record.
          barPutRecord.timestamp(),
          barPutRecord.timestampType(),
          barPutRecord.key(),
          barPutRecord.value()
      );

      ConsumerRecords mockResult2 = createConsumerRecords(barStartRecord, barPutRecord, barPutRecord,
          duplicateBarPutRecord);

      // FOO: End of Push
      ConsumerRecord fooEndRecord =
          producer.getControlRecord(ControlMessageType.END_OF_PUSH, currentFooKafkaOffset++, PARTITION_FOO);
      ConsumerRecords mockResult3 = createConsumerRecords(fooEndRecord);

      // BAR: End of Push
      ConsumerRecord barEndRecord =
          producer.getControlRecord(ControlMessageType.END_OF_PUSH, currentBarKafkaOffset++, PARTITION_BAR);
      ConsumerRecords mockResult4 = createConsumerRecords(barEndRecord);

      Mockito.doReturn(mockResult1)
          .doReturn(mockResult2)
          .doReturn(mockResult3)
          .doReturn(mockResult4)
          .when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      // Verify KafkaConsumer#poll is invoked.
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);
      Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR,
          barLastOffset);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testThrottling() throws Exception {
    final int PARTITION_FOO = 1;
    int currentFooKafkaOffset = 0;

    try (StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO)) {
      mockStoreConsumptionTask.subscribePartition(topic, PARTITION_FOO);
      MockProducer producer = new MockProducer(topic, SystemTime.INSTANCE);

      ConsumerRecord fooStartRecord =
        producer.getControlRecord(ControlMessageType.START_OF_PUSH, currentFooKafkaOffset++, PARTITION_FOO);

      ConsumerRecord fooPutRecord =
          producer.getPutConsumerRecord(PARTITION_FOO, currentFooKafkaOffset++, putKey, putValue, -1);

      ConsumerRecord fooDeleteRecord = producer.getDeleteConsumerRecord(PARTITION_FOO, currentFooKafkaOffset++,
          deleteKey);

      ConsumerRecords mockResult1 = createConsumerRecords(fooStartRecord, fooPutRecord, fooDeleteRecord);

      ConsumerRecords emptyResult = new ConsumerRecords<>(new HashMap<>());
      Mockito.doReturn(emptyResult)
          .doReturn(mockResult1)
          .doReturn(emptyResult)
          .when(mockKafkaConsumer).poll(StoreConsumptionTask.READ_CYCLE_DELAY_MS);

      // Prepare mockStoreRepository to send a mock storage engine.
      Mockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

      // MockKafkaConsumer is prepared. Schedule for polling.
      Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

      int totalRecordsSize = putKey.length + putValue.length + deleteKey.length;
      Mockito.verify(mockThrottler, Mockito.timeout(TIMEOUT).times(1)).maybeThrottle(totalRecordsSize);

      mockStoreConsumptionTask.close();
      testSubscribeTaskFuture.get(10, TimeUnit.SECONDS);
    }
  }
}
