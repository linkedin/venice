package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.notifier.KafkaNotifier;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 */
@PrepareForTest({StoreConsumptionTask.class, ApacheKafkaConsumer.class})
public class StoreConsumptionTaskTest extends PowerMockTestCase {

  public static final int TIMEOUT;

  static {
    StoreConsumptionTask.READ_CYCLE_DELAY_MS = 500;
    TIMEOUT = 5 * StoreConsumptionTask.READ_CYCLE_DELAY_MS;
  }

  private KafkaConsumer mockKafkaConsumer;
  private StoreRepository mockStoreRepository;
  private VeniceNotifier mockNotifier;
  private Properties mockKafkaConsumerProperties;
  private OffsetManager mockOffSetManager;
  private AbstractStorageEngine mockAbstractStorageEngine;

  private ExecutorService taskPollingService;

  private final int nodeId = 0;
  private final String topic = "TestTopic";
  private final int testPartition = 0;
  private final TopicPartition testTopicPartition = new TopicPartition(topic, testPartition);

  private final String putTestKey = "TestKeyPut";
  private final String putTestValue = "TestValuePut";
  private final String deleteTestKey = "TestKeyDelete";

  @BeforeSuite
  public void setUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    taskPollingService.shutdown();
  }

  private StoreConsumptionTask getKafkaPerStoreConsumptionTask(int... partitions) throws Exception {
    mockKafkaConsumer = PowerMockito.mock(KafkaConsumer.class);
    mockStoreRepository = PowerMockito.mock(StoreRepository.class);
    mockNotifier = PowerMockito.mock(KafkaNotifier.class);
    mockKafkaConsumerProperties = PowerMockito.mock(Properties.class);
    mockAbstractStorageEngine = PowerMockito.mock(AbstractStorageEngine.class);
    mockOffSetManager = PowerMockito.mock(OffsetManager.class);
    for(int partition: partitions) {
      PowerMockito.doReturn(OffsetRecord.NON_EXISTENT_OFFSET).when(mockOffSetManager)
          .getLastOffset(topic, partition);
    }

    PowerMockito.whenNew(KafkaConsumer.class).withParameterTypes(Properties.class)
        .withArguments(mockKafkaConsumerProperties).thenReturn(mockKafkaConsumer);

    Queue<VeniceNotifier> notifiers = new ConcurrentLinkedQueue<>();
    notifiers.add(mockNotifier);

    StoreConsumptionTask task = new StoreConsumptionTask(mockKafkaConsumerProperties, mockStoreRepository, mockOffSetManager,
            notifiers, nodeId, topic);
    return task;
  }

  /**
   * Verifies that the KafkaTaskMessages are processed appropriately by invoking corresponding method on the
   * KafkaConsumer.
   *
   * TODO: This test needs to be fixed. It's flaky.
   */
  @Test
  public void testKafkaTaskMessagesProcessing() throws Exception {
    // Get KafkaPerStoreConsumptionTask with fresh mocks to test & schedule it.
    StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask(testPartition);
    Future testSubscribeTaskFuture = taskPollingService.submit(mockStoreConsumptionTask);

    Set<TopicPartition> mockKafkaConsumerSubscriptions = new HashSet<>();
    mockKafkaConsumerSubscriptions.add(testTopicPartition);

    // Verifies KafkaPerStoreConsumptionTask#subscribePartition invokes KafkaConsumer#subscribe with expected arguments.
    mockStoreConsumptionTask.subscribePartition(topic, testPartition);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).assign(
            new ArrayList<>(mockKafkaConsumerSubscriptions));

    // Prepare the Mocked KafkaConsumer to correctly reflect the subscribed partition.
    PowerMockito.doReturn(mockKafkaConsumerSubscriptions).when(mockKafkaConsumer).assignment();

    /*
     * Verifies KafkaPerStoreConsumptionTask#resetPartitionConsumptionOffset invokes KafkaConsumer#seekToBeginning
     */
    mockStoreConsumptionTask.resetPartitionConsumptionOffset(topic, testPartition);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).seekToBeginning(testTopicPartition);

    // Verifies KafkaPerStoreConsumptionTask#unSubscribePartition invokes KafkaConsumer#unsubscribe with expected arguments.
    mockStoreConsumptionTask.unSubscribePartition(topic, testPartition);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).assign(
        new ArrayList<>(mockKafkaConsumerSubscriptions));

    mockStoreConsumptionTask.close();
    testSubscribeTaskFuture.get();

    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).close();
  }

  private ConsumerRecord<KafkaKey, KafkaValue> getConsumerRecord(OperationType type, int partition,long offset, byte[] key, byte[] value) {
    return new ConsumerRecord<>(topic, partition, offset, 0, TimestampType.NO_TIMESTAMP_TYPE,
        new KafkaKey(OperationType.WRITE, key), new KafkaValue(type, value));
  }

  private ConsumerRecord<KafkaKey, KafkaValue> getPutConsumerRecord(int partition, long offset,  byte[] key, byte[] value) {
    return getConsumerRecord(OperationType.PUT, partition, offset, key, value);
  }

  private ConsumerRecord<KafkaKey, KafkaValue> getDeleteConsumerRecord(int partition, long offset, byte[] key) {
    return getConsumerRecord(OperationType.DELETE, partition, offset, key, new byte[0]);
  }

  private ConsumerRecord<KafkaKey, KafkaValue> getControlRecord(OperationType type, long offset, int partition) {
    long jobId = -1;
    if (type != OperationType.BEGIN_OF_PUSH && type != OperationType.END_OF_PUSH) {
      throw new IllegalArgumentException("Only begin and end are control messages");
    }
    KafkaKey key = new ControlFlagKafkaKey(type, new byte[]{}, jobId);
    KafkaValue value = new KafkaValue(type);

    return new ConsumerRecord<>(topic, partition, offset, 0, TimestampType.NO_TIMESTAMP_TYPE, key, value);
  }

  private void  mockKafkaPollResult( ConsumerRecord<KafkaKey, KafkaValue>... records) {
    Map<TopicPartition, List<ConsumerRecord>> mockPollResult = new HashMap<>();

    List<ConsumerRecord> testVeniceMessages = null;
    if(records.length > 0) {
      for(ConsumerRecord record: records) {
        TopicPartition topicPartition = new TopicPartition(topic , record.partition());
        if(mockPollResult.containsKey(topicPartition)) {
          mockPollResult.get(topicPartition).add(record);
        } else {
          List<ConsumerRecord> list = new ArrayList<ConsumerRecord>();
          list.add(record);
          mockPollResult.put(topicPartition, list);
        }
      }
    }

    ConsumerRecords mockResult =  new ConsumerRecords(mockPollResult);
    PowerMockito.doReturn(mockResult).when(mockKafkaConsumer).poll(Mockito.anyLong());
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   *   1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   *   2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#put.
   */
  @Test
  public void testVeniceMessagesProcessing() throws Exception {

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    StoreConsumptionTask testSubscribeTask = getKafkaPerStoreConsumptionTask(testPartition);
    testSubscribeTask.subscribePartition(topic, testPartition);

    final long LAST_OFFSET= 15;
    ConsumerRecord testPutRecord = getPutConsumerRecord(testPartition, 10, putTestKey.getBytes(), putTestValue.getBytes());
    ConsumerRecord testDeleteRecord = getDeleteConsumerRecord(testPartition, LAST_OFFSET, deleteTestKey.getBytes());
    ConsumerRecord ignorePutRecord = getPutConsumerRecord(testPartition, 13, "Low-Offset-Ignored".getBytes(), "ignored-put".getBytes());
    ConsumerRecord ignoreDeleteRecord = getDeleteConsumerRecord(testPartition, 15, "Equal-Offset-Ignored".getBytes());

    // Prepare the mockKafkaConsumer to send the test poll results.
    mockKafkaPollResult(testPutRecord, testDeleteRecord, ignorePutRecord, ignoreDeleteRecord);

    // Prepare mockStoreRepository to send a mock storage engine.
    PowerMockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

    // MockKafkaConsumer is prepared. Schedule for polling.
    Future testSubscribeTaskFuture = taskPollingService.submit(testSubscribeTask);

    // Verify it retrieves the offset from the OffSet Manager
    Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).getLastOffset(topic, testPartition);

    // Verify KafkaConsumer#poll is invoked.
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
    // Verify StorageEngine#put is invoked only once and with appropriate key & value.
    Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
            .put(eq(testPartition), any(), any());
    Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1))
        .put(testPartition, putTestKey.getBytes(), putTestValue.getBytes());

    // Verify StorageEngine#Delete is invoked only once and with appropriate key.
    Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1)).delete(eq(testPartition),
            any());
    Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).times(1)).delete(testPartition,
            deleteTestKey.getBytes());

    // Verify it commits the offset to Offset Manager
    OffsetRecord expected = new OffsetRecord(LAST_OFFSET);
    Mockito.verify(mockOffSetManager, Mockito.timeout(TIMEOUT).times(1)).recordOffset(topic, testPartition, expected);


    testSubscribeTask.close();
    testSubscribeTaskFuture.get();
  }

  @Test
  public void testNotifier() throws Exception {
    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    final int PARTITION_FOO = 1;
    final int PARTITION_BAR = 2;
    int currentOffset = 0;

    StoreConsumptionTask testSubscribeTask = getKafkaPerStoreConsumptionTask(PARTITION_FOO, PARTITION_BAR);
    testSubscribeTask.subscribePartition(topic, PARTITION_FOO);
    testSubscribeTask.subscribePartition(topic, PARTITION_BAR);

    Map<TopicPartition, List<ConsumerRecord>> mockPollResult = new HashMap<>();
    ConsumerRecord fooStartRecord = getControlRecord(OperationType.BEGIN_OF_PUSH, currentOffset++ , PARTITION_FOO);
    int fooLastOffset = currentOffset++;
    ConsumerRecord fooPutRecord = getPutConsumerRecord(PARTITION_FOO , fooLastOffset, putTestKey.getBytes(), putTestValue.getBytes());

    mockKafkaPollResult(fooStartRecord, fooPutRecord);

    // Prepare mockStoreRepository to send a mock storage engine.
    PowerMockito.doReturn(mockAbstractStorageEngine).when(mockStoreRepository).getLocalStorageEngine(topic);

    // MockKafkaConsumer is prepared. Schedule for polling.
    Future testSubscribeTaskFuture = taskPollingService.submit(testSubscribeTask);

    // Verify KafkaConsumer#poll is invoked.
    Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_FOO);

    ConsumerRecord barStartRecord = getControlRecord(OperationType.BEGIN_OF_PUSH, currentOffset++ , PARTITION_BAR);
    int barLastOffset = currentOffset++;
    ConsumerRecord barPutRecord = getPutConsumerRecord(PARTITION_BAR , barLastOffset, putTestKey.getBytes(), putTestValue.getBytes());

    mockKafkaPollResult(barStartRecord, barPutRecord);
    Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).started(topic, PARTITION_BAR);

    ConsumerRecord fooEndRecord = getControlRecord(OperationType.END_OF_PUSH, currentOffset ++, PARTITION_FOO);
    mockKafkaPollResult(fooEndRecord);
    Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset);

    ConsumerRecord barEndRecord = getControlRecord(OperationType.END_OF_PUSH, currentOffset ++ , PARTITION_BAR);
    mockKafkaPollResult(barEndRecord);
    Mockito.verify(mockNotifier, Mockito.timeout(TIMEOUT).atLeastOnce()).completed(topic, PARTITION_BAR, barLastOffset);

    testSubscribeTask.close();
    testSubscribeTaskFuture.get();
  }
}