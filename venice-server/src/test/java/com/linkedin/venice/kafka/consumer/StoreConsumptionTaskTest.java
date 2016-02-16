package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 */
@PrepareForTest({StoreConsumptionTask.class, ApacheKafkaConsumer.class})
public class StoreConsumptionTaskTest extends PowerMockTestCase {

  public static final int TIMEOUT = 1000;
  private KafkaConsumer mockKafkaConsumer;
  private StoreRepository mockStoreRepository;
  private VeniceNotifier mockNotifier;
  private Properties mockKafkaConsumerProperties;
  private AbstractStorageEngine mockAbstractStorageEngine;

  private ExecutorService taskPollingService;

  private final int nodeId = 0;
  private final String topic = "TestTopic";
  private final int testPartition = 0;
  private final TopicPartition testTopicPartition = new TopicPartition(topic, testPartition);

  private final String putTestKey = "TestKeyPut";
  private final String putTestValue = "TestValuePut";
  private final String deleteTestKey = "TestKeyDelete";
  private final String deleteTestValue = "TestValueDelete";

  @BeforeSuite
  public void setUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);
  }

  @AfterSuite
  public void tearDown() throws Exception {
    taskPollingService.shutdown();
  }

  private StoreConsumptionTask getKafkaPerStoreConsumptionTask() throws Exception {
    mockKafkaConsumer = PowerMockito.mock(KafkaConsumer.class);
    mockStoreRepository = PowerMockito.mock(StoreRepository.class);
    mockNotifier = PowerMockito.mock(KafkaNotifier.class);
    mockKafkaConsumerProperties = PowerMockito.mock(Properties.class);
    mockAbstractStorageEngine = PowerMockito.mock(AbstractStorageEngine.class);

    PowerMockito.whenNew(KafkaConsumer.class).withParameterTypes(Properties.class)
        .withArguments(mockKafkaConsumerProperties).thenReturn(mockKafkaConsumer);

    return new StoreConsumptionTask(mockKafkaConsumerProperties, mockStoreRepository,
            mockNotifier, nodeId, topic);
  }

  /**
   * Verifies that the KafkaTaskMessages are processed appropriately by invoking corresponding method on the
   * KafkaConsumer.
   */
  @Test
  public void testKafkaTaskMessagesProcessing() throws Exception {
    // Get KafkaPerStoreConsumptionTask with fresh mocks to test & schedule it.
    StoreConsumptionTask mockStoreConsumptionTask = getKafkaPerStoreConsumptionTask();
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
     * Verifies KafkaPerStoreConsumptionTask#resetPartitionConsumptionOffset invokes KafkaConsumer#seekToBeginning &
     * KafkaConsumer#commit with expected arguments.
     */
    mockStoreConsumptionTask.resetPartitionConsumptionOffset(topic, testPartition);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).seekToBeginning(testTopicPartition);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).commit(Mockito.anyMap(),
        Mockito.eq(CommitType.SYNC));

    // Verifies KafkaPerStoreConsumptionTask#unSubscribePartition invokes KafkaConsumer#unsubscribe with expected arguments.
    mockStoreConsumptionTask.unSubscribePartition(topic, testPartition);
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).times(1)).assign(
            new ArrayList<>(mockKafkaConsumerSubscriptions));

    mockStoreConsumptionTask.close();
    testSubscribeTaskFuture.get();
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   *   1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   *   2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#put.
   */
  @Test
  public void testVeniceMessagesProcessing() throws Exception {

    // Get the KafkaPerStoreConsumptionTask with fresh mocks.
    StoreConsumptionTask testSubscribeTask = getKafkaPerStoreConsumptionTask();
    testSubscribeTask.subscribePartition(topic, testPartition);

    // Prepare poll results.
    Map<TopicPartition, List<ConsumerRecord>> mockPollResult = new HashMap<>();
    List<ConsumerRecord> testVeniceMessages = new ArrayList<>();
    ConsumerRecord<KafkaKey, KafkaValue> testPutRecord = new ConsumerRecord(topic, testPartition, 10, new KafkaKey(
        OperationType.WRITE, putTestKey.getBytes()), new KafkaValue(OperationType.PUT, putTestValue.getBytes()));
    ConsumerRecord<KafkaKey, KafkaValue> testDeleteRecord =
        new ConsumerRecord(topic, testPartition, 11, new KafkaKey(OperationType.WRITE, deleteTestKey.getBytes()),
            new KafkaValue(OperationType.DELETE, deleteTestValue.getBytes()));
    testVeniceMessages.add(testPutRecord);
    testVeniceMessages.add(testDeleteRecord);
    mockPollResult.put(testTopicPartition, testVeniceMessages);
    ConsumerRecords testPollConsumerRecords = new ConsumerRecords(mockPollResult);

    // Prepare the mockKafkaConsumer to send the test poll results.
    PowerMockito.when(mockKafkaConsumer.poll(Mockito.anyLong())).thenReturn(testPollConsumerRecords);
    // Prepare mockStoreRepository to send a mock storage engine.
    PowerMockito.when(mockStoreRepository.getLocalStorageEngine(topic)).thenReturn(mockAbstractStorageEngine);

    // MockKafkaConsumer is prepared. Schedule for polling.
    Future testSubscribeTaskFuture = taskPollingService.submit(testSubscribeTask);

    // Verify KafkaConsumer#poll is invoked.
    Mockito.verify(mockKafkaConsumer, Mockito.timeout(TIMEOUT).atLeastOnce()).poll(Mockito.anyLong());
    // Verify StorageEngine#put is invoked with appropriate key & value.
    Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).atLeastOnce())
        .put(testPartition, putTestKey.getBytes(), putTestValue.getBytes());
    // Verify StorageEngine#put is invoked with appropriate key.
    Mockito.verify(mockAbstractStorageEngine, Mockito.timeout(TIMEOUT).atLeastOnce()).delete(testPartition,
        deleteTestKey.getBytes());

    testSubscribeTask.close();
    testSubscribeTaskFuture.get();
  }

}