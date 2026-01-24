package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ConsumptionTaskTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private ExecutorService crossTpProcessingPool;
  private AggKafkaConsumerServiceStats mockAggStats;
  private KafkaConsumerServiceStats mockStoreStats;
  private ConsumerSubscriptionCleaner mockCleaner;
  private ConsumerPollTracker mockPollTracker;

  @BeforeMethod
  public void setUp() {
    crossTpProcessingPool = Executors.newFixedThreadPool(4);
    mockAggStats = mock(AggKafkaConsumerServiceStats.class);
    mockStoreStats = mock(KafkaConsumerServiceStats.class);
    when(mockAggStats.getStoreStats(any())).thenReturn(mockStoreStats);
    mockCleaner = mock(ConsumerSubscriptionCleaner.class);
    mockPollTracker = mock(ConsumerPollTracker.class);
  }

  @AfterMethod
  public void tearDown() {
    if (crossTpProcessingPool != null) {
      crossTpProcessingPool.shutdownNow();
    }
  }

  @Test
  public void testSequentialProcessingWhenPoolIsNull() throws Exception {
    // Create a ConsumptionTask without cross-TP pool (sequential mode)
    AtomicInteger pollCount = new AtomicInteger(0);
    AtomicBoolean stopFlag = new AtomicBoolean(false);

    PubSubTopic versionTopic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(versionTopic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(versionTopic, 1);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    List<DefaultPubSubMessage> messages1 = createMockMessages(5);
    List<DefaultPubSubMessage> messages2 = createMockMessages(3);
    pollResult.put(tp1, messages1);
    pollResult.put(tp2, messages2);

    Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction = () -> {
      if (pollCount.incrementAndGet() == 1) {
        return pollResult;
      }
      stopFlag.set(true);
      return new HashMap<>();
    };

    ConsumedDataReceiver<List<DefaultPubSubMessage>> mockReceiver1 = mock(ConsumedDataReceiver.class);
    ConsumedDataReceiver<List<DefaultPubSubMessage>> mockReceiver2 = mock(ConsumedDataReceiver.class);
    when(mockReceiver1.destinationIdentifier()).thenReturn(versionTopic);
    when(mockReceiver2.destinationIdentifier()).thenReturn(versionTopic);

    ConsumptionTask task = new ConsumptionTask(
        "test-consumer",
        0,
        10,
        pollFunction,
        bytes -> {},
        records -> {},
        mockAggStats,
        mockCleaner,
        mockPollTracker,
        null // No cross-TP pool - sequential mode
    );

    task.setDataReceiver(tp1, mockReceiver1);
    task.setDataReceiver(tp2, mockReceiver2);

    // Run the task in a separate thread and stop after first poll
    Thread taskThread = new Thread(() -> {
      while (!stopFlag.get()) {
        try {
          // Simulate one iteration
          task.run();
        } catch (Exception e) {
          break;
        }
      }
    });
    taskThread.start();

    // Wait for processing
    Thread.sleep(100);
    task.stop();
    taskThread.join(1000);

    // Verify both receivers were called
    verify(mockReceiver1, times(1)).write(messages1);
    verify(mockReceiver2, times(1)).write(messages2);
  }

  @Test
  public void testParallelProcessingWithMultipleTPs() throws Exception {
    // Create a ConsumptionTask with cross-TP pool (parallel mode)
    AtomicInteger pollCount = new AtomicInteger(0);
    CountDownLatch processingStarted = new CountDownLatch(2);
    CountDownLatch allowCompletion = new CountDownLatch(1);

    PubSubTopic versionTopic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(versionTopic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(versionTopic, 1);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    List<DefaultPubSubMessage> messages1 = createMockMessages(5);
    List<DefaultPubSubMessage> messages2 = createMockMessages(3);
    pollResult.put(tp1, messages1);
    pollResult.put(tp2, messages2);

    Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction = () -> {
      if (pollCount.incrementAndGet() == 1) {
        return pollResult;
      }
      return new HashMap<>();
    };

    ConsumedDataReceiver<List<DefaultPubSubMessage>> mockReceiver1 = mock(ConsumedDataReceiver.class);
    ConsumedDataReceiver<List<DefaultPubSubMessage>> mockReceiver2 = mock(ConsumedDataReceiver.class);
    when(mockReceiver1.destinationIdentifier()).thenReturn(versionTopic);
    when(mockReceiver2.destinationIdentifier()).thenReturn(versionTopic);

    // Track when processing starts for each TP to verify parallelism
    doAnswer(invocation -> {
      processingStarted.countDown();
      if (!allowCompletion.await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timed out waiting for allowCompletion latch");
      }
      return null;
    }).when(mockReceiver1).write(any());

    doAnswer(invocation -> {
      processingStarted.countDown();
      if (!allowCompletion.await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timed out waiting for allowCompletion latch");
      }
      return null;
    }).when(mockReceiver2).write(any());

    ConsumptionTask task = new ConsumptionTask(
        "test-consumer",
        0,
        10,
        pollFunction,
        bytes -> {},
        records -> {},
        mockAggStats,
        mockCleaner,
        mockPollTracker,
        crossTpProcessingPool // Enable parallel processing
    );

    task.setDataReceiver(tp1, mockReceiver1);
    task.setDataReceiver(tp2, mockReceiver2);

    // Run the task in a separate thread
    Thread taskThread = new Thread(task);
    taskThread.start();

    // Wait for both TPs to start processing (proves parallelism)
    boolean bothStarted = processingStarted.await(2, TimeUnit.SECONDS);
    Assert.assertTrue(bothStarted, "Both TPs should start processing in parallel");

    // Allow completion
    allowCompletion.countDown();

    // Wait a bit and stop
    Thread.sleep(100);
    task.stop();
    taskThread.join(1000);

    // Verify both receivers were called
    verify(mockReceiver1, times(1)).write(messages1);
    verify(mockReceiver2, times(1)).write(messages2);
  }

  @Test
  public void testSingleTPFallsBackToSequential() throws Exception {
    // When there's only 1 TP, even with pool enabled, it should use sequential path
    AtomicInteger pollCount = new AtomicInteger(0);

    PubSubTopic versionTopic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(versionTopic, 0);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    List<DefaultPubSubMessage> messages1 = createMockMessages(5);
    pollResult.put(tp1, messages1);

    Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction = () -> {
      if (pollCount.incrementAndGet() == 1) {
        return pollResult;
      }
      return new HashMap<>();
    };

    ConsumedDataReceiver<List<DefaultPubSubMessage>> mockReceiver1 = mock(ConsumedDataReceiver.class);
    when(mockReceiver1.destinationIdentifier()).thenReturn(versionTopic);

    ConsumptionTask task = new ConsumptionTask(
        "test-consumer",
        0,
        10,
        pollFunction,
        bytes -> {},
        records -> {},
        mockAggStats,
        mockCleaner,
        mockPollTracker,
        crossTpProcessingPool // Pool enabled but only 1 TP
    );

    task.setDataReceiver(tp1, mockReceiver1);

    // Run the task in a separate thread
    Thread taskThread = new Thread(task);
    taskThread.start();

    // Wait for processing
    Thread.sleep(100);
    task.stop();
    taskThread.join(1000);

    // Verify receiver was called
    verify(mockReceiver1, times(1)).write(messages1);
  }

  @Test
  public void testMissingReceiverHandledInParallelMode() throws Exception {
    AtomicInteger pollCount = new AtomicInteger(0);

    PubSubTopic versionTopic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(versionTopic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(versionTopic, 1);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    List<DefaultPubSubMessage> messages1 = createMockMessages(5);
    List<DefaultPubSubMessage> messages2 = createMockMessages(3);
    pollResult.put(tp1, messages1);
    pollResult.put(tp2, messages2);

    Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction = () -> {
      if (pollCount.incrementAndGet() == 1) {
        return pollResult;
      }
      return new HashMap<>();
    };

    ConsumedDataReceiver<List<DefaultPubSubMessage>> mockReceiver1 = mock(ConsumedDataReceiver.class);
    when(mockReceiver1.destinationIdentifier()).thenReturn(versionTopic);
    // Note: No receiver set for tp2

    ConsumptionTask task = new ConsumptionTask(
        "test-consumer",
        0,
        10,
        pollFunction,
        bytes -> {},
        records -> {},
        mockAggStats,
        mockCleaner,
        mockPollTracker,
        crossTpProcessingPool);

    task.setDataReceiver(tp1, mockReceiver1);
    // tp2 has no receiver - should be handled gracefully

    // Run the task in a separate thread
    Thread taskThread = new Thread(task);
    taskThread.start();

    // Wait for processing
    Thread.sleep(100);
    task.stop();
    taskThread.join(1000);

    // Verify only tp1's receiver was called
    verify(mockReceiver1, times(1)).write(messages1);
  }

  private List<DefaultPubSubMessage> createMockMessages(int count) {
    List<DefaultPubSubMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
      when(msg.getPayloadSize()).thenReturn(100);
      messages.add(msg);
    }
    return messages;
  }
}
