package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ConsumptionTaskTest {
  private static final int DEFAULT_PAYLOAD_SIZE = 100;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private ExecutorService crossTpProcessingPool;
  private AggKafkaConsumerServiceStats mockAggStats;
  private KafkaConsumerServiceStats mockStoreStats;
  private ConsumerSubscriptionCleaner mockCleaner;
  private ConsumerPollTracker mockPollTracker;

  @BeforeMethod
  public void setUp() {
    crossTpProcessingPool = Executors.newFixedThreadPool(4, new DaemonThreadFactory("ConsumptionTaskTest"));
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

  /**
   * Tests sequential processing (pool=null) with metrics verification.
   * Verifies: message delivery, poll latency, producing latency, message count, throttling.
   */
  @Test
  public void testSequentialProcessingAndMetrics() throws Exception {
    CountDownLatch firstPollComplete = new CountDownLatch(1);
    AtomicInteger pollCount = new AtomicInteger(0);
    AtomicInteger bandwidthBytes = new AtomicInteger(0);
    AtomicInteger recordCount = new AtomicInteger(0);

    PubSubTopic topic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(topic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(topic, 1);

    List<DefaultPubSubMessage> messages1 = createMockMessages(5, 100); // 500 bytes
    List<DefaultPubSubMessage> messages2 = createMockMessages(3, 200); // 600 bytes
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = createPollResult(tp1, messages1, tp2, messages2);

    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver1 = createMockReceiver(topic);
    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver2 = createMockReceiver(topic);

    ConsumptionTask task = createTask(
        createOneShotPollFunction(pollCount, pollResult, firstPollComplete),
        bandwidthBytes::addAndGet,
        recordCount::addAndGet,
        null); // Sequential mode

    task.setDataReceiver(tp1, receiver1);
    task.setDataReceiver(tp2, receiver2);

    runTaskUntilLatch(task, firstPollComplete);

    // Verify message delivery
    verify(receiver1, times(1)).write(messages1);
    verify(receiver2, times(1)).write(messages2);

    // Verify metrics
    verify(mockAggStats, atLeastOnce()).recordTotalPollRequestLatency(any(Double.class));
    verify(mockAggStats, times(1)).recordTotalConsumerRecordsProducingToWriterBufferLatency(any(Double.class));
    verify(mockAggStats, times(1)).recordTotalNonZeroPollResultNum(8);
    verify(mockStoreStats, times(1)).recordPollResultNum(8);
    verify(mockStoreStats, times(1)).recordByteSizePerPoll(1100);

    // Verify throttling
    Assert.assertEquals(bandwidthBytes.get(), 1100);
    Assert.assertEquals(recordCount.get(), 8);
  }

  /**
   * Tests parallel processing proves both TPs start processing concurrently.
   */
  @Test
  public void testParallelProcessingWithMultipleTPs() throws Exception {
    AtomicInteger pollCount = new AtomicInteger(0);
    CountDownLatch processingStarted = new CountDownLatch(2);
    CountDownLatch allowCompletion = new CountDownLatch(1);

    PubSubTopic topic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(topic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(topic, 1);

    List<DefaultPubSubMessage> messages1 = createMockMessages(5);
    List<DefaultPubSubMessage> messages2 = createMockMessages(3);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = createPollResult(tp1, messages1, tp2, messages2);

    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver1 = createMockReceiver(topic);
    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver2 = createMockReceiver(topic);

    // Block receivers to verify parallel start
    doAnswer(inv -> {
      processingStarted.countDown();
      if (!allowCompletion.await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timeout waiting for allowCompletion latch");
      }
      return null;
    }).when(receiver1).write(any());
    doAnswer(inv -> {
      processingStarted.countDown();
      if (!allowCompletion.await(5, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timeout waiting for allowCompletion latch");
      }
      return null;
    }).when(receiver2).write(any());

    ConsumptionTask task = createTask(
        createOneShotPollFunction(pollCount, pollResult, null),
        bytes -> {},
        records -> {},
        crossTpProcessingPool);

    task.setDataReceiver(tp1, receiver1);
    task.setDataReceiver(tp2, receiver2);

    Thread taskThread = new Thread(task);
    taskThread.start();

    // Both TPs should start concurrently (proves parallelism)
    Assert.assertTrue(processingStarted.await(2, TimeUnit.SECONDS), "Both TPs should start in parallel");
    allowCompletion.countDown();

    Thread.sleep(50);
    task.stop();
    taskThread.join(1000);

    verify(receiver1, times(1)).write(messages1);
    verify(receiver2, times(1)).write(messages2);
  }

  /**
   * Tests single TP falls back to sequential even with pool enabled.
   */
  @Test
  public void testSingleTPFallsBackToSequential() throws Exception {
    CountDownLatch firstPollComplete = new CountDownLatch(1);
    AtomicInteger pollCount = new AtomicInteger(0);

    PubSubTopic topic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(topic, 0);

    List<DefaultPubSubMessage> messages = createMockMessages(5);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    pollResult.put(tp1, messages);

    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver = createMockReceiver(topic);

    ConsumptionTask task = createTask(
        createOneShotPollFunction(pollCount, pollResult, firstPollComplete),
        bytes -> {},
        records -> {},
        crossTpProcessingPool); // Pool enabled but only 1 TP

    task.setDataReceiver(tp1, receiver);
    runTaskUntilLatch(task, firstPollComplete);

    verify(receiver, times(1)).write(messages);
  }

  /**
   * Tests missing receiver handling with metrics (parallel mode).
   * Verifies: graceful handling, missing receiver metric, cleaner.unsubscribe called.
   */
  @Test
  public void testMissingReceiverHandlingAndMetrics() throws Exception {
    CountDownLatch firstPollComplete = new CountDownLatch(1);
    AtomicInteger pollCount = new AtomicInteger(0);

    PubSubTopic topic = pubSubTopicRepository.getTopic("test_store_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(topic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(topic, 1);

    List<DefaultPubSubMessage> messages1 = createMockMessages(5);
    List<DefaultPubSubMessage> messages2 = createMockMessages(3);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = createPollResult(tp1, messages1, tp2, messages2);

    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver1 = createMockReceiver(topic);
    // Note: No receiver for tp2

    ConsumptionTask task = createTask(
        createOneShotPollFunction(pollCount, pollResult, firstPollComplete),
        bytes -> {},
        records -> {},
        crossTpProcessingPool);

    task.setDataReceiver(tp1, receiver1);
    runTaskUntilLatch(task, firstPollComplete);

    // Verify tp1 processed, tp2 gracefully handled
    verify(receiver1, times(1)).write(messages1);

    // Verify metrics for missing receiver
    verify(mockAggStats, atLeastOnce()).recordTotalDetectedNoRunningIngestionTopicPartitionNum(1);
    verify(mockCleaner, atLeastOnce()).unsubscribe(any());
  }

  /**
   * Tests empty poll records latency but not producing stats.
   */
  @Test
  public void testEmptyPollMetrics() throws Exception {
    CountDownLatch pollOccurred = new CountDownLatch(1);

    ConsumptionTask task = createTask(() -> {
      pollOccurred.countDown();
      return new HashMap<>();
    }, bytes -> {}, records -> {}, null);

    runTaskUntilLatch(task, pollOccurred);

    verify(mockAggStats, atLeastOnce()).recordTotalPollRequestLatency(any(Double.class));
    verify(mockAggStats, never()).recordTotalConsumerRecordsProducingToWriterBufferLatency(any(Double.class));
    verify(mockAggStats, never()).recordTotalNonZeroPollResultNum(any(Integer.class));
  }

  /**
   * Tests poll error metric recording.
   */
  @Test
  public void testPollErrorMetrics() throws Exception {
    CountDownLatch errorsOccurred = new CountDownLatch(2);
    AtomicInteger pollCount = new AtomicInteger(0);

    ConsumptionTask task = createTask(() -> {
      if (pollCount.incrementAndGet() <= 2) {
        errorsOccurred.countDown();
        throw new RuntimeException("Simulated poll error");
      }
      return new HashMap<>();
    }, bytes -> {}, records -> {}, null);

    runTaskUntilLatch(task, errorsOccurred);

    verify(mockAggStats, atLeast(2)).recordTotalPollError();
  }

  /**
   * Tests per-store metrics when multiple stores have partitions in same poll.
   */
  @Test
  public void testPerStoreMetrics() throws Exception {
    CountDownLatch firstPollComplete = new CountDownLatch(1);
    AtomicInteger pollCount = new AtomicInteger(0);

    PubSubTopic store1Topic = pubSubTopicRepository.getTopic("store1_v1");
    PubSubTopic store2Topic = pubSubTopicRepository.getTopic("store2_v1");
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(store1Topic, 0);
    PubSubTopicPartition tp2 = new PubSubTopicPartitionImpl(store2Topic, 0);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    pollResult.put(tp1, createMockMessages(5, 100)); // store1: 500 bytes
    pollResult.put(tp2, createMockMessages(3, 200)); // store2: 600 bytes

    KafkaConsumerServiceStats store1Stats = mock(KafkaConsumerServiceStats.class);
    KafkaConsumerServiceStats store2Stats = mock(KafkaConsumerServiceStats.class);
    when(mockAggStats.getStoreStats("store1")).thenReturn(store1Stats);
    when(mockAggStats.getStoreStats("store2")).thenReturn(store2Stats);

    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver1 = createMockReceiver(store1Topic);
    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver2 = createMockReceiver(store2Topic);

    ConsumptionTask task = createTask(
        createOneShotPollFunction(pollCount, pollResult, firstPollComplete),
        bytes -> {},
        records -> {},
        null);

    task.setDataReceiver(tp1, receiver1);
    task.setDataReceiver(tp2, receiver2);

    runTaskUntilLatch(task, firstPollComplete);

    // Verify per-store stats
    verify(store1Stats, times(1)).recordPollResultNum(5);
    verify(store1Stats, times(1)).recordByteSizePerPoll(500);
    verify(store2Stats, times(1)).recordPollResultNum(3);
    verify(store2Stats, times(1)).recordByteSizePerPoll(600);
    verify(mockAggStats, times(1)).recordTotalNonZeroPollResultNum(8);
  }

  // ==================== Helper Methods ====================

  private ConsumptionTask createTask(
      Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction,
      IntConsumer bandwidthThrottler,
      IntConsumer recordsThrottler,
      ExecutorService pool) {
    return new ConsumptionTask(
        "test-consumer",
        0,
        10,
        pollFunction,
        bandwidthThrottler,
        recordsThrottler,
        mockAggStats,
        mockCleaner,
        mockPollTracker,
        pool);
  }

  private Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> createOneShotPollFunction(
      AtomicInteger pollCount,
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult,
      CountDownLatch signalAfterFirst) {
    return () -> {
      int count = pollCount.incrementAndGet();
      if (count == 1) {
        return pollResult;
      }
      if (signalAfterFirst != null) {
        signalAfterFirst.countDown();
      }
      return new HashMap<>();
    };
  }

  private void runTaskUntilLatch(ConsumptionTask task, CountDownLatch latch) throws Exception {
    Thread taskThread = new Thread(task);
    taskThread.start();
    Assert.assertTrue(latch.await(2, TimeUnit.SECONDS), "Latch should be triggered");
    task.stop();
    taskThread.join(1000);
  }

  @SuppressWarnings("unchecked")
  private ConsumedDataReceiver<List<DefaultPubSubMessage>> createMockReceiver(PubSubTopic topic) {
    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver = mock(ConsumedDataReceiver.class);
    when(receiver.destinationIdentifier()).thenReturn(topic);
    return receiver;
  }

  private Map<PubSubTopicPartition, List<DefaultPubSubMessage>> createPollResult(
      PubSubTopicPartition tp1,
      List<DefaultPubSubMessage> msgs1,
      PubSubTopicPartition tp2,
      List<DefaultPubSubMessage> msgs2) {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> result = new HashMap<>();
    result.put(tp1, msgs1);
    result.put(tp2, msgs2);
    return result;
  }

  private List<DefaultPubSubMessage> createMockMessages(int count) {
    return createMockMessages(count, DEFAULT_PAYLOAD_SIZE);
  }

  private List<DefaultPubSubMessage> createMockMessages(int count, int payloadSize) {
    List<DefaultPubSubMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
      when(msg.getPayloadSize()).thenReturn(payloadSize);
      messages.add(msg);
    }
    return messages;
  }
}
