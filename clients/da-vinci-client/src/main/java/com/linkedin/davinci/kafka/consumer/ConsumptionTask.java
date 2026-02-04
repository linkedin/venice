package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.Rate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This {@link Runnable} is a loop encapsulating the minimal amount of state and function handles in order to pipe
 * consumed messages into their intended {@link ConsumedDataReceiver} instances.
 *
 * It can poll messages but without knowing the exact polling strategy, nor can it do anything else directly to mutate
 * the state of the consumer. It can do so indirectly however by interacting with a {@link ConsumerSubscriptionCleaner}.
 * In the future, we may want to consider adding more subscription APIs into this class such that it could become the
 * only point of entry into a given consumer instance, which could allow us to ditch the {@link SharedKafkaConsumer}
 * and to safely use an unsynchronized consumer instead. For now, there are still multiple code paths which can end up
 * affecting the consumer used by this task, so it is still necessary to use a threadsafe one.
 *
 * Besides polling, on each iteration of the {@link #run()} loop, the following responsibilities are also fulfilled:
 * 1. Invoking the functions in {@link ConsumerSubscriptionCleaner} to ensure the consumer's subscriptions are valid.
 * 2. Invoking two throttlers, for bandwidth and records throughput.
 * 3. Recording some stats.
 */
class ConsumptionTask implements Runnable {
  private static final MetricConfig DEFAULT_METRIC_CONFIG = new MetricConfig();
  private static final PartitionStats EMPTY_PARTITION_STATS = new PartitionStats();

  private final Logger LOGGER;
  private final int taskId;
  private final String consumptionTaskIdStr;
  private final Map<PubSubTopicPartition, ConsumedDataReceiver<List<DefaultPubSubMessage>>> dataReceiverMap =
      new VeniceConcurrentHashMap<>();
  private final long readCycleDelayMs;
  private final Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction;
  private final IntConsumer bandwidthThrottler;
  private final IntConsumer recordsThrottler;
  private final AggKafkaConsumerServiceStats aggStats;
  private final ConsumerSubscriptionCleaner cleaner;
  private final ConsumerPollTracker consumerPollTracker;
  private final ExecutorService crossTpProcessingPool;

  /**
   * Maintain rate counter with default window size to calculate the message and bytes rate at topic partition level.
   * Those topic partition level information will not be emitted out as a metric, to avoid emitting too many metrics per
   * server host, they are for admin tool debugging purpose.
   */
  private final Map<PubSubTopicPartition, LivePartitionStats> partitionToStatsMap = new VeniceConcurrentHashMap<>();

  private volatile boolean running = true;

  /**
   * Timestamp of the last poll. Initialized at construction time, in case the consumer task thread gets stuck from
   * the get-go.
   */
  private volatile long lastSuccessfulPollTimestamp = System.currentTimeMillis();

  /**
   * If a topic partition has not got any record polled back, we use -1 for the last poll timestamp.
   */
  public final static long DEFAULT_TOPIC_PARTITION_NO_POLL_TIMESTAMP = -1L;

  public ConsumptionTask(
      final String consumerNamePrefix,
      final int taskId,
      final long readCycleDelayMs,
      final Supplier<Map<PubSubTopicPartition, List<DefaultPubSubMessage>>> pollFunction,
      final IntConsumer bandwidthThrottler,
      final IntConsumer recordsThrottler,
      final AggKafkaConsumerServiceStats aggStats,
      final ConsumerSubscriptionCleaner cleaner,
      final ConsumerPollTracker consumerPollTracker,
      final ExecutorService crossTpProcessingPool) {
    this.readCycleDelayMs = readCycleDelayMs;
    this.pollFunction = pollFunction;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.aggStats = aggStats;
    this.cleaner = cleaner;
    this.consumerPollTracker = consumerPollTracker;
    this.crossTpProcessingPool = crossTpProcessingPool;
    this.taskId = taskId;
    this.consumptionTaskIdStr = Utils.getSanitizedStringForLogger(consumerNamePrefix) + " - " + taskId;
    this.LOGGER = LogManager.getLogger(getClass().getSimpleName() + "[ " + consumptionTaskIdStr + " ]");
  }

  @Override
  public void run() {
    boolean addSomeDelay = false;
    Set<PubSubTopicPartition> topicPartitionsToUnsub = new HashSet<>();
    Map<String, StorePollCounter> storePollCounterMap = new HashMap<>();

    try {
      while (running) {
        try {
          if (addSomeDelay) {
            synchronized (this) {
              wait(readCycleDelayMs);
            }
          }
          if (!running) {
            break;
          }
          long beforePollingTimestamp = System.currentTimeMillis();
          processAndClearUnsubscriptions(topicPartitionsToUnsub);

          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledMessages = pollFunction.get();
          lastSuccessfulPollTimestamp = System.currentTimeMillis();
          aggStats.recordTotalPollRequestLatency(lastSuccessfulPollTimestamp - beforePollingTimestamp);

          if (polledMessages.isEmpty()) {
            addSomeDelay = true;
          } else {
            addSomeDelay = processPollResults(
                polledMessages,
                lastSuccessfulPollTimestamp,
                topicPartitionsToUnsub,
                storePollCounterMap);
          }
        } catch (Exception e) {
          if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
            LOGGER.error("Received InterruptedException, will exit");
            break;
          }
          LOGGER.error("Received exception while polling, will retry", e);
          aggStats.recordTotalPollError();
          addSomeDelay = true;
        }
      }
    } catch (Throwable t) {
      LOGGER.error(
          "Shared consumer thread: {} exited due to an unexpected exception",
          Thread.currentThread().getName(),
          t);
    } finally {
      LOGGER.info("Shared consumer thread: {} exited", Thread.currentThread().getName());
    }
  }

  /**
  * Processes pending unsubscriptions by removing partitions from data receiver map,
  * notifying receivers of topic deletion, and updating the poll tracker.
  * The set is cleared by the cleaner at the start and can be reused for collecting
  * partitions with missing receivers during poll processing.
  *
  * @param topicPartitionsToUnsub reusable set (cleared by cleaner, populated with partitions to unsubscribe)
  */
  private void processAndClearUnsubscriptions(Set<PubSubTopicPartition> topicPartitionsToUnsub) {
    cleaner.getTopicPartitionsToUnsubscribe(topicPartitionsToUnsub);
    for (PubSubTopicPartition topicPartitionToUnSub: topicPartitionsToUnsub) {
      ConsumedDataReceiver<List<DefaultPubSubMessage>> dataReceiver = dataReceiverMap.remove(topicPartitionToUnSub);
      if (dataReceiver != null) {
        dataReceiver.notifyOfTopicDeletion(topicPartitionToUnSub.getPubSubTopic().getName());
      }
      consumerPollTracker.removeTopicPartition(topicPartitionToUnSub);
    }
    topicPartitionsToUnsub.clear();
  }

  /**
   * Processes poll results when messages are available. Aggregates results from all topic-partitions,
   * records stats, applies throttling, and handles cleanup of partitions with missing receivers.
   *
   * @param polledPubSubMessages messages from poll
   * @param pollTimestamp when poll completed
   * @param topicPartitionsToUnsub set to collect partitions needing unsubscription
   * @param storePollCounterMap reusable map for per-store poll counters (cleared at start)
   * @return true if errors occurred and delay should be added, false otherwise
   */
  private boolean processPollResults(
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledPubSubMessages,
      long pollTimestamp,
      Set<PubSubTopicPartition> topicPartitionsToUnsub,
      Map<String, StorePollCounter> storePollCounterMap) {

    int payloadBytesConsumed = 0;
    int messageCount = 0;
    long beforeProducingTimestamp = System.currentTimeMillis();
    storePollCounterMap.clear();

    List<TpProcessingResult> results = processAllTopicPartitions(polledPubSubMessages, pollTimestamp);

    boolean hasError = false;
    for (TpProcessingResult result: results) {
      if (result.isMissingReceiver()) {
        LOGGER.error(
            "Couldn't find consumed data receiver for topic partition : {} after receiving records from `poll` request",
            result.getTopicPartition());
        topicPartitionsToUnsub.add(result.getTopicPartition());
      } else if (result.getError() != null) {
        LOGGER.error("Error processing topic partition: {}", result.getTopicPartition(), result.getError());
        hasError = true;
      } else {
        messageCount += result.getMsgCount();
        payloadBytesConsumed += result.getPayloadSize();
        storePollCounterMap
            .computeIfAbsent(result.getTopicPartition().getPubSubTopic().getStoreName(), this::newStorePollCounter)
            .record(result.getMsgCount(), result.getPayloadSize());
      }
    }

    if (hasError) {
      aggStats.recordTotalPollError();
    }

    recordAggregateStats(beforeProducingTimestamp, messageCount, storePollCounterMap);
    applyThrottling(payloadBytesConsumed, messageCount);
    cleanupUnsubscribedPartitions(topicPartitionsToUnsub);

    return hasError;
  }

  /**
   * Records aggregate statistics after processing poll results.
   */
  private void recordAggregateStats(
      long beforeProducingTimestamp,
      int messageCount,
      Map<String, StorePollCounter> storePollCounterMap) {

    aggStats.recordTotalConsumerRecordsProducingToWriterBufferLatency(
        LatencyUtils.getElapsedTimeFromMsToMs(beforeProducingTimestamp));
    aggStats.recordTotalNonZeroPollResultNum(messageCount);

    for (Map.Entry<String, StorePollCounter> entry: storePollCounterMap.entrySet()) {
      KafkaConsumerServiceStats storeStats = aggStats.getStoreStats(entry.getKey());
      storeStats.recordPollResultNum(entry.getValue().msgCount);
      storeStats.recordByteSizePerPoll(entry.getValue().byteSize);
    }
  }

  /**
   * Applies bandwidth and records throttling.
   */
  private void applyThrottling(int payloadBytesConsumed, int messageCount) {
    bandwidthThrottler.accept(payloadBytesConsumed);
    recordsThrottler.accept(messageCount);
  }

  /**
   * Cleans up partitions that have missing receivers by unsubscribing and recording stats.
   */
  private void cleanupUnsubscribedPartitions(Set<PubSubTopicPartition> topicPartitionsToUnsub) {
    cleaner.unsubscribe(topicPartitionsToUnsub);
    aggStats.recordTotalDetectedNoRunningIngestionTopicPartitionNum(topicPartitionsToUnsub.size());
  }

  void stop() {
    running = false;
    synchronized (this) {
      notifyAll();
    }
  }

  long getLastSuccessfulPollTimestamp() {
    return lastSuccessfulPollTimestamp;
  }

  int getTaskId() {
    return taskId;
  }

  String getTaskIdStr() {
    return consumptionTaskIdStr;
  }

  void setDataReceiver(
      PubSubTopicPartition pubSubTopicPartition,
      ConsumedDataReceiver<List<DefaultPubSubMessage>> consumedDataReceiver) {
    ConsumedDataReceiver<List<DefaultPubSubMessage>> previousConsumedDataReceiver =
        dataReceiverMap.put(pubSubTopicPartition, consumedDataReceiver);
    if (previousConsumedDataReceiver != null
        && !previousConsumedDataReceiver.destinationIdentifier().equals(consumedDataReceiver.destinationIdentifier())) {
      // Defensive coding. Should never happen except in case of a regression.
      throw new IllegalStateException(
          "It is not allowed to set multiple " + ConsumedDataReceiver.class.getSimpleName() + " instances for the same "
              + "topic-partition of a given consumer. Previous: " + previousConsumedDataReceiver + ", New: "
              + consumedDataReceiver);
    }
    synchronized (this) {
      notifyAll();
    }
  }

  PubSubTopic getDestinationIdentifier(PubSubTopicPartition topicPartition) {
    ConsumedDataReceiver<List<DefaultPubSubMessage>> dataReceiver = dataReceiverMap.get(topicPartition);
    return dataReceiver == null ? null : dataReceiver.destinationIdentifier();
  }

  public PartitionStats getPartitionStats(PubSubTopicPartition topicPartition) {
    PartitionStats partitionStats = this.partitionToStatsMap.get(topicPartition);
    return partitionStats == null ? EMPTY_PARTITION_STATS : partitionStats;
  }

  void removeDataReceiver(PubSubTopicPartition topicPartition) {
    this.dataReceiverMap.remove(topicPartition);
    this.partitionToStatsMap.remove(topicPartition);
  }

  private LivePartitionStats newPartitionStats(PubSubTopicPartition pubSubTopicPartition) {
    return new LivePartitionStats(this.lastSuccessfulPollTimestamp);
  }

  /**
   * Process all topic-partitions from a poll result. Uses parallel processing if enabled and there are
   * multiple topic-partitions, otherwise processes sequentially.
   *
   * @param polledPubSubMessages the messages polled from PubSub, grouped by topic-partition
   * @param pollTimestamp the timestamp when the poll completed
   * @return list of processing results for each topic-partition
   */
  private List<TpProcessingResult> processAllTopicPartitions(
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledPubSubMessages,
      long pollTimestamp) {
    List<TpProcessingResult> results = new ArrayList<>(polledPubSubMessages.size());

    if (crossTpProcessingPool != null && polledPubSubMessages.size() > 1) {
      // Parallel processing: submit each TP's processing to the thread pool
      List<CompletableFuture<TpProcessingResult>> futures = new ArrayList<>(polledPubSubMessages.size());
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: polledPubSubMessages.entrySet()) {
        final PubSubTopicPartition pubSubTopicPartition = entry.getKey();
        final List<DefaultPubSubMessage> messages = entry.getValue();
        final long ts = pollTimestamp;
        futures.add(
            CompletableFuture
                .supplyAsync(() -> processTopicPartition(pubSubTopicPartition, messages, ts), crossTpProcessingPool));
      }

      // Wait for all TPs to complete and collect results
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      for (CompletableFuture<TpProcessingResult> future: futures) {
        results.add(future.join());
      }
    } else {
      // Sequential processing
      for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: polledPubSubMessages.entrySet()) {
        results.add(processTopicPartition(entry.getKey(), entry.getValue(), pollTimestamp));
      }
    }

    return results;
  }

  /**
   * Process messages for a single topic-partition. This method handles:
   * - Receiver lookup and missing receiver detection
   * - Per-poll bookkeeping (message count, payload size, stats recording)
   * - Writing messages to the receiver
   * - Exception handling
   *
   * <p><b>Exception Handling Contract:</b> This method catches all exceptions internally and stores
   * them in {@link TpProcessingResult#setError(Exception)}. It is guaranteed to never throw an
   * exception to the caller. This contract is critical for parallel processing in
   * {@link #processAllTopicPartitions}, where this method is executed via
   * {@link CompletableFuture#supplyAsync}. If this method were to throw,
   * the future would complete exceptionally, causing {@link CompletableFuture#join()}
   * to throw a {@link CompletionException}. By catching all exceptions here,
   * we ensure that errors are handled uniformly through the result object rather than through
   * exception propagation.
   *
   * <p>Note: Framework-level exceptions (e.g., {@link RejectedExecutionException}
   * from the thread pool) are not caught here and would cause the future to complete exceptionally.
   * Such exceptions indicate a severe system issue and should propagate to fail fast.
   *
   * @param pubSubTopicPartition the topic-partition being processed
   * @param messages the messages to process
   * @param pollTimestamp the timestamp when the poll completed
   * @return the processing result containing counts, sizes, and any errors (never null)
   */
  private TpProcessingResult processTopicPartition(
      PubSubTopicPartition pubSubTopicPartition,
      List<DefaultPubSubMessage> messages,
      long pollTimestamp) {
    TpProcessingResult result = new TpProcessingResult(pubSubTopicPartition);
    ConsumedDataReceiver<List<DefaultPubSubMessage>> receiver = dataReceiverMap.get(pubSubTopicPartition);

    if (receiver == null) {
      result.setMissingReceiver(true);
      return result;
    }

    try {
      // Per-poll bookkeeping
      consumerPollTracker.recordMessageReceived(pubSubTopicPartition);
      int msgCnt = messages.size();
      int payloadSize = 0;
      for (DefaultPubSubMessage pubSubMessage: messages) {
        payloadSize += pubSubMessage.getPayloadSize();
      }
      result.setMsgCount(msgCnt);
      result.setPayloadSize(payloadSize);
      partitionToStatsMap.computeIfAbsent(pubSubTopicPartition, this::newPartitionStats)
          .record(pollTimestamp, msgCnt, payloadSize);

      receiver.write(messages);
    } catch (Exception e) {
      result.setError(e);
    }

    return result;
  }

  public static class PartitionStats {
    public double getMessageRate() {
      return 0.0D;
    }

    public double getBytesRate() {
      return 0.0D;
    }

    public long getLastSuccessfulPollTimestamp() {
      return DEFAULT_TOPIC_PARTITION_NO_POLL_TIMESTAMP;
    }
  }

  /**
   * These stats are not meant to be emitted as metrics, since partitions are too numerous, but can be accessed via the
   * admin tool on an on-demand basis.
   */
  private static class LivePartitionStats extends PartitionStats {
    protected final Rate messageRate;
    protected final Rate bytesRate;
    protected long lastSuccessfulPollTimestamp;

    private LivePartitionStats(long lastSuccessfulPollTimestamp) {
      this.messageRate = new Rate();
      this.messageRate.init(DEFAULT_METRIC_CONFIG, lastSuccessfulPollTimestamp);
      this.bytesRate = new Rate();
      this.bytesRate.init(DEFAULT_METRIC_CONFIG, lastSuccessfulPollTimestamp);
      this.lastSuccessfulPollTimestamp = lastSuccessfulPollTimestamp;
    }

    @Override
    public double getMessageRate() {
      return this.messageRate.measure(DEFAULT_METRIC_CONFIG, System.currentTimeMillis());
    }

    @Override
    public double getBytesRate() {
      return this.bytesRate.measure(DEFAULT_METRIC_CONFIG, System.currentTimeMillis());
    }

    @Override
    public long getLastSuccessfulPollTimestamp() {
      return this.lastSuccessfulPollTimestamp;
    }

    void record(long lastSuccessfulPollTimestamp, int messageCount, int bytesCount) {
      this.lastSuccessfulPollTimestamp = lastSuccessfulPollTimestamp;
      this.messageRate.record(messageCount, lastSuccessfulPollTimestamp);
      this.bytesRate.record(bytesCount, lastSuccessfulPollTimestamp);
    }
  }

  StorePollCounter newStorePollCounter(String storeName) {
    return new StorePollCounter(0, 0);
  }

  /**
   * This class is used to count the number of messages and the byte size of the messages for a given store per poll.
   */
  static class StorePollCounter {
    protected int msgCount;
    protected int byteSize;

    StorePollCounter(int msgCount, int byteSize) {
      this.msgCount = msgCount;
      this.byteSize = byteSize;
    }

    void record(int msgCount, int byteSize) {
      this.msgCount += msgCount;
      this.byteSize += byteSize;
    }
  }

  /**
   * This class holds the result of processing a single topic-partition in the parallel processing path.
   * It aggregates the processing outcome including message count, payload size, and any errors.
   */
  private static class TpProcessingResult {
    private final PubSubTopicPartition topicPartition;
    private int msgCount;
    private int payloadSize;
    private boolean missingReceiver;
    private Exception error;

    TpProcessingResult(PubSubTopicPartition topicPartition) {
      this.topicPartition = topicPartition;
    }

    PubSubTopicPartition getTopicPartition() {
      return topicPartition;
    }

    int getMsgCount() {
      return msgCount;
    }

    void setMsgCount(int msgCount) {
      this.msgCount = msgCount;
    }

    int getPayloadSize() {
      return payloadSize;
    }

    void setPayloadSize(int payloadSize) {
      this.payloadSize = payloadSize;
    }

    boolean isMissingReceiver() {
      return missingReceiver;
    }

    void setMissingReceiver(boolean missingReceiver) {
      this.missingReceiver = missingReceiver;
    }

    Exception getError() {
      return error;
    }

    void setError(Exception error) {
      this.error = error;
    }
  }
}
