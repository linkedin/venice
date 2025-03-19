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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
      final ConsumerSubscriptionCleaner cleaner) {
    this.readCycleDelayMs = readCycleDelayMs;
    this.pollFunction = pollFunction;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.aggStats = aggStats;
    this.cleaner = cleaner;
    this.taskId = taskId;
    this.consumptionTaskIdStr = Utils.getSanitizedStringForLogger(consumerNamePrefix) + " - " + taskId;
    this.LOGGER = LogManager.getLogger(getClass().getSimpleName() + "[ " + consumptionTaskIdStr + " ]");
  }

  @Override
  public void run() {
    boolean addSomeDelay = false;

    // Pre-allocate some variables to clobber in the loop
    long beforePollingTimeStamp;
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledPubSubMessages;
    long beforeProducingToWriteBufferTimestamp;
    ConsumedDataReceiver<List<DefaultPubSubMessage>> consumedDataReceiver;
    Set<PubSubTopicPartition> topicPartitionsToUnsub = new HashSet<>();
    List<DefaultPubSubMessage> topicPartitionMessages;
    int msgCount;
    int payloadBytesConsumedInOnePoll;
    int polledPubSubMessagesCount;
    int payloadSizePerTopicPartition;
    KafkaConsumerServiceStats storeStats;
    Map<String, StorePollCounter> storePollCounterMap = new HashMap<>();
    try {
      while (running) {
        try {
          if (addSomeDelay) {
            synchronized (this) {
              /**
               * N.B. Using {@link #wait(long)} here so that it can be interrupted by the notification of {@link #stop()}
               * or {@link #setDataReceiver(TopicPartition, ConsumedDataReceiver)}.
               */
              wait(readCycleDelayMs);
            }
            addSomeDelay = false;
          }
          beforePollingTimeStamp = System.currentTimeMillis();
          // N.B. cheap call
          topicPartitionsToUnsub = cleaner.getTopicPartitionsToUnsubscribe(topicPartitionsToUnsub);
          for (PubSubTopicPartition topicPartitionToUnSub: topicPartitionsToUnsub) {
            ConsumedDataReceiver<List<DefaultPubSubMessage>> dataReceiver =
                dataReceiverMap.remove(topicPartitionToUnSub);
            if (dataReceiver != null) {
              dataReceiver.notifyOfTopicDeletion(topicPartitionToUnSub.getPubSubTopic().getName());
            }
          }
          topicPartitionsToUnsub.clear();

          /**
           * N.B. The poll function could be synchronized here if implementing the idea presented in the top of class
           * JavaDoc, about how this class could become the sole entry point for all consumer-related interactions,
           * and thus be capable of operating on a non-threadsafe consumer.
           */
          polledPubSubMessages = pollFunction.get();
          lastSuccessfulPollTimestamp = System.currentTimeMillis();
          aggStats.recordTotalPollRequestLatency(lastSuccessfulPollTimestamp - beforePollingTimeStamp);
          if (!polledPubSubMessages.isEmpty()) {
            payloadBytesConsumedInOnePoll = 0;
            polledPubSubMessagesCount = 0;
            beforeProducingToWriteBufferTimestamp = System.currentTimeMillis();
            storePollCounterMap.clear();
            for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: polledPubSubMessages.entrySet()) {
              PubSubTopicPartition pubSubTopicPartition = entry.getKey();
              consumedDataReceiver = dataReceiverMap.get(pubSubTopicPartition);
              if (consumedDataReceiver == null) {
                // defensive code
                LOGGER.error(
                    "Couldn't find consumed data receiver for topic partition : {} after receiving records from `poll` request",
                    pubSubTopicPartition);
                topicPartitionsToUnsub.add(pubSubTopicPartition);
                continue;
              }

              topicPartitionMessages = entry.getValue();

              // Per-poll bookkeeping
              msgCount = topicPartitionMessages.size();
              polledPubSubMessagesCount += msgCount;
              payloadSizePerTopicPartition = 0;
              for (DefaultPubSubMessage pubSubMessage: topicPartitionMessages) {
                payloadSizePerTopicPartition += pubSubMessage.getPayloadSize();
              }
              payloadBytesConsumedInOnePoll += payloadSizePerTopicPartition;
              storePollCounterMap
                  .computeIfAbsent(pubSubTopicPartition.getPubSubTopic().getStoreName(), this::newStorePollCounter)
                  .record(msgCount, payloadSizePerTopicPartition);
              this.partitionToStatsMap.computeIfAbsent(pubSubTopicPartition, this::newPartitionStats)
                  .record(this.lastSuccessfulPollTimestamp, msgCount, payloadSizePerTopicPartition);

              consumedDataReceiver.write(topicPartitionMessages);
            }
            aggStats.recordTotalConsumerRecordsProducingToWriterBufferLatency(
                LatencyUtils.getElapsedTimeFromMsToMs(beforeProducingToWriteBufferTimestamp));
            aggStats.recordTotalNonZeroPollResultNum(polledPubSubMessagesCount);
            for (Map.Entry<String, StorePollCounter> entry: storePollCounterMap.entrySet()) {
              storeStats = aggStats.getStoreStats(entry.getKey());
              storeStats.recordPollResultNum(entry.getValue().msgCount);
              storeStats.recordByteSizePerPoll(entry.getValue().byteSize);
            }
            bandwidthThrottler.accept(payloadBytesConsumedInOnePoll);
            recordsThrottler.accept(polledPubSubMessagesCount);
            cleaner.unsubscribe(topicPartitionsToUnsub);
            aggStats.recordTotalDetectedNoRunningIngestionTopicPartitionNum(topicPartitionsToUnsub.size());
          } else {
            // No result came back, here will add some delay
            addSomeDelay = true;
          }
        } catch (Exception e) {
          if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
            // We sometimes wrap InterruptedExceptions, so not taking any chances...
            LOGGER.error("Received InterruptedException, will exit");
            break;
          }
          LOGGER.error("Received exception while polling, will retry", e);
          addSomeDelay = true;
          aggStats.recordTotalPollError();
        }
      }
    } catch (Throwable t) {
      // This is a catch-all to ensure that the thread doesn't die unexpectedly. If it does, we want to know about it.
      LOGGER.error(
          "Shared consumer thread: {} exited due to an unexpected exception",
          Thread.currentThread().getName(),
          t);
    } finally {
      LOGGER.info("Shared consumer thread: {} exited", Thread.currentThread().getName());
    }
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
}
