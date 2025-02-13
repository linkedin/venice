package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.Rate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
  private final Logger LOGGER;
  private final int taskId;
  private final String consumptionTaskIdStr;
  private final Map<PubSubTopicPartition, ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> dataReceiverMap =
      new VeniceConcurrentHashMap<>();
  private final long readCycleDelayMs;
  private final Supplier<Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> pollFunction;

  private final Function<PubSubTopicPartition, Long> offsetLagGetter;
  private final IntConsumer bandwidthThrottler;
  private final IntConsumer recordsThrottler;
  private final AggKafkaConsumerServiceStats aggStats;
  private final ConsumerSubscriptionCleaner cleaner;

  /**
   * Maintain rate counter with default window size to calculate the message and bytes rate at topic partition level.
   * Those topic partition level information will not be emitted out as a metric, to avoid emitting too many metrics per
   * server host, they are for admin tool debugging purpose.
   */
  private final Map<PubSubTopicPartition, Rate> messageRatePerTopicPartition = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Rate> bytesRatePerTopicPartition = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Rate> pollRatePerTopicPartition = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> processingResultTimeMap = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> processingResultMsgMap = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> processingResultByteMap = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> accumulateResultTimeMap = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> accumulateResultMsgMap = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> pollWallTimeMap = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, Long> accumulatePollWallTimeMap = new VeniceConcurrentHashMap<>();

  private long nonEmptyPollCount = 0;
  private long pollCount = 0;
  private long accumulatePollTimeNs = 0;
  private long accumulateNonEmptyPollTime = 0;
  private long accumulateDelayTime = 0;
  private long accumulateProcessTime = 0;
  private long previousReportTime = 0;

  private final Lazy<Rate> overallConsumerPollRate;
  private final RedundantExceptionFilter redundantExceptionFilter;
  private final Map<PubSubTopicPartition, Long> lastSuccessfulPollTimestampPerTopicPartition =
      new VeniceConcurrentHashMap<>();

  private final MetricConfig metricConfig = new MetricConfig();

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
      final Supplier<Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>>> pollFunction,
      final IntConsumer bandwidthThrottler,
      final IntConsumer recordsThrottler,
      final AggKafkaConsumerServiceStats aggStats,
      final ConsumerSubscriptionCleaner cleaner,
      Function<PubSubTopicPartition, Long> offsetLagGetter,
      RedundantExceptionFilter redundantExceptionFilter) {
    this.readCycleDelayMs = readCycleDelayMs;
    this.pollFunction = pollFunction;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.aggStats = aggStats;
    this.cleaner = cleaner;
    this.taskId = taskId;
    this.offsetLagGetter = offsetLagGetter;
    this.redundantExceptionFilter = redundantExceptionFilter;
    this.overallConsumerPollRate = Lazy.of(() -> createRate(System.currentTimeMillis()));
    this.consumptionTaskIdStr = Utils.getSanitizedStringForLogger(consumerNamePrefix) + " - " + taskId;
    this.LOGGER = LogManager.getLogger(getClass().getSimpleName() + "[ " + consumptionTaskIdStr + " ]");
  }

  @Override
  public void run() {
    boolean addSomeDelay = false;

    // Pre-allocate some variables to clobber in the loop
    long beforePollingTimeStamp;
    long beforePollingTimeNs;
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledPubSubMessages;
    long beforeProducingToWriteBufferTimestamp;
    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver;
    Set<PubSubTopicPartition> topicPartitionsToUnsub = new HashSet<>();
    int payloadBytesConsumedInOnePoll;
    int polledPubSubMessagesCount = 0;
    Map<String, StorePollCounter> storePollCounterMap = new HashMap<>();
    try {
      while (running) {
        try {
          long beforeDelayTimeNs = System.nanoTime();
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
          beforePollingTimeNs = System.nanoTime();
          accumulateDelayTime += beforePollingTimeNs - beforeDelayTimeNs;
          topicPartitionsToUnsub = cleaner.getTopicPartitionsToUnsubscribe(topicPartitionsToUnsub); // N.B. cheap call
          for (PubSubTopicPartition topicPartitionToUnSub: topicPartitionsToUnsub) {
            ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> dataReceiver =
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
          long lastSuccessfulPollNs = System.nanoTime();
          long pollTimeNs = lastSuccessfulPollNs - beforePollingTimeNs;
          accumulatePollTimeNs += pollTimeNs;
          pollCount += 1;
          aggStats.recordTotalPollRequestLatency(lastSuccessfulPollTimestamp - beforePollingTimeStamp);
          if (!polledPubSubMessages.isEmpty()) {
            nonEmptyPollCount += 1;
            accumulateNonEmptyPollTime += pollTimeNs;
            payloadBytesConsumedInOnePoll = 0;
            polledPubSubMessagesCount = 0;
            beforeProducingToWriteBufferTimestamp = System.currentTimeMillis();

            for (Map.Entry<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> entry: polledPubSubMessages
                .entrySet()) {
              long startTimestamp = System.currentTimeMillis();
              PubSubTopicPartition pubSubTopicPartition = entry.getKey();
              String storeName = Version.parseStoreFromKafkaTopicName(pubSubTopicPartition.getTopicName());
              StorePollCounter counter =
                  storePollCounterMap.computeIfAbsent(storeName, k -> new StorePollCounter(0, 0));
              List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> topicPartitionMessages = entry.getValue();
              consumedDataReceiver = dataReceiverMap.get(pubSubTopicPartition);
              if (consumedDataReceiver == null) {
                // defensive code
                LOGGER.error(
                    "Couldn't find consumed data receiver for topic partition : {} after receiving records from `poll` request",
                    pubSubTopicPartition);
                topicPartitionsToUnsub.add(pubSubTopicPartition);
                continue;
              }
              polledPubSubMessagesCount += topicPartitionMessages.size();
              counter.msgCount += topicPartitionMessages.size();
              int payloadSizePerTopicPartition = 0;
              for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage: topicPartitionMessages) {
                payloadSizePerTopicPartition += pubSubMessage.getPayloadSize();
              }
              counter.byteSize += payloadSizePerTopicPartition;
              payloadBytesConsumedInOnePoll += payloadSizePerTopicPartition;

              lastSuccessfulPollTimestampPerTopicPartition.put(pubSubTopicPartition, lastSuccessfulPollTimestamp);
              messageRatePerTopicPartition
                  .computeIfAbsent(pubSubTopicPartition, tp -> createRate(lastSuccessfulPollTimestamp))
                  .record(topicPartitionMessages.size(), lastSuccessfulPollTimestamp);
              bytesRatePerTopicPartition
                  .computeIfAbsent(pubSubTopicPartition, tp -> createRate(lastSuccessfulPollTimestamp))
                  .record(payloadSizePerTopicPartition, lastSuccessfulPollTimestamp);
              pollRatePerTopicPartition
                  .computeIfAbsent(pubSubTopicPartition, tp -> createRate(lastSuccessfulPollTimestamp))
                  .record(1, lastSuccessfulPollTimestamp);
              long beforeProcessingNs = System.nanoTime();
              consumedDataReceiver.write(topicPartitionMessages);
              long messageProcessingTimeNs = System.nanoTime() - beforeProcessingNs;
              int finalPayloadBytesConsumedInOnePoll = payloadBytesConsumedInOnePoll;
              processingResultTimeMap.compute(
                  entry.getKey(),
                  (k, v) -> (v == null) ? messageProcessingTimeNs : messageProcessingTimeNs + v);
              processingResultByteMap.compute(
                  entry.getKey(),
                  (k, v) -> (v == null) ? finalPayloadBytesConsumedInOnePoll : finalPayloadBytesConsumedInOnePoll + v);
              processingResultMsgMap.compute(
                  entry.getKey(),
                  (k, v) -> (v == null) ? topicPartitionMessages.size() : topicPartitionMessages.size() + v);
              accumulateResultTimeMap.compute(
                  entry.getKey(),
                  (k, v) -> (v == null) ? messageProcessingTimeNs : messageProcessingTimeNs + v);
              accumulateResultMsgMap.compute(
                  entry.getKey(),
                  (k, v) -> (v == null) ? topicPartitionMessages.size() : topicPartitionMessages.size() + v);
              checkSlowPartitionWithHighLag(pubSubTopicPartition);
              long wallClockTime = System.currentTimeMillis() - startTimestamp;
              pollWallTimeMap.compute(entry.getKey(), (k, v) -> (v == null) ? wallClockTime : wallClockTime + v);
              accumulatePollWallTimeMap
                  .compute(entry.getKey(), (k, v) -> (v == null) ? wallClockTime : wallClockTime + v);
            }
            accumulateProcessTime += System.nanoTime() - lastSuccessfulPollNs;
            maybeLogConsumerPollDebugInfo(System.currentTimeMillis() - beforeProducingToWriteBufferTimestamp);
            processingResultByteMap.clear();
            processingResultMsgMap.clear();
            processingResultTimeMap.clear();
            pollWallTimeMap.clear();
            overallConsumerPollRate.get().record(1, lastSuccessfulPollTimestamp);
            aggStats.recordTotalConsumerRecordsProducingToWriterBufferLatency(
                LatencyUtils.getElapsedTimeFromMsToMs(beforeProducingToWriteBufferTimestamp));
            aggStats.recordTotalNonZeroPollResultNum(polledPubSubMessagesCount);
            storePollCounterMap.forEach((storeName, counter) -> {
              aggStats.getStoreStats(storeName).recordPollResultNum(counter.msgCount);
              aggStats.getStoreStats(storeName).recordByteSizePerPoll(counter.byteSize);
            });
            bandwidthThrottler.accept(payloadBytesConsumedInOnePoll);
            recordsThrottler.accept(polledPubSubMessagesCount);
            cleaner.unsubscribe(topicPartitionsToUnsub);
            aggStats.recordTotalDetectedNoRunningIngestionTopicPartitionNum(topicPartitionsToUnsub.size());
            storePollCounterMap.clear();
          } else {
            // No result came back, here will add some delay
            // SET IT TO FALSE TO NOT SLEEP
            addSomeDelay = false;
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
      ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumedDataReceiver) {
    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> previousConsumedDataReceiver =
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

  private Rate createRate(long now) {
    Rate rate = new Rate();
    rate.init(metricConfig, now);
    return rate;
  }

  Double getMessageRate(PubSubTopicPartition topicPartition) {
    if (messageRatePerTopicPartition.containsKey(topicPartition)) {
      return messageRatePerTopicPartition.get(topicPartition).measure(metricConfig, System.currentTimeMillis());
    }
    return 0.0D;
  }

  Double getByteRate(PubSubTopicPartition topicPartition) {
    if (bytesRatePerTopicPartition.containsKey(topicPartition)) {
      return bytesRatePerTopicPartition.get(topicPartition).measure(metricConfig, System.currentTimeMillis());
    }
    return 0.0D;
  }

  Double getPollRate(PubSubTopicPartition topicPartition) {
    if (pollRatePerTopicPartition.containsKey(topicPartition)) {
      return pollRatePerTopicPartition.get(topicPartition).measure(metricConfig, System.currentTimeMillis());
    }
    return 0.0D;
  }

  private void maybeLogConsumerPollDebugInfo(long singlePollMs) {
    long timeSinceLastReport = System.currentTimeMillis() - previousReportTime;
    if (timeSinceLastReport > TimeUnit.MINUTES.toMillis(1)) {
      boolean skipLoggingUnrelatedConsumer = true;
      for (PubSubTopicPartition pubSubTopicPartition: processingResultTimeMap.keySet()) {
        if (pubSubTopicPartition.getTopicName().startsWith("MultiSurfaceMemberEbr")) {
          skipLoggingUnrelatedConsumer = false;
          break;
        }
      }
      if (skipLoggingUnrelatedConsumer) {
        return;
      }
      // LOGGER.warn("Ns4 EBR store consumer: {}. SinglePollMs: {}, TimeMap: {}, WallTimeMap: {}, MsgMap: {}, ByteMap:
      // {}", consumptionTaskIdStr, singlePollMs, processingResultTimeMap, pollWallTimeMap, processingResultMsgMap,
      // processingResultByteMap);
      LOGGER.warn(
          "Ns5 EBR store consumer: {}. Aggregate time: {}, Poll Count: {}, Non-Empty Poll Count: {}, TimeMap: {}, MsgMap: {}",
          consumptionTaskIdStr,
          timeSinceLastReport,
          pollCount,
          nonEmptyPollCount,
          accumulateResultTimeMap,
          accumulateResultMsgMap);
      LOGGER.warn(
          "Ns5 EBR store consumer: {}. Time split. Delay Time: {}, Poll Time: {}, Non-Empty Poll Time: {}, Process Time: {}",
          consumptionTaskIdStr,
          accumulateDelayTime,
          accumulatePollTimeNs,
          accumulateNonEmptyPollTime,
          accumulateProcessTime);
      accumulateProcessTime = 0;
      accumulateDelayTime = 0;
      accumulatePollTimeNs = 0;
      accumulateResultMsgMap.clear();
      accumulateResultTimeMap.clear();
      accumulatePollWallTimeMap.clear();
      nonEmptyPollCount = 0;
      accumulateNonEmptyPollTime = 0;
      pollCount = 0;
      previousReportTime = System.currentTimeMillis();
    }
  }

  private void checkSlowPartitionWithHighLag(PubSubTopicPartition pubSubTopicPartition) {
    Long offsetLag = offsetLagGetter.apply(pubSubTopicPartition);
    Double messageRate = getMessageRate(pubSubTopicPartition);
    Double pollRate = getPollRate(pubSubTopicPartition);
    Double consumerPollRate = overallConsumerPollRate.get().measure(metricConfig, System.currentTimeMillis());
    String slowTaskWithPartitionStr = consumptionTaskIdStr + " - " + pubSubTopicPartition;
    if (offsetLag > 200000 && messageRate < 600
        && !redundantExceptionFilter.isRedundantException(slowTaskWithPartitionStr)) {
      LOGGER.warn(
          "Slow partition with high lag detected: {}. Lag: {}, Message Rate: {}, Poll Rate: {}, Consumer Poll Rate: {}",
          pubSubTopicPartition,
          offsetLag,
          messageRate,
          pollRate,
          consumerPollRate);
    }
    if (pubSubTopicPartition.getTopicName().startsWith("MultiSurfaceMemberEbr")
        && !redundantExceptionFilter.isRedundantException(slowTaskWithPartitionStr)) {
      LOGGER.warn(
          "Future push rate check for MultiSurface store: {}. Lag: {}, Message Rate: {}, Poll Rate: {}, Consumer Poll Rate: {}",
          pubSubTopicPartition,
          offsetLag,
          messageRate,
          pollRate,
          consumerPollRate);
    }
  }

  PubSubTopic getDestinationIdentifier(PubSubTopicPartition topicPartition) {
    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> dataReceiver =
        dataReceiverMap.get(topicPartition);
    return dataReceiver == null ? null : dataReceiver.destinationIdentifier();
  }

  Long getLastSuccessfulPollTimestamp(PubSubTopicPartition topicPartition) {
    if (lastSuccessfulPollTimestampPerTopicPartition.containsKey(topicPartition)) {
      return lastSuccessfulPollTimestampPerTopicPartition.get(topicPartition);
    }
    return DEFAULT_TOPIC_PARTITION_NO_POLL_TIMESTAMP;
  }

  void removeDataReceiver(PubSubTopicPartition topicPartition) {
    dataReceiverMap.remove(topicPartition);
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
  }
}
