package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
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
  private final Logger logger;
  private final int taskId;
  private final Map<TopicPartition, ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>>> dataReceiverMap =
      new VeniceConcurrentHashMap<>();
  private final long readCycleDelayMs;
  private final Supplier<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> pollFunction;
  private final Consumer<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> bandwidthThrottler;
  private final Consumer<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> recordsThrottler;
  private final KafkaConsumerServiceStats stats;
  private final ConsumerSubscriptionCleaner cleaner;

  private volatile boolean running = true;

  /**
   * Timestamp of the last poll. Initialized at construction time, in case the consumer task thread gets stuck from
   * the get-go.
   */
  private volatile long lastSuccessfulPollTimestamp = System.currentTimeMillis();

  public ConsumptionTask(
      final String kafkaUrl,
      final int taskId,
      final long readCycleDelayMs,
      final Supplier<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> pollFunction,
      final Consumer<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> bandwidthThrottler,
      final Consumer<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> recordsThrottler,
      final KafkaConsumerServiceStats stats,
      final ConsumerSubscriptionCleaner cleaner) {
    this.taskId = taskId;
    this.readCycleDelayMs = readCycleDelayMs;
    this.pollFunction = pollFunction;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.stats = stats;
    this.cleaner = cleaner;
    this.logger = LogManager.getLogger(getClass().getSimpleName() + "[ " + kafkaUrl + " - " + taskId + " ]");
  }

  @Override
  public void run() {
    boolean addSomeDelay = false;

    // Pre-allocate some variables to clobber in the loop
    long beforePollingTimeStamp;
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records;
    long beforeProducingToWriteBufferTimestamp;
    ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumedDataReceiver;
    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> partitionRecords;
    Set<TopicPartition> topicPartitionsToUnsub = new HashSet<>();
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
        topicPartitionsToUnsub = cleaner.getTopicPartitionsToUnsubscribe(topicPartitionsToUnsub); // N.B. cheap call
        for (TopicPartition topicPartitionToUnSub: topicPartitionsToUnsub) {
          ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> dataReceiver =
              dataReceiverMap.remove(topicPartitionToUnSub);
          if (dataReceiver != null) {
            dataReceiver.notifyOfTopicDeletion(topicPartitionToUnSub.topic());
          }
        }
        topicPartitionsToUnsub.clear();

        /**
         * N.B. The poll function could be synchronized here if implementing the idea presented in the top of class
         * JavaDoc, about how this class could become the sole entry point for all consumer-related interactions,
         * and thus be capable of operating on a non-threadsafe consumer.
         */
        records = pollFunction.get();
        lastSuccessfulPollTimestamp = System.currentTimeMillis();
        stats.recordPollRequestLatency(lastSuccessfulPollTimestamp - beforePollingTimeStamp);
        stats.recordPollResultNum(records.count());
        if (!records.isEmpty()) {
          beforeProducingToWriteBufferTimestamp = System.currentTimeMillis();
          for (TopicPartition topicPartition: records.partitions()) {
            consumedDataReceiver = dataReceiverMap.get(topicPartition);
            if (consumedDataReceiver == null) {
              // defensive code
              logger.error(
                  "Couldn't find consumed data receiver for topic partition : {} after receiving records from `poll` request",
                  topicPartition);
              topicPartitionsToUnsub.add(topicPartition);
              continue;
            }
            partitionRecords = records.records(topicPartition);
            consumedDataReceiver.write(partitionRecords);
          }
          stats.recordConsumerRecordsProducingToWriterBufferLatency(
              LatencyUtils.getElapsedTimeInMs(beforeProducingToWriteBufferTimestamp));
          bandwidthThrottler.accept(records);
          recordsThrottler.accept(records);
          cleaner.unsubscribe(topicPartitionsToUnsub);
          stats.recordDetectedNoRunningIngestionTopicPartitionNum(topicPartitionsToUnsub.size());
        } else {
          // No result came back, here will add some delay
          addSomeDelay = true;
        }
      } catch (Exception e) {
        if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
          // We sometimes wrap InterruptedExceptions, so not taking any chances...
          logger.error("Received InterruptedException, will exit");
          break;
        }
        logger.error("Received exception while polling, will retry", e);
        addSomeDelay = true;
        stats.recordPollError();
      }
    }
    logger.info("Shared consumer thread: {} exited", Thread.currentThread().getName());
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

  void setDataReceiver(
      TopicPartition topicPartition,
      ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumedDataReceiver) {
    ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> previousConsumedDataReceiver =
        dataReceiverMap.put(topicPartition, consumedDataReceiver);
    if (previousConsumedDataReceiver != null
        && !previousConsumedDataReceiver.destinationIdentifier().equals(consumedDataReceiver.destinationIdentifier())) {
      // Defensive coding. Should never happen except in case of a regression.
      throw new IllegalStateException(
          "It is not allowed to set multiple " + ConsumedDataReceiver.class.getSimpleName() + " instances for the same "
              + TopicPartition.class.getSimpleName() + " of a given consumer. Previous: " + previousConsumedDataReceiver
              + ", New: " + consumedDataReceiver);
    }
    synchronized (this) {
      notifyAll();
    }
  }

  void removeDataReceiver(TopicPartition topicPartition) {
    dataReceiverMap.remove(topicPartition);
  }
}
