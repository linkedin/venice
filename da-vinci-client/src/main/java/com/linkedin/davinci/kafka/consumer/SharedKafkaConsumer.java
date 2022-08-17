package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a synchronized version of {@link KafkaConsumerWrapper}.
 * Especially, this class adds the special support for function: {@link #close(Set)} since this consumer could
 * subscript to multiple topics and the customer could only remove the subscriptions belonging to the specified
 * topics. Also the support for function: {@link #hasSubscribedAnyTopic(Set)} since it could be shared by multiple users.
 *
 * In addition to the existing API of {@link KafkaConsumerWrapper}, this class also adds specific functions: {@link #attach},
 * {@link #unsubscribeAll}, which will be used by {@link KafkaConsumerService}.
 */
abstract class SharedKafkaConsumer implements KafkaConsumerWrapper {
  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaConsumer.class);

  /**
   * After this number of poll requests, shared consumer will check whether all the subscribed topics exist or not.
   * And it will clean up the subscription to the topics, which don't exist any more.
   */
  private final int sanitizeTopicSubscriptionAfterPollTimes;
  private int pollTimesSinceLastSanitization = 0;

  /**
   * This is used to control how much time we should wait before cleaning up the corresponding ingestion task
   * when an non-existing topic is discovered.
   * The reason to introduce this config is that `consumer#listTopics` could only guarantee eventual consistency, so
   * `consumer#listTopics` not returning the topic doesn't mean the topic doesn't exist in Kafka.
   * If `consumer#listTopics` still doesn't return the topic after the configured delay, Venice SN will unsubscribe the topic,
   * and fail the corresponding ingestion job.
   */
  private final long nonExistingTopicCleanupDelayMS;
  /**
   * This field is to maintain a mapping between the non-existing topic and the discovered time for the first time.
   * The function: {@link #sanitizeConsumerSubscription()} can add a new topic to this map if it discovers
   * a new non-existing topic.
   * There are two ways to clean up one entry from this map:
   * 1. The non-existing topic starts showing up.
   * 2. The non-existing period lasts longer than {@link #nonExistingTopicCleanupDelayMS}, and the corresponding task
   *    will fail.
   */
  private final Map<String, Long> nonExistingTopicDiscoverTimestampMap = new VeniceConcurrentHashMap<>();

  protected final KafkaConsumerWrapper delegate;
  /**
   * This field records the reference to the {@link KafkaConsumerService} that creates this {@link SharedKafkaConsumer}
   */
  protected final KafkaConsumerService kafkaConsumerService;
  /**
   * This cached assignment is for performance optimization purpose since {@link #hasSubscription} could be invoked frequently.
   * This set should be unmodifiable.
   */
  private Set<TopicPartition> currentAssignment;

  /**
   * This field is used to cache the size information of the current assignment in order to reduce threads contention because
   * getting the size no longer requires calling the synchronized {@link SharedKafkaConsumer#getAssignment}
   */
  private final AtomicInteger currentAssignmentSize;

  /**
   * This indicates if there is any thread waiting for a poll to happen
   */
  private final AtomicBoolean waitingForPoll = new AtomicBoolean(false);

  /**
   * an ever increasing count of number of time poll has been invoked.
   */
  private volatile long pollTimes = 0;

  private final Time time;

  private final TopicExistenceChecker topicExistenceChecker;

  public SharedKafkaConsumer(
      final KafkaConsumerWrapper delegate,
      final KafkaConsumerService service,
      final long nonExistingTopicCleanupDelayMS,
      TopicExistenceChecker topicExistenceChecker) {
    this(delegate, service, 1000, nonExistingTopicCleanupDelayMS, new SystemTime(), topicExistenceChecker);
  }

  SharedKafkaConsumer(
      final KafkaConsumerWrapper delegate,
      final KafkaConsumerService service,
      int sanitizeTopicSubscriptionAfterPollTimes,
      long nonExistingTopicCleanupDelayMS,
      Time time,
      TopicExistenceChecker topicExistenceChecker) {
    this.delegate = delegate;
    this.kafkaConsumerService = service;
    this.sanitizeTopicSubscriptionAfterPollTimes = sanitizeTopicSubscriptionAfterPollTimes;
    this.nonExistingTopicCleanupDelayMS = nonExistingTopicCleanupDelayMS;
    this.time = time;
    this.topicExistenceChecker = topicExistenceChecker;
    this.currentAssignment = Collections.emptySet();
    this.currentAssignmentSize = new AtomicInteger(0);
  }

  protected synchronized void updateCurrentAssignment(Set<TopicPartition> newAssignment) {
    final long updateCurrentAssignmentStartTime = System.currentTimeMillis();
    currentAssignmentSize.set(newAssignment.size());
    currentAssignment = Collections.unmodifiableSet(newAssignment);
    kafkaConsumerService.setPartitionsNumSubscribed(this, newAssignment.size());
    kafkaConsumerService.getStats()
        .recordUpdateCurrentAssignmentLatency(LatencyUtils.getElapsedTimeInMs(updateCurrentAssignmentStartTime));
  }

  @Override
  public synchronized void subscribe(String topic, int partition, long lastReadOffset) {
    long delegateSubscribeStartTime = System.currentTimeMillis();
    this.delegate.subscribe(topic, partition, lastReadOffset);
    kafkaConsumerService.getStats()
        .recordDelegateSubscribeLatency(LatencyUtils.getElapsedTimeInMs(delegateSubscribeStartTime));
    updateCurrentAssignment(delegate.getAssignment());
  }

  /**
   * There is an additional goal of this function which is to make sure that all the records consumed for this {topic,partition} prior to
   * calling unsubscribe here is produced to drainer service. {@literal KafkaConsumerService.ConsumptionTask#run()} calls
   * {@link SharedKafkaConsumer#poll(long)} and produceToStoreBufferService sequentially. So waiting for at least one more
   * invocation of {@link SharedKafkaConsumer#poll(long)} achieves the above objective.
   */
  @Override
  public synchronized void unSubscribe(String topic, int partition) {
    unSubscribeAction(() -> {
      this.delegate.unSubscribe(topic, partition);
      return 1;
    });
  }

  /**
   * This function encapsulates the logging, bookkeeping and required waiting period surrounding the action of
   * unsubscribing some partition(s).
   *
   * @param action which performs the unsubscription and returns the number of partitions which were unsubscribed
   */
  protected synchronized void unSubscribeAction(IntSupplier action) {
    long currentPollTimes = pollTimes;
    long startTime = System.currentTimeMillis();
    int numberOfUnsubbedPartitions = action.getAsInt();
    long elapsedTime = System.currentTimeMillis() - startTime;

    LOGGER.info(
        "Shared consumer {} unsubscribed {} partition(s) in {} ms.",
        this.getClass().getSimpleName(),
        numberOfUnsubbedPartitions,
        elapsedTime);
    updateCurrentAssignment(delegate.getAssignment());
    waitAfterUnsubscribe(currentPollTimes);
  }

  protected void waitAfterUnsubscribe(long currentPollTimes) {
    currentPollTimes++;
    waitingForPoll.set(true);
    // Wait for the next poll or maximum 10 seconds. Interestingly wait api does not provide any indication if wait
    // returned
    // due to timeout. So an explicit time check is necessary.
    long timeoutMs = (time.getNanoseconds() / Time.NS_PER_MS) + (10 * Time.MS_PER_SECOND);
    try {
      while (currentPollTimes > pollTimes) {
        long waitMs = timeoutMs - (time.getNanoseconds() / Time.NS_PER_MS);
        if (waitMs <= 0) {
          break;
        }
        wait(waitMs);
      }
      // no action to take actually, just return;
    } catch (InterruptedException e) {
      LOGGER.info("Wait for poll request in `unSubscribe` function got interrupted.");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public synchronized void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException {
    this.delegate.resetOffset(topic, partition);
  }

  @Override
  public synchronized void close() {
    this.delegate.close();
    updateCurrentAssignment(Collections.emptySet());
  }

  @Override
  public synchronized void close(Set<String> topics) {
    // Get the current subscription for this topic and unsubscribe them
    Set<TopicPartition> currentAssignment = getAssignment();
    Set<TopicPartition> newAssignment = new HashSet<>();
    for (TopicPartition topicPartition: currentAssignment) {
      if (!topics.contains(topicPartition.topic())) {
        newAssignment.add(topicPartition);
      }
    }
    if (newAssignment.size() == currentAssignment.size()) {
      // nothing changed.
      return;
    }
    updateCurrentAssignment(newAssignment);
    this.delegate.assign(newAssignment);
  }

  /**
   * This function is used to detect whether there is any subscription to the non-existing topics.
   * If yes, this function will remove the topic partition subscriptions to these non-existing topics.
   */
  private void sanitizeConsumerSubscription() {
    // Get the subscriptions
    Set<TopicPartition> assignment = getAssignment();
    if (assignment.isEmpty()) {
      return;
    }
    Set<String> nonExistingTopics = new HashSet<>();
    long currentTimestamp = time.getMilliseconds();
    assignment.forEach(tp -> {
      String topic = tp.topic();
      boolean isExistingTopic = topicExistenceChecker.checkTopicExists(topic);
      if (!isExistingTopic) {
        nonExistingTopics.add(topic);
      } else {
        /**
         * Check whether we should remove any topic from {@link #nonExistingTopicDiscoverTimestampMap} detected previously.
         * Since this logic will be executed before comparing the diff with the delay threshold, so it is possible that
         * even the delay is exhausted here, we will still resume the ingestion.
         */
        if (nonExistingTopicDiscoverTimestampMap.containsKey(topic)) {
          long detectedTimestamp = nonExistingTopicDiscoverTimestampMap.remove(topic);
          long diff = currentTimestamp - detectedTimestamp;
          LOGGER.info(
              "The non-existing topic detected previously: " + topic + " show up after " + diff + " ms. "
                  + "and it will be removed from `nonExistingTopicDiscoverTimestampMap`");
        }
      }
    });
    Set<String> topicsToUnsubscribe = new HashSet<>(nonExistingTopics);
    if (!nonExistingTopics.isEmpty()) {
      LOGGER.error("Detected the following non-existing topics: " + nonExistingTopics);
      nonExistingTopics.forEach(topic -> {
        Long firstDetectedTimestamp = nonExistingTopicDiscoverTimestampMap.get(topic);
        Set<StoreIngestionTask> storeIngestionTasks = getIngestionTasksForTopic(topic);
        for (StoreIngestionTask storeIngestionTask: storeIngestionTasks) {
          if (storeIngestionTask != null) {
            if (null == firstDetectedTimestamp) {
              // The first time to detect this non-existing topic.
              nonExistingTopicDiscoverTimestampMap.put(topic, currentTimestamp);
              firstDetectedTimestamp = currentTimestamp;
            }
            /**
             * Calculate the delay, and compare it with {@link nonExistingTopicCleanupDelayMS},
             * if the delay is over the threshold, will fail the attached ingestion task
             */
            long diff = currentTimestamp - firstDetectedTimestamp;
            if (diff >= nonExistingTopicCleanupDelayMS) {
              LOGGER.error(
                  "The non-existing topic hasn't showed up after " + diff
                      + " ms, so we will fail the attached ingestion task");
              storeIngestionTask.setLastConsumerException(new VeniceException("Topic: " + topic + " got deleted"));
              nonExistingTopicDiscoverTimestampMap.remove(topic);
            } else {
              /**
               * We shouldn't unsubscribe the non-existing topic now since currently the delay hasn't been exhausted yet.
               */
              topicsToUnsubscribe.remove(topic);
            }
          } else {
            // defensive coding
            LOGGER.error(
                "Detected an non-existing topic: " + topic
                    + ", which is present in consumer assignment, but without any attached ingestion task");
            if (firstDetectedTimestamp != null) {
              LOGGER.info(
                  "There is no associated ingestion task with this non-existing topic: " + topic + ", so it will be"
                      + " removed from `nonExistingTopicDiscoverTimestampMap` directly");
              nonExistingTopicDiscoverTimestampMap.remove(topic);
            }
          }
        }
      });
      close(topicsToUnsubscribe);
      /*
      TODO: This metric will not be accurate when the shared consumer is not topic-wise, we need to let KafkaConsumerService
            aggregate this topic level metric from all consumers.
      */
      kafkaConsumerService.getStats().recordDetectedDeletedTopicNum(nonExistingTopics.size());
    }
  }

  @Override
  public synchronized ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeoutMs) {
    /**
     * Always invoke this method no matter whether the consumer have subscription or not. Therefore we could notify any
     * waiter who might be waiting for a invocation of poll to happen even if the consumer does not have subscription
     * after calling {@link SharedKafkaConsumer#unSubscribe(String, int)}.
     */
    pollTimes++;
    if (waitingForPoll.get()) {
      waitingForPoll.set(false);
      notifyAll();
    }

    if (++pollTimesSinceLastSanitization == sanitizeTopicSubscriptionAfterPollTimes) {
      /**
       * The reasons only to sanitize the subscription periodically:
       * 1. This is a heavy operation.
       * 2. The behavior of subscripting non-existing topics is not common.
       * 3. The subscription to the non-existing topics will only cause some inefficiency because of the logging in each
       *    poll request, but not correctness.
       */
      pollTimesSinceLastSanitization = 0;
      sanitizeConsumerSubscription();
    }

    /**
     * If the consumer does not have subscription, sleep the specified timeout and return.
     */
    try {
      if (!hasAnySubscription()) {
        // TODO: removing this sleep inside the poll with synchronization, this sleep should be added by the logic
        // calling this poll method.
        Thread.sleep(timeoutMs);
        return ConsumerRecords.empty();
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Shared Consumer poll sleep got interrupted", e);
    }

    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = this.delegate.poll(timeoutMs);
    sanitizeTopicsWithoutCorrespondingIngestionTask(records);
    /**
     * Here, this function still returns the original records because of the following reasons:
     * 1. The behavior of not having corresponding ingestion tasks is not common, and most-likely it is caused by resource leaking/partial failure in edge cases.
     * 2. Copying them to a new {@link ConsumerRecords} is not GC friendly.
     * 3. Even copying them to  a new {@link ConsumerRecords} couldn't guarantee the consistency between returned consumer records
     *    and the corresponding ingestion tasks since the caller will try to fetch the ingestion task for each record again, and
     *    {@link #unsubscribeAll} could happen in-between, so the caller has to handle this situation of non corresponding ingestion task anyway.
     *
     * The consumer subscription cleanup will take effect in the next {@link #poll} request.
     */
    return records;
  }

  protected abstract void sanitizeTopicsWithoutCorrespondingIngestionTask(
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records);

  @Override
  public boolean hasAnySubscription() {
    return !this.currentAssignment.isEmpty();
  }

  /**
   * This function will return true as long as any topic in the passed {@param topics} has been subscribed.
   */
  @Override
  public boolean hasSubscribedAnyTopic(Set<String> topics) {
    for (TopicPartition subscribedTopicPartition: currentAssignment) {
      if (topics.contains(subscribedTopicPartition.topic())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasSubscription(String topic, int partition) {
    return currentAssignment.contains(new TopicPartition(topic, partition));
  }

  @Override
  public synchronized Map<TopicPartition, Long> beginningOffsets(List<TopicPartition> topicPartitions) {
    return this.delegate.beginningOffsets(topicPartitions);
  }

  @Override
  public synchronized Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions) {
    return this.delegate.endOffsets(topicPartitions);
  }

  @Override
  public synchronized void seek(TopicPartition topicPartition, long nextOffset) {
    this.delegate.seek(topicPartition, nextOffset);
  }

  @Override
  public synchronized void pause(String topic, int partition) {
    this.delegate.pause(topic, partition);
  }

  @Override
  public synchronized void resume(String topic, int partition) {
    this.delegate.resume(topic, partition);
  }

  @Override
  public synchronized Set<TopicPartition> paused() {
    return this.delegate.paused();
  }

  @Override
  public synchronized Set<TopicPartition> getAssignment() {
    return currentAssignment; // The assignment set is unmodifiable
  }

  public int getAssignmentSize() {
    return currentAssignmentSize.get();
  }

  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   */
  public abstract void attach(String topic, StoreIngestionTask ingestionTask);

  /**
   * Stop all subscription associated with the given version topic.
   */
  public abstract void unsubscribeAll(String versionTopic);

  /**
   * Get the all corresponding {@link StoreIngestionTask}s for a particular topic.
   */
  protected abstract Set<StoreIngestionTask> getIngestionTasksForTopic(String topic);

  /**
   * Get the corresponding {@link StoreIngestionTask} for the subscribed topic partition.
   */
  public abstract StoreIngestionTask getIngestionTaskForTopicPartition(TopicPartition topicPartition);

  /**
   * Get the {@link KafkaConsumerService} that creates this consumer.
   */
  public KafkaConsumerService getKafkaConsumerService() {
    return this.kafkaConsumerService;
  }

  // Visible for testing
  synchronized void setCurrentAssignment(Set<TopicPartition> assignment) {
    this.currentAssignment = assignment;
    this.currentAssignmentSize.set(assignment.size());
  }

  @Override
  public long getOffsetLag(String topic, int partition) {
    return delegate.getOffsetLag(topic, partition);
  }

  @Override
  public long getLatestOffset(String topic, int partition) {
    return delegate.getLatestOffset(topic, partition);
  }
}
