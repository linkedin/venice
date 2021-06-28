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
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;


/**
 * This class is a synchronized version of {@link KafkaConsumerWrapper}.
 * Especially, this class adds the special support for function: {@link #close(Set)} since this consumer could
 * subscript to multiple topics and the customer could only remove the subscriptions belonging to the specified
 * topics. Also the support for function: {@link #hasSubscription(Set)} since it could be shared by multiple users.
 *
 * In addition to the existing API of {@link KafkaConsumerWrapper}, this class also adds specific functions: {@link #attach},
 * {@link #detach}, which will be used by {@link KafkaConsumerService}.
 */
public class SharedKafkaConsumer implements KafkaConsumerWrapper {
  private static final Logger LOGGER = Logger.getLogger(SharedKafkaConsumer.class);

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
  /**
   * This set is used to store the topics without corresponding ingestion tasks in each poll request.
   */
  private Set<String> topicsWithoutCorrespondingIngestionTask = new HashSet<>();

  private final KafkaConsumerWrapper delegate;
  /**
   * This field records the reference to the {@link KafkaConsumerService} that creates this {@link SharedKafkaConsumer}
   */
  private final KafkaConsumerService kafkaConsumerService;
  /**
   * This cached assignment is for performance optimization purpose since {@link #hasSubscription} could be invoked frequently.
   */
  private Set<TopicPartition> currentAssignment = Collections.emptySet();

  /**
   * This field contains the mapping between subscribed topics and the corresponding {@link StoreIngestionTask} to handle
   * all the messages from those topics.
   * The reason to maintain a mapping here since different {@link SharedKafkaConsumer} could subscribe the same topic, and
   * one use case is Hybrid store, and multiple store versions will consume the same real-time topic.
   */
  private final Map<String, StoreIngestionTask> topicToIngestionTaskMap = new VeniceConcurrentHashMap<>();

  /**
   * This indicates if there is any thread waiting for a poll to happen
   */
  private final AtomicBoolean waitingForPoll = new AtomicBoolean(false);

  /**
   * an ever increasing count of number of time poll has been invoked.
   */
  private volatile long pollTimes = 0 ;

  private final Time time;

  private boolean enableOffsetCollection = false;
  long lastMetricsCollectedTime = 0;
  private static final int METRICS_UPDATE_INTERVAL_MS = 30 * Time.MS_PER_MINUTE; //30 seconds
  private Map<TopicPartition, Double> topicPartitionCurrentOffset = new VeniceConcurrentHashMap<>();
  private Map<TopicPartition, Double> topicPartitionEndOffset = new VeniceConcurrentHashMap<>();

  private final TopicExistenceChecker topicExistenceChecker;

  public SharedKafkaConsumer(final KafkaConsumerWrapper delegate, final KafkaConsumerService service, final long nonExistingTopicCleanupDelayMS,
      boolean enableOffsetCollection, TopicExistenceChecker topicExistenceChecker) {
    this(delegate, service, 1000, nonExistingTopicCleanupDelayMS, enableOffsetCollection, topicExistenceChecker);
  }

  public SharedKafkaConsumer(final KafkaConsumerWrapper delegate, final KafkaConsumerService service,
      int sanitizeTopicSubscriptionAfterPollTimes, long nonExistingTopicCleanupDelayMS, boolean enableOffsetCollection, TopicExistenceChecker topicExistenceChecker) {
    this(delegate, service, sanitizeTopicSubscriptionAfterPollTimes, nonExistingTopicCleanupDelayMS, new SystemTime(), enableOffsetCollection, topicExistenceChecker);
  }

  SharedKafkaConsumer(final KafkaConsumerWrapper delegate, final KafkaConsumerService service,
      int sanitizeTopicSubscriptionAfterPollTimes, long nonExistingTopicCleanupDelayMS, Time time, boolean enableOffsetCollection, TopicExistenceChecker topicExistenceChecker) {
    this.delegate = delegate;
    this.kafkaConsumerService = service;
    this.sanitizeTopicSubscriptionAfterPollTimes = sanitizeTopicSubscriptionAfterPollTimes;
    this.nonExistingTopicCleanupDelayMS = nonExistingTopicCleanupDelayMS;
    this.time = time;
    this.enableOffsetCollection = enableOffsetCollection;
    this.topicExistenceChecker = topicExistenceChecker;
  }


  /**
   * If {@param isClosed} is true, this function will update the {@link #currentAssignment} to be empty.
   */
  private void updateCurrentAssignment(boolean isClosed) {
    if (isClosed) {
      this.currentAssignment = Collections.emptySet();
    } else {
      this.currentAssignment = this.delegate.getAssignment();
    }
  }

  @Override
  public synchronized void subscribe(String topic, int partition, long lastReadOffset) {
    this.delegate.subscribe(topic, partition, lastReadOffset);
    updateCurrentAssignment(false);
  }

  /**
   * There is an additional goal of this function which is to make sure that all the records consumed for this {topic,partition} prior to
   * calling unsubscribe here is produced to drainer service. {@literal KafkaConsumerService.ConsumptionTask#run()} calls
   * {@link SharedKafkaConsumer#poll(long)} and produceToStoreBufferService sequentially. So waiting for at least one more
   * invocation of {@link SharedKafkaConsumer#poll(long)} achieves the above objective.
   */
  @Override
  public synchronized void unSubscribe(String topic, int partition) {
    long currentPollTimes = pollTimes;
    Instant startTime = Instant.now();
    this.delegate.unSubscribe(topic, partition);

    LOGGER.info(String.format("Shared consumer %s unsubscribed topic %s partition %d: . Took %d ms.",
        this, topic, partition, Instant.now().toEpochMilli() - startTime.toEpochMilli()));
    updateCurrentAssignment(false);
    currentPollTimes++;
    waitingForPoll.set(true);
    //Wait for the next poll or maximum 10 seconds. Interestingly wait api does not provide any indication if wait returned
    //due to timeout. So an explicit time check is necessary.
    long timeoutMs = (time.getNanoseconds()/Time.NS_PER_MS) + (10 * Time.MS_PER_SECOND);
    try {
      while (currentPollTimes > pollTimes) {
        long waitMs = timeoutMs - (time.getNanoseconds()/Time.NS_PER_MS);
        if (waitMs <= 0) {
          break;
        }
        wait(waitMs);
      }
      //no action to take actually, just return;
    } catch (InterruptedException e) {
      LOGGER.info("Wait for poll request in `unSubscribe` function got interrupted.");
      Thread.currentThread().interrupt();
    }

    //remove this topic partition from offset cache.
    if (enableOffsetCollection) {
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionCurrentOffset.remove(tp);
      topicPartitionEndOffset.remove(tp);
    }
  }

  @Override
  public synchronized void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException {
    this.delegate.resetOffset(topic, partition);
  }

  @Override
  public synchronized void close() {
    this.delegate.close();
    updateCurrentAssignment(true);
  }

  @Override
  public synchronized void close(Set<String> topics) {
    // Get the current subscription for this topic and unsubscribe them
    Set<TopicPartition> currentAssignment = getAssignment();
    List<TopicPartition> newAssignment = new LinkedList<>();
    for (TopicPartition topicPartition : currentAssignment) {
      if (!topics.contains(topicPartition.topic())) {
        newAssignment.add(topicPartition);
      }
    }
    if (newAssignment.size() == currentAssignment.size()) {
      // nothing changed.
      return;
    }
    this.delegate.assign(newAssignment);
    updateCurrentAssignment(false);
  }

  /**
   * This function is used to detect whether there is any subscription to the non-existing topics.
   * If yes, this function will remove the subscriptions to these non-existing topics.
   */
  private void sanitizeConsumerSubscription() {
    // Get the subscriptions
    Set<TopicPartition> assignment = getAssignment();
    if (assignment.isEmpty()) {
      return;
    }
    Set<String> nonExistingTopics = new HashSet<>();
    long currentTimestamp = time.getMilliseconds();
    assignment.forEach( tp -> {
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
          LOGGER.info("The non-existing topic detected previously: " + topic + " show up after " + diff + " ms. "
              + "and it will be removed from `nonExistingTopicDiscoverTimestampMap`");
        }
      }
    });
    Set<String> topicsToUnsubscribe = new HashSet<>(nonExistingTopics);
    if (!nonExistingTopics.isEmpty()) {
      LOGGER.error("Detected the following non-existing topics: " + nonExistingTopics);
      nonExistingTopics.forEach( topic -> {
        Long firstDetectedTimestamp = nonExistingTopicDiscoverTimestampMap.get(topic);
        StoreIngestionTask task = getIngestionTaskForTopic(topic);
        if (task != null) {
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
            LOGGER.error("The non-existing topic hasn't showed up after " + diff +
                " ms, so we will fail the attached ingestion task");
            task.setLastConsumerException(new VeniceException("Topic: " + topic + " got deleted"));
            nonExistingTopicDiscoverTimestampMap.remove(topic);
          } else {
            /**
             * We shouldn't unsubscribe the non-existing topic now since currently the delay hasn't been exhausted yet.
             */
            topicsToUnsubscribe.remove(topic);
          }
        } else {
          // defensive coding
          LOGGER.error("Detected an non-existing topic: " + topic + ", which is present in consumer assignment, but without any attached ingestion task");
          if (firstDetectedTimestamp != null) {
            LOGGER.info("There is no associated ingestion task with this non-existing topic: " + topic +
                ", so it will be" + " removed from `nonExistingTopicDiscoverTimestampMap` directly");
            nonExistingTopicDiscoverTimestampMap.remove(topic);
          }
        }
      });
      close(topicsToUnsubscribe);
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
      if (!hasSubscription()) {
        Thread.sleep(timeoutMs);
        return ConsumerRecords.empty();
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Shared Consumer poll sleep got interrupted", e);
    }

    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = this.delegate.poll(timeoutMs);
    // Check whether the returned records, which don't have the corresponding ingestion tasks
    topicsWithoutCorrespondingIngestionTask.clear();
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
      if (getIngestionTaskForTopic(record.topic()) == null) {
        topicsWithoutCorrespondingIngestionTask.add(record.topic());
      }
    }
    if (!topicsWithoutCorrespondingIngestionTask.isEmpty()) {
      LOGGER.error("Detected the following topics without attached ingestion task, and will unsubscribe them: " + topicsWithoutCorrespondingIngestionTask);
      close(topicsWithoutCorrespondingIngestionTask);
      kafkaConsumerService.getStats().recordDetectedNoRunningIngestionTopicNum(topicsWithoutCorrespondingIngestionTask.size());
    }
    /**
     * Here, this function still returns the original records because of the following reasons:
     * 1. The behavior of not having corresponding ingestion tasks is not common, and most-likely it is caused by resource leaking/partial failure in edge cases.
     * 2. Copying them to a new {@link ConsumerRecords} is not GC friendly.
     * 3. Even copying them to  a new {@link ConsumerRecords} couldn't guarantee the consistency between returned consumer records
     *    and the corresponding ingestion tasks since the caller will try to fetch the ingestion task for each record again, and
     *    {@link #detach} could happen in-between, so the caller has to handle this situation of non corresponding ingestion task anyway.
     *
     * The consumer subscription cleanup will take effect in the next {@link #poll} request.
     */

    if (enableOffsetCollection) {
      //Update current offset cache for all topics partition.
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        topicPartitionCurrentOffset.put(tp, (double) record.offset());
      }

      if (LatencyUtils.getElapsedTimeInMs(lastMetricsCollectedTime) > METRICS_UPDATE_INTERVAL_MS) {
        lastMetricsCollectedTime = System.currentTimeMillis();
        Map<MetricName, Double> metrics = delegate.getMeasurableConsumerMetrics();
        //clear since it will process a complete snapshot of lags of all topic partition currently.
        for (Map.Entry<MetricName, Double> metric : metrics.entrySet()) {
          try {
            if (metric.getKey().name().equals("records-lag")) {
              TopicPartition tp = new TopicPartition(metric.getKey().tags().get("topic"), Integer.valueOf(metric.getKey().tags().get("partition")));
              if (topicPartitionCurrentOffset.containsKey(tp)) {
                topicPartitionEndOffset.put(tp, metric.getValue() + topicPartitionCurrentOffset.getOrDefault(tp, 0.0));
              }
            }
          } catch (Exception e) {
            LOGGER.error("Exception in collecting offset lag ", e);
          }
        }
      }
    }

    return records;
  }

  @Override
  public synchronized boolean hasSubscription() {
    return !this.currentAssignment.isEmpty();
  }

  /**
   * This function will return true as long as any topic in the passed {@param topics} has been subscribed.
   */
  @Override
  public synchronized boolean hasSubscription(Set<String> topics) {
    for (TopicPartition topicPartition : currentAssignment) {
      if (topics.contains(topicPartition.topic())) {
        return true;
      }
    }
    return false;
  }


  @Override
  public synchronized boolean hasSubscription(String topic, int partition) {
    return this.currentAssignment.contains(new TopicPartition(topic, partition));
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
  public synchronized void assign(List<TopicPartition> topicPartitions) {
    this.delegate.assign(topicPartitions);
    updateCurrentAssignment(false);
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
    return Collections.unmodifiableSet(currentAssignment);
  }

  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   */
  public void attach(String topic, StoreIngestionTask ingestionTask) {
    topicToIngestionTaskMap.put(topic, ingestionTask);
    LOGGER.info("Attached the message processing of topic: " + topic + " to the ingestion task belonging to version topic: "
        + ingestionTask.getVersionTopic());
  }

  /**
   * Detach the messages processing belonging to the topics of the passed {@param ingestionTask}
   */
  public void detach(StoreIngestionTask ingestionTask) {
    Set<String> subscribedTopics = ingestionTask.getEverSubscribedTopics();
    /**
     * This logic is used to guard the resource leaking situation when the unsubscription doesn't happen before the detaching.
     */
    close(subscribedTopics);
    subscribedTopics.forEach(topic -> topicToIngestionTaskMap.remove(topic));
    LOGGER.info("Detached ingestion task, which has subscribed topics: " + subscribedTopics);
  }

  /**
   * Get the corresponding {@link StoreIngestionTask} for the subscribed topic.
   */
  public StoreIngestionTask getIngestionTaskForTopic(String topic) {
    return topicToIngestionTaskMap.get(topic);
  }

  /**
   * Get the {@link KafkaConsumerService} that creates this consumer.
   */
  public KafkaConsumerService getKafkaConsumerService() {
    return this.kafkaConsumerService;
  }

  // Visible for testing
  void setCurrentAssignment(Set<TopicPartition> assignment) {
     this.currentAssignment = assignment;
  }

  @Override
  public Map<MetricName, Double> getMeasurableConsumerMetrics() {
    return delegate.getMeasurableConsumerMetrics();
  }

  @Override
  public Optional<Long> getLatestOffset(String topic, int partition) {
    return Optional.of(topicPartitionEndOffset.get(new TopicPartition(topic, partition)).longValue());
  }
}
