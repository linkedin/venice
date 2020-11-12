package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
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


  public SharedKafkaConsumer(final KafkaConsumerWrapper delegate, final KafkaConsumerService service) {
    this(delegate, service, 1000);
  }

  public SharedKafkaConsumer(final KafkaConsumerWrapper delegate, final KafkaConsumerService service, int sanitizeTopicSubscriptionAfterPollTimes) {
    this.delegate = delegate;
    this.kafkaConsumerService = service;
    this.sanitizeTopicSubscriptionAfterPollTimes = sanitizeTopicSubscriptionAfterPollTimes;
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
   * calling unsubscribe here is produced to drainer service. {@link KafkaConsumerService.ConsumptionTask#run()} calls
   * {@link SharedKafkaConsumer#poll(long)} and produceToStoreBufferService sequentially. So waiting for at least one more
   * invocation of {@link SharedKafkaConsumer#poll(long)} achieves the above objective.
   */
  @Override
  public synchronized void unSubscribe(String topic, int partition) {
    long currentPollTimes = pollTimes;
    this.delegate.unSubscribe(topic, partition);
    updateCurrentAssignment(false);

    currentPollTimes++;
    waitingForPoll.set(true);
    try {
      while (currentPollTimes > pollTimes) {
        wait();
      }
      //no action to take actually, just return;
    } catch (InterruptedException e) {
      throw new VeniceException("Wait for poll request in `unSubscribe` function got interrupted.");
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
    Map<String, List<PartitionInfo>> existingTopicPartitions = listTopics();
    Set<String> deletedTopics = new HashSet<>();
    assignment.forEach( tp -> {
      if (!existingTopicPartitions.containsKey(tp.topic())) {
        deletedTopics.add(tp.topic());
      }
    });
    if (!deletedTopics.isEmpty()) {
      LOGGER.error("Detected the following deleted topics, and will unsubscribe them: " + deletedTopics);
      deletedTopics.forEach( topic -> {
        StoreIngestionTask task = getIngestionTaskForTopic(topic);
        if (task != null) {
          task.setLastConsumerException(new VeniceException("Topic: " + topic + " got deleted"));
        }
      });
      close(deletedTopics);
      kafkaConsumerService.getStats().recordDetectedDeletedTopicNum(deletedTopics.size());
    }
  }

  @Override
  public synchronized ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeout) {
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

    /**
     * If the consumer does not have subscription, sleep the specified timeout and return.
     */
    try {
      if (!hasSubscription()) {
        Thread.sleep(timeout);
        return ConsumerRecords.empty();
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Shared Consumer poll sleep got interrupted", e);
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
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = this.delegate.poll(timeout);
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
  public synchronized Map<String, List<PartitionInfo>> listTopics() {
    return this.delegate.listTopics();
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
}
