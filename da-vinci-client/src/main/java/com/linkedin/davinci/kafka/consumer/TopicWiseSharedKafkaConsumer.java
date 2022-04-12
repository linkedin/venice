package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TopicWiseSharedKafkaConsumer extends SharedKafkaConsumer {
  private static final Logger LOGGER = LogManager.getLogger(TopicWiseSharedKafkaConsumer.class);

  /** This set is used to store the topics without corresponding ingestion tasks in each poll request. */
  private Set<String> topicsWithoutCorrespondingIngestionTask = new HashSet<>();

  /**
   * This field contains the mapping between subscribed topics and the corresponding {@link StoreIngestionTask} to handle
   * all the messages from those topics.
   * The reason to maintain a mapping here since different {@link SharedKafkaConsumer} could subscribe the same topic, and
   * one use case is Hybrid store, and multiple store versions will consume the same real-time topic.
   */
  private final Map<String, StoreIngestionTask> topicToIngestionTaskMap = new VeniceConcurrentHashMap<>();

  public TopicWiseSharedKafkaConsumer(KafkaConsumerWrapper delegate, KafkaConsumerService service,
      long nonExistingTopicCleanupDelayMS, TopicExistenceChecker topicExistenceChecker) {
    super(delegate, service, nonExistingTopicCleanupDelayMS, topicExistenceChecker);
  }

  TopicWiseSharedKafkaConsumer(KafkaConsumerWrapper delegate, KafkaConsumerService service,
      int sanitizeTopicSubscriptionAfterPollTimes, long nonExistingTopicCleanupDelayMS, Time time,
      TopicExistenceChecker topicExistenceChecker) {
    super(delegate, service, sanitizeTopicSubscriptionAfterPollTimes, nonExistingTopicCleanupDelayMS, time,
        topicExistenceChecker);
  }

  protected void sanitizeTopicsWithoutCorrespondingIngestionTask(
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records) {
    // Check whether the returned records, which don't have the corresponding ingestion tasks
    topicsWithoutCorrespondingIngestionTask.clear();
    for (TopicPartition topicPartition: records.partitions()) {
      if (getIngestionTaskForTopic(topicPartition.topic()) == null) {
        topicsWithoutCorrespondingIngestionTask.add(topicPartition.topic());
      }
    }
    if (!topicsWithoutCorrespondingIngestionTask.isEmpty()) {
      LOGGER.error("Detected the following topics without attached ingestion task, and will unsubscribe them: " + topicsWithoutCorrespondingIngestionTask);
      close(topicsWithoutCorrespondingIngestionTask);
      kafkaConsumerService.getStats().recordDetectedNoRunningIngestionTopicNum(topicsWithoutCorrespondingIngestionTask.size());
    }
  }

  @Override
  public synchronized void assign(Collection<TopicPartition> topicPartitions) {
    updateCurrentAssignment(new HashSet<>(topicPartitions));
    this.delegate.assign(topicPartitions);
  }

  @Override
  public synchronized void batchUnsubscribe(Set<TopicPartition> topicPartitionSet) {
    unSubscribeAction(() -> {
      this.delegate.batchUnsubscribe(topicPartitionSet);
      return topicPartitionSet.size();
    });
  }

  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   */
  public void attach(String topic, StoreIngestionTask ingestionTask) {
    topicToIngestionTaskMap.put(topic, ingestionTask);
    LOGGER.info("Attached the message processing of topic: " + topic + " to the ingestion task belonging to version topic: "
        + ingestionTask.getVersionTopic());
  }

  @Override
  public void unsubscribeAll(String versionTopic) {
    StoreIngestionTask ingestionTask = topicToIngestionTaskMap.get(versionTopic);
    if (ingestionTask == null) {
      return;
    }
    Set<String> subscribedTopics = ingestionTask.getEverSubscribedTopics();
    /**
     * This logic is used to guard the resource leaking situation when the unsubscription doesn't happen before the detaching.
     */
    close(subscribedTopics);
    subscribedTopics.forEach(topic -> topicToIngestionTaskMap.remove(topic));
    LOGGER.info("Detached ingestion task, which has subscribed topics: " + subscribedTopics);
  }

  /**
   * Get the all corresponding {@link StoreIngestionTask}s for a particular topic.
   */
  protected Set<StoreIngestionTask> getIngestionTasksForTopic(String topic) {
    return Collections.singleton(getIngestionTaskForTopic(topic));
  }

  /**
   * Get the corresponding {@link StoreIngestionTask} for the subscribed topic partition.
   */
  public StoreIngestionTask getIngestionTaskForTopicPartition(TopicPartition topicPartition) {
    return topicToIngestionTaskMap.get(topicPartition.topic());
  }

  /**
   * Get the corresponding {@link StoreIngestionTask} for the subscribed topic.
   */
  private StoreIngestionTask getIngestionTaskForTopic(String topic) {
    return topicToIngestionTaskMap.get(topic);
  }

  /**
   * Package-private visibility, intended for testing only.
   *
   * @return a read-only view of this internal state
   */
  Set<String> getTopicsWithoutCorrespondingIngestionTask() {
    return Collections.unmodifiableSet(topicsWithoutCorrespondingIngestionTask);
  }
}
