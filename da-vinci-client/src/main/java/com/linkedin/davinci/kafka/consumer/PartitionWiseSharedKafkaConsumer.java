package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.KafkaConsumerException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a synchronized version of {@link KafkaConsumerWrapper}.
 * Especially, this class removes the topic-wise assumption from {@link SharedKafkaConsumer}. It is able to
 * subscribe to multiple topic partitions and only remove the subscriptions belonging to the specified
 * topic partitions. It does not support topic level attach to {@link StoreIngestionTask}, and we only record metric at
 * topic partition level.
 */
public class PartitionWiseSharedKafkaConsumer extends SharedKafkaConsumer {

  private static final Logger LOGGER = LogManager.getLogger(PartitionWiseSharedKafkaConsumer.class);

  private final Map<TopicPartition, StoreIngestionTask> topicPartitionStoreIngestionTaskMap = new VeniceConcurrentHashMap<>();
  private final Set<TopicPartition> topicPartitionsWithoutCorrespondingIngestionTask = new HashSet<>();

  public PartitionWiseSharedKafkaConsumer(KafkaConsumerWrapper delegate, KafkaConsumerService service,
      long nonExistingTopicCleanupDelayMS, TopicExistenceChecker topicExistenceChecker) {
    super(delegate, service, nonExistingTopicCleanupDelayMS, topicExistenceChecker);
  }

  public PartitionWiseSharedKafkaConsumer(final KafkaConsumerWrapper delegate, final KafkaConsumerService service,
      int sanitizeTopicSubscriptionAfterPollTimes, long nonExistingTopicCleanupDelayMS, Time time, TopicExistenceChecker topicExistenceChecker) {
    super(delegate, service, sanitizeTopicSubscriptionAfterPollTimes, nonExistingTopicCleanupDelayMS, time, topicExistenceChecker);
  }

  @Override
  protected Set<StoreIngestionTask> getIngestionTasksForTopic(String topic) {
    Set<StoreIngestionTask> storeIngestionTasksOfSameTopic = new HashSet<>();
    Set<TopicPartition> currentAssignment = getAssignment();
    for (TopicPartition topicPartition : currentAssignment) {
      if (topic.equals(topicPartition.topic())) {
        Optional<StoreIngestionTask> ingestionTaskForTopicPartition = getIngestionTaskForTopicPartition(topicPartition);
        if (ingestionTaskForTopicPartition.isPresent()) {
          storeIngestionTasksOfSameTopic.add(ingestionTaskForTopicPartition.get());
        }
      }
    }
    return storeIngestionTasksOfSameTopic;
  }

  protected void sanitizeTopicsWithoutCorrespondingIngestionTask(ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records) {
    // Check whether the returned records, which don't have the corresponding ingestion tasks
    topicPartitionsWithoutCorrespondingIngestionTask.clear();
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
      TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
      if (!getIngestionTaskForTopicPartition(topicPartition).isPresent()) {
        topicPartitionsWithoutCorrespondingIngestionTask.add(topicPartition);
      }
    }
    if (!topicPartitionsWithoutCorrespondingIngestionTask.isEmpty()) {
      LOGGER.error("Detected the following topic partitions without attached ingestion task, and will unsubscribe them: "
          + topicPartitionsWithoutCorrespondingIngestionTask);
      unSubscribeTopicPartitions(topicPartitionsWithoutCorrespondingIngestionTask);
      kafkaConsumerService.getStats().recordDetectedNoRunningIngestionTopicPartitionNum(topicPartitionsWithoutCorrespondingIngestionTask.size());
    }
  }

  public synchronized void unSubscribeTopicPartitions(Set<TopicPartition> topicPartitions) {
    // Get the current subscription for this topic and unsubscribe them
    Set<TopicPartition> currentAssignment = getAssignment();
    Set<TopicPartition> newAssignment = new HashSet<>();
    for (TopicPartition topicPartition : currentAssignment) {
      if (!topicPartitions.contains(topicPartition)) {
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

  @Override
  public void attach(String topic, StoreIngestionTask ingestionTask) {
    throw new VeniceUnsupportedOperationException("Do not support topic level attach, only attach at topic partition level.");
  }

  @Override
  public Optional<StoreIngestionTask> getIngestionTaskForTopicPartition(TopicPartition topicPartition) {
    return Optional.ofNullable(topicPartitionStoreIngestionTaskMap.get(topicPartition));
  }

  /**
   * This will build the mapping from ingestion task to topic partition inside this consumer, and it will only be called
   * during partition subscription. We will not allow two ingestion tasks share one topic partition in one shared consumer,
   * assignment logic will prevent this from happening. Also add safeguard logic to ensure this will not happen.
   * @param topicPartition
   * @param storeIngestionTask
   */
  public void addIngestionTaskForTopicPartition(TopicPartition topicPartition, StoreIngestionTask storeIngestionTask) {
    topicPartitionStoreIngestionTaskMap.compute(topicPartition, (key, previousStoreIngestionTask) -> {
      if (previousStoreIngestionTask != null && previousStoreIngestionTask != storeIngestionTask) {
        throw new KafkaConsumerException("Cannot subscribe same topic partition: " + topicPartition +
            " for different store ingestion tasks belonging to two topics: " + previousStoreIngestionTask.getVersionTopic() +
            " and " + storeIngestionTask.getVersionTopic());
      }
      return storeIngestionTask;
    });
  }

  @Override
  public void detach(StoreIngestionTask storeIngestionTask) {
    Set<TopicPartition> topicPartitionsToRemove = new HashSet<>();
    for(Map.Entry<TopicPartition, StoreIngestionTask> entry : topicPartitionStoreIngestionTaskMap.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      StoreIngestionTask ingestionTaskForCurrentTopicPartition = entry.getValue();
      if (ingestionTaskForCurrentTopicPartition == storeIngestionTask) {
        topicPartitionsToRemove.add(topicPartition);
      }
    }
    topicPartitionsToRemove.forEach(topicPartition -> topicPartitionStoreIngestionTaskMap.remove(topicPartition));
    unSubscribeTopicPartitions(topicPartitionsToRemove);
  }

}
