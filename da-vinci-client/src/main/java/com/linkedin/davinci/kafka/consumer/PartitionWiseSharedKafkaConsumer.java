package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.KafkaConsumerException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
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


/**
 * This class is a synchronized version of {@link KafkaConsumerWrapper}.
 * Especially, this class removes the topic-wise assumption from {@link SharedKafkaConsumer}. It is able to
 * subscribe to multiple topic partitions and only remove the subscriptions belonging to the specified
 * topic partitions. It does not support topic level attach to {@link StoreIngestionTask}, and we only record metric at
 * topic partition level.
 */
public class PartitionWiseSharedKafkaConsumer extends SharedKafkaConsumer {

  private static final Logger LOGGER = LogManager.getLogger(PartitionWiseSharedKafkaConsumer.class);

  private static final String exceptionMessageForImproperUsage(String methodName) {
    return methodName + " should never be called on " + PartitionWiseSharedKafkaConsumer.class.getSimpleName() +
        " but should rather be called on " + PartitionWiseKafkaConsumerService.VirtualSharedKafkaConsumer.class.getSimpleName();
  }

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
        StoreIngestionTask ingestionTaskForTopicPartition = getIngestionTaskForTopicPartition(topicPartition);
        if (ingestionTaskForTopicPartition != null) {
          storeIngestionTasksOfSameTopic.add(ingestionTaskForTopicPartition);
        }
      }
    }
    return storeIngestionTasksOfSameTopic;
  }

  @Override
  protected void sanitizeTopicsWithoutCorrespondingIngestionTask(ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records) {
    // Check whether the returned records, which don't have the corresponding ingestion tasks
    topicPartitionsWithoutCorrespondingIngestionTask.clear();
    for (TopicPartition topicPartition: records.partitions()) {
      if (getIngestionTaskForTopicPartition(topicPartition) == null) {
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
    for(TopicPartition topicPartition : topicPartitions) {
      topicPartitionStoreIngestionTaskMap.remove(topicPartition);
    }
  }

  @Override
  public void attach(String topic, StoreIngestionTask ingestionTask) {
    throw new VeniceUnsupportedOperationException(
        "Do not support topic level attach, only attach at topic partition level. This should only be called by "
            + TopicWiseKafkaConsumerService.class.getSimpleName() + " which should never interact with any instance of "
            + PartitionWiseSharedKafkaConsumer.class.getSimpleName());
  }

  @Override
  public StoreIngestionTask getIngestionTaskForTopicPartition(TopicPartition topicPartition) {
    return topicPartitionStoreIngestionTaskMap.get(topicPartition);
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
  public synchronized void unSubscribe(String topic, int partition) {
    super.unSubscribe(topic, partition);
    // Remove mapping.
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    topicPartitionStoreIngestionTaskMap.remove(topicPartition);
  }

  @Override
  public synchronized void assign(Collection<TopicPartition> topicPartitions) {
    throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("assign"));
  }

  @Override
  public synchronized void batchUnsubscribe(Set<TopicPartition> topicPartitionSet) {
    throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("batchUnsubscribe"));
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
    unSubscribeTopicPartitions(topicPartitionsToRemove);
  }


  /**
   * Package-private visibility, intended for testing only.
   *
   * @return a read-only view of this internal state
   */
  Set<TopicPartition> getTopicPartitionsWithoutCorrespondingIngestionTask() {
    return Collections.unmodifiableSet(topicPartitionsWithoutCorrespondingIngestionTask);
  }
}
