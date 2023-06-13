package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link TopicWiseKafkaConsumerService} is used to allocate share consumer from consumer pool at topic granularity.
 * One shared consumer may have multiple topics, while each topic can only take one consumer at most. All the partitions
 * from same topic will always be subscribed in the chosen consumer. Before consumer assignment happen, the consumer
 * with least partitions subscribed will be chosen ideally.
 */
public class TopicWiseKafkaConsumerService extends KafkaConsumerService {
  /**
   * This field is used to maintain the mapping between version topic and the corresponding ingestion task.
   * In theory, One version topic should only be mapped to one ingestion task, and if this assumption is violated
   * in the future, we need to change the design of this service.
   */
  private final Map<PubSubTopic, SharedKafkaConsumer> versionTopicToConsumerMap = new VeniceConcurrentHashMap<>();
  private final Map<SharedKafkaConsumer, Set<String>> consumerToStoresMap = new VeniceConcurrentHashMap<>();
  private final Logger LOGGER;

  TopicWiseKafkaConsumerService(
      final PubSubConsumerAdapterFactory consumerFactory,
      final Properties consumerProperties,
      final long readCycleDelayMs,
      final int numOfConsumersPerKafkaCluster,
      final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler,
      final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      final String kafkaClusterAlias,
      final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final TopicExistenceChecker topicExistenceChecker,
      final boolean liveConfigBasedKafkaThrottlingEnabled,
      final PubSubMessageDeserializer pubSubDeserializer,
      final Time time,
      final KafkaConsumerServiceStats stats,
      final boolean isKafkaConsumerOffsetCollectionEnabled) {
    super(
        consumerFactory,
        consumerProperties,
        readCycleDelayMs,
        numOfConsumersPerKafkaCluster,
        bandwidthThrottler,
        recordsThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        kafkaClusterAlias,
        sharedConsumerNonExistingTopicCleanupDelayMS,
        topicExistenceChecker,
        liveConfigBasedKafkaThrottlingEnabled,
        pubSubDeserializer,
        time,
        stats,
        isKafkaConsumerOffsetCollectionEnabled);
    LOGGER = LogManager.getLogger(TopicWiseKafkaConsumerService.class + " [" + kafkaUrl + "]");
  }

  /**
   * This function will return a consumer for the passed {@link StoreIngestionTask}.
   * If the version topic of the passed {@link StoreIngestionTask} has been attached before, the previously assigned
   * consumer will be returned.
   *
   * This function will also try to avoid assigning the same consumer to the version topics, which are belonging to
   * the same store since for Hybrid store, the ingestion tasks for different store versions will subscribe the same
   * Real-time topic, which won't work if they are using the same shared consumer.
   */
  @Override
  protected synchronized SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    // Check whether this version topic has been subscribed before or not.
    SharedKafkaConsumer chosenConsumer = versionTopicToConsumerMap.get(versionTopic);
    if (chosenConsumer != null) {
      LOGGER.info(
          "The version topic: {} has been subscribed previously, so this function will return the previously assigned shared consumer directly",
          versionTopic);
    } else {

      boolean freshConsumer = false;
      int minAssignmentPerConsumer = Integer.MAX_VALUE;
      for (SharedKafkaConsumer consumer: consumerToConsumptionTask.keySet()) {
        /**
         * A Venice server host may consume from 2 version topics that belongs to the same store because each store has 2
         * versions. We need to make sure multiple store versions won't share the same consumer. Because for Hybrid stores,
         * both the store versions will consume the same RT topic with different offset.
         * To simplify the logic here, we ensure that one consumer consumes from only one version topic of a specific store.
         */
        if (checkWhetherConsumerHasSubscribedSameStore(consumer, versionTopic)) {
          LOGGER.info(
              "Current consumer has already subscribed the same store as the new topic: {}, will skip it and try next consumer in consumer pool",
              versionTopic);
          continue;
        }
        /** If any consumer is idle, choose it. */
        if (!isConsumerAssignedTopic(consumer)) {
          chosenConsumer = consumer;
          freshConsumer = true;
          break;
        }

        // Find the least loaded consumer by partitions
        final int assignedPartitions = consumer.getAssignmentSize();
        if (assignedPartitions < minAssignmentPerConsumer) {
          minAssignmentPerConsumer = assignedPartitions;
          chosenConsumer = consumer;
        }
      }
      if (chosenConsumer == null) {
        stats.recordConsumerSelectionForTopicError();
        throw new VeniceException(
            "Failed to find consumer for topic: " + versionTopic + ", and it might be caused by that all"
                + " the existing consumers have subscribed the same store, and that might be caused by a bug or resource leaking");
      }
      if (freshConsumer) {
        LOGGER.info(
            "Assigned a shared consumer with index of {} for topic: {} with least # of partitions assigned ({})"
                + " and subscribed it to {}",
            consumerToConsumptionTask.indexOf(chosenConsumer),
            versionTopic,
            minAssignmentPerConsumer,
            topicPartition);
      } else {
        LOGGER.info(
            "Assigned a shared consumer with index of {} for topic: {} without any partitions assigned"
                + " and subscribed it to {}",
            consumerToConsumptionTask.indexOf(chosenConsumer),
            versionTopic,
            topicPartition);
      }
    }
    assignVersionTopicToConsumer(versionTopic, chosenConsumer);
    return chosenConsumer;
  }

  /**
   * This function is used to check whether the passed {@param consumer} has already subscribed the topics
   * belonging to the same store, which owns the passed {@param versionTopic} or not.
   */
  private boolean checkWhetherConsumerHasSubscribedSameStore(SharedKafkaConsumer consumer, PubSubTopic versionTopic) {
    String storeName = versionTopic.getStoreName();
    Set<String> stores = consumerToStoresMap.get(consumer);
    return stores != null && stores.contains(storeName);
  }

  /**
   * Detach the messages processing belonging to the topics of the passed {@param ingestionTask}
   */
  @Override
  public synchronized void unsubscribeAll(PubSubTopic versionTopic) {
    SharedKafkaConsumer sharedKafkaConsumer = versionTopicToConsumerMap.get(versionTopic);
    if (sharedKafkaConsumer == null) {
      LOGGER.warn("No assigned shared consumer found for this version topic: {}", versionTopic);
      return;
    }
    removeTopicFromConsumer(versionTopic, sharedKafkaConsumer);
    super.unsubscribeAll(versionTopic);
  }

  /**
   * This function will check a consumer is assigned topic or not. Since we may not have too many consumers and this
   * function will be only called when {@link #pickConsumerForPartition(PubSubTopic, PubSubTopicPartition)} is called the first
   * time.
   */
  private boolean isConsumerAssignedTopic(SharedKafkaConsumer consumer) {
    return consumerToStoresMap.containsKey(consumer);
  }

  private void assignVersionTopicToConsumer(PubSubTopic versionTopic, SharedKafkaConsumer consumer) {
    versionTopicToConsumerMap.put(versionTopic, consumer);
    consumerToStoresMap.computeIfAbsent(consumer, k -> new HashSet<>()).add(versionTopic.getStoreName());
  }

  private void removeTopicFromConsumer(PubSubTopic versionTopic, SharedKafkaConsumer consumer) {
    versionTopicToConsumerMap.remove(versionTopic);
    final String storeName = versionTopic.getStoreName();
    consumerToStoresMap.compute(consumer, (k, assignedStores) -> {
      if (assignedStores != null) {
        assignedStores.remove(storeName);
        return assignedStores.isEmpty() ? null : assignedStores;
      }
      return null;
    });
  }
}
