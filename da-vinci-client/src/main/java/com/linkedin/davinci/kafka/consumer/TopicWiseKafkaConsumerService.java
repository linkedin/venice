package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
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
  private final Map<String, SharedKafkaConsumer> versionTopicToConsumerMap = new VeniceConcurrentHashMap<>();
  private final Map<SharedKafkaConsumer, Set<String>> consumerToStoresMap = new VeniceConcurrentHashMap<>();
  private final Logger logger;

  public TopicWiseKafkaConsumerService(final KafkaClientFactory consumerFactory, final Properties consumerProperties,
      final long readCycleDelayMs, final int numOfConsumersPerKafkaCluster, final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler, final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final KafkaConsumerServiceStats stats, final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final TopicExistenceChecker topicExistenceChecker, final boolean liveConfigBasedKafkaThrottlingEnabled) {
    super(consumerFactory, consumerProperties, readCycleDelayMs, numOfConsumersPerKafkaCluster, bandwidthThrottler,
        recordsThrottler, kafkaClusterBasedRecordThrottler, stats, sharedConsumerNonExistingTopicCleanupDelayMS,
        topicExistenceChecker, liveConfigBasedKafkaThrottlingEnabled);
    logger = LogManager.getLogger(TopicWiseKafkaConsumerService.class + " [" + kafkaUrl + "]");
  }

  @Override
  public SharedKafkaConsumer createSharedKafkaConsumer(final KafkaConsumerWrapper kafkaConsumerWrapper, final long nonExistingTopicCleanupDelayMS,
      TopicExistenceChecker topicExistenceChecker) {
    return new TopicWiseSharedKafkaConsumer(kafkaConsumerWrapper, this, nonExistingTopicCleanupDelayMS, topicExistenceChecker);
  }

  @Override
  public KafkaConsumerWrapper getConsumerAssignedToVersionTopic(String versionTopic) {
    return versionTopicToConsumerMap.get(versionTopic);
  }

  /**
   * This function will return a consumer for the passed {@link StoreIngestionTask}.
   * If the version topic of the passed {@link StoreIngestionTask} has been attached before, the previously assigned
   * consumer will be returned.
   *
   * This function will also try to avoid assigning the same consumer to the version topics, which are belonging to
   * the same store since for Hybrid store, the ingestion tasks for different store versions will subscribe the same
   * Real-time topic, which won't work if they are using the same shared consumer.
   * @param ingestionTask
   * @return
   */
  @Override
  public synchronized KafkaConsumerWrapper assignConsumerFor(StoreIngestionTask ingestionTask) {
    String versionTopic = ingestionTask.getVersionTopic();
    // Check whether this version topic has been subscribed before or not.
    SharedKafkaConsumer chosenConsumer = null;
    chosenConsumer = versionTopicToConsumerMap.get(versionTopic);
    if (null != chosenConsumer) {
      logger.info("The version topic: " + versionTopic + " has been subscribed previously,"
          + " so this function will return the previously assigned shared consumer directly");
      return chosenConsumer;
    }

    int minAssignmentPerConsumer = Integer.MAX_VALUE;
    for (SharedKafkaConsumer consumer : readOnlyConsumersList) {
      /**
       * A Venice server host may consume from 2 version topics that belongs to the same store because each store has 2
       * versions. We need to make sure multiple store versions won't share the same consumer. Because for Hybrid stores,
       * both the store versions will consume the same RT topic with different offset.
       * To simplify the logic here, we ensure that one consumer consumes from only one version topic of a specific store.
       */
      if (checkWhetherConsumerHasSubscribedSameStore(consumer, versionTopic)) {
        logger.info("Current consumer has already subscribed the same store as the new topic: " + versionTopic + ", "
            + "will skip it and try next consumer in consumer pool");
        continue;
      }
      /**
       * Find the zero loaded consumer by topics. There is a delay between {@link SharedKafkaConsumer#attach(String, StoreIngestionTask)}
       * and {@link SharedKafkaConsumer#assign(List)} partitions, so we should guarantee every {@link SharedKafkaConsumer}
       * is assigned with {@linkStoreIngestionTask} first.
       */
      if (!isConsumerAssignedTopic(consumer)) {
        chosenConsumer = consumer;
        assignVersionTopicToConsumer(versionTopic, chosenConsumer);
        logger.info("Assigned a shared consumer with index of " + readOnlyConsumersList.indexOf(chosenConsumer) +
            " without any assigned topic for topic: " + versionTopic);
        return chosenConsumer;
      }

      // Find the least loaded consumer by partitions
      final int assignedPartitions = consumer.getAssignmentSize();
      if (assignedPartitions < minAssignmentPerConsumer) {
        minAssignmentPerConsumer = assignedPartitions;
        chosenConsumer = consumer;
      }
    }
    if (null == chosenConsumer) {
      stats.recordConsumerSelectionForTopicError();
      throw new VeniceException("Failed to find consumer for topic: " + versionTopic + ", and it might be caused by that all"
          + " the existing consumers have subscribed the same store, and that might be caused by a bug or resource leaking");
    }
    assignVersionTopicToConsumer(versionTopic, chosenConsumer);
    logger.info("Assigned a shared consumer with index of " + readOnlyConsumersList.indexOf(chosenConsumer) + " for topic: "
        + versionTopic + " with least # of partitions assigned: " + minAssignmentPerConsumer);
    return chosenConsumer;
  }

  /**
   * This function is used to check whether the passed {@param consumer} has already subscribed the topics
   * belonging to the same store, which owns the passed {@param versionTopic} or not.
   * @param consumer
   * @param versionTopic
   * @return
   */
  private boolean checkWhetherConsumerHasSubscribedSameStore(SharedKafkaConsumer consumer, String versionTopic)  {
    String storeName = Version.parseStoreFromKafkaTopicName(versionTopic);
    Set<String> stores = consumerToStoresMap.get(consumer);
    return stores != null && stores.contains(storeName);
  }

  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   */
  @Override
  public void attach(KafkaConsumerWrapper consumer, String topic, StoreIngestionTask ingestionTask) {
    if (!(consumer instanceof SharedKafkaConsumer)) {
      throw new VeniceException("The `consumer` passed must be a `SharedKafkaConsumer`");
    }
    SharedKafkaConsumer sharedConsumer = (SharedKafkaConsumer)consumer;
    if (!readOnlyConsumersList.contains(sharedConsumer)) {
      throw new VeniceException("Unknown shared consumer passed");
    }
    sharedConsumer.attach(topic, ingestionTask);
  }

  /**
   * Detach the messages processing belonging to the topics of the passed {@param ingestionTask}
   */
  @Override
  public synchronized void unsubscribeAll(String versionTopic) {
    SharedKafkaConsumer sharedKafkaConsumer = versionTopicToConsumerMap.get(versionTopic);
    if (null == sharedKafkaConsumer) {
      logger.warn("No assigned shared consumer found for this version topic: " + versionTopic);
      return;
    }
    removeTopicFromConsumer(versionTopic, sharedKafkaConsumer);
    sharedKafkaConsumer.unsubscribeAll(versionTopic);
  }

  /**
   * This function will check a consumer is assigned topic or not. Since we may not have too many consumers and this
   * function will be only called when {@link KafkaConsumerService#assignConsumerFor(StoreIngestionTask)} called at the
   * first time.
   */
  private boolean isConsumerAssignedTopic(SharedKafkaConsumer consumer) {
    return consumerToStoresMap.containsKey(consumer);
  }

  private void assignVersionTopicToConsumer(String versionTopic, SharedKafkaConsumer consumer) {
    versionTopicToConsumerMap.put(versionTopic, consumer);
    consumerToStoresMap.computeIfAbsent(consumer, k -> new HashSet<>()).add(Version.parseStoreFromKafkaTopicName(versionTopic));
  }

  private void removeTopicFromConsumer(String versionTopic, SharedKafkaConsumer consumer) {
    versionTopicToConsumerMap.remove(versionTopic);
    final String storeName = Version.parseStoreFromKafkaTopicName(versionTopic);
    consumerToStoresMap.compute(consumer, (k, assignedStores) -> {
      if (assignedStores != null) {
        assignedStores.remove(storeName);
        return assignedStores.isEmpty() ? null : assignedStores;
      }
      return null;
    });
  }
}
