package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;


/**
 * {@link StoreAwarePartitionWiseKafkaConsumerService} is used to allocate share consumer from consumer pool at partition
 * granularity. One shared consumer may have multiple topics, and each topic may have multiple consumers.
 * This is store-aware version of topic-wise shared consumer service. The topic partition assignment in this service has
 * a heuristic that we should distribute the all the subscriptions related to a same store / version as even as possible.
 * The load calculation for each consumer will be:
 * Hybrid store: Consumer assignment size + IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER * subscription count for the same store;
 * Batch only store: Consumer assignment size + IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER * subscription count for the same store version;
 * and we will pick the least loaded consumer for a new topic partition request.
 */
public class StoreAwarePartitionWiseKafkaConsumerService extends PartitionWiseKafkaConsumerService {
  // This constant makes sure the store / store version count will always be prioritized over consumer assignment count.
  private static final int IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER = 10000;
  /**
   * Resource identifier to Consumer Count Map. Resource identifier is the token that determines replica distribution
   * for different consumer. For batch only store, resource identifier is topic name. For hybrid store, resource
   * identifier is the store name.
   */
  private final Map<String, Map<PubSubConsumerAdapter, Integer>> resourceIdentifierToConsumerMap =
      new VeniceConcurrentHashMap<>();
  private final ReadOnlyStoreRepository storeRepository;

  StoreAwarePartitionWiseKafkaConsumerService(
      final ConsumerPoolType poolType,
      final PubSubConsumerAdapterFactory consumerFactory,
      final Properties consumerProperties,
      final long readCycleDelayMs,
      final int numOfConsumersPerKafkaCluster,
      final IngestionThrottler ingestionThrottler,
      final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      final String kafkaClusterAlias,
      final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final TopicExistenceChecker topicExistenceChecker,
      final boolean liveConfigBasedKafkaThrottlingEnabled,
      final PubSubMessageDeserializer pubSubDeserializer,
      final Time time,
      final AggKafkaConsumerServiceStats stats,
      final boolean isKafkaConsumerOffsetCollectionEnabled,
      final ReadOnlyStoreRepository metadataRepository,
      final boolean isUnregisterMetricForDeletedStoreEnabled) {
    super(
        poolType,
        consumerFactory,
        consumerProperties,
        readCycleDelayMs,
        numOfConsumersPerKafkaCluster,
        ingestionThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        kafkaClusterAlias,
        sharedConsumerNonExistingTopicCleanupDelayMS,
        topicExistenceChecker,
        liveConfigBasedKafkaThrottlingEnabled,
        pubSubDeserializer,
        time,
        stats,
        isKafkaConsumerOffsetCollectionEnabled,
        metadataRepository,
        isUnregisterMetricForDeletedStoreEnabled,
        StoreAwarePartitionWiseKafkaConsumerService.class.toString());
    this.storeRepository = metadataRepository;
  }

  @Override
  protected synchronized SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    String resourceIdentifier = getResourceIdentifier(versionTopic);
    int minLoad = Integer.MAX_VALUE;
    SharedKafkaConsumer minLoadConsumer = null;
    Map<Integer, Integer> consumerToLoadMap = new VeniceConcurrentHashMap<>();
    for (SharedKafkaConsumer consumer: getConsumerToConsumptionTask().keySet()) {
      int index = getConsumerToConsumptionTask().indexOf(consumer);
      if (topicPartition.getPubSubTopic().isRealTime()
          && alreadySubscribedRealtimeTopicPartition(consumer, topicPartition)) {
        getLOGGER().info(
            "Consumer id: {} has already subscribed the same real time topic-partition: {} and thus cannot be picked",
            index,
            topicPartition);
        continue;
      }
      int baseAssignmentCount = consumer.getAssignmentSize();
      int storeSubscriptionCount =
          getResourceIdentifierToConsumerMap().getOrDefault(resourceIdentifier, Collections.emptyMap())
              .getOrDefault(consumer, 0);
      int overallLoad = storeSubscriptionCount * IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER + baseAssignmentCount;
      if (overallLoad < minLoad) {
        minLoadConsumer = consumer;
        minLoad = overallLoad;
      }
      consumerToLoadMap.put(index, overallLoad);
    }
    if (minLoad == Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Unable to find least loaded consumer entry. Current load map: " + consumerToLoadMap);
    }
    // Update resource identifier to consumer load map.
    Map<PubSubConsumerAdapter, Integer> consumerMap = getResourceIdentifierToConsumerMap()
        .computeIfAbsent(resourceIdentifier, key -> new VeniceConcurrentHashMap<>());
    int existCount = consumerMap.getOrDefault(minLoadConsumer, 0);
    consumerMap.put(minLoadConsumer, existCount + 1);

    // Update RT topic partition consumer map.
    if (topicPartition.getPubSubTopic().isRealTime()) {
      getRtTopicPartitionToConsumerMap().computeIfAbsent(topicPartition, key -> new HashSet<>()).add(minLoadConsumer);
    }

    getLOGGER().info(
        "Picked consumer id: {}, assignment size: {}, computed load: {} for topic partition: {}, version topic: {}",
        getConsumerToConsumptionTask().indexOf(minLoadConsumer),
        minLoadConsumer.getAssignmentSize(),
        minLoad,
        topicPartition,
        versionTopic);
    return minLoadConsumer;
  }

  @Override
  void handleUnsubscription(
      SharedKafkaConsumer consumer,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    super.handleUnsubscription(consumer, versionTopic, pubSubTopicPartition);
    // Update resource identifier to consumer assignment map.
    String resourceIdentifier = getResourceIdentifier(versionTopic);
    decreaseResourceIdentifierToConsumerCount(resourceIdentifier, consumer);
  }

  synchronized void decreaseResourceIdentifierToConsumerCount(String resourceIdentifier, SharedKafkaConsumer consumer) {
    getResourceIdentifierToConsumerMap().compute(resourceIdentifier, (key, consumerCountMap) -> {
      if (consumerCountMap == null) {
        throw new IllegalStateException(
            "Resource identifier consumer map does not contain expected resource identifier: " + resourceIdentifier);
      }
      consumerCountMap.compute(consumer, (k, count) -> {
        if (count == null || count <= 0) {
          throw new IllegalStateException(
              "Consumer assignment count map does not contain matching consumer: " + consumer);
        }
        return count == 1 ? null : count - 1;
      });
      return consumerCountMap.isEmpty() ? null : consumerCountMap;
    });
  }

  String getResourceIdentifier(PubSubTopic topic) {
    String storeName = topic.getStoreName();
    Store store = getStoreRepository().getStore(storeName);
    if (store == null) {
      getLOGGER().warn("Unable to locate store: {}, will default to use store name as resource identifier.", storeName);
      return storeName;
    }
    int versionNumber = Version.parseVersionFromVersionTopicName(topic.getName());
    Version version = store.getVersion(versionNumber);
    if (version == null) {
      getLOGGER().warn(
          "Unable to locate version: {} for store name: {}, will rely on store's hybrid config to decide resource identifier.",
          versionNumber,
          storeName);
      return store.isHybrid() ? storeName : topic.getName();
    }
    if (version.getHybridStoreConfig() == null) {
      return topic.getName();
    }
    return storeName;
  }

  Map<String, Map<PubSubConsumerAdapter, Integer>> getResourceIdentifierToConsumerMap() {
    return resourceIdentifierToConsumerMap;
  }

  ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }
}
