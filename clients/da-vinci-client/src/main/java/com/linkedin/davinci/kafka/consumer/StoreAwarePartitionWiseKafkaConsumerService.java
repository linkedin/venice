package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;


/**
 * {@link StoreAwarePartitionWiseKafkaConsumerService} is used to allocate share consumer from consumer pool at partition
 * granularity. One shared consumer may have multiple topics, and each topic may have multiple consumers.
 * This is store-aware version of topic-wise shared consumer service. The topic partition assignment in this service has
 * a heuristic that we should distribute the all the subscriptions related to a same store / version as even as possible.
 * The load calculation for each consumer will be:
 * Consumer assignment size + IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER * subscription count for the same store;
 * and we will pick the least loaded consumer for a new topic partition request. If there is no eligible consumer, it
 * will throw {@link IllegalStateException}
 */
public class StoreAwarePartitionWiseKafkaConsumerService extends PartitionWiseKafkaConsumerService {
  // This constant makes sure the store subscription count will always be prioritized over consumer assignment count.
  private static final int IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER = 10000;
  private final Map<SharedKafkaConsumer, Integer> consumerToBaseLoadCount = new VeniceConcurrentHashMap<>();
  private final Map<SharedKafkaConsumer, Map<String, Integer>> consumerToStoreLoadCount =
      new VeniceConcurrentHashMap<>();

  StoreAwarePartitionWiseKafkaConsumerService(
      final ConsumerPoolType poolType,
      final Properties consumerProperties,
      final long readCycleDelayMs,
      final int numOfConsumersPerKafkaCluster,
      final IngestionThrottler ingestionThrottler,
      final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      final String kafkaClusterAlias,
      final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final StaleTopicChecker staleTopicChecker,
      final boolean liveConfigBasedKafkaThrottlingEnabled,
      final Time time,
      final AggKafkaConsumerServiceStats stats,
      final boolean isKafkaConsumerOffsetCollectionEnabled,
      final ReadOnlyStoreRepository metadataRepository,
      final boolean isUnregisterMetricForDeletedStoreEnabled,
      final VeniceServerConfig veniceServerConfig,
      final PubSubContext pubSubContext,
      final ExecutorService crossTpProcessingPool) {
    super(
        poolType,
        consumerProperties,
        readCycleDelayMs,
        numOfConsumersPerKafkaCluster,
        ingestionThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        kafkaClusterAlias,
        sharedConsumerNonExistingTopicCleanupDelayMS,
        staleTopicChecker,
        liveConfigBasedKafkaThrottlingEnabled,
        time,
        stats,
        isKafkaConsumerOffsetCollectionEnabled,
        metadataRepository,
        isUnregisterMetricForDeletedStoreEnabled,
        StoreAwarePartitionWiseKafkaConsumerService.class.toString(),
        veniceServerConfig,
        pubSubContext,
        crossTpProcessingPool);
  }

  @Override
  protected synchronized SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    String storeName = versionTopic.getStoreName();
    int minLoad = Integer.MAX_VALUE;
    SharedKafkaConsumer minLoadConsumer = null;
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
      int overallLoad = getConsumerStoreLoad(consumer, storeName);
      if (overallLoad < minLoad) {
        minLoadConsumer = consumer;
        minLoad = overallLoad;
      }
    }
    if (minLoad == Integer.MAX_VALUE) {
      throw new IllegalStateException("Unable to find least loaded consumer entry.");
    }

    // Update RT topic partition consumer map.
    if (topicPartition.getPubSubTopic().isRealTime()) {
      getRtTopicPartitionToConsumerMap().computeIfAbsent(topicPartition, key -> new HashSet<>()).add(minLoadConsumer);
    }

    getLOGGER().info(
        "Picked consumer id: {}, assignment size: {}, computed load: {} for topic partition: {}, version topic: {}",
        getConsumerToConsumptionTask().indexOf(minLoadConsumer),
        getConsumerToBaseLoadCount().getOrDefault(minLoadConsumer, 0),
        minLoad,
        topicPartition,
        versionTopic);
    increaseConsumerStoreLoad(minLoadConsumer, storeName);
    return minLoadConsumer;
  }

  @Override
  void handleUnsubscription(
      SharedKafkaConsumer consumer,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    super.handleUnsubscription(consumer, versionTopic, pubSubTopicPartition);
    decreaseConsumerStoreLoad(consumer, versionTopic);
  }

  int getConsumerStoreLoad(SharedKafkaConsumer consumer, String storeName) {
    int baseAssignmentCount = getConsumerToBaseLoadCount().getOrDefault(consumer, 0);
    int storeSubscriptionCount =
        getConsumerToStoreLoadCount().getOrDefault(consumer, Collections.emptyMap()).getOrDefault(storeName, 0);
    return storeSubscriptionCount * IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER + baseAssignmentCount;
  }

  void increaseConsumerStoreLoad(SharedKafkaConsumer consumer, String storeName) {
    getConsumerToBaseLoadCount().compute(consumer, (k, v) -> (v == null) ? 1 : v + 1);
    getConsumerToStoreLoadCount().computeIfAbsent(consumer, k -> new VeniceConcurrentHashMap<>())
        .compute(storeName, (k, v) -> (v == null) ? 1 : v + 1);
  }

  void decreaseConsumerStoreLoad(SharedKafkaConsumer consumer, PubSubTopic versionTopic) {
    /**
     * When versionTopic is null, it means a specific Topic-Partition has been unsubscribed for more than 1 time. This
     * can happen during version deprecation, where {@link ParticipantStoreConsumptionTask} is also trying to unsubscribe
     * every partitions
     */
    if (versionTopic == null) {
      getLOGGER().warn(
          "Incoming versionTopic is null, will skip decreasing store load for consumer: {} with index: {}",
          consumer,
          getConsumerToConsumptionTask().indexOf(consumer));
      return;
    }
    String storeName = versionTopic.getStoreName();
    if (!getConsumerToBaseLoadCount().containsKey(consumer)) {
      throw new IllegalStateException("Consumer to base load count map does not contain consumer: " + consumer);
    }
    if (!getConsumerToStoreLoadCount().containsKey(consumer)) {
      throw new IllegalStateException("Consumer to store load count map does not contain consumer: " + consumer);
    }
    if (!getConsumerToStoreLoadCount().get(consumer).containsKey(storeName)) {
      throw new IllegalStateException("Consumer to store load count map does not contain store: " + storeName);
    }
    getConsumerToBaseLoadCount().computeIfPresent(consumer, (k, v) -> (v == 1) ? null : v - 1);
    getConsumerToStoreLoadCount().computeIfPresent(consumer, (k, innerMap) -> {
      innerMap.computeIfPresent(storeName, (s, c) -> (c == 1) ? null : c - 1);
      return innerMap.isEmpty() ? null : innerMap;
    });
  }

  Map<SharedKafkaConsumer, Map<String, Integer>> getConsumerToStoreLoadCount() {
    return consumerToStoreLoadCount;
  }

  Map<SharedKafkaConsumer, Integer> getConsumerToBaseLoadCount() {
    return consumerToBaseLoadCount;
  }
}
