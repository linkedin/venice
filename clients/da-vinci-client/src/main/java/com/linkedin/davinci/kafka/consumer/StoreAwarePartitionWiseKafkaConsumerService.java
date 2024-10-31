package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Properties;


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
  }

  @Override
  protected synchronized SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    String storeName = versionTopic.getStoreName();
    long minLoad = Long.MAX_VALUE;
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
      long overallLoad = getConsumerStoreLoad(consumer, storeName);
      if (overallLoad < minLoad) {
        minLoadConsumer = consumer;
        minLoad = overallLoad;
      }
    }
    if (minLoad == Long.MAX_VALUE) {
      throw new IllegalStateException("Unable to find least loaded consumer entry.");
    }

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

  long getConsumerStoreLoad(SharedKafkaConsumer consumer, String storeName) {
    long baseAssignmentCount = consumer.getAssignmentSize();
    long storeSubscriptionCount = consumer.getAssignment()
        .stream()
        .filter(x -> Version.parseStoreFromKafkaTopicName(x.getTopicName()).equals(storeName))
        .count();
    return storeSubscriptionCount * IMPOSSIBLE_MAX_PARTITION_COUNT_PER_CONSUMER + baseAssignmentCount;
  }
}
