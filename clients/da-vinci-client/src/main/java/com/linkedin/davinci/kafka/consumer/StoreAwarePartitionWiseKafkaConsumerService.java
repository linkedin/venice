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
import java.util.Optional;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;


/**
 * {@link StoreAwarePartitionWiseKafkaConsumerService} is used to allocate share consumer from consumer pool at partition
 * granularity. One shared consumer may have multiple topics, and each topic may have multiple consumers.
 * This is store-aware version of topic-wise shared consumer service. The topic partition assignment in this service has
 * a heuristic that we should distribute the all the subscriptions related to a same store / version as even as possible.
 * The load calculation for each consumer will be:
 * Hybrid store: Consumer assignment size + STORE_SUBSCRIPTION_LOAD * subscription count for the same store;
 * Batch only store: Consumer assignment size + STORE_SUBSCRIPTION_LOAD * subscription count for the same store version;
 * and we will pick the least loaded consumer for a new topic partition request.
 */
public class StoreAwarePartitionWiseKafkaConsumerService extends PartitionWiseKafkaConsumerService {
  private static final long DEFAULT_STORE_SUBSCRIPTION_LOAD = 100;
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
        isUnregisterMetricForDeletedStoreEnabled);
    this.LOGGER =
        LogManager.getLogger(StoreAwarePartitionWiseKafkaConsumerService.class + " [" + kafkaUrlForLogger + "]");
    this.storeRepository = metadataRepository;
  }

  @Override
  protected synchronized SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    String resourceIdentifier = getResourceIdentifier(versionTopic);
    Map<Integer, Long> consumerToLoadMap = new VeniceConcurrentHashMap<>();
    for (int i = 0; i < getConsumerToConsumptionTask().size(); i++) {
      SharedKafkaConsumer consumer = getConsumerToConsumptionTask().getByIndex(i).getKey();
      if (topicPartition.getPubSubTopic().isRealTime()
          && alreadySubscribedRealtimeTopicPartition(consumer, topicPartition)) {
        getLOGGER().info(
            "Consumer ID: {} has already subscribed the same real time topic-partition: {}, will assign MAX load to avoid being picked.",
            i,
            topicPartition);
        consumerToLoadMap.put(i, Long.MAX_VALUE);
        continue;
      }
      long baseAssignmentCount = consumer.getAssignmentSize();
      long storeSubscriptionCount =
          getResourceIdentifierToConsumerMap().getOrDefault(resourceIdentifier, Collections.emptyMap())
              .getOrDefault(consumer, 0);
      long overallLoad = storeSubscriptionCount * DEFAULT_STORE_SUBSCRIPTION_LOAD + baseAssignmentCount;
      consumerToLoadMap.put(i, overallLoad);
    }

    // Sort consumer by computed load.
    Optional<Map.Entry<Integer, Long>> leastLoadedEntry =
        consumerToLoadMap.entrySet().stream().min(Map.Entry.comparingByValue());
    if (!leastLoadedEntry.isPresent()) {
      throw new IllegalStateException(
          "Unable to find least loaded consumer entry. Current load map: " + consumerToLoadMap);
    }

    int index = leastLoadedEntry.get().getKey();
    long load = leastLoadedEntry.get().getValue();

    if (load == Long.MAX_VALUE) {
      throw new IllegalStateException("Did not find a suitable consumer with valid load.");
    }

    SharedKafkaConsumer consumer = getConsumerToConsumptionTask().getByIndex(index).getKey();
    // Update resource identifier to consumer load map.
    Map<PubSubConsumerAdapter, Integer> consumerMap = getResourceIdentifierToConsumerMap()
        .computeIfAbsent(resourceIdentifier, key -> new VeniceConcurrentHashMap<>());
    int existCount = consumerMap.getOrDefault(consumer, 0);
    consumerMap.put(consumer, existCount + 1);

    // Update RT topic partition consumer map.
    if (topicPartition.getPubSubTopic().isRealTime()) {
      getRtTopicPartitionToConsumerMap().computeIfAbsent(topicPartition, key -> new HashSet<>()).add(consumer);
    }

    getLOGGER().info(
        "Picked consumer id: {}, assignment size: {}, computed load: {} for topic partition: {}, version topic: {}",
        index,
        consumer.getAssignmentSize(),
        load,
        topicPartition,
        versionTopic);
    return consumer;
  }

  @Override
  void handleUnsubscription(
      SharedKafkaConsumer consumer,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    super.handleUnsubscription(consumer, versionTopic, pubSubTopicPartition);

    // Update resource identifier to consumer assignment map.
    String resourceIdentifier = getResourceIdentifier(versionTopic);
    if (!getResourceIdentifierToConsumerMap().containsKey(resourceIdentifier)) {
      throw new IllegalStateException(
          "Resource identifier consumer map does not contain expected resource identifier: " + resourceIdentifier);
    }
    int count = getResourceIdentifierToConsumerMap().get(resourceIdentifier).getOrDefault(consumer, 0);
    if (count <= 0) {
      throw new IllegalStateException("Consumer assignment count map does not contain matching consumer: " + consumer);
    }
    if (count == 1) {
      getResourceIdentifierToConsumerMap().get(resourceIdentifier).remove(consumer);
      if (getResourceIdentifierToConsumerMap().get(resourceIdentifier).isEmpty()) {
        getResourceIdentifierToConsumerMap().remove(resourceIdentifier);
      }
    } else {
      getResourceIdentifierToConsumerMap().get(resourceIdentifier).put(consumer, count - 1);
    }
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

  public Map<String, Map<PubSubConsumerAdapter, Integer>> getResourceIdentifierToConsumerMap() {
    return resourceIdentifierToConsumerMap;
  }

  public ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }
}
