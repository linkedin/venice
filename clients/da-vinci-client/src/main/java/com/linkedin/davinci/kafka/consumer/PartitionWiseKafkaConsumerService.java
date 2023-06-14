package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
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
 * {@link PartitionWiseKafkaConsumerService} is used to allocate share consumer from consumer pool at partition granularity.
 * One shared consumer may have multiple topics, and each topic may have multiple consumers.
 *
 * For this basic implementation, we rely on round-robin to allocate next consumer from pool to achieve efficient
 * and balanced shared consumer partition assignment load. We can improve this allocation strategy if we need to.
 */
public class PartitionWiseKafkaConsumerService extends KafkaConsumerService {
  /**
   * Mapping from real-time topic partition to consumers. For hybrid store, different version topics from one store
   * have same real-time topics, we should avoid same real-time topic partition from different version topics sharing
   * the same consumer from consumer pool.
   */
  private final Map<PubSubTopicPartition, Set<PubSubConsumerAdapter>> rtTopicPartitionToConsumerMap =
      new VeniceConcurrentHashMap<>();

  private final Logger logger;

  private int shareConsumerIndex = 0;

  PartitionWiseKafkaConsumerService(
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
    this.logger = LogManager.getLogger(PartitionWiseKafkaConsumerService.class + " [" + kafkaUrl + "]");
  }

  @Override
  protected synchronized SharedKafkaConsumer pickConsumerForPartition(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition) {
    // Basic case, round-robin search to find next consumer for this partition.
    boolean seekNewConsumer = true;
    int consumerIndex = -1;
    int consumersChecked = 0;
    SharedKafkaConsumer consumer = null;

    while (seekNewConsumer) {

      // Safeguard logic, avoid infinite loops for searching consumer.
      if (consumersChecked == consumerToConsumptionTask.size()) {
        throw new VeniceException(
            "Can not find consumer for topic: " + topicPartition.getPubSubTopic().getName() + " and partition: "
                + topicPartition.getPartitionNumber() + " from the ingestion task belonging to version topic: "
                + versionTopic);
      }

      consumer = consumerToConsumptionTask.getByIndex(shareConsumerIndex).getKey();
      consumerIndex = shareConsumerIndex;
      shareConsumerIndex++;
      if (shareConsumerIndex == consumerToConsumptionTask.size()) {
        shareConsumerIndex = 0;
      }
      seekNewConsumer = false;

      if (topicPartition.getPubSubTopic().isRealTime()) {
        /**
         * For Hybrid stores, all the store versions will consume the same RT topic with different offset.
         * But one consumer cannot consume from several offsets of one partition at the same time.
         */
        if (alreadySubscribedRealtimeTopicPartition(consumer, topicPartition)) {
          logger.info(
              "Current consumer has already subscribed the same real time topic-partition: {} will skip it and try next consumer in consumer pool",
              topicPartition);
          seekNewConsumer = true;
        } else {
          rtTopicPartitionToConsumerMap.computeIfAbsent(topicPartition, key -> new HashSet<>()).add(consumer);
        }
      }

      consumersChecked++;
    }
    if (consumer == null) {
      throw new IllegalStateException(
          "Did not find a suitable consumer after checking " + consumersChecked + " instances.");
    }
    logger.info(
        "Get shared consumer for: {} from the ingestion task belonging to version topic: {} with index: {}",
        topicPartition,
        versionTopic,
        consumerIndex);
    return consumer;
  }

  private boolean alreadySubscribedRealtimeTopicPartition(
      SharedKafkaConsumer consumer,
      PubSubTopicPartition topicPartition) {
    Set<PubSubConsumerAdapter> consumers = rtTopicPartitionToConsumerMap.get(topicPartition);
    return consumers != null && consumers.contains(consumer);
  }

  @Override
  void handleUnsubscription(SharedKafkaConsumer consumer, PubSubTopicPartition pubSubTopicPartition) {
    if (pubSubTopicPartition.getPubSubTopic().isRealTime()) {
      Set<PubSubConsumerAdapter> rtTopicConsumers = rtTopicPartitionToConsumerMap.get(pubSubTopicPartition);
      if (rtTopicConsumers != null) {
        rtTopicConsumers.remove(consumer);
      }
    }
  }
}
