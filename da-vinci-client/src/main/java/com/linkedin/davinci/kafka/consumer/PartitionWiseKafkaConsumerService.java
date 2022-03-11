package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;


/**
 * {@link PartitionWiseKafkaConsumerService} is used to allocate share consumer from consumer pool at partition granularity.
 * One shared consumer may have multiple topics, and each topic may have multiple consumers.
 * We will prepare a virtual consumer for each topic during {@link KafkaConsumerService#getConsumer(StoreIngestionTask)} phase.
 * When {@link StoreIngestionTask#subscribe(KafkaConsumerWrapper, String, int, long)} is called, it will allocate real shared consumer
 * class to that specific topic-partition.
 *
 * For this basic implementation, we rely on round-robin to allocate next consumer from pool to achieve efficient
 * and balanced shared consumer partition assignment load. We can improve this allocation strategy if we need to.
 */
public class PartitionWiseKafkaConsumerService extends KafkaConsumerService {
  private static final Logger logger = LogManager.getLogger(PartitionWiseKafkaConsumerService.class);
  private static final String exceptionMessageForImproperUsage(String methodName) {
    return methodName + " should never be called on " + VirtualSharedKafkaConsumer.class.getSimpleName() +
        " but should rather be called on " + PartitionWiseSharedKafkaConsumer.class.getSimpleName();
  }

  private final Map<String, VirtualSharedKafkaConsumer> versionTopicToVirtualConsumerMap;
  /**
   * Mapping from real-time topic partition to consumers. For hybrid store, different version topics from one store
   * have same real-time topics, we should avoid same real-time topic partition from different version topics sharing
   * the same consumer from consumer pool.
   */
  private final Map<TopicPartition, Set<KafkaConsumerWrapper>> rtTopicPartitionToConsumerMap;
  private Integer shareConsumerIndex;

  public PartitionWiseKafkaConsumerService(final KafkaClientFactory consumerFactory, final Properties consumerProperties,
      final long readCycleDelayMs, final int numOfConsumersPerKafkaCluster, final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler, final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final KafkaConsumerServiceStats stats, final long sharedConsumerNonExistingTopicCleanupDelayMS,
      final TopicExistenceChecker topicExistenceChecker, final boolean liveConfigBasedKafkaThrottlingEnabled) {
    super(consumerFactory, consumerProperties, readCycleDelayMs, numOfConsumersPerKafkaCluster, bandwidthThrottler,
        recordsThrottler, kafkaClusterBasedRecordThrottler, stats, sharedConsumerNonExistingTopicCleanupDelayMS,
        topicExistenceChecker, liveConfigBasedKafkaThrottlingEnabled);
    this.versionTopicToVirtualConsumerMap = new VeniceConcurrentHashMap<>();
    this.rtTopicPartitionToConsumerMap = new VeniceConcurrentHashMap<>();
    this.shareConsumerIndex = 0;
  }

  @Override
  public SharedKafkaConsumer createSharedKafkaConsumer(final KafkaConsumerWrapper kafkaConsumerWrapper, final long nonExistingTopicCleanupDelayMS,
      TopicExistenceChecker topicExistenceChecker) {
    return new PartitionWiseSharedKafkaConsumer(kafkaConsumerWrapper, this, nonExistingTopicCleanupDelayMS, topicExistenceChecker);
  }

  @Override
  public KafkaConsumerWrapper getConsumer(StoreIngestionTask ingestionTask) {
    // Check whether this version topic has been subscribed before or not.
    String versionTopic = ingestionTask.getVersionTopic();
    if (versionTopicToVirtualConsumerMap.containsKey(versionTopic)) {
      logger.info("The version topic: " + versionTopic + " has already been subscribed,"
          + " so this function will return the previously assigned shared consumer directly");
    }
    VirtualSharedKafkaConsumer chosenConsumer = versionTopicToVirtualConsumerMap.computeIfAbsent(
        versionTopic, vt -> new VirtualSharedKafkaConsumer(ingestionTask));
    return chosenConsumer;
  }

  @Override
  public void attach(KafkaConsumerWrapper consumer, String topic, StoreIngestionTask ingestionTask) {
    // We should not do anything here.
  }

  /**
   * Detach the messages processing belonging to the topics of the passed {@param ingestionTask}
   */
  @Override
  public void detach(StoreIngestionTask ingestionTask) {
    String versionTopic = ingestionTask.getVersionTopic();
    if (!versionTopicToVirtualConsumerMap.containsKey(versionTopic)) {
      logger.warn("No assigned consumer found for this version topic: " + versionTopic);
      return;
    }
    versionTopicToVirtualConsumerMap.computeIfPresent(versionTopic, (vt, consumer) -> {
      consumer.detach(ingestionTask);
      return null;
    });
  }

  private synchronized SharedKafkaConsumer getSharedConsumerForPartition(StoreIngestionTask ingestionTask, String topic, int partition) {
    // Basic case, round-robin search to find next consumer for this partition.
    boolean seekNewConsumer = true;
    String versionTopic = ingestionTask.getVersionTopic();
    int consumerIndex = -1;
    int consumersChecked = 0;

    while(seekNewConsumer) {

      // Safeguard logic, avoid infinite loops for searching consumer.
      if (consumersChecked == readOnlyConsumersList.size()) {
        throw new VeniceException("Can not find consumer for topic: " + topic + " and partition: " + partition
            +  " from the ingestion task belonging to version topic: " + versionTopic);
      }

      SharedKafkaConsumer consumer = readOnlyConsumersList.get(shareConsumerIndex);
      consumerIndex = shareConsumerIndex;
      shareConsumerIndex++;
      if (shareConsumerIndex == readOnlyConsumersList.size()) {
        shareConsumerIndex = 0;
      }
      seekNewConsumer = false;

      if (Version.isRealTimeTopic(topic)) {
        /**
         * For Hybrid stores, all the store versions will consume the same RT topic with different offset.
         * But one consumer cannot consume from several offsets of one partition at the same time.
         */
        if (alreadySubscribedRealtimeTopicPartition(consumer, topic, partition)) {
          logger.info("Current consumer has already subscribed the same real time topic: " + topic + ", partition: " + partition
              + " will skip it and try next consumer in consumer pool");
          seekNewConsumer = true;
        } else {
          TopicPartition rtTopicPartition = new TopicPartition(topic, partition);
          rtTopicPartitionToConsumerMap.computeIfAbsent(rtTopicPartition, key -> new HashSet<>()).add(consumer);
        }
      }
      consumersChecked++;
    }
    logger.info("Get shared consumer for: " + topic + " from the ingestion task belonging to version topic:" + versionTopic
        + " for partition: " + partition + " with index: " + consumerIndex);
    return readOnlyConsumersList.get(consumerIndex);
  }


  private boolean alreadySubscribedRealtimeTopicPartition(SharedKafkaConsumer consumer, String rtTopic, int partition)  {
    TopicPartition rtTopicPartition = new TopicPartition(rtTopic, partition);
    Set<KafkaConsumerWrapper> consumers = rtTopicPartitionToConsumerMap.get(rtTopicPartition);
    return consumers != null && consumers.contains(consumer);
  }

  private void removeRealTimeTopicToSharedConsumerMapping(SharedKafkaConsumer consumer, String rtTopic, int partition) {
    TopicPartition rtTopicPartition = new TopicPartition(rtTopic, partition);
    Set<KafkaConsumerWrapper> rtTopicConsumers = rtTopicPartitionToConsumerMap.get(rtTopicPartition);
    if (rtTopicConsumers != null) {
      rtTopicConsumers.remove(consumer);
    }
  }

  /**
   * {@link VirtualSharedKafkaConsumer} is a virtual topic-wise consumer for {@link KafkaConsumerService#getConsumer(StoreIngestionTask)}.
   * It only maintains the mapping from partition to real {@link SharedKafkaConsumer}. This mapping will only be decided
   * by calling {@link StoreIngestionTask#subscribe(KafkaConsumerWrapper, String, int, long)} for a specific partition,
   * then {@link PartitionWiseKafkaConsumerService#getSharedConsumerForPartition(StoreIngestionTask, String, int)} will find real
   * {@link PartitionWiseSharedKafkaConsumer} for that partition.
   */
  class VirtualSharedKafkaConsumer implements KafkaConsumerWrapper {
    private final Map<TopicPartition, PartitionWiseSharedKafkaConsumer> sharedKafkaConsumerMap;
    private final StoreIngestionTask storeIngestionTask;

    public VirtualSharedKafkaConsumer(@Nonnull StoreIngestionTask storeIngestionTask) {
      Validate.notNull(storeIngestionTask);
      this.sharedKafkaConsumerMap = new VeniceConcurrentHashMap<>();
      this.storeIngestionTask = storeIngestionTask;
    }

    public Map<TopicPartition, PartitionWiseSharedKafkaConsumer> getSharedKafkaConsumerMap() {
      return this.sharedKafkaConsumerMap;
    }

    @Override
    public void subscribe(String topic, int partition, long lastReadOffset) {
      // This is just for partition subscription. We rely on partition-wise kafka consumer service to obtain consumer.
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      // Check whether topic partition subscription has been done to avoid searching for a consumer multiple times.
      if (sharedKafkaConsumerMap.containsKey(topicPartition)) {
        return;
      }
      KafkaConsumerWrapper consumer = getSharedConsumerForPartition(storeIngestionTask, topic, partition);
      if (!(consumer instanceof PartitionWiseSharedKafkaConsumer)) {
        throw new IllegalStateException(
            "The consumer returned should be real " + PartitionWiseSharedKafkaConsumer.class.getSimpleName());
      }
      PartitionWiseSharedKafkaConsumer chosenSharedConsumer = (PartitionWiseSharedKafkaConsumer) consumer;
      chosenSharedConsumer.subscribe(topic, partition, lastReadOffset);
      chosenSharedConsumer.addIngestionTaskForTopicPartition(topicPartition, storeIngestionTask);
      sharedKafkaConsumerMap.put(topicPartition, chosenSharedConsumer);
    }

    @Override
    public void unSubscribe(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      PartitionWiseSharedKafkaConsumer consumer = sharedKafkaConsumerMap.get(topicPartition);
      if (consumer == null) {
        logger.warn("No shared consumer found for topic partition: " + topicPartition);
        return;
      }
      consumer.unSubscribe(topic, partition);
      sharedKafkaConsumerMap.remove(topicPartition);
      if (Version.isRealTimeTopic(topic)) {
        removeRealTimeTopicToSharedConsumerMapping(consumer, topic, partition);
      }
    }

    @Override
    public void batchUnsubscribe(Set<TopicPartition> topicPartitionSet) {
      for (TopicPartition topicPartition : topicPartitionSet) {
        unSubscribe(topicPartition.topic(), topicPartition.partition());
      }
    }

    @Override
    public void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      SharedKafkaConsumer sharedKafkaConsumer = sharedKafkaConsumerMap.get(topicPartition);
      Validate.notNull(sharedKafkaConsumer);
      sharedKafkaConsumer.resetOffset(topic, partition);
    }

    @Override
    public void close() {
      // We should not do anything here.
    }

    @Override
    public ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeoutMs) {
      throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("poll"));
    }

    @Override
    public boolean hasSubscription() {
      return !sharedKafkaConsumerMap.isEmpty();
    }

    @Override
    public boolean hasSubscription(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      return sharedKafkaConsumerMap.containsKey(topicPartition);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(List<TopicPartition> topicPartitions) {
      throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("beginningOffsets"));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions) {
      throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("endOffsets"));
    }

    @Override
    public void assign(Collection<TopicPartition> topicPartitions) {
      throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("assign"));
    }

    @Override
    public void seek(TopicPartition topicPartition, long nextOffset) {
      SharedKafkaConsumer sharedKafkaConsumer = sharedKafkaConsumerMap.get(topicPartition);
      Validate.notNull(sharedKafkaConsumer);
      sharedKafkaConsumer.seek(topicPartition, nextOffset);
    }

    @Override
    public void pause(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      SharedKafkaConsumer sharedKafkaConsumer = sharedKafkaConsumerMap.get(topicPartition);
      Validate.notNull(sharedKafkaConsumer);
      sharedKafkaConsumer.pause(topic, partition);
    }

    @Override
    public void resume(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      SharedKafkaConsumer sharedKafkaConsumer = sharedKafkaConsumerMap.get(topicPartition);
      Validate.notNull(sharedKafkaConsumer);
      sharedKafkaConsumer.resume(topic, partition);
    }

    @Override
    public Set<TopicPartition> paused() {
      throw new VeniceUnsupportedOperationException(exceptionMessageForImproperUsage("paused"));
    }

    @Override
    public Set<TopicPartition> getAssignment() {
      return this.sharedKafkaConsumerMap.keySet();
    }

    public void detach(StoreIngestionTask ingestionTask) {
      sharedKafkaConsumerMap.forEach((topicPartition, consumer) -> consumer.detach(ingestionTask));
      sharedKafkaConsumerMap.clear();
    }
  }

}
