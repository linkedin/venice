package com.linkedin.davinci.kafka.consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Because get real-time topic offset, get producer timestamp, and check topic existence are expensive, so we will only
 * retrieve such information after the predefined ttlMs
 */
class CachedKafkaMetadataGetter {
  private static final Logger LOGGER = LogManager.getLogger(CachedKafkaMetadataGetter.class);
  private static final int DEFAULT_MAX_RETRY = 10;

  private final long ttlNs;
  private final Map<KafkaMetadataCacheKey, ValueAndExpiryTime<Boolean>> topicExistenceCache;
  private final Map<KafkaMetadataCacheKey, ValueAndExpiryTime<Long>> offsetCache;
  private final Map<KafkaMetadataCacheKey, ValueAndExpiryTime<Long>> lastProducerTimestampCache;

  CachedKafkaMetadataGetter(long timeToLiveMs) {
    this.ttlNs = MILLISECONDS.toNanos(timeToLiveMs);
    this.topicExistenceCache = new VeniceConcurrentHashMap<>();
    this.offsetCache = new VeniceConcurrentHashMap<>();
    this.lastProducerTimestampCache = new VeniceConcurrentHashMap<>();
  }

  /**
   * @return Users of this method should be aware that Kafka will actually
   * return the next available offset rather the latest used offset. Therefore,
   * the value will be 1 offset greater than what's expected.
   */
  long getOffset(TopicManager topicManager, PubSubTopic pubSubTopic, int partitionId) {
    final String sourceKafkaServer = topicManager.getKafkaBootstrapServers();
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partitionId);
    try {
      return fetchMetadata(
          new KafkaMetadataCacheKey(sourceKafkaServer, pubSubTopicPartition),
          offsetCache,
          () -> topicManager.getPartitionLatestOffsetAndRetry(pubSubTopicPartition, DEFAULT_MAX_RETRY));
    } catch (TopicDoesNotExistException e) {
      // It's observed in production that with java based admin client the topic may not be found temporarily, return
      // error code
      LOGGER.error("Failed to get offset for topic partition {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
  }

  long getEarliestOffset(TopicManager topicManager, PubSubTopicPartition pubSubTopicPartition) {
    final String sourceKafkaServer = topicManager.getKafkaBootstrapServers();
    try {
      return fetchMetadata(
          new KafkaMetadataCacheKey(sourceKafkaServer, pubSubTopicPartition),
          offsetCache,
          () -> topicManager.getPartitionEarliestOffsetAndRetry(pubSubTopicPartition, DEFAULT_MAX_RETRY));
    } catch (TopicDoesNotExistException e) {
      // It's observed in production that with java based admin client the topic may not be found temporarily, return
      // error code
      LOGGER.error("Failed to get offset for topic partition {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
  }

  long getProducerTimestampOfLastDataMessage(TopicManager topicManager, PubSubTopicPartition pubSubTopicPartition) {
    try {
      return fetchMetadata(
          new KafkaMetadataCacheKey(topicManager.getKafkaBootstrapServers(), pubSubTopicPartition),
          lastProducerTimestampCache,
          () -> topicManager.getProducerTimestampOfLastDataRecord(pubSubTopicPartition, DEFAULT_MAX_RETRY));
    } catch (TopicDoesNotExistException e) {
      // It's observed in production that with java based admin client the topic may not be found temporarily, return
      // error code
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
  }

  boolean containsTopic(TopicManager topicManager, PubSubTopic pubSubTopic) {
    return fetchMetadata(
        new KafkaMetadataCacheKey(
            topicManager.getKafkaBootstrapServers(),
            new PubSubTopicPartitionImpl(pubSubTopic, -1)),
        topicExistenceCache,
        () -> topicManager.containsTopic(pubSubTopic));
  }

  /**
   * Helper function to fetch metadata from cache or Kafka.
   * @param key cache key: Topic name or TopicPartition
   * @param metadataCache cache for this specific metadata
   * @param valueSupplier function to fetch metadata from Kafka
   * @param <T> type of the metadata
   * @return the cache value or the fresh metadata from Kafka
   */
  <T> T fetchMetadata(
      KafkaMetadataCacheKey key,
      Map<KafkaMetadataCacheKey, ValueAndExpiryTime<T>> metadataCache,
      Supplier<T> valueSupplier) {
    final long now = System.nanoTime();
    final ValueAndExpiryTime<T> cachedValue =
        metadataCache.computeIfAbsent(key, k -> new ValueAndExpiryTime<>(valueSupplier.get(), now + ttlNs));

    // For a given key in the given cache, we will only issue one async request at the same time.
    if (cachedValue.getExpiryTimeNs() <= now && cachedValue.valueUpdateInProgress.compareAndSet(false, true)) {
      CompletableFuture.runAsync(() -> {
        try {
          T newValue = valueSupplier.get();
          metadataCache.put(key, new ValueAndExpiryTime<>(newValue, System.nanoTime() + ttlNs));
        } catch (Exception e) {
          metadataCache.remove(key);
        }
      });
    }
    return cachedValue.getValue();
  }

  static class KafkaMetadataCacheKey {
    private final String kafkaServer;
    private final PubSubTopicPartition pubSubTopicPartition;

    KafkaMetadataCacheKey(String kafkaServer, PubSubTopicPartition pubSubTopicPartition) {
      this.kafkaServer = kafkaServer;
      this.pubSubTopicPartition = pubSubTopicPartition;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + (kafkaServer == null ? 0 : kafkaServer.hashCode());
      result = 31 * result + (pubSubTopicPartition == null ? 0 : pubSubTopicPartition.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof KafkaMetadataCacheKey)) {
        return false;
      }

      final KafkaMetadataCacheKey other = (KafkaMetadataCacheKey) o;
      return pubSubTopicPartition.equals(other.pubSubTopicPartition) && Objects.equals(kafkaServer, other.kafkaServer);
    }
  }

  /**
   * A POJO contains a value and its expiry time in milliseconds.
   *
   * @param <T> Type of the value.
   */
  static class ValueAndExpiryTime<T> {
    private final T value;
    private final long expiryTimeNs;
    private final AtomicBoolean valueUpdateInProgress = new AtomicBoolean(false);

    ValueAndExpiryTime(T value, long expiryTimeNs) {
      this.value = value;
      this.expiryTimeNs = expiryTimeNs;
    }

    T getValue() {
      return value;
    }

    long getExpiryTimeNs() {
      return expiryTimeNs;
    }
  }
}
