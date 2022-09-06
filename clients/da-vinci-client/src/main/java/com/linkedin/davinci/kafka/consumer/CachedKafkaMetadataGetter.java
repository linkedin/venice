package com.linkedin.davinci.kafka.consumer;

import static java.util.concurrent.TimeUnit.*;

import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;


/**
 * Because get real-time topic offset, get producer timestamp, and check topic existence are expensive, so we will only
 * retrieve such information after the predefined ttlMs
 */
class CachedKafkaMetadataGetter {
  private final int DEFAULT_MAX_RETRY = 10;

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
  long getOffset(TopicManager topicManager, String topicName, int partitionId) {
    final String sourceKafkaServer = topicManager.getKafkaBootstrapServers();
    try {
      return fetchMetadata(
          new KafkaMetadataCacheKey(sourceKafkaServer, topicName, partitionId),
          offsetCache,
          () -> topicManager.getPartitionLatestOffsetAndRetry(topicName, partitionId, DEFAULT_MAX_RETRY));
    } catch (TopicDoesNotExistException e) {
      // It's observed in production that with java based admin client the topic may not be found temporarily, return
      // error code
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
  }

  long getProducerTimestampOfLastDataMessage(TopicManager topicManager, String topicName, int partitionId) {
    try {
      return fetchMetadata(
          new KafkaMetadataCacheKey(topicManager.getKafkaBootstrapServers(), topicName, partitionId),
          lastProducerTimestampCache,
          () -> topicManager.getProducerTimestampOfLastDataRecord(topicName, partitionId, DEFAULT_MAX_RETRY));
    } catch (TopicDoesNotExistException e) {
      // It's observed in production that with java based admin client the topic may not be found temporarily, return
      // error code
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
  }

  boolean containsTopic(TopicManager topicManager, String topicName) {
    return fetchMetadata(
        new KafkaMetadataCacheKey(topicManager.getKafkaBootstrapServers(), topicName, -1),
        topicExistenceCache,
        () -> topicManager.containsTopic(topicName));
  }

  /**
   * Helper function to fetch metadata from cache or Kafka.
   * @param key cache key: Topic name or TopicPartition
   * @param metadataCache cache for this specific metadata
   * @param valueSupplier function to fetch metadata from Kafka
   * @param <K> type of the cache key
   * @param <T> type of the metadata
   * @return the cache value or the fresh metadata from Kafka
   */
  private <K, T> T fetchMetadata(K key, Map<K, ValueAndExpiryTime<T>> metadataCache, Supplier<T> valueSupplier) {
    final long now = System.nanoTime();

    ValueAndExpiryTime<T> cachedValue = metadataCache.get(key);
    /**
     * The first entry of the pair is the expired time of this metadata; if the expired time is bigger than the current time,
     * reuse the cached value.
     */
    if (cachedValue != null && cachedValue.getExpiryTimeNs() > now) {
      return cachedValue.getValue();
    }
    T newValue = valueSupplier.get();
    if (cachedValue == null) {
      cachedValue = new ValueAndExpiryTime<>(newValue, now + ttlNs);
      metadataCache.put(key, cachedValue);
    } else {
      // Reuse the existing object instead of creating a one to be more memory efficient.
      cachedValue.setValueAndExpiryTimeMs(newValue, now + ttlNs);
    }
    return cachedValue.getValue();
  }

  private static class KafkaMetadataCacheKey {
    private final String kafkaServer;
    private final String topicName;
    private final int partitionId;

    private KafkaMetadataCacheKey(String kafkaServer, String topicName, int partitionId) {
      this.kafkaServer = kafkaServer;
      this.topicName = topicName;
      this.partitionId = partitionId;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + (kafkaServer == null ? 0 : kafkaServer.hashCode());
      result = 31 * result + (topicName == null ? 0 : topicName.hashCode());
      result = 31 * result + partitionId;
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
      return partitionId == other.partitionId && Objects.equals(topicName, other.topicName)
          && Objects.equals(kafkaServer, other.kafkaServer);
    }
  }

  /**
   * A POJO contains a value and its expiry time in milliseconds.
   *
   * @param <T> Type of the value.
   */
  private static class ValueAndExpiryTime<T> {
    private T value;
    private long expiryTimeNs;

    private ValueAndExpiryTime(T value, long expiryTimeNs) {
      this.value = value;
      this.expiryTimeNs = expiryTimeNs;
    }

    T getValue() {
      return value;
    }

    long getExpiryTimeNs() {
      return expiryTimeNs;
    }

    void setValueAndExpiryTimeMs(T newValue, long newExpiryTimeNs) {
      value = newValue;
      expiryTimeNs = newExpiryTimeNs;
    }
  }
}
