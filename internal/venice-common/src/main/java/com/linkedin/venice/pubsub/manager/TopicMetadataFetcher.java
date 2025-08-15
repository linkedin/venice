package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.getPubsubOffsetApiTimeoutDurationDefaultValue;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CONSUMER_ACQUISITION_WAIT_TIME;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_OFFSET_FOR_TIME;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_PARTITION_END_POSITION;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_PARTITION_START_POSITION;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_TOPIC_END_POSITIONS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_TOPIC_START_POSITIONS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.PARTITIONS_FOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@Threadsafe
class TopicMetadataFetcher implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataFetcher.class);
  private static final int DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY = 5;
  private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
  private static final List<Class<? extends Throwable>> PUBSUB_RETRIABLE_FAILURES =
      Collections.singletonList(Exception.class);

  /**
   * Blocking queue is used to ensure single-threaded access to a consumer as PubSubConsumerAdapter
   * implementations are not guaranteed to be thread-safe.
   */
  private final BlockingQueue<PubSubConsumerAdapter> pubSubConsumerPool;
  private final List<Closeable> closeables;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final PubSubAdminAdapter pubSubAdminAdapter;
  private final TopicManagerStats stats;
  private final String pubSubClusterAddress;

  /**
   * The following caches store metadata related to topics, including details such as the latest offset,
   * earliest offset, and topic existence. Cached values are set to expire after a specified time-to-live
   * duration. When a value is not present in the cache, it is fetched synchronously. If a cached value is
   * expired, it is retrieved and updated asynchronously.
   * To avoid overwhelming metadata fetcher, only one asynchronous request is issued at a time for a given
   * key within the specific cache.
   */
  private final Map<PubSubTopic, ValueAndExpiryTime<Boolean>> topicExistenceCache = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> latestOffsetCache = new VeniceConcurrentHashMap<>();
  private final Map<PubSubTopicPartition, ValueAndExpiryTime<Long>> lastProducerTimestampCache =
      new VeniceConcurrentHashMap<>();
  private final long cachedEntryTtlInNs;
  private final AtomicInteger consumerWaitListSize = new AtomicInteger(0);

  TopicMetadataFetcher(
      String pubSubClusterAddress,
      TopicManagerContext topicManagerContext,
      TopicManagerStats stats,
      PubSubAdminAdapter pubSubAdminAdapter) {
    this.pubSubClusterAddress = pubSubClusterAddress;
    this.stats = stats;
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.pubSubConsumerPool = new LinkedBlockingQueue<>(topicManagerContext.getTopicMetadataFetcherConsumerPoolSize());
    this.closeables = new ArrayList<>(topicManagerContext.getTopicMetadataFetcherConsumerPoolSize());
    this.cachedEntryTtlInNs = MILLISECONDS.toNanos(topicManagerContext.getTopicOffsetCheckIntervalMs());
    PubSubMessageDeserializer pubSubMessageDeserializer = PubSubMessageDeserializer.createDefaultDeserializer();
    VeniceProperties pubSubProperties = topicManagerContext.getPubSubProperties(pubSubClusterAddress);
    PubSubConsumerAdapterFactory<PubSubConsumerAdapter> pubSubConsumerFactory =
        topicManagerContext.getPubSubConsumerAdapterFactory();
    PubSubConsumerAdapterContext.Builder pubSubConsumerContextBuilder =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(pubSubProperties)
            .setIsOffsetCollectionEnabled(false)
            .setPubSubMessageDeserializer(pubSubMessageDeserializer)
            .setPubSubPositionTypeRegistry(topicManagerContext.getPubSubPositionTypeRegistry())
            .setPubSubTopicRepository(topicManagerContext.getPubSubTopicRepository())
            .setMetricsRepository(topicManagerContext.getMetricsRepository());
    for (int i = 0; i < topicManagerContext.getTopicMetadataFetcherConsumerPoolSize(); i++) {
      pubSubConsumerContextBuilder.setConsumerName("TopicManager-" + i);
      PubSubConsumerAdapter pubSubConsumerAdapter = pubSubConsumerFactory.create(pubSubConsumerContextBuilder.build());
      closeables.add(pubSubConsumerAdapter);
      if (!pubSubConsumerPool.offer(pubSubConsumerAdapter)) {
        throw new VeniceException("Failed to initialize consumer pool for topic metadata fetcher");
      }
    }

    /**
     * A thread-pool used to execute all async methods in this class
     */
    threadPoolExecutor = new ThreadPoolExecutor(
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize(),
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize(),
        15L,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("TopicMetadataFetcherThreadPool", topicManagerContext.getLogContext()));
    threadPoolExecutor.allowCoreThreadTimeOut(true);

    stats.registerTopicMetadataFetcherSensors(this);

    LOGGER.info(
        "Initialized TopicMetadataFetcher for pubSubClusterAddress: {} with consumer pool size: {} and thread pool size: {}",
        pubSubClusterAddress,
        topicManagerContext.getTopicMetadataFetcherConsumerPoolSize(),
        topicManagerContext.getTopicMetadataFetcherThreadPoolSize());
  }

  // Constructor for unit testing only
  TopicMetadataFetcher(
      String pubSubClusterAddress,
      TopicManagerStats stats,
      PubSubAdminAdapter pubSubAdminAdapter,
      BlockingQueue<PubSubConsumerAdapter> pubSubConsumerPool,
      ThreadPoolExecutor threadPoolExecutor,
      long cachedEntryTtlInNs) {
    this.pubSubClusterAddress = pubSubClusterAddress;
    this.stats = stats;
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.pubSubConsumerPool = pubSubConsumerPool;
    this.threadPoolExecutor = threadPoolExecutor;
    this.cachedEntryTtlInNs = cachedEntryTtlInNs;
    this.closeables = new ArrayList<>(pubSubConsumerPool);
  }

  // acquire the consumer from the pool
  PubSubConsumerAdapter acquireConsumer() {
    try {
      consumerWaitListSize.incrementAndGet();
      long startTime = System.nanoTime();
      PubSubConsumerAdapter consumerAdapter = pubSubConsumerPool.take();
      stats.recordLatency(CONSUMER_ACQUISITION_WAIT_TIME, startTime);
      return consumerAdapter;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while acquiring pubSubConsumerAdapter", e);
    } finally {
      consumerWaitListSize.decrementAndGet();
    }
  }

  // release the consumer back to the pool
  void releaseConsumer(PubSubConsumerAdapter pubSubConsumerAdapter) {
    if (!pubSubConsumerPool.offer(pubSubConsumerAdapter)) {
      LOGGER.error("Failed to release pubSubConsumerAdapter back to the pool.");
    }
  }

  // package private for unit testing
  void validateTopicPartition(PubSubTopicPartition pubSubTopicPartition) {
    Objects.requireNonNull(pubSubTopicPartition, "pubSubTopicPartition cannot be null");
    if (pubSubTopicPartition.getPartitionNumber() < 0) {
      throw new IllegalArgumentException("Invalid partition number: " + pubSubTopicPartition.getPartitionNumber());
    }
    try {
      if (containsTopicCached(pubSubTopicPartition.getPubSubTopic())) {
        return;
      }
    } catch (PubSubClientRetriableException e) {
      // in case there is any retriable exception, we will retry the operation
      LOGGER.debug("Failed to check if topic exists: {}", pubSubTopicPartition.getPubSubTopic(), e);
    }

    boolean topicExists = RetryUtils.executeWithMaxAttempt(
        () -> containsTopic(pubSubTopicPartition.getPubSubTopic()),
        3,
        INITIAL_RETRY_DELAY,
        PUBSUB_RETRIABLE_FAILURES);
    if (!topicExists) {
      throw new PubSubTopicDoesNotExistException("Topic does not exist: " + pubSubTopicPartition.getPubSubTopic());
    }
  }

  @Override
  public void close() throws IOException {
    LOGGER.info(
        "Closing TopicMetadataFetcher for pubSubClusterAddress: {} with num of consumers: {}",
        pubSubClusterAddress,
        closeables.size());
    threadPoolExecutor.shutdown();
    try {
      if (!threadPoolExecutor.awaitTermination(50, MILLISECONDS)) {
        threadPoolExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long waitUntil = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
    while (System.currentTimeMillis() <= waitUntil && !closeables.isEmpty()) {
      PubSubConsumerAdapter pubSubConsumerAdapter = null;
      try {
        pubSubConsumerAdapter = pubSubConsumerPool.poll(5, MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (pubSubConsumerAdapter != null) {
        closeables.remove(pubSubConsumerAdapter);
        Utils.closeQuietlyWithErrorLogged(pubSubConsumerAdapter);
      }
    }
    if (closeables.isEmpty()) {
      LOGGER.info("Closed all metadata fetcher consumers for pubSubClusterAddress: {}", pubSubClusterAddress);
      return;
    }
    LOGGER.warn(
        "Failed to close metadata fetcher consumers for pubSubClusterAddress: {}. Forcibly closing "
            + "remaining {} consumers. This may be caused by an improper shutdown sequence.",
        pubSubClusterAddress,
        closeables.size());
    for (Closeable closeable: closeables) {
      Utils.closeQuietlyWithErrorLogged(closeable);
    }
  }

  /**
   * Check if a topic exists in the pubsub cluster
   * @param topic topic to check
   * @return true if the topic exists, false otherwise
   */
  boolean containsTopic(PubSubTopic topic) {
    try {
      long startTime = System.nanoTime();
      boolean containsTopic = pubSubAdminAdapter.containsTopic(topic);
      stats.recordLatency(CONTAINS_TOPIC, startTime);
      return containsTopic;
    } catch (Exception e) {
      LOGGER.debug("Failed to check if topic exists: {}", topic, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  CompletableFuture<Boolean> containsTopicAsync(PubSubTopic topic) {
    return CompletableFuture.supplyAsync(() -> containsTopic(topic), threadPoolExecutor);
  }

  boolean containsTopicCached(PubSubTopic topic) {
    ValueAndExpiryTime<Boolean> cachedValue =
        topicExistenceCache.computeIfAbsent(topic, k -> new ValueAndExpiryTime<>(containsTopic(topic)));
    updateCacheAsync(topic, cachedValue, topicExistenceCache, () -> containsTopicAsync(topic));
    return cachedValue.getValue();
  }

  public boolean containsTopicWithRetries(PubSubTopic pubSubTopic, int retries) {
    return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        () -> containsTopic(pubSubTopic),
        retries,
        INITIAL_RETRY_DELAY,
        Duration.ofSeconds(5),
        Duration.ofMinutes(5),
        PUBSUB_RETRIABLE_FAILURES);
  }

  /**
   * Get information about all partitions of a topic. This is a blocking call.
   * @param topic topic to get partition info for
   * @return a list of partition info. If the topic does not exist, NULL is returned.
   */
  List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic topic) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      long startTime = System.nanoTime();
      List<PubSubTopicPartitionInfo> res = pubSubConsumerAdapter.partitionsFor(topic);
      stats.recordLatency(PARTITIONS_FOR, startTime);
      return res;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  // Acquire/release wrapper
  private <T> T withConsumer(Function<PubSubConsumerAdapter, T> fn) {
    PubSubConsumerAdapter consumer = acquireConsumer();
    try {
      return fn.apply(consumer);
    } finally {
      releaseConsumer(consumer);
    }
  }

  // Get all partitions for a topic, record PARTITIONS_FOR once
  private Collection<PubSubTopicPartition> listPartitionsForTopic(PubSubConsumerAdapter consumer, PubSubTopic topic) {
    long t0 = System.nanoTime();
    List<PubSubTopicPartitionInfo> infos = consumer.partitionsFor(topic);
    stats.recordLatency(PARTITIONS_FOR, t0);

    if (infos == null || infos.isEmpty()) {
      LOGGER.warn("Topic: {} may not exist or has no partitions. Returning empty map.", topic);
      return Collections.emptyList();
    }
    Collection<PubSubTopicPartition> parts = new HashSet<>(infos.size());
    for (PubSubTopicPartitionInfo info: infos) {
      parts.add(info.getTopicPartition());
    }
    return parts;
  }

  Map<PubSubTopicPartition, PubSubPosition> getEndPositionsForTopic(PubSubTopic topic) {
    return withConsumer(consumer -> {
      Collection<PubSubTopicPartition> parts = listPartitionsForTopic(consumer, topic);
      if (parts.isEmpty()) {
        return Collections.emptyMap();
      }
      long t0 = System.nanoTime();
      Map<PubSubTopicPartition, PubSubPosition> res =
          consumer.endPositions(parts, getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_TOPIC_END_POSITIONS, t0);
      return res;
    });
  }

  Map<PubSubTopicPartition, PubSubPosition> getStartPositionsForTopic(PubSubTopic topic) {
    return withConsumer(consumer -> {
      Collection<PubSubTopicPartition> parts = listPartitionsForTopic(consumer, topic);
      if (parts.isEmpty()) {
        return Collections.emptyMap();
      }
      long t0 = System.nanoTime();
      Map<PubSubTopicPartition, PubSubPosition> res =
          consumer.beginningPositions(parts, getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_TOPIC_START_POSITIONS, t0);
      return res;
    });
  }

  PubSubPosition getEndPositionsForPartition(PubSubTopicPartition partition) {
    return withConsumer(consumer -> {
      long t0 = System.nanoTime();
      Map<PubSubTopicPartition, PubSubPosition> res =
          consumer.endPositions(Collections.singleton(partition), getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_PARTITION_END_POSITION, t0);
      PubSubPosition pos = res.get(partition);
      if (pos == null) {
        LOGGER.error("Received null position for topic-partition: {}", partition);
        throw new VeniceException("Got null position for: " + partition);
      }
      return pos;
    });
  }

  PubSubPosition getStartPositionsForPartition(PubSubTopicPartition partition) {
    validateTopicPartition(partition);
    return withConsumer(consumer -> {
      long t0 = System.nanoTime();
      Map<PubSubTopicPartition, PubSubPosition> res = consumer
          .beginningPositions(Collections.singleton(partition), getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_PARTITION_START_POSITION, t0);
      PubSubPosition pos = res.get(partition);
      if (pos == null) {
        LOGGER.error("Received null position for topic-partition: {}", partition);
        throw new VeniceException("Got null position for: " + partition);
      }
      return pos;
    });
  }

  long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    return getEndPositionsForPartition(pubSubTopicPartition).getNumericOffset();
  }

  long getLatestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(() -> {
      validateTopicPartition(pubSubTopicPartition);
      return getLatestOffset(pubSubTopicPartition);
    }, retries, INITIAL_RETRY_DELAY, Duration.ofSeconds(5), Duration.ofMinutes(5), PUBSUB_RETRIABLE_FAILURES);
  }

  CompletableFuture<Long> getLatestOffsetNoRetry(PubSubTopicPartition pubSubTopicPartition) {
    return CompletableFuture.supplyAsync(() -> {
      validateTopicPartition(pubSubTopicPartition);
      return getLatestOffset(pubSubTopicPartition);
    }, threadPoolExecutor);
  }

  CompletableFuture<Long> getLatestOffsetWithRetriesAsync(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return CompletableFuture
        .supplyAsync(() -> getLatestOffsetWithRetries(pubSubTopicPartition, retries), threadPoolExecutor);
  }

  long getLatestOffsetCachedNonBlocking(PubSubTopicPartition pubSubTopicPartition) {
    ValueAndExpiryTime<Long> cachedValue;
    cachedValue = latestOffsetCache.get(pubSubTopicPartition);
    updateCacheAsync(
        pubSubTopicPartition,
        cachedValue,
        latestOffsetCache,
        () -> getLatestOffsetNoRetry(pubSubTopicPartition));
    if (cachedValue == null) {
      cachedValue = latestOffsetCache.get(pubSubTopicPartition);
      if (cachedValue == null) {
        return PubSubConstants.UNKNOWN_LATEST_OFFSET;
      }
    }
    return cachedValue.getValue();
  }

  long getLatestOffsetCached(PubSubTopicPartition pubSubTopicPartition) {
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = latestOffsetCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long latestOffset =
            getLatestOffsetWithRetries(pubSubTopicPartition, DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
        return new ValueAndExpiryTime<>(latestOffset);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get end offset for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
    updateCacheAsync(
        pubSubTopicPartition,
        cachedValue,
        latestOffsetCache,
        () -> getLatestOffsetWithRetriesAsync(
            pubSubTopicPartition,
            DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY));
    return cachedValue.getValue();
  }

  // load the cache with the latest offset
  void populateCacheWithLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    CompletableFuture.runAsync(() -> {
      validateTopicPartition(pubSubTopicPartition);
      getLatestOffsetCached(pubSubTopicPartition);
    }, threadPoolExecutor);
  }

  /**
   * Get the offset for a given timestamp. This is a blocking call.
   * @param pubSubTopicPartition topic partition to get the offset for
   * @param timestamp timestamp to get the offset for
   * @return the offset for the given timestamp
   */
  long getOffsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    validateTopicPartition(pubSubTopicPartition);
    // We start by retrieving the latest offset. If the provided timestamp is out of range,
    // we return the latest offset. This ensures that we don't miss any records produced
    // after the 'offsetForTime' call when the latest offset is obtained after timestamp checking.
    long latestOffset = getLatestOffset(pubSubTopicPartition);

    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      long startTime = System.nanoTime();
      Long result = pubSubConsumerAdapter
          .offsetForTime(pubSubTopicPartition, timestamp, getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_OFFSET_FOR_TIME, startTime);
      if (result != null) {
        return result;
      }
      // When the offset is null, it indicates that the provided timestamp is either out of range
      // or the topic does not contain any messages. In such cases, we log a warning and return
      // the offset of the last message available for the topic-partition.
      LOGGER.warn(
          "Received null offset for timestamp: {} on topic-partition: {}. This may occur if the timestamp is beyond "
              + "the latest message timestamp or if the topic has no messages. Returning the latest offset: {}",
          timestamp,
          pubSubTopicPartition,
          latestOffset);
      return latestOffset;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  /**
   * Get the offset for a given timestamp with retries. This is a blocking call.
   * @param pubSubTopicPartition topic partition to get the offset for
   * @param timestamp timestamp to get the offset for
   * @param retries number of retries
   * @return the offset for the given timestamp
   */
  long getOffsetForTimeWithRetries(PubSubTopicPartition pubSubTopicPartition, long timestamp, int retries) {
    return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        () -> getOffsetForTime(pubSubTopicPartition, timestamp),
        retries,
        INITIAL_RETRY_DELAY,
        Duration.ofSeconds(5),
        Duration.ofMinutes(5),
        PUBSUB_RETRIABLE_FAILURES);
  }

  public PubSubPosition resolvePosition(PubSubTopicPartition partition, int positionTypeId, ByteBuffer buffer) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      return pubSubConsumerAdapter.decodePosition(partition, positionTypeId, buffer);
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  public long diffPosition(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      return pubSubConsumerAdapter.positionDifference(partition, position1, position2);
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  public long comparePosition(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      return pubSubConsumerAdapter.comparePositions(partition, position1, position2);
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  void invalidateKey(PubSubTopicPartition pubSubTopicPartition) {
    latestOffsetCache.remove(pubSubTopicPartition);
    lastProducerTimestampCache.remove(pubSubTopicPartition);
    lastProducerTimestampCache.remove(pubSubTopicPartition);
  }

  CompletableFuture<Void> invalidateKeyAsync(PubSubTopic pubSubTopic) {
    return CompletableFuture.runAsync(() -> invalidateKey(pubSubTopic));
  }

  void invalidateKey(PubSubTopic pubSubTopic) {
    long startTime = System.nanoTime();
    LOGGER.info("Invalidating cache for topic: {}", pubSubTopic);
    topicExistenceCache.remove(pubSubTopic);
    Set<PubSubTopicPartition> topicPartitions = new HashSet<>();

    for (PubSubTopicPartition pubSubTopicPartition: latestOffsetCache.keySet()) {
      if (pubSubTopicPartition.getPubSubTopic().equals(pubSubTopic)) {
        topicPartitions.add(pubSubTopicPartition);
      }
    }
    for (PubSubTopicPartition pubSubTopicPartition: lastProducerTimestampCache.keySet()) {
      if (pubSubTopicPartition.getPubSubTopic().equals(pubSubTopic)) {
        topicPartitions.add(pubSubTopicPartition);
      }
    }

    for (PubSubTopicPartition pubSubTopicPartition: topicPartitions) {
      invalidateKey(pubSubTopicPartition);
    }

    LOGGER.info("Invalidated cache for topic: {} in {} ns", pubSubTopic, System.nanoTime() - startTime);
  }

  /**
   * Asynchronously updates the cache for the specified key if necessary.
   *
   * If there is no entry in the cache or the entry has expired and no update is in progress,
   * this method will attempt to update the cache using the provided {@code completableFutureSupplier}.
   *
   * @param <K> the type of the cache key
   * @param <T> the type of the cached value
   * @param key the key for which the cache should be updated
   * @param cachedValue the current cached value for the specified key
   * @param cache the cache to be updated
   * @param completableFutureSupplier a supplier providing a CompletableFuture for the asynchronous update
   */
  <K, T> void updateCacheAsync(
      K key,
      ValueAndExpiryTime<T> cachedValue,
      Map<K, ValueAndExpiryTime<T>> cache,
      Supplier<CompletableFuture<T>> completableFutureSupplier) {

    if (cachedValue != null
        && (cachedValue.getExpiryTimeNs() > System.nanoTime() || !cachedValue.tryAcquireUpdateLock())) {
      return;
    }

    // check if the value has been already updated by another thread; if so, release the lock and return
    if (cachedValue != null && cachedValue.getExpiryTimeNs() > System.nanoTime()) {
      cachedValue.releaseUpdateLock();
      return;
    }

    completableFutureSupplier.get().whenComplete((value, throwable) -> {
      if (throwable != null) {
        cache.remove(key);

        T cachedContents = (cachedValue != null) ? cachedValue.getValue() : null;
        LOGGER.warn("Failed to update cachedValue for key: {} cachedValue: {}", key, cachedContents, throwable);
        return;
      }
      putLatestValueInCache(key, value, cache);
    });
  }

  // update with the latest value
  private <K, T> void putLatestValueInCache(K key, T latestVal, Map<K, ValueAndExpiryTime<T>> cache) {
    cache.compute(key, (k, v) -> {
      if (v == null) {
        return new ValueAndExpiryTime<>(latestVal);
      } else {
        v.updateValue(latestVal);
        return v;
      }
    });
  }

  /**
   * This class is used to store the value and expiry time of a cached value.
   *
   * Visible for unit testing.
   */
  class ValueAndExpiryTime<T> {
    private volatile T value;
    private volatile long expiryTimeNs;
    private final AtomicBoolean isUpdateInProgress = new AtomicBoolean(false);

    ValueAndExpiryTime(T value) {
      this.value = value;
      this.expiryTimeNs = System.nanoTime() + cachedEntryTtlInNs;
    }

    T getValue() {
      return value;
    }

    long getExpiryTimeNs() {
      return expiryTimeNs;
    }

    boolean tryAcquireUpdateLock() {
      return isUpdateInProgress.compareAndSet(false, true);
    }

    // release the lock
    boolean releaseUpdateLock() {
      return isUpdateInProgress.compareAndSet(true, false);
    }

    void updateValue(T value) {
      this.value = value;
      this.expiryTimeNs = System.nanoTime() + cachedEntryTtlInNs; // update the expiry time
      this.isUpdateInProgress.set(false); // release the lock
    }

    // test only methods
    void setExpiryTimeNs(long expiryTimeNs) {
      this.expiryTimeNs = expiryTimeNs;
    }

    void setUpdateInProgressStatus(boolean status) {
      this.isUpdateInProgress.set(status);
    }
  }

  int getCurrentConsumerPoolSize() {
    return pubSubConsumerPool.size();
  }

  int getAsyncTaskActiveThreadCount() {
    return threadPoolExecutor.getActiveCount();
  }

  int getAsyncTaskQueueLength() {
    return threadPoolExecutor.getQueue().size();
  }

  int getConsumerWaitListSize() {
    return consumerWaitListSize.get();
  }

  @Override
  public String toString() {
    return "TopicMetadataFetcher{" + "pubSubClusterAddress='" + pubSubClusterAddress + '\'' + ", numOfConsumers="
        + closeables.size() + ", threadPoolExecutor=" + threadPoolExecutor + '}';
  }
}
