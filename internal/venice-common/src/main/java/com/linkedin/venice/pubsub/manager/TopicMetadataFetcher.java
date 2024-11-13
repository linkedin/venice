package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.getPubsubOffsetApiTimeoutDurationDefaultValue;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CONSUMER_ACQUISITION_WAIT_TIME;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_OFFSET_FOR_TIME;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_PARTITION_LATEST_OFFSETS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_PRODUCER_TIMESTAMP_OF_LAST_DATA_MESSAGE;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_TOPIC_LATEST_OFFSETS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.PARTITIONS_FOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@Threadsafe
class TopicMetadataFetcher implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicMetadataFetcher.class);
  private static final int DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY = 5;
  private static final Duration INITIAL_RETRY_DELAY = Duration.ofMillis(100);
  private static final List<Class<? extends Throwable>> PUBSUB_RETRIABLE_FAILURES =
      Arrays.asList(PubSubTopicDoesNotExistException.class, PubSubOpTimeoutException.class);

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
    PubSubMessageDeserializer pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    for (int i = 0; i < topicManagerContext.getTopicMetadataFetcherConsumerPoolSize(); i++) {
      PubSubConsumerAdapter pubSubConsumerAdapter = topicManagerContext.getPubSubConsumerAdapterFactory()
          .create(
              topicManagerContext.getPubSubProperties(pubSubClusterAddress),
              false,
              pubSubMessageDeserializer,
              pubSubClusterAddress);

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
        new DaemonThreadFactory("TopicMetadataFetcherThreadPool"));
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
   * Get the latest offsets for all partitions of a topic. This is a blocking call.
   * @param topic topic to get latest offsets for
   * @return a map of partition id to latest offset. If the topic does not exist, an empty map is returned.
   */
  Int2LongMap getTopicLatestOffsets(PubSubTopic topic) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      long startTime = System.nanoTime();
      List<PubSubTopicPartitionInfo> partitionInfoList = pubSubConsumerAdapter.partitionsFor(topic);
      stats.recordLatency(PARTITIONS_FOR, startTime);

      if (partitionInfoList == null || partitionInfoList.isEmpty()) {
        LOGGER.warn("Topic: {} may not exist or has no partitions. Returning empty map.", topic);
        return Int2LongMaps.EMPTY_MAP;
      }

      Collection<PubSubTopicPartition> topicPartitions = new HashSet<>(partitionInfoList.size());
      for (PubSubTopicPartitionInfo partitionInfo: partitionInfoList) {
        topicPartitions.add(partitionInfo.getTopicPartition());
      }

      startTime = System.nanoTime();
      Map<PubSubTopicPartition, Long> offsetsMap =
          pubSubConsumerAdapter.endOffsets(topicPartitions, getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_TOPIC_LATEST_OFFSETS, startTime);
      Int2LongMap result = new Int2LongOpenHashMap(offsetsMap.size());
      for (Map.Entry<PubSubTopicPartition, Long> entry: offsetsMap.entrySet()) {
        result.put(entry.getKey().getPartitionNumber(), entry.getValue().longValue());
      }
      return result;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
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

  /**
   * Retrieves the latest offset for the specified partition of a PubSub topic.
   *
   * @param pubSubTopicPartition The topic and partition number to query for the latest offset.
   * @return The latest offset for the specified partition.
   * @throws PubSubTopicDoesNotExistException If the topic does not exist.
   * @throws IllegalArgumentException If the partition number is negative.
   * @throws VeniceException If the offset returned by the consumer is null.
   *                         This could indicate a bug in the PubSubConsumerAdapter implementation.
   * @throws PubSubOpTimeoutException If the consumer times out. This could indicate that the topic does not exist
   *                         or the partition does not exist.
   */
  long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    try {
      long startTime = System.nanoTime();
      Map<PubSubTopicPartition, Long> offsetMap = pubSubConsumerAdapter
          .endOffsets(Collections.singleton(pubSubTopicPartition), getPubsubOffsetApiTimeoutDurationDefaultValue());
      stats.recordLatency(GET_PARTITION_LATEST_OFFSETS, startTime);
      Long offset = offsetMap.get(pubSubTopicPartition);
      if (offset == null) {
        LOGGER.error("Received null offset for topic-partition: {}", pubSubTopicPartition);
        // This should never happen; if it does, it's a bug in the PubSubConsumerAdapter implementation
        throw new VeniceException("Got null as latest offset for: " + pubSubTopicPartition);
      }
      return offset;
    } finally {
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  long getLatestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(() -> {
      validateTopicPartition(pubSubTopicPartition);
      return getLatestOffset(pubSubTopicPartition);
    }, retries, INITIAL_RETRY_DELAY, Duration.ofSeconds(5), Duration.ofMinutes(5), PUBSUB_RETRIABLE_FAILURES);
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
        () -> getLatestOffsetWithRetriesAsync(
            pubSubTopicPartition,
            DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY));
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

  /**
   * Get the producer timestamp of the last data message in a topic partition
   * @param pubSubTopicPartition topic partition to get the producer timestamp for
   * @return the producer timestamp of the last data message in the topic partition
   * @throws VeniceException if failed to get the producer timestamp
   */
  long getProducerTimestampOfLastDataMessage(PubSubTopicPartition pubSubTopicPartition) {
    int fetchSize = 10;
    int totalAttempts = 3;
    int fetchedRecordsCount;
    long startTime = System.nanoTime();
    do {
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> lastConsumedRecords =
          consumeLatestRecords(pubSubTopicPartition, fetchSize);
      // if there are no records in this topic partition, return a special timestamp
      if (lastConsumedRecords.isEmpty()) {
        return PubSubConstants.PUBSUB_NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
      }
      fetchedRecordsCount = lastConsumedRecords.size();
      // iterate in reverse order to find the first data message (not control message) from the end
      for (int i = lastConsumedRecords.size() - 1; i >= 0; i--) {
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = lastConsumedRecords.get(i);
        if (!record.getKey().isControlMessage()) {
          stats.recordLatency(GET_PRODUCER_TIMESTAMP_OF_LAST_DATA_MESSAGE, startTime);
          // note that the timestamp is the producer timestamp and not the pubsub message (broker) timestamp
          return record.getValue().getProducerMetadata().getMessageTimestamp();
        }
      }
      fetchSize = 50;
    } while (--totalAttempts > 0);

    String errorMsg = String.format(
        "No data message found in topic-partition: %s when fetching producer timestamp of the last data message. Consumed %d records from the end.",
        pubSubTopicPartition,
        fetchedRecordsCount);
    LOGGER.warn(errorMsg);
    throw new VeniceException(errorMsg);
  }

  long getProducerTimestampOfLastDataMessageWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return RetryUtils.executeWithMaxAttempt(
        () -> getProducerTimestampOfLastDataMessage(pubSubTopicPartition),
        retries,
        INITIAL_RETRY_DELAY,
        PUBSUB_RETRIABLE_FAILURES);
  }

  CompletableFuture<Long> getProducerTimestampOfLastDataMessageWithRetriesAsync(
      PubSubTopicPartition pubSubTopicPartition,
      int retries) {
    return CompletableFuture.supplyAsync(
        () -> getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, retries),
        threadPoolExecutor);
  }

  long getProducerTimestampOfLastDataMessageCached(PubSubTopicPartition pubSubTopicPartition) {
    ValueAndExpiryTime<Long> cachedValue;
    try {
      cachedValue = lastProducerTimestampCache.computeIfAbsent(pubSubTopicPartition, k -> {
        long producerTimestamp = getProducerTimestampOfLastDataMessageWithRetries(
            pubSubTopicPartition,
            DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY);
        return new ValueAndExpiryTime<>(producerTimestamp);
      });
    } catch (PubSubTopicDoesNotExistException | PubSubOpTimeoutException e) {
      LOGGER.error("Failed to get producer timestamp for topic-partition: {}", pubSubTopicPartition, e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }

    updateCacheAsync(
        pubSubTopicPartition,
        cachedValue,
        lastProducerTimestampCache,
        () -> getProducerTimestampOfLastDataMessageWithRetriesAsync(
            pubSubTopicPartition,
            DEFAULT_MAX_RETRIES_FOR_POPULATING_TMD_CACHE_ENTRY));

    return cachedValue.getValue();
  }

  /**
   * This method retrieves last {@code lastRecordsCount} records from a topic partition and there are 4 steps below.
   *  1. Find the current end offset N
   *  2. Seek back {@code lastRecordsCount} records from the end offset N
   *  3. Keep consuming records until the last consumed offset is greater than or equal to N
   *  4. Return all consumed records
   *
   * There are 2 things to note:
   *   1. When this method returns, these returned records are not necessarily the "last" records because after step 2,
   *      there could be more records produced to this topic partition and this method only consume records until the end
   *      offset retrieved at the above step 2.
   *
   *   2. This method might return more than {@code lastRecordsCount} records since the consumer poll method gets a batch
   *      of consumer records each time and the batch size is arbitrary.
   */
  List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumeLatestRecords(
      PubSubTopicPartition pubSubTopicPartition,
      int lastRecordsCount) {
    if (lastRecordsCount < 1) {
      throw new IllegalArgumentException(
          "Last record count must be greater than or equal to 1. Got: " + lastRecordsCount);
    }
    validateTopicPartition(pubSubTopicPartition);
    PubSubConsumerAdapter pubSubConsumerAdapter = acquireConsumer();
    boolean subscribed = false;
    try {
      // find the end offset
      Map<PubSubTopicPartition, Long> offsetMap = pubSubConsumerAdapter
          .endOffsets(Collections.singletonList(pubSubTopicPartition), getPubsubOffsetApiTimeoutDurationDefaultValue());
      if (offsetMap == null || offsetMap.isEmpty()) {
        throw new VeniceException("Failed to get the end offset for topic-partition: " + pubSubTopicPartition);
      }
      long latestOffset = offsetMap.get(pubSubTopicPartition);
      if (latestOffset <= 0) {
        return Collections.emptyList(); // no records in this topic partition
      }

      // find the beginning offset
      long earliestOffset =
          pubSubConsumerAdapter.beginningOffset(pubSubTopicPartition, getPubsubOffsetApiTimeoutDurationDefaultValue());
      if (earliestOffset == latestOffset) {
        return Collections.emptyList(); // no records in this topic partition
      }

      // consume latest records
      ensureConsumerHasNoSubscriptions(pubSubConsumerAdapter);
      long consumePastOffset = Math.max(Math.max(latestOffset - lastRecordsCount, earliestOffset) - 1, -1);
      pubSubConsumerAdapter.subscribe(pubSubTopicPartition, consumePastOffset);
      subscribed = true;
      LOGGER.info(
          "Subscribed to topic partition: {} starting from offset: {}",
          pubSubTopicPartition,
          consumePastOffset + 1);

      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> allConsumedRecords = new ArrayList<>(lastRecordsCount);

      // Keep consuming records from that topic-partition until the last consumed record's
      // offset is greater or equal to the partition end offset retrieved before.
      do {
        List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumedBatch = Collections.emptyList();
        int pollAttempt = 1;
        while (pollAttempt <= PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT
            && (consumedBatch == null || consumedBatch.isEmpty())) {
          consumedBatch =
              pubSubConsumerAdapter.poll(Math.max(10, getPubsubOffsetApiTimeoutDurationDefaultValue().toMillis()))
                  .get(pubSubTopicPartition);
          pollAttempt++;
        }

        // If batch is still empty after retries, give up.
        if (consumedBatch == null || consumedBatch.isEmpty()) {
          String message = String.format(
              "Failed to get records from topic-partition: %s after %d attempts",
              pubSubTopicPartition,
              PubSubConstants.PUBSUB_CONSUMER_POLLING_FOR_METADATA_RETRY_MAX_ATTEMPT);
          LOGGER.error(message);
          throw new VeniceException(message);
        }
        allConsumedRecords.addAll(consumedBatch);
      } while (allConsumedRecords.get(allConsumedRecords.size() - 1).getOffset() + 1 < latestOffset);

      return allConsumedRecords;
    } finally {
      if (subscribed) {
        pubSubConsumerAdapter.unSubscribe(pubSubTopicPartition);
      }
      releaseConsumer(pubSubConsumerAdapter);
    }
  }

  void ensureConsumerHasNoSubscriptions(PubSubConsumerAdapter pubSubConsumerAdapter) {
    Set<PubSubTopicPartition> assignedPartitions = pubSubConsumerAdapter.getAssignment();
    if (assignedPartitions.isEmpty()) {
      return;
    }
    LOGGER.warn(
        "Consumer: {} of has lingering subscriptions: {}. Unsubscribing from all of them." + " Consumer belongs to {}",
        pubSubConsumerAdapter,
        assignedPartitions,
        this);
    pubSubConsumerAdapter.batchUnsubscribe(assignedPartitions);
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
