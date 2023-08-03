package com.linkedin.venice.kafka;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcher;
import com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcherFactory;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubInstrumentedAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubInvalidReplicationFactorException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Topic Manager is shared by multiple cluster's controllers running in one physical Venice controller instance.
 *
 * This class contains one global {@link PubSubConsumerAdapter}, which is not thread-safe, so when you add new functions,
 * which is using this global consumer, please add 'synchronized' keyword, otherwise this {@link TopicManager}
 * won't be thread-safe, and Kafka consumer will report the following error when multiple threads are trying to
 * use the same consumer: PubSubConsumerAdapter is not safe for multi-threaded access.
 */
public class TopicManager implements Closeable {
  private static final int MINIMUM_TOPIC_DELETION_STATUS_POLL_TIMES = 10;
  private static final int FAST_KAFKA_OPERATION_TIMEOUT_MS = Time.MS_PER_SECOND;
  protected static final long ETERNAL_TOPIC_RETENTION_POLICY_MS = Long.MAX_VALUE;

  public static final long DEFAULT_TOPIC_RETENTION_POLICY_MS = 5 * Time.MS_PER_DAY;
  public static final long BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN = 2 * Time.MS_PER_DAY;

  public static final int DEFAULT_KAFKA_OPERATION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  public static final long UNKNOWN_TOPIC_RETENTION = Long.MIN_VALUE;
  public static final int MAX_TOPIC_DELETE_RETRIES = 3;
  public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 3;

  /**
   * Default setting is that no log compaction should happen for hybrid store version topics
   * if the messages are produced within 24 hours; otherwise servers could encounter MISSING
   * data DIV errors for reprocessing jobs which could potentially generate lots of
   * duplicate keys.
   */
  public static final long DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS = 24 * Time.MS_PER_HOUR;

  private static final List<Class<? extends Throwable>> CREATE_TOPIC_RETRIABLE_EXCEPTIONS = Collections
      .unmodifiableList(Arrays.asList(PubSubInvalidReplicationFactorException.class, PubSubOpTimeoutException.class));

  // Immutable state
  private final Logger logger;
  private final String pubSubBootstrapServers;
  private final long kafkaOperationTimeoutMs;
  private final long topicDeletionStatusPollIntervalMs;
  private final long topicMinLogCompactionLagMs;
  private final PubSubAdminAdapterFactory<PubSubAdminAdapter> pubSubAdminAdapterFactory;
  // TODO: Use single PubSubAdminAdapter for both read and write operations
  private final Lazy<PubSubAdminAdapter> pubSubWriteOnlyAdminAdapter;
  private final Lazy<PubSubAdminAdapter> pubSubReadOnlyAdminAdapter;
  private final PartitionOffsetFetcher partitionOffsetFetcher;

  // It's expensive to grab the topic config over and over again, and it changes infrequently. So we temporarily cache
  // queried configs.
  Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache =
      Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

  public TopicManager(TopicManagerRepository.Builder builder, String pubSubBootstrapServers) {
    this.logger = LogManager.getLogger(this.getClass().getSimpleName() + " [" + pubSubBootstrapServers + "]");
    this.kafkaOperationTimeoutMs = builder.getKafkaOperationTimeoutMs();
    this.topicDeletionStatusPollIntervalMs = builder.getTopicDeletionStatusPollIntervalMs();
    this.topicMinLogCompactionLagMs = builder.getTopicMinLogCompactionLagMs();
    this.pubSubAdminAdapterFactory = builder.getPubSubAdminAdapterFactory();
    this.pubSubBootstrapServers = pubSubBootstrapServers;

    TopicManagerRepository.SSLPropertiesSupplier pubSubProperties = builder.getPubSubProperties();
    PubSubTopicRepository pubSubTopicRepository = builder.getPubSubTopicRepository();

    Optional<MetricsRepository> optionalMetricsRepository = Optional.ofNullable(builder.getMetricsRepository());

    this.pubSubReadOnlyAdminAdapter = Lazy.of(() -> {
      PubSubAdminAdapter pubSubReadOnlyAdmin =
          pubSubAdminAdapterFactory.create(pubSubProperties.get(pubSubBootstrapServers), pubSubTopicRepository);
      pubSubReadOnlyAdmin = createInstrumentedPubSubAdmin(
          optionalMetricsRepository,
          "ReadOnlyKafkaAdminStats",
          pubSubReadOnlyAdmin,
          pubSubBootstrapServers);
      logger.info(
          "{} is using read-only pubsub admin client of class: {}",
          this.getClass().getSimpleName(),
          pubSubReadOnlyAdmin.getClassName());
      return pubSubReadOnlyAdmin;
    });

    this.pubSubWriteOnlyAdminAdapter = Lazy.of(() -> {
      PubSubAdminAdapter pubSubWriteOnlyAdmin =
          pubSubAdminAdapterFactory.create(pubSubProperties.get(pubSubBootstrapServers), pubSubTopicRepository);
      pubSubWriteOnlyAdmin = createInstrumentedPubSubAdmin(
          optionalMetricsRepository,
          "WriteOnlyKafkaAdminStats",
          pubSubWriteOnlyAdmin,
          pubSubBootstrapServers);
      logger.info(
          "{} is using write-only pubsub admin client of class: {}",
          this.getClass().getSimpleName(),
          pubSubWriteOnlyAdmin.getClassName());
      return pubSubWriteOnlyAdmin;
    });

    this.partitionOffsetFetcher = PartitionOffsetFetcherFactory.createDefaultPartitionOffsetFetcher(
        builder.getPubSubConsumerAdapterFactory(),
        pubSubProperties.get(pubSubBootstrapServers),
        pubSubBootstrapServers,
        pubSubReadOnlyAdminAdapter,
        kafkaOperationTimeoutMs,
        optionalMetricsRepository);
  }

  private PubSubAdminAdapter createInstrumentedPubSubAdmin(
      Optional<MetricsRepository> optionalMetricsRepository,
      String statsNamePrefix,
      PubSubAdminAdapter pubSubAdmin,
      String pubSubBootstrapServers) {
    if (optionalMetricsRepository.isPresent()) {
      // Use pub sub bootstrap server to identify which pub sub admin client stats it is
      final String pubSubAdminStatsName =
          String.format("%s_%s_%s", statsNamePrefix, pubSubAdmin.getClassName(), pubSubBootstrapServers);
      PubSubAdminAdapter instrumentedPubSubAdminAdapter =
          new PubSubInstrumentedAdminAdapter(pubSubAdmin, optionalMetricsRepository.get(), pubSubAdminStatsName);
      logger.info(
          "Created instrumented pubsub admin client for pubsub cluster with bootstrap "
              + "servers: {} and with stat name prefix: {}",
          pubSubBootstrapServers,
          statsNamePrefix);
      return instrumentedPubSubAdminAdapter;
    } else {
      logger.info(
          "Created non-instrumented pubsub admin client for pubsub cluster with bootstrap servers: {}",
          pubSubBootstrapServers);
      return pubSubAdmin;
    }
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @see {@link #createTopic(PubSubTopic, int, int, boolean, boolean, Optional)}
   */
  public void createTopic(PubSubTopic topicName, int numPartitions, int replication, boolean eternal) {
    createTopic(topicName, numPartitions, replication, eternal, false, Optional.empty(), false);
  }

  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      boolean eternal,
      boolean logCompaction,
      Optional<Integer> minIsr) {
    createTopic(topicName, numPartitions, replication, eternal, logCompaction, minIsr, true);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param eternal if true, the topic will have "infinite" (~250 mil years) retention
   *                if false, its retention will be set to {@link #DEFAULT_TOPIC_RETENTION_POLICY_MS} by default
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, Kafka cluster defaults will be used
   * @param useFastKafkaOperationTimeout if false, normal kafka operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      boolean eternal,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastKafkaOperationTimeout) {
    long retentionTimeMs;
    if (eternal) {
      retentionTimeMs = ETERNAL_TOPIC_RETENTION_POLICY_MS;
    } else {
      retentionTimeMs = DEFAULT_TOPIC_RETENTION_POLICY_MS;
    }
    createTopic(
        topicName,
        numPartitions,
        replication,
        retentionTimeMs,
        logCompaction,
        minIsr,
        useFastKafkaOperationTimeout);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param retentionTimeMs Retention time, in ms, for the topic
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, Kafka cluster defaults will be used
   * @param useFastKafkaOperationTimeout if false, normal kafka operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      long retentionTimeMs,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastKafkaOperationTimeout) {

    long startTime = System.currentTimeMillis();
    long deadlineMs =
        startTime + (useFastKafkaOperationTimeout ? FAST_KAFKA_OPERATION_TIMEOUT_MS : kafkaOperationTimeoutMs);
    PubSubTopicConfiguration pubSubTopicConfiguration =
        new PubSubTopicConfiguration(Optional.of(retentionTimeMs), logCompaction, minIsr, topicMinLogCompactionLagMs);
    logger.info(
        "Creating topic: {} partitions: {} replication: {}, configuration: {}",
        topicName,
        numPartitions,
        replication,
        pubSubTopicConfiguration);

    try {
      RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> pubSubWriteOnlyAdminAdapter.get()
              .createTopic(topicName, numPartitions, replication, pubSubTopicConfiguration),
          10,
          Duration.ofMillis(200),
          Duration.ofSeconds(1),
          Duration.ofMillis(useFastKafkaOperationTimeout ? FAST_KAFKA_OPERATION_TIMEOUT_MS : kafkaOperationTimeoutMs),
          CREATE_TOPIC_RETRIABLE_EXCEPTIONS);
    } catch (Exception e) {
      if (ExceptionUtils.recursiveClassEquals(e, PubSubTopicExistsException.class)) {
        logger.info("Topic: {} already exists, will update retention policy.", topicName);
        waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
        updateTopicRetention(topicName, retentionTimeMs);
        logger.info("Updated retention policy to be {}ms for topic: {}", retentionTimeMs, topicName);
        return;
      } else {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + topicName + ". Topic still does not exist after "
                + (deadlineMs - startTime) + "ms.",
            e);
      }
    }
    waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
    boolean eternal = retentionTimeMs == ETERNAL_TOPIC_RETENTION_POLICY_MS;
    logger.info("Successfully created {}topic: {}", eternal ? "eternal " : "", topicName);
  }

  protected void waitUntilTopicCreated(PubSubTopic topicName, int partitionCount, long deadlineMs) {
    long startTime = System.currentTimeMillis();
    while (!containsTopicAndAllPartitionsAreOnline(topicName, partitionCount)) {
      if (System.currentTimeMillis() > deadlineMs) {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + topicName + ".  Topic still did not pass all the checks after "
                + (deadlineMs - startTime) + "ms.");
      }
      Utils.sleep(200);
    }
  }

  /**
   * This method sends a delete command to Kafka and immediately returns with a future. The future could be null if the
   * underlying Kafka admin client doesn't support it. In both cases, deletion will occur asynchronously.
   * @param topicName
   */
  private Future<Void> ensureTopicIsDeletedAsync(PubSubTopic topicName) {
    // TODO: Stop using Kafka APIs which depend on ZK.
    logger.info("Deleting topic: {}", topicName);
    return pubSubWriteOnlyAdminAdapter.get().deleteTopic(topicName);
  }

  /**
   * Update retention for the given topic.
   * If the topic doesn't exist, this operation will throw {@link PubSubTopicDoesNotExistException}
   * @param topicName
   * @param retentionInMS
   * @return true if the retention time config of the input topic gets updated; return false if nothing gets updated
   */
  public boolean updateTopicRetention(PubSubTopic topicName, long retentionInMS)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    return updateTopicRetention(topicName, retentionInMS, pubSubTopicConfiguration);
  }

  /**
   * Update retention for the given topic given a {@link Properties}.
   * @param topicName
   * @param expectedRetentionInMs
   * @param pubSubTopicConfiguration
   * @return true if the retention time gets updated; false if no update is needed.
   */
  public boolean updateTopicRetention(
      PubSubTopic topicName,
      long expectedRetentionInMs,
      PubSubTopicConfiguration pubSubTopicConfiguration) throws PubSubTopicDoesNotExistException {
    Optional<Long> retentionTimeMs = pubSubTopicConfiguration.retentionInMs();
    if (!retentionTimeMs.isPresent() || expectedRetentionInMs != retentionTimeMs.get()) {
      pubSubTopicConfiguration.setRetentionInMs(Optional.of(expectedRetentionInMs));
      pubSubWriteOnlyAdminAdapter.get().setTopicConfig(topicName, pubSubTopicConfiguration);
      logger.info(
          "Updated topic: {} with retention.ms: {} in cluster [{}]",
          topicName,
          expectedRetentionInMs,
          this.pubSubBootstrapServers);
      return true;
    }
    // Retention time has already been updated for this topic before
    return false;
  }

  public synchronized void updateTopicCompactionPolicy(PubSubTopic topic, boolean expectedLogCompacted) {
    updateTopicCompactionPolicy(topic, expectedLogCompacted, -1);
  }

  /**
   * Update topic compaction policy.
   * @param topic
   * @param expectedLogCompacted
   * @param minLogCompactionLagMs the overrode min log compaction lag. If this is specified and a valid number (> 0), it will
   *                              override the default config
   * @throws PubSubTopicDoesNotExistException, if the topic doesn't exist
   */
  public synchronized void updateTopicCompactionPolicy(
      PubSubTopic topic,
      boolean expectedLogCompacted,
      long minLogCompactionLagMs) throws PubSubTopicDoesNotExistException {
    long expectedMinLogCompactionLagMs = 0l;
    if (expectedLogCompacted) {
      if (minLogCompactionLagMs > 0) {
        expectedMinLogCompactionLagMs = minLogCompactionLagMs;
      } else {
        expectedMinLogCompactionLagMs = topicMinLogCompactionLagMs;
      }
    }

    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topic);
    boolean currentLogCompacted = pubSubTopicConfiguration.isLogCompacted();
    long currentMinLogCompactionLagMs = pubSubTopicConfiguration.minLogCompactionLagMs();
    if (expectedLogCompacted != currentLogCompacted
        || expectedLogCompacted && expectedMinLogCompactionLagMs != currentMinLogCompactionLagMs) {
      pubSubTopicConfiguration.setLogCompacted(expectedLogCompacted);
      pubSubTopicConfiguration.setMinLogCompactionLagMs(expectedMinLogCompactionLagMs);
      pubSubWriteOnlyAdminAdapter.get().setTopicConfig(topic, pubSubTopicConfiguration);
      logger.info(
          "Kafka compaction policy for topic: {} has been updated from {} to {}, min compaction lag updated from {} to {}",
          topic,
          currentLogCompacted,
          expectedLogCompacted,
          currentMinLogCompactionLagMs,
          expectedMinLogCompactionLagMs);
    }
  }

  public boolean isTopicCompactionEnabled(PubSubTopic topicName) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(topicName);
    return topicProperties.isLogCompacted();
  }

  public long getTopicMinLogCompactionLagMs(PubSubTopic topicName) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(topicName);
    return topicProperties.minLogCompactionLagMs();
  }

  public boolean updateTopicMinInSyncReplica(PubSubTopic topicName, int minISR)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    Optional<Integer> currentMinISR = pubSubTopicConfiguration.minInSyncReplicas();
    // config doesn't exist config is different
    if (!currentMinISR.isPresent() || !currentMinISR.get().equals(minISR)) {
      pubSubTopicConfiguration.setMinInSyncReplicas(Optional.of(minISR));
      pubSubWriteOnlyAdminAdapter.get().setTopicConfig(topicName, pubSubTopicConfiguration);
      logger.info("Updated topic: {} with min.insync.replicas: {}", topicName, minISR);
      return true;
    }
    // min.insync.replicas has already been updated for this topic before
    return false;
  }

  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return pubSubReadOnlyAdminAdapter.get().getAllTopicRetentions();
  }

  /**
   * Return topic retention time in MS.
   */
  public long getTopicRetention(PubSubTopic topicName) throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    return getTopicRetention(pubSubTopicConfiguration);
  }

  public long getTopicRetention(PubSubTopicConfiguration pubSubTopicConfiguration) {
    if (pubSubTopicConfiguration.retentionInMs().isPresent()) {
      return pubSubTopicConfiguration.retentionInMs().get();
    }
    return UNKNOWN_TOPIC_RETENTION;
  }

  /**
   * Check whether topic is absent or truncated
   * @param topicName
   * @param truncatedTopicMaxRetentionMs
   * @return true if the topic does not exist or if it exists but its retention time is below truncated threshold
   *         false if the topic exists and its retention time is above truncated threshold
   */
  public boolean isTopicTruncated(PubSubTopic topicName, long truncatedTopicMaxRetentionMs) {
    try {
      return isRetentionBelowTruncatedThreshold(getTopicRetention(topicName), truncatedTopicMaxRetentionMs);
    } catch (PubSubTopicDoesNotExistException e) {
      return true;
    }
  }

  public boolean isRetentionBelowTruncatedThreshold(long retention, long truncatedTopicMaxRetentionMs) {
    return retention != UNKNOWN_TOPIC_RETENTION && retention <= truncatedTopicMaxRetentionMs;
  }

  /**
   * This operation is a little heavy, since it will pull the configs for all the topics.
   */
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topicName) throws PubSubTopicDoesNotExistException {
    final PubSubTopicConfiguration pubSubTopicConfiguration =
        pubSubReadOnlyAdminAdapter.get().getTopicConfig(topicName);
    topicConfigCache.put(topicName, pubSubTopicConfiguration);
    return pubSubTopicConfiguration;
  }

  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topicName) {
    final PubSubTopicConfiguration pubSubTopicConfiguration =
        pubSubReadOnlyAdminAdapter.get().getTopicConfigWithRetry(topicName);
    topicConfigCache.put(topicName, pubSubTopicConfiguration);
    return pubSubTopicConfiguration;
  }

  /**
   * Still heavy, but can be called repeatedly to amortize that cost.
   */
  public PubSubTopicConfiguration getCachedTopicConfig(PubSubTopic topicName) {
    // query the cache first, if it doesn't have it, query it from kafka and store it.
    PubSubTopicConfiguration pubSubTopicConfiguration = topicConfigCache.getIfPresent(topicName);
    if (pubSubTopicConfiguration == null) {
      pubSubTopicConfiguration = getTopicConfigWithRetry(topicName);
    }
    return pubSubTopicConfiguration;
  }

  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    final Map<PubSubTopic, PubSubTopicConfiguration> topicConfigs =
        pubSubReadOnlyAdminAdapter.get().getSomeTopicConfigs(topicNames);
    for (Map.Entry<PubSubTopic, PubSubTopicConfiguration> topicConfig: topicConfigs.entrySet()) {
      topicConfigCache.put(topicConfig.getKey(), topicConfig.getValue());
    }
    return topicConfigs;
  }

  /**
   * This function is used to address the following problem:
   * 1. Topic deletion is a async operation in Kafka;
   * 2. Topic deletion triggered by Venice could happen in the middle of other Kafka operation;
   * 3. Kafka operations against non-existing topic will hang;
   * By using this function, the topic deletion is a sync op, which bypasses the hanging issue of
   * non-existing topic operations.
   * Once Kafka addresses the hanging issue of non-existing topic operations, we can safely revert back
   * to use the async version: {@link #ensureTopicIsDeletedAsync(PubSubTopic)}
   *
   * It is intentional to make this function to be non-synchronized since it could lock
   * {@link TopicManager} for a pretty long time (up to 30 seconds) if topic deletion is slow.
   * When topic deletion slowness happens, it will cause other operations, such as {@link #getTopicLatestOffsets(PubSubTopic)}
   * to be blocked for a long time, and this could cause push job failure.
   *
   * Even with non-synchronized function, Venice could still guarantee there will be only one topic
   * deletion at one time since all the topic deletions are handled by topic cleanup service serially.
   *
   * @param topicName
   */
  public void ensureTopicIsDeletedAndBlock(PubSubTopic topicName) throws ExecutionException {
    if (!containsTopicAndAllPartitionsAreOnline(topicName)) {
      // Topic doesn't exist
      return;
    }

    Future<Void> future = ensureTopicIsDeletedAsync(topicName);
    if (future != null) {
      // Skip additional checks for Java kafka client since the result of the future can guarantee that the topic is
      // deleted.
      try {
        future.get(kafkaOperationTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new VeniceException("Thread interrupted while waiting to delete topic: " + topicName);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof PubSubTopicDoesNotExistException) {
          // No-op. Topic is deleted already, consider this as a successful deletion.
        } else {
          throw e;
        }
      } catch (TimeoutException e) {
        throw new PubSubOpTimeoutException(
            "Failed to delete kafka topic: " + topicName + " after " + kafkaOperationTimeoutMs);
      }
      logger.info("Topic: {} has been deleted", topicName);
      // TODO: Remove the checks below once we have fully migrated to use the Kafka admin client.
      return;
    }
    // Since topic deletion is async, we would like to poll until topic doesn't exist any more
    long MAX_TIMES = topicDeletionStatusPollIntervalMs == 0
        ? kafkaOperationTimeoutMs
        : (kafkaOperationTimeoutMs / topicDeletionStatusPollIntervalMs);
    /**
     * In case we have bad config, MAX_TIMES can not be smaller than {@link #MINIMUM_TOPIC_DELETION_STATUS_POLL_TIMES}.
     */
    MAX_TIMES = Math.max(MAX_TIMES, MINIMUM_TOPIC_DELETION_STATUS_POLL_TIMES);
    final int MAX_CONSUMER_RECREATION_INTERVAL = 100;
    int current = 0;
    int lastConsumerRecreation = 0;
    int consumerRecreationInterval = 5;
    while (++current <= MAX_TIMES) {
      Utils.sleep(topicDeletionStatusPollIntervalMs);
      // Re-create consumer every once in a while, in case it's wedged on some stale state.
      boolean closeAndRecreateConsumer = (current - lastConsumerRecreation) == consumerRecreationInterval;
      if (closeAndRecreateConsumer) {
        /**
         * Exponential back-off:
         * Recreate the consumer after polling status for 2 times, (2+)4 times, (2+4+)8 times... and maximum 100 times
         */
        lastConsumerRecreation = current;
        consumerRecreationInterval = Math.min(consumerRecreationInterval * 2, MAX_CONSUMER_RECREATION_INTERVAL);
        if (consumerRecreationInterval <= 0) {
          // In case it overflows
          consumerRecreationInterval = MAX_CONSUMER_RECREATION_INTERVAL;
        }
      }
    }
    throw new PubSubOpTimeoutException(
        "Failed to delete kafka topic: " + topicName + " after " + kafkaOperationTimeoutMs + " ms (" + current
            + " attempts).");
  }

  public void ensureTopicIsDeletedAndBlockWithRetry(PubSubTopic topicName) throws ExecutionException {
    // Topic deletion may time out, so go ahead and retry the operation up the max number of attempts, if we
    // simply cannot succeed, bubble the exception up.
    int attempts = 0;
    while (true) {
      try {
        ensureTopicIsDeletedAndBlock(topicName);
        return;
      } catch (PubSubOpTimeoutException e) {
        attempts++;
        logger.warn(
            "Topic deletion for topic {} timed out!  Retry attempt {} / {}",
            topicName,
            attempts,
            MAX_TOPIC_DELETE_RETRIES);
        if (attempts == MAX_TOPIC_DELETE_RETRIES) {
          logger.error("Topic deletion for topic {} timed out! Giving up!!", topicName, e);
          throw e;
        }
      } catch (ExecutionException e) {
        attempts++;
        logger.warn(
            "Topic deletion for topic {} errored out!  Retry attempt {} / {}",
            topicName,
            attempts,
            MAX_TOPIC_DELETE_RETRIES);
        if (attempts == MAX_TOPIC_DELETE_RETRIES) {
          logger.error("Topic deletion for topic {} errored out! Giving up!!", topicName, e);
          throw e;
        }
      }
    }
  }

  public synchronized Set<PubSubTopic> listTopics() {
    return pubSubReadOnlyAdminAdapter.get().listAllTopics();
  }

  /**
   * A quick check to see whether the topic exists.
   */
  public boolean containsTopic(PubSubTopic topic) {
    return pubSubReadOnlyAdminAdapter.get().containsTopic(topic);
  }

  /**
   * See Java doc of {@link PubSubAdminAdapter#containsTopicWithExpectationAndRetry} which provides exactly the same
   * semantics.
   */
  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult) {
    return pubSubReadOnlyAdminAdapter.get().containsTopicWithExpectationAndRetry(topic, maxAttempts, expectedResult);
  }

  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration) {
    return pubSubReadOnlyAdminAdapter.get()
        .containsTopicWithExpectationAndRetry(
            topic,
            maxAttempts,
            expectedResult,
            initialBackoff,
            maxBackoff,
            maxDuration);
  }

  /**
   * @see {@link #containsTopicAndAllPartitionsAreOnline(PubSubTopic, Integer)}
   */
  public boolean containsTopicAndAllPartitionsAreOnline(PubSubTopic topic) {
    return containsTopicAndAllPartitionsAreOnline(topic, null);
  }

  /**
   * This is an extensive check to mitigate an edge-case where a topic is not yet created in all the brokers.
   *
   * @return true if the topic exists and all its partitions have at least one in-sync replica
   *         false if the topic does not exist at all or if it exists but isn't completely available
   */
  public synchronized boolean containsTopicAndAllPartitionsAreOnline(
      PubSubTopic topic,
      Integer expectedPartitionCount) {
    if (!containsTopic(topic)) {
      return false;
    }
    List<PubSubTopicPartitionInfo> partitionInfoList = partitionOffsetFetcher.partitionsFor(topic);
    if (partitionInfoList == null) {
      logger.warn("getConsumer().partitionsFor() returned null for topic: {}", topic);
      return false;
    }

    if (expectedPartitionCount != null && partitionInfoList.size() != expectedPartitionCount) {
      // Unexpected. Should perhaps even throw...
      logger.error(
          "getConsumer().partitionsFor() returned the wrong number of partitions for topic: {}, "
              + "expectedPartitionCount: {}, actual size: {}, partitionInfoList: {}",
          topic,
          expectedPartitionCount,
          partitionInfoList.size(),
          Arrays.toString(partitionInfoList.toArray()));
      return false;
    }

    boolean allPartitionsHaveAnInSyncReplica =
        partitionInfoList.stream().allMatch(PubSubTopicPartitionInfo::hasInSyncReplicas);
    if (allPartitionsHaveAnInSyncReplica) {
      logger.trace("The following topic has the at least one in-sync replica for each partition: {}", topic);
    } else {
      logger.info(
          "getConsumer().partitionsFor() returned some partitionInfo with no in-sync replica for topic: {}, partitionInfoList: {}",
          topic,
          Arrays.toString(partitionInfoList.toArray()));
    }
    return allPartitionsHaveAnInSyncReplica;
  }

  /**
   * Generate a map from partition number to the last offset available for that partition
   * @param topic
   * @return a Map of partition to the latest offset, or an empty map if there's any problem
   */
  public Int2LongMap getTopicLatestOffsets(PubSubTopic topic) {
    return partitionOffsetFetcher.getTopicLatestOffsets(topic);
  }

  public long getPartitionLatestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return partitionOffsetFetcher.getPartitionLatestOffsetAndRetry(pubSubTopicPartition, retries);
  }

  public long getProducerTimestampOfLastDataRecord(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return partitionOffsetFetcher.getProducerTimestampOfLastDataRecord(pubSubTopicPartition, retries);
  }

  public long getPartitionEarliestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return partitionOffsetFetcher.getPartitionEarliestOffsetAndRetry(pubSubTopicPartition, retries);
  }

  /**
   * Get offsets for only one partition with a specific timestamp.
   */
  public long getPartitionOffsetByTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return partitionOffsetFetcher.getPartitionOffsetByTime(pubSubTopicPartition, timestamp);
  }

  /**
   * Get a list of {@link PubSubTopicPartitionInfo} objects for the specified topic.
   * @param topic
   * @return
   */
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    return partitionOffsetFetcher.partitionsFor(topic);
  }

  public String getPubSubBootstrapServers() {
    return this.pubSubBootstrapServers;
  }

  @Override
  public synchronized void close() {
    Utils.closeQuietlyWithErrorLogged(partitionOffsetFetcher);
    pubSubReadOnlyAdminAdapter.ifPresent(Utils::closeQuietlyWithErrorLogged);
    pubSubWriteOnlyAdminAdapter.ifPresent(Utils::closeQuietlyWithErrorLogged);
  }

  // For testing only
  public void setTopicConfigCache(Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache) {
    this.topicConfigCache = topicConfigCache;
  }

  /**
   * The default retention time for the RT topic is defined in {@link TopicManager#DEFAULT_TOPIC_RETENTION_POLICY_MS},
   * but if the rewind time is larger than this, then the RT topic's retention time needs to be set even higher,
   * in order to guarantee that buffer replays do not lose data. In order to achieve this, the retention time is
   * set to the max of either:
   *
   * 1. {@link TopicManager#DEFAULT_TOPIC_RETENTION_POLICY_MS}; or
   * 2. {@link HybridStoreConfig#getRewindTimeInSeconds()} + {@link Store#getBootstrapToOnlineTimeoutInHours()} + {@value #BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN};
   *
   * This is a convenience function, and thus must be ignored by the JSON machinery.
   *
   * @return the retention time for the RT topic, in milliseconds.
   */
  public static long getExpectedRetentionTimeInMs(Store store, HybridStoreConfig hybridConfig) {
    long rewindTimeInMs = hybridConfig.getRewindTimeInSeconds() * Time.MS_PER_SECOND;
    long bootstrapToOnlineTimeInMs = (long) store.getBootstrapToOnlineTimeoutInHours() * Time.MS_PER_HOUR;
    long minimumRetentionInMs = rewindTimeInMs + bootstrapToOnlineTimeInMs + BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN;
    return Math.max(minimumRetentionInMs, TopicManager.DEFAULT_TOPIC_RETENTION_POLICY_MS);
  }
}
