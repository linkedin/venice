package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.CREATE_TOPIC_RETRIABLE_EXCEPTIONS;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_TOPIC_RETENTION_POLICY_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_FAST_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_DELETE_RETRY_TIMES;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION;
import static com.linkedin.venice.pubsub.PubSubConstants.TOPIC_METADATA_OP_RETRIABLE_EXCEPTIONS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CONTAINS_TOPIC_WITH_RETRY;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CREATE_TOPIC;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.DELETE_TOPIC;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_ALL_TOPIC_RETENTIONS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_SOME_TOPIC_CONFIGS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_TOPIC_CONFIG;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.GET_TOPIC_CONFIG_WITH_RETRY;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.LIST_ALL_TOPICS;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.SET_TOPIC_CONFIG;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Topic manager is responsible for creating, deleting, and updating topics. It also provides APIs to query topic metadata.
 *
 * It is essentially a wrapper over {@link PubSubAdminAdapter} and {@link PubSubConsumerAdapter} with additional
 * features such as caching, metrics, and retry.
 *
 * TODO: We still have retries in the {@link PubSubAdminAdapter}, we will eventually move them here.
 */
public class TopicManager implements Closeable {
  private final Logger logger;
  private final String pubSubClusterAddress;
  private final TopicManagerContext topicManagerContext;
  private final PubSubAdminAdapter pubSubAdminAdapter;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final TopicManagerStats stats;
  private final TopicMetadataFetcher topicMetadataFetcher;
  private AtomicBoolean isClosed = new AtomicBoolean(false);

  // TODO: Consider moving this cache to TopicMetadataFetcher
  // It's expensive to grab the topic config over and over again, and it changes infrequently.
  // So we temporarily cache queried configs.
  Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache =
      Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

  public TopicManager(String pubSubClusterAddress, TopicManagerContext context) {
    this.logger = LogManager.getLogger(
        TopicManager.class.getSimpleName() + " [" + Utils.getSanitizedStringForLogger(pubSubClusterAddress) + "]");
    this.pubSubClusterAddress = Objects.requireNonNull(pubSubClusterAddress, "pubSubClusterAddress cannot be null");
    this.topicManagerContext = context;
    this.stats = new TopicManagerStats(context.getMetricsRepository(), pubSubClusterAddress);
    this.pubSubTopicRepository = context.getPubSubTopicRepository();
    this.pubSubAdminAdapter = context.getPubSubAdminAdapterFactory()
        .create(context.getPubSubProperties(pubSubClusterAddress), pubSubTopicRepository);
    this.topicMetadataFetcher = new TopicMetadataFetcher(pubSubClusterAddress, context, stats, pubSubAdminAdapter);
    this.logger.info(
        "Created a topic manager for the pubsub cluster address: {} with context: {}",
        pubSubClusterAddress,
        context);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value PubSubConstants#PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE}, after which this function will throw a VeniceException.
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
   * {@value PubSubConstants#PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param eternal if true, the topic will have "infinite" (~250 mil years) retention
   *                if false, its retention will be set to {@link PubSubConstants#DEFAULT_TOPIC_RETENTION_POLICY_MS} by default
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, PubSub cluster defaults will be used
   * @param useFastPubSubOperationTimeout if false, normal PubSub operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      boolean eternal,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastPubSubOperationTimeout) {
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
        useFastPubSubOperationTimeout);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value PubSubConstants#PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param retentionTimeMs Retention time, in ms, for the topic
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, PubSub cluster defaults will be used
   * @param useFastPubSubOperationTimeout if false, normal PubSub operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      long retentionTimeMs,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastPubSubOperationTimeout) {
    long startTimeMs = System.currentTimeMillis();
    long deadlineMs = startTimeMs + (useFastPubSubOperationTimeout
        ? PUBSUB_FAST_OPERATION_TIMEOUT_MS
        : topicManagerContext.getPubSubOperationTimeoutMs());
    PubSubTopicConfiguration pubSubTopicConfiguration = new PubSubTopicConfiguration(
        Optional.of(retentionTimeMs),
        logCompaction,
        minIsr,
        topicManagerContext.getTopicMinLogCompactionLagMs(),
        Optional.empty());
    logger.info(
        "Creating topic: {} partitions: {} replication: {}, configuration: {}",
        topicName,
        numPartitions,
        replication,
        pubSubTopicConfiguration);

    try {
      RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> createTopic(topicName, numPartitions, replication, pubSubTopicConfiguration),
          10,
          Duration.ofMillis(200),
          Duration.ofSeconds(1),
          Duration.ofMillis(
              useFastPubSubOperationTimeout
                  ? PUBSUB_FAST_OPERATION_TIMEOUT_MS
                  : topicManagerContext.getPubSubOperationTimeoutMs()),
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
                + (deadlineMs - startTimeMs) + "ms.",
            e);
      }
    }
    waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
    boolean eternal = retentionTimeMs == PubSubConstants.ETERNAL_TOPIC_RETENTION_POLICY_MS;
    logger.info("Successfully created {}topic: {}", eternal ? "eternal " : "", topicName);
  }

  protected void waitUntilTopicCreated(PubSubTopic topicName, int partitionCount, long deadlineMs) {
    long startTimeMs = System.nanoTime();
    while (!containsTopicAndAllPartitionsAreOnline(topicName, partitionCount)) {
      if (System.currentTimeMillis() > deadlineMs) {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + topicName + ".  Topic still did not pass all the checks after "
                + (deadlineMs - startTimeMs) + "ms.");
      }
      Utils.sleep(200);
    }
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
      setTopicConfig(topicName, pubSubTopicConfiguration);
      logger.info(
          "Updated topic: {} with retention.ms: {} in cluster [{}]",
          topicName,
          expectedRetentionInMs,
          this.pubSubClusterAddress);
      return true;
    }
    // Retention time has already been updated for this topic before
    return false;
  }

  public boolean updateTopicRetentionWithRetries(PubSubTopic topicName, long expectedRetentionInMs) {
    PubSubTopicConfiguration topicConfiguration;
    try {
      topicConfiguration = getCachedTopicConfig(topicName).clone();
    } catch (Exception e) {
      logger.error("Failed to get topic config for topic: {}", topicName, e);
      throw new VeniceException(
          "Failed to update topic retention for topic: " + topicName + " with retention: " + expectedRetentionInMs
              + " in cluster: " + this.pubSubClusterAddress,
          e);
    }
    if (topicConfiguration.retentionInMs().isPresent()
        && topicConfiguration.retentionInMs().get() == expectedRetentionInMs) {
      // Retention time has already been updated for this topic before
      return false;
    }

    topicConfiguration.setRetentionInMs(Optional.of(expectedRetentionInMs));
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        () -> setTopicConfig(topicName, topicConfiguration),
        5,
        Duration.ofMillis(200),
        Duration.ofSeconds(1),
        Duration.ofMinutes(2),
        TOPIC_METADATA_OP_RETRIABLE_EXCEPTIONS);
    topicConfigCache.put(topicName, topicConfiguration);
    return true;
  }

  public void updateTopicCompactionPolicy(PubSubTopic topic, boolean expectedLogCompacted) {
    updateTopicCompactionPolicy(topic, expectedLogCompacted, -1, Optional.empty());
  }

  /**
   * Update topic compaction policy.
   * @param topic
   * @param expectedLogCompacted
   * @param minLogCompactionLagMs the overrode min log compaction lag. If this is specified and a valid number (> 0), it will
   *                              override the default config
   * @throws PubSubTopicDoesNotExistException, if the topic doesn't exist
   */
  public void updateTopicCompactionPolicy(
      PubSubTopic topic,
      boolean expectedLogCompacted,
      long minLogCompactionLagMs,
      Optional<Long> maxLogCompactionLagMs) throws PubSubTopicDoesNotExistException {
    long expectedMinLogCompactionLagMs = 0l;
    Optional<Long> expectedMaxLogCompactionLagMs = maxLogCompactionLagMs;

    if (expectedLogCompacted) {
      if (minLogCompactionLagMs > 0) {
        expectedMinLogCompactionLagMs = minLogCompactionLagMs;
      } else {
        expectedMinLogCompactionLagMs = topicManagerContext.getTopicMinLogCompactionLagMs();
      }
      expectedMaxLogCompactionLagMs = maxLogCompactionLagMs;

      if (expectedMaxLogCompactionLagMs.isPresent()
          && expectedMaxLogCompactionLagMs.get() < expectedMinLogCompactionLagMs) {
        throw new VeniceException(
            "'expectedMaxLogCompactionLagMs': " + expectedMaxLogCompactionLagMs.get()
                + " shouldn't be smaller than 'expectedMinLogCompactionLagMs': " + expectedMinLogCompactionLagMs
                + " when updating compaction policy for topic: " + topic);
      }
    }

    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topic);
    boolean currentLogCompacted = pubSubTopicConfiguration.isLogCompacted();
    long currentMinLogCompactionLagMs = pubSubTopicConfiguration.minLogCompactionLagMs();
    Optional<Long> currentMaxLogCompactionLagMs = pubSubTopicConfiguration.getMaxLogCompactionLagMs();
    if (expectedLogCompacted != currentLogCompacted
        || expectedLogCompacted && (expectedMinLogCompactionLagMs != currentMinLogCompactionLagMs
            || expectedMaxLogCompactionLagMs.equals(currentMaxLogCompactionLagMs))) {
      pubSubTopicConfiguration.setLogCompacted(expectedLogCompacted);
      pubSubTopicConfiguration.setMinLogCompactionLagMs(expectedMinLogCompactionLagMs);
      pubSubTopicConfiguration.setMaxLogCompactionLagMs(expectedMaxLogCompactionLagMs);
      setTopicConfig(topic, pubSubTopicConfiguration);
      logger.info(
          "Kafka compaction policy for topic: {} has been updated from {} to {}, min compaction lag updated from"
              + " {} to {}, max compaction lag updated from {} to {}",
          topic,
          currentLogCompacted,
          expectedLogCompacted,
          currentMinLogCompactionLagMs,
          expectedMinLogCompactionLagMs,
          currentMaxLogCompactionLagMs.isPresent() ? currentMaxLogCompactionLagMs.get() : " not set",
          expectedMaxLogCompactionLagMs.isPresent() ? expectedMaxLogCompactionLagMs.get() : " not set");
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

  public Optional<Long> getTopicMaxLogCompactionLagMs(PubSubTopic topicName) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(topicName);
    return topicProperties.getMaxLogCompactionLagMs();
  }

  public boolean updateTopicMinInSyncReplica(PubSubTopic topicName, int minISR)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    Optional<Integer> currentMinISR = pubSubTopicConfiguration.minInSyncReplicas();
    // config doesn't exist config is different
    if (!currentMinISR.isPresent() || !currentMinISR.get().equals(minISR)) {
      pubSubTopicConfiguration.setMinInSyncReplicas(Optional.of(minISR));
      setTopicConfig(topicName, pubSubTopicConfiguration);
      logger.info("Updated topic: {} with min.insync.replicas: {}", topicName, minISR);
      return true;
    }
    // min.insync.replicas has already been updated for this topic before
    return false;
  }

  /**
   * Get retention time for all topics in the pubsub cluster.
   * @return a map of topic name to retention time in MS.
   */
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    long startTime = System.nanoTime();
    try {
      Map<PubSubTopic, Long> topicRetentions = pubSubAdminAdapter.getAllTopicRetentions();
      stats.recordLatency(GET_ALL_TOPIC_RETENTIONS, startTime);
      return topicRetentions;
    } catch (Exception e) {
      logger.debug("Failed to get all topic retentions", e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  /**
   * Return topic retention time in MS.
   */
  public long getTopicRetention(PubSubTopic topicName) throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(topicName);
    return getTopicRetention(pubSubTopicConfiguration);
  }

  public static long getTopicRetention(PubSubTopicConfiguration pubSubTopicConfiguration) {
    if (pubSubTopicConfiguration.retentionInMs().isPresent()) {
      return pubSubTopicConfiguration.retentionInMs().get();
    }
    return PUBSUB_TOPIC_UNKNOWN_RETENTION;
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
    return retention != PUBSUB_TOPIC_UNKNOWN_RETENTION && retention <= truncatedTopicMaxRetentionMs;
  }

  private void createTopic(
      PubSubTopic pubSubTopic,
      int numPartitions,
      int replicationFactor,
      PubSubTopicConfiguration topicConfiguration) {
    try {
      long startTime = System.nanoTime();
      pubSubAdminAdapter.createTopic(pubSubTopic, numPartitions, replicationFactor, topicConfiguration);
      stats.recordLatency(CREATE_TOPIC, startTime);
    } catch (Exception e) {
      logger.debug("Failed to create topic: {}", pubSubTopic, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  private void setTopicConfig(PubSubTopic pubSubTopic, PubSubTopicConfiguration pubSubTopicConfiguration) {
    try {
      long startTime = System.nanoTime();
      pubSubAdminAdapter.setTopicConfig(pubSubTopic, pubSubTopicConfiguration);
      stats.recordLatency(SET_TOPIC_CONFIG, startTime);
    } catch (Exception e) {
      logger.info("Failed to set topic config for topic: {}", pubSubTopic, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  /**
   * This operation is a little heavy, since it will pull the configs for all the topics.
   */
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic pubSubTopic) {
    try {
      long startTime = System.nanoTime();
      PubSubTopicConfiguration pubSubTopicConfiguration = pubSubAdminAdapter.getTopicConfig(pubSubTopic);
      topicConfigCache.put(pubSubTopic, pubSubTopicConfiguration);
      stats.recordLatency(GET_TOPIC_CONFIG, startTime);
      return pubSubTopicConfiguration;
    } catch (Exception e) {
      logger.debug("Failed to get topic config for topic: {}", pubSubTopic, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topicName) {
    try {
      long startTime = System.nanoTime();
      PubSubTopicConfiguration pubSubTopicConfiguration = pubSubAdminAdapter.getTopicConfigWithRetry(topicName);
      topicConfigCache.put(topicName, pubSubTopicConfiguration);
      stats.recordLatency(GET_TOPIC_CONFIG_WITH_RETRY, startTime);
      return pubSubTopicConfiguration;
    } catch (Exception e) {
      logger.debug("Failed to get topic config for topic: {}", topicName, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  /**
   * Still heavy, but can be called repeatedly to amortize that cost.
   */
  public PubSubTopicConfiguration getCachedTopicConfig(PubSubTopic topicName) {
    // query the cache first, if it doesn't have it, query it from PubSub and store it.
    PubSubTopicConfiguration pubSubTopicConfiguration = topicConfigCache.getIfPresent(topicName);
    if (pubSubTopicConfiguration == null) {
      pubSubTopicConfiguration = getTopicConfigWithRetry(topicName);
    }
    return pubSubTopicConfiguration;
  }

  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    try {
      long startTime = System.nanoTime();
      Map<PubSubTopic, PubSubTopicConfiguration> topicConfigs = pubSubAdminAdapter.getSomeTopicConfigs(topicNames);
      for (Map.Entry<PubSubTopic, PubSubTopicConfiguration> topicConfig: topicConfigs.entrySet()) {
        topicConfigCache.put(topicConfig.getKey(), topicConfig.getValue());
      }
      stats.recordLatency(GET_SOME_TOPIC_CONFIGS, startTime);
      return topicConfigs;
    } catch (Exception e) {
      logger.debug("Failed to get topic configs for topics: {}", topicNames, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  /**
   * Delete a topic and block until it is deleted or operation times out.
   * @param pubSubTopic topic to delete
   */
  public void ensureTopicIsDeletedAndBlock(PubSubTopic pubSubTopic) {
    if (!containsTopicAndAllPartitionsAreOnline(pubSubTopic)) {
      // Topic doesn't exist
      return;
    }

    logger.info("Deleting topic: {}", pubSubTopic);
    try {
      long startTime = System.nanoTime();
      pubSubAdminAdapter.deleteTopic(pubSubTopic, Duration.ofMillis(topicManagerContext.getPubSubOperationTimeoutMs()));
      stats.recordLatency(DELETE_TOPIC, startTime);
      logger.info("Topic: {} has been deleted", pubSubTopic);
    } catch (PubSubOpTimeoutException e) {
      stats.recordPubSubAdminOpFailure();
      logger.warn(
          "Failed to delete topic: {} after {} ms",
          pubSubTopic,
          topicManagerContext.getPubSubOperationTimeoutMs());
    } catch (PubSubTopicDoesNotExistException e) {
      // No-op. Topic is deleted already, consider this as a successful deletion.
    } catch (PubSubClientRetriableException | PubSubClientException e) {
      stats.recordPubSubAdminOpFailure();
      logger.error("Failed to delete topic: {}", pubSubTopic, e);
      throw e;
    }

    // let's make sure the topic is deleted
    if (containsTopic(pubSubTopic)) {
      throw new PubSubTopicExistsException("Topic: " + pubSubTopic.getName() + " still exists after deletion");
    }
  }

  /**
   * Delete a topic with retry and block until it is deleted or operation times out.
   * @param pubSubTopic
   */
  public void ensureTopicIsDeletedAndBlockWithRetry(PubSubTopic pubSubTopic) {
    int attempts = 0;
    while (attempts++ < PUBSUB_TOPIC_DELETE_RETRY_TIMES) {
      try {
        logger.debug(
            "Deleting topic: {} with retry attempt {} / {}",
            pubSubTopic,
            attempts,
            PUBSUB_TOPIC_DELETE_RETRY_TIMES);
        ensureTopicIsDeletedAndBlock(pubSubTopic);
        return;
      } catch (PubSubTopicExistsException | PubSubClientRetriableException e) {
        if (attempts == PUBSUB_TOPIC_DELETE_RETRY_TIMES) {
          logger.error(
              "Topic deletion for topic {} {}! Giving up after {} retries",
              pubSubTopic,
              e instanceof PubSubOpTimeoutException ? "timed out" : "errored out",
              attempts,
              e);
          throw e;
        }
      }
    }
  }

  // TODO: Evaluate if we need synchronized here
  public synchronized Set<PubSubTopic> listTopics() {
    try {
      long startTime = System.nanoTime();
      Set<PubSubTopic> topics = pubSubAdminAdapter.listAllTopics();
      stats.recordLatency(LIST_ALL_TOPICS, startTime);
      return topics;
    } catch (Exception e) {
      logger.debug("Failed to list topics", e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  /**
   * See Java doc of {@link PubSubAdminAdapter#containsTopicWithExpectationAndRetry} which provides exactly the same
   * semantics.
   */
  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult) {
    try {
      long startTime = System.nanoTime();
      boolean containsTopic =
          pubSubAdminAdapter.containsTopicWithExpectationAndRetry(topic, maxAttempts, expectedResult);
      stats.recordLatency(CONTAINS_TOPIC_WITH_RETRY, startTime);
      return containsTopic;
    } catch (Exception e) {
      logger.debug("Failed to check if topic: {} exists", topic, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
  }

  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration) {
    try {
      long startTime = System.nanoTime();
      boolean containsTopic = pubSubAdminAdapter.containsTopicWithExpectationAndRetry(
          topic,
          maxAttempts,
          expectedResult,
          initialBackoff,
          maxBackoff,
          maxDuration);
      stats.recordLatency(CONTAINS_TOPIC_WITH_RETRY, startTime);
      return containsTopic;
    } catch (Exception e) {
      logger.debug("Failed to check if topic: {} exists", topic, e);
      stats.recordPubSubAdminOpFailure();
      throw e;
    }
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
  public boolean containsTopicAndAllPartitionsAreOnline(PubSubTopic topic, Integer expectedPartitionCount) {
    if (!containsTopic(topic)) {
      return false;
    }
    List<PubSubTopicPartitionInfo> partitionInfoList = topicMetadataFetcher.getTopicPartitionInfo(topic);
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

  // For testing only
  public void setTopicConfigCache(Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache) {
    this.topicConfigCache = topicConfigCache;
  }

  /**
   * Get information about all partitions for a given topic.
   * @param pubSubTopic the topic to get partition info for
   * @return a list of {@link PubSubTopicPartitionInfo} for the given topic
   */
  public List<PubSubTopicPartitionInfo> getTopicPartitionInfo(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.getTopicPartitionInfo(pubSubTopic);
  }

  /**
   * Get the latest offsets for all partitions of a given topic.
   * @param pubSubTopic the topic to get latest offsets for
   * @return a Map of partition to the latest offset, or an empty map if there's any problem getting the offsets
   */
  public Int2LongMap getTopicLatestOffsets(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.getTopicLatestOffsets(pubSubTopic);
  }

  /**
   * Get partition count for a given topic.
   * @param pubSubTopic  the topic to get partition count for
   * @return the number of partitions for the given topic
   * @throws PubSubTopicDoesNotExistException if the topic does not exist
   */
  public int getPartitionCount(PubSubTopic pubSubTopic) {
    List<PubSubTopicPartitionInfo> partitionInfoList = getTopicPartitionInfo(pubSubTopic);
    if (partitionInfoList == null) {
      throw new PubSubTopicDoesNotExistException("Topic: " + pubSubTopic + " does not exist");
    }
    return partitionInfoList.size();
  }

  public boolean containsTopic(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.containsTopic(pubSubTopic);
  }

  public boolean containsTopicCached(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.containsTopicCached(pubSubTopic);
  }

  public boolean containsTopicWithRetries(PubSubTopic pubSubTopic, int retries) {
    return topicMetadataFetcher.containsTopicWithRetries(pubSubTopic, retries);
  }

  public long getLatestOffsetWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return topicMetadataFetcher.getLatestOffsetWithRetries(pubSubTopicPartition, retries);
  }

  public long getLatestOffsetCached(PubSubTopic pubSubTopic, int partitionId) {
    return topicMetadataFetcher.getLatestOffsetCached(new PubSubTopicPartitionImpl(pubSubTopic, partitionId));
  }

  public long getLatestOffsetCachedNonBlocking(PubSubTopic pubSubTopic, int partitionId) {
    return topicMetadataFetcher
        .getLatestOffsetCachedNonBlocking(new PubSubTopicPartitionImpl(pubSubTopic, partitionId));
  }

  public long getProducerTimestampOfLastDataMessageWithRetries(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return topicMetadataFetcher.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, retries);
  }

  public long getProducerTimestampOfLastDataMessageCached(PubSubTopicPartition pubSubTopicPartition) {
    return topicMetadataFetcher.getProducerTimestampOfLastDataMessageCached(pubSubTopicPartition);
  }

  /**
   * Get offsets for only one partition with a specific timestamp.
   */
  public long getOffsetByTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return topicMetadataFetcher.getOffsetForTimeWithRetries(pubSubTopicPartition, timestamp, 25);
  }

  /**
   * Invalidate the cache for the given topic and all its partitions.
   * @param pubSubTopic the topic to invalidate
   */
  public CompletableFuture<Void> invalidateCache(PubSubTopic pubSubTopic) {
    return topicMetadataFetcher.invalidateKeyAsync(pubSubTopic);
  }

  /**
   * Prefetch and cache the latest offset for the given topic-partition.
   * @param pubSubTopicPartition the topic-partition to prefetch and cache the latest offset for
   */
  public void prefetchAndCacheLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    topicMetadataFetcher.populateCacheWithLatestOffset(pubSubTopicPartition);
  }

  public String getPubSubClusterAddress() {
    return this.pubSubClusterAddress;
  }

  /**
   * Resolve the position bytes to a PubSubPosition object.
   * @param topicPartition the topic partition to resolve the position for
   * @param positionBytes the position bytes to resolve
   * @return the resolved PubSubPosition object
   */
  public PubSubPosition resolvePosition(PubSubTopicPartition topicPartition, byte[] positionBytes) {
    // todo(sushantmane): This is a temporary solution. We should be using the PubSubPosition class
    // to resolve the position bytes.
    return null;
  }

  @Override
  public void close() {
    if (isClosed.get()) {
      logger.warn("{} is already closed", this);
      return;
    }
    CompletableFuture.runAsync(() -> {
      if (isClosed.compareAndSet(false, true)) {
        logger.info("Closing {}", this);
        Utils.closeQuietlyWithErrorLogged(topicMetadataFetcher);
        Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapter);
      }
    });
  }

  @Override
  public String toString() {
    return "TopicManager{pubSubClusterAddress=" + pubSubClusterAddress + "}";
  }
}
