package com.linkedin.venice.kafka;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcher;
import com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcherFactory;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubInstrumentedAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
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
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Topic Manager is shared by multiple cluster's controllers running in one physical Venice controller instance.
 *
 * This class contains per cluster and a default {@link PubSubConsumerAdapter}, which is not thread-safe, so when you add new functions,
 * which is using this global consumer, please add 'synchronized' keyword, otherwise this {@link TopicManager}
 * won't be thread-safe, and Kafka consumer will report the following error when multiple threads are trying to
 * use the same consumer: PubSubConsumerAdapter is not safe for multi-thread access.
 */
public class TopicManager implements Closeable {
  private static final int FAST_KAFKA_OPERATION_TIMEOUT_MS = Time.MS_PER_SECOND;
  protected static final long ETERNAL_TOPIC_RETENTION_POLICY_MS = Long.MAX_VALUE;

  public static final long DEFAULT_TOPIC_RETENTION_POLICY_MS = 5 * Time.MS_PER_DAY;
  public static final long BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN = 2 * Time.MS_PER_DAY;

  public static final int DEFAULT_KAFKA_OPERATION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  public static final int MAX_TOPIC_DELETE_RETRIES = 3;
  public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 3;

  /**
   * Default setting is that no log compaction should happen for hybrid store version topics
   * if the messages are produced within 24 hours; otherwise servers could encounter MISSING
   * data DIV errors for reprocessing jobs which could potentially generate lots of
   * duplicate keys.
   */
  public static final long DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS = 24 * Time.MS_PER_HOUR;

  private static final List<Class<? extends Throwable>> CREATE_TOPIC_RETRIABLE_EXCEPTIONS =
      Collections.unmodifiableList(Arrays.asList(PubSubOpTimeoutException.class, PubSubClientRetriableException.class));

  // Immutable state
  private final Logger logger;
  private final String pubSubBootstrapServers;
  private final long pubSubOperationTimeoutMs;
  private final long topicMinLogCompactionLagMs;
  private final Lazy<PubSubAdminAdapter> pubSubDefaultAdminAdapter;
  private final Lazy<PubSubConsumerAdapter> pubSubDefaultConsumerAdapter;
  private final Map<String, PubSubAdminAdapter> pubSubAdminAdapterMap = new VeniceConcurrentHashMap<>();
  private final Map<String, PubSubConsumerAdapter> pubSubConsumerAdapterMap = new VeniceConcurrentHashMap<>();
  private final PartitionOffsetFetcher partitionOffsetFetcher;
  private final Map<String, PubSubClientsFactory> pubSubClientsFactoryMap;
  private final PubSubTopicRepository pubSubTopicRepository;

  private final PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
      new KafkaValueSerializer(),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new));
  private final Optional<MetricsRepository> optionalMetricsRepository;
  private final TopicManagerRepository.ClusterNameSupplier clusterNameSupplier;
  private final TopicManagerRepository.SSLPropertiesSupplier pubSubPropertiesSupplier;
  // It's expensive to grab the topic config over and over again, and it changes infrequently. So we temporarily cache
  // queried configs.
  Cache<PubSubTopic, PubSubTopicConfiguration> topicConfigCache =
      Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();

  private PubSubConsumerAdapter getConsumerAdapter(PubSubTopic pubSubTopic) {
    Optional<String> clusterName = clusterNameSupplier.get(pubSubTopic);
    if (!clusterName.isPresent() || pubSubClientsFactoryMap == null) {
      return pubSubDefaultConsumerAdapter.get();
    }
    if (!pubSubClientsFactoryMap.containsKey(clusterName.get())) {
      throw new PubSubClientException("No PubSubClientsFactory found for cluster: " + clusterName.get());
    }
    return pubSubConsumerAdapterMap.computeIfAbsent(
        clusterName.get(),
        c -> pubSubClientsFactoryMap.get(c)
            .getConsumerAdapterFactory()
            .create(
                pubSubPropertiesSupplier.get(pubSubBootstrapServers),
                false,
                pubSubMessageDeserializer,
                pubSubBootstrapServers));
  }

  private PubSubAdminAdapter getAdminAdapter(PubSubTopic pubSubTopic) {
    Optional<String> clusterName = clusterNameSupplier.get(pubSubTopic);
    if (!clusterName.isPresent() || pubSubClientsFactoryMap == null) {
      return pubSubDefaultAdminAdapter.get();
    }
    if (!pubSubClientsFactoryMap.containsKey(clusterName.get())) {
      throw new PubSubClientException("No PubSubClientsFactory found for cluster: " + clusterName.get());
    }
    return pubSubAdminAdapterMap.computeIfAbsent(clusterName.get(), c -> {
      PubSubAdminAdapterFactory pubSubAdminAdapterFactory = pubSubClientsFactoryMap.get(c).getAdminAdapterFactory();
      VeniceProperties veniceProperties = pubSubPropertiesSupplier.get(pubSubBootstrapServers);
      PubSubAdminAdapter adminAdapter = pubSubAdminAdapterFactory.create(veniceProperties, pubSubTopicRepository);
      adminAdapter = createInstrumentedPubSubAdmin(
          optionalMetricsRepository,
          "PubSubAdminStats_" + adminAdapter.getClassName() + "_" + clusterName,
          adminAdapter,
          pubSubBootstrapServers);
      logger.info(
          "{} is creating pubsub admin client of class: {} for cluster: {}",
          this.getClass().getSimpleName(),
          adminAdapter.getClassName(),
          clusterName);
      return adminAdapter;
    });
  }

  public TopicManager(TopicManagerRepository.Builder builder, String pubSubBootstrapServers) {
    String pubSubServersForLogger = Utils.getSanitizedStringForLogger(pubSubBootstrapServers);
    this.logger = LogManager.getLogger(this.getClass().getSimpleName() + " [" + pubSubServersForLogger + "]");
    this.pubSubOperationTimeoutMs = builder.getKafkaOperationTimeoutMs();
    this.topicMinLogCompactionLagMs = builder.getTopicMinLogCompactionLagMs();

    this.pubSubBootstrapServers = pubSubBootstrapServers;
    this.pubSubClientsFactoryMap = builder.getPubSubClientsFactoryMap();

    this.pubSubPropertiesSupplier = builder.getPubSubProperties();
    this.clusterNameSupplier = builder.getClusterNameSupplier();
    this.pubSubTopicRepository = builder.getPubSubTopicRepository();

    this.optionalMetricsRepository = Optional.ofNullable(builder.getMetricsRepository());

    PubSubAdminAdapterFactory pubSubAdminAdapterFactory = builder.getPubSubClientsFactory().getAdminAdapterFactory();
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory =
        builder.getPubSubClientsFactory().getConsumerAdapterFactory();

    this.pubSubDefaultAdminAdapter = Lazy.of(() -> {
      PubSubAdminAdapter pubSubAdminAdapter =
          pubSubAdminAdapterFactory.create(pubSubPropertiesSupplier.get(pubSubBootstrapServers), pubSubTopicRepository);
      pubSubAdminAdapter = createInstrumentedPubSubAdmin(
          optionalMetricsRepository,
          "DefaultKafkaAdminStats",
          pubSubAdminAdapter,
          pubSubBootstrapServers);
      logger.info(
          "{} is using default pubsub admin client of class: {}",
          this.getClass().getSimpleName(),
          pubSubAdminAdapter.getClassName());
      return pubSubAdminAdapter;
    });

    this.pubSubDefaultConsumerAdapter = Lazy.of(
        () -> pubSubConsumerAdapterFactory.create(
            pubSubPropertiesSupplier.get(pubSubBootstrapServers),
            false,
            pubSubMessageDeserializer,
            pubSubBootstrapServers));

    this.partitionOffsetFetcher = PartitionOffsetFetcherFactory.createDefaultPartitionOffsetFetcher(
        this::getConsumerAdapter,
        this::getAdminAdapter,
        pubSubBootstrapServers,
        pubSubOperationTimeoutMs,
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
  public void createTopic(PubSubTopic pubSubTopic, int numPartitions, int replication, boolean eternal) {
    createTopic(pubSubTopic, numPartitions, replication, eternal, false, Optional.empty(), false);
  }

  public void createTopic(
      PubSubTopic pubSubTopic,
      int numPartitions,
      int replication,
      boolean eternal,
      boolean logCompaction,
      Optional<Integer> minIsr) {
    createTopic(pubSubTopic, numPartitions, replication, eternal, logCompaction, minIsr, true);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param pubSubTopic Name for the new topic
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
      PubSubTopic pubSubTopic,
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
        pubSubTopic,
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
   * @param pubSubTopic Name for the new topic
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
      PubSubTopic pubSubTopic,
      int numPartitions,
      int replication,
      long retentionTimeMs,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastKafkaOperationTimeout) {

    long startTime = System.currentTimeMillis();
    long deadlineMs =
        startTime + (useFastKafkaOperationTimeout ? FAST_KAFKA_OPERATION_TIMEOUT_MS : pubSubOperationTimeoutMs);
    PubSubTopicConfiguration pubSubTopicConfiguration =
        new PubSubTopicConfiguration(Optional.of(retentionTimeMs), logCompaction, minIsr, topicMinLogCompactionLagMs);
    logger.info(
        "Creating topic: {} partitions: {} replication: {}, configuration: {}",
        pubSubTopic,
        numPartitions,
        replication,
        pubSubTopicConfiguration);

    try {
      RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> getAdminAdapter(pubSubTopic)
              .createTopic(pubSubTopic, numPartitions, replication, pubSubTopicConfiguration),
          10,
          Duration.ofMillis(200),
          Duration.ofSeconds(1),
          Duration.ofMillis(useFastKafkaOperationTimeout ? FAST_KAFKA_OPERATION_TIMEOUT_MS : pubSubOperationTimeoutMs),
          CREATE_TOPIC_RETRIABLE_EXCEPTIONS);
    } catch (Exception e) {
      if (ExceptionUtils.recursiveClassEquals(e, PubSubTopicExistsException.class)) {
        logger.info("Topic: {} already exists, will update retention policy.", pubSubTopic);
        waitUntilTopicCreated(pubSubTopic, numPartitions, deadlineMs);
        updateTopicRetention(pubSubTopic, retentionTimeMs);
        logger.info("Updated retention policy to be {}ms for topic: {}", retentionTimeMs, pubSubTopic);
        return;
      } else {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + pubSubTopic + ". Topic still does not exist after "
                + (deadlineMs - startTime) + "ms.",
            e);
      }
    }
    waitUntilTopicCreated(pubSubTopic, numPartitions, deadlineMs);
    boolean eternal = retentionTimeMs == ETERNAL_TOPIC_RETENTION_POLICY_MS;
    logger.info("Successfully created {}topic: {}", eternal ? "eternal " : "", pubSubTopic);
  }

  protected void waitUntilTopicCreated(PubSubTopic pubSubTopic, int partitionCount, long deadlineMs) {
    long startTime = System.currentTimeMillis();
    while (!containsTopicAndAllPartitionsAreOnline(pubSubTopic, partitionCount)) {
      if (System.currentTimeMillis() > deadlineMs) {
        throw new PubSubOpTimeoutException(
            "Timeout while creating topic: " + pubSubTopic + ".  Topic still did not pass all the checks after "
                + (deadlineMs - startTime) + "ms.");
      }
      Utils.sleep(200);
    }
  }

  /**
   * Update retention for the given topic.
   * If the topic doesn't exist, this operation will throw {@link PubSubTopicDoesNotExistException}
   * @param pubSubTopic
   * @param retentionInMS
   * @return true if the retention time config of the input topic gets updated; return false if nothing gets updated
   */
  public boolean updateTopicRetention(PubSubTopic pubSubTopic, long retentionInMS)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(pubSubTopic);
    return updateTopicRetention(pubSubTopic, retentionInMS, pubSubTopicConfiguration);
  }

  /**
   * Update retention for the given topic given a {@link Properties}.
   * @param pubSubTopic
   * @param expectedRetentionInMs
   * @param pubSubTopicConfiguration
   * @return true if the retention time gets updated; false if no update is needed.
   */
  public boolean updateTopicRetention(
      PubSubTopic pubSubTopic,
      long expectedRetentionInMs,
      PubSubTopicConfiguration pubSubTopicConfiguration) throws PubSubTopicDoesNotExistException {
    Optional<Long> retentionTimeMs = pubSubTopicConfiguration.retentionInMs();
    if (!retentionTimeMs.isPresent() || expectedRetentionInMs != retentionTimeMs.get()) {
      pubSubTopicConfiguration.setRetentionInMs(Optional.of(expectedRetentionInMs));
      getAdminAdapter(pubSubTopic).setTopicConfig(pubSubTopic, pubSubTopicConfiguration);
      logger.info(
          "Updated topic: {} with retention.ms: {} in cluster [{}]",
          pubSubTopic,
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
      pubSubDefaultAdminAdapter.get().setTopicConfig(topic, pubSubTopicConfiguration);
      logger.info(
          "Kafka compaction policy for topic: {} has been updated from {} to {}, min compaction lag updated from {} to {}",
          topic,
          currentLogCompacted,
          expectedLogCompacted,
          currentMinLogCompactionLagMs,
          expectedMinLogCompactionLagMs);
    }
  }

  public boolean isTopicCompactionEnabled(PubSubTopic pubSubTopic) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(pubSubTopic);
    return topicProperties.isLogCompacted();
  }

  public long getTopicMinLogCompactionLagMs(PubSubTopic pubSubTopic) {
    PubSubTopicConfiguration topicProperties = getCachedTopicConfig(pubSubTopic);
    return topicProperties.minLogCompactionLagMs();
  }

  public boolean updateTopicMinInSyncReplica(PubSubTopic pubSubTopic, int minISR)
      throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(pubSubTopic);
    Optional<Integer> currentMinISR = pubSubTopicConfiguration.minInSyncReplicas();
    // config doesn't exist config is different
    if (!currentMinISR.isPresent() || !currentMinISR.get().equals(minISR)) {
      pubSubTopicConfiguration.setMinInSyncReplicas(Optional.of(minISR));
      pubSubDefaultAdminAdapter.get().setTopicConfig(pubSubTopic, pubSubTopicConfiguration);
      logger.info("Updated topic: {} with min.insync.replicas: {}", pubSubTopic, minISR);
      return true;
    }
    // min.insync.replicas has already been updated for this topic before
    return false;
  }

  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return pubSubDefaultAdminAdapter.get().getAllTopicRetentions();
  }

  /**
   * Return topic retention time in MS.
   */
  public long getTopicRetention(PubSubTopic pubSubTopic) throws PubSubTopicDoesNotExistException {
    PubSubTopicConfiguration pubSubTopicConfiguration = getTopicConfig(pubSubTopic);
    return getTopicRetention(pubSubTopicConfiguration);
  }

  public long getTopicRetention(PubSubTopicConfiguration pubSubTopicConfiguration) {
    if (pubSubTopicConfiguration.retentionInMs().isPresent()) {
      return pubSubTopicConfiguration.retentionInMs().get();
    }
    return PubSubConstants.UNKNOWN_TOPIC_RETENTION;
  }

  /**
   * Check whether topic is absent or truncated
   * @param pubSubTopic
   * @param truncatedTopicMaxRetentionMs
   * @return true if the topic does not exist or if it exists but its retention time is below truncated threshold
   *         false if the topic exists and its retention time is above truncated threshold
   */
  public boolean isTopicTruncated(PubSubTopic pubSubTopic, long truncatedTopicMaxRetentionMs) {
    try {
      return isRetentionBelowTruncatedThreshold(getTopicRetention(pubSubTopic), truncatedTopicMaxRetentionMs);
    } catch (PubSubTopicDoesNotExistException e) {
      return true;
    }
  }

  public boolean isRetentionBelowTruncatedThreshold(long retention, long truncatedTopicMaxRetentionMs) {
    return retention != PubSubConstants.UNKNOWN_TOPIC_RETENTION && retention <= truncatedTopicMaxRetentionMs;
  }

  /**
   * This operation is a little heavy, since it will pull the configs for all the topics.
   */
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic pubSubTopic) throws PubSubTopicDoesNotExistException {
    final PubSubTopicConfiguration pubSubTopicConfiguration = getAdminAdapter(pubSubTopic).getTopicConfig(pubSubTopic);
    topicConfigCache.put(pubSubTopic, pubSubTopicConfiguration);
    return pubSubTopicConfiguration;
  }

  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic pubSubTopic) {
    final PubSubTopicConfiguration pubSubTopicConfiguration =
        getAdminAdapter(pubSubTopic).getTopicConfigWithRetry(pubSubTopic);
    topicConfigCache.put(pubSubTopic, pubSubTopicConfiguration);
    return pubSubTopicConfiguration;
  }

  /**
   * Still heavy, but can be called repeatedly to amortize that cost.
   */
  public PubSubTopicConfiguration getCachedTopicConfig(PubSubTopic pubSubTopic) {
    // query the cache first, if it doesn't have it, query it from kafka and store it.
    PubSubTopicConfiguration pubSubTopicConfiguration = topicConfigCache.getIfPresent(pubSubTopic);
    if (pubSubTopicConfiguration == null) {
      pubSubTopicConfiguration = getTopicConfigWithRetry(pubSubTopic);
    }
    return pubSubTopicConfiguration;
  }

  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> pubSubTopics) {
    final Map<PubSubTopic, PubSubTopicConfiguration> topicConfigs =
        pubSubDefaultAdminAdapter.get().getSomeTopicConfigs(pubSubTopics);
    for (Map.Entry<PubSubTopic, PubSubTopicConfiguration> topicConfig: topicConfigs.entrySet()) {
      topicConfigCache.put(topicConfig.getKey(), topicConfig.getValue());
    }
    return topicConfigs;
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
      pubSubDefaultAdminAdapter.get().deleteTopic(pubSubTopic, Duration.ofMillis(pubSubOperationTimeoutMs));
      logger.info("Topic: {} has been deleted", pubSubTopic);
    } catch (PubSubOpTimeoutException e) {
      logger.warn("Failed to delete topic: {} after {} ms", pubSubTopic, pubSubOperationTimeoutMs);
    } catch (PubSubTopicDoesNotExistException e) {
      // No-op. Topic is deleted already, consider this as a successful deletion.
    } catch (PubSubClientRetriableException | PubSubClientException e) {
      logger.error("Failed to delete topic: {}", pubSubTopic, e);
      throw e;
    }

    // let's make sure the topic is deleted
    if (pubSubDefaultAdminAdapter.get().containsTopic(pubSubTopic)) {
      throw new PubSubTopicExistsException("Topic: " + pubSubTopic.getName() + " still exists after deletion");
    }
  }

  public void ensureTopicIsDeletedAndBlockWithRetry(PubSubTopic pubSubTopic) {
    int attempts = 0;
    while (attempts++ < MAX_TOPIC_DELETE_RETRIES) {
      try {
        logger.debug("Deleting topic: {} with retry attempt {} / {}", pubSubTopic, attempts, MAX_TOPIC_DELETE_RETRIES);
        ensureTopicIsDeletedAndBlock(pubSubTopic);
        return;
      } catch (PubSubClientRetriableException e) {
        String errorMessage = e instanceof PubSubOpTimeoutException ? "timed out" : "errored out";
        logger.warn(
            "Topic deletion for topic: {} {}! Retry attempt {} / {}",
            pubSubTopic,
            errorMessage,
            attempts,
            MAX_TOPIC_DELETE_RETRIES);
        if (attempts == MAX_TOPIC_DELETE_RETRIES) {
          logger.error("Topic deletion for topic {} {}! Giving up!!", pubSubTopic, errorMessage, e);
          throw e;
        }
      }
    }
  }

  public synchronized Set<PubSubTopic> listTopics() {
    return pubSubDefaultAdminAdapter.get().listAllTopics();
  }

  /**
   * A quick check to see whether the topic exists.
   */
  public boolean containsTopic(PubSubTopic topic) {
    return getAdminAdapter(topic).containsTopic(topic);
  }

  /**
   * See Java doc of {@link PubSubAdminAdapter#containsTopicWithExpectationAndRetry} which provides exactly the same
   * semantics.
   */
  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult) {
    return getAdminAdapter(topic).containsTopicWithExpectationAndRetry(topic, maxAttempts, expectedResult);
  }

  public boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration) {
    return getAdminAdapter(topic).containsTopicWithExpectationAndRetry(
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
    pubSubConsumerAdapterMap.forEach((clusterName, adapter) -> Utils.closeQuietlyWithErrorLogged(adapter));
    pubSubAdminAdapterMap.forEach((clusterName, adapter) -> Utils.closeQuietlyWithErrorLogged(adapter));
    pubSubDefaultAdminAdapter.ifPresent(Utils::closeQuietlyWithErrorLogged);
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
