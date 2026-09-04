package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_ENCRYPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.StuckConsumerRepairStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext.PubSubPropertiesSupplier;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link AggKafkaConsumerService} supports Kafka consumer pool for multiple Kafka clusters from different data centers;
 * for each Kafka bootstrap server URL and consumer decryption mode, {@link AggKafkaConsumerService} will create one
 * {@link KafkaConsumerService}.
 */
public class AggKafkaConsumerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(AggKafkaConsumerService.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final PubSubConsumerAdapterFactory consumerFactory;
  private final VeniceServerConfig serverConfig;
  private final long readCycleDelayMs;
  private final long sharedConsumerNonExistingTopicCleanupDelayMS;
  private final IngestionThrottler ingestionThrottler;
  private final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private final MetricsRepository metricsRepository;
  private final StaleTopicChecker staleTopicChecker;
  private final boolean liveConfigBasedKafkaThrottlingEnabled;
  private final boolean isKafkaConsumerOffsetCollectionEnabled;
  private final KafkaConsumerService.ConsumerAssignmentStrategy sharedConsumerAssignmentStrategy;
  private final Map<ConsumerServiceKey, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap =
      new VeniceConcurrentHashMap<>();
  private final Map<String, String> kafkaClusterUrlToAliasMap;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  private final Function<String, String> kafkaClusterUrlResolver;
  private final PubSubPropertiesSupplier pubSubPropertiesSupplier;
  private final PubSubContext pubSubContext;

  private final Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new VeniceConcurrentHashMap<>();
  private ScheduledExecutorService stuckConsumerRepairExecutorService;
  private final ReadOnlyStoreRepository metadataRepository;

  private final StuckConsumerRepairStats stuckConsumerStats;
  private final ThreadPoolExecutor crossTpProcessingPool;
  private final ThreadPoolStats crossTpProcessingStats;

  private final static String STUCK_CONSUMER_MSG =
      "Didn't find any suspicious ingestion task, and please contact developers to investigate it further";
  private static final String CONSUMER_POLL_WARNING_MESSAGE_PREFIX =
      "Consumer poll tracker found stale topic partitions for consumer service: ";

  private final VeniceJsonSerializer<Map<String, Map<String, TopicPartitionIngestionInfo>>> topicPartitionIngestionContextJsonSerializer =
      new VeniceJsonSerializer<>(new TypeReference<Map<String, Map<String, TopicPartitionIngestionInfo>>>() {
      });

  static final class ConsumerServiceKey {
    private final String resolvedKafkaUrl;
    private final boolean decryptionEnabled;

    ConsumerServiceKey(String resolvedKafkaUrl, boolean decryptionEnabled) {
      this.resolvedKafkaUrl = Objects.requireNonNull(resolvedKafkaUrl);
      this.decryptionEnabled = decryptionEnabled;
    }

    String getResolvedKafkaUrl() {
      return resolvedKafkaUrl;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ConsumerServiceKey)) {
        return false;
      }
      ConsumerServiceKey that = (ConsumerServiceKey) o;
      return decryptionEnabled == that.decryptionEnabled && resolvedKafkaUrl.equals(that.resolvedKafkaUrl);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resolvedKafkaUrl, decryptionEnabled);
    }

    @Override
    public String toString() {
      return resolvedKafkaUrl + " (decryptionEnabled=" + decryptionEnabled + ")";
    }
  }

  public AggKafkaConsumerService(
      final PubSubPropertiesSupplier pubSubPropertiesSupplier,
      final VeniceServerConfig serverConfig,
      final IngestionThrottler ingestionThrottler,
      KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      StaleTopicChecker staleTopicChecker,
      Consumer<String> killIngestionTaskRunnable,
      ReadOnlyStoreRepository metadataRepository,
      PubSubContext pubSubContext) {
    this.serverConfig = serverConfig;
    this.consumerFactory = pubSubContext.getPubSubClientsFactory().getConsumerAdapterFactory();
    this.readCycleDelayMs = serverConfig.getKafkaReadCycleDelayMs();
    this.sharedConsumerNonExistingTopicCleanupDelayMS = serverConfig.getSharedConsumerNonExistingTopicCleanupDelayMS();
    this.ingestionThrottler = ingestionThrottler;
    this.kafkaClusterBasedRecordThrottler = kafkaClusterBasedRecordThrottler;
    this.metricsRepository = metricsRepository;
    this.staleTopicChecker = staleTopicChecker;
    this.liveConfigBasedKafkaThrottlingEnabled = serverConfig.isLiveConfigBasedKafkaThrottlingEnabled();
    this.sharedConsumerAssignmentStrategy = serverConfig.getSharedConsumerAssignmentStrategy();
    this.kafkaClusterUrlToAliasMap = serverConfig.getKafkaClusterUrlToAliasMap();
    this.kafkaClusterUrlToIdMap = serverConfig.getKafkaClusterUrlToIdMap();
    this.isKafkaConsumerOffsetCollectionEnabled = serverConfig.isKafkaConsumerOffsetCollectionEnabled();
    this.kafkaClusterUrlResolver = serverConfig.getKafkaClusterUrlResolver();
    this.metadataRepository = metadataRepository;
    if (serverConfig.isStuckConsumerRepairEnabled()) {
      this.stuckConsumerStats = new StuckConsumerRepairStats(metricsRepository, serverConfig.getClusterName());
      this.stuckConsumerRepairExecutorService = Executors.newSingleThreadScheduledExecutor(
          new DaemonThreadFactory(this.getClass().getName() + "-StuckConsumerRepair", serverConfig.getLogContext()));
      int intervalInSeconds = serverConfig.getStuckConsumerRepairIntervalSecond();
      this.stuckConsumerRepairExecutorService.scheduleAtFixedRate(
          getStuckConsumerDetectionAndRepairRunnable(
              LOGGER,
              SystemTime.INSTANCE,
              kafkaServerToConsumerServiceMap,
              versionTopicStoreIngestionTaskMapping,
              TimeUnit.SECONDS.toMillis(serverConfig.getStuckConsumerDetectionRepairThresholdSecond()),
              TimeUnit.SECONDS.toMillis(serverConfig.getNonExistingTopicIngestionTaskKillThresholdSecond()),
              TimeUnit.SECONDS.toMillis(serverConfig.getNonExistingTopicCheckRetryIntervalSecond()),
              TimeUnit.SECONDS.toMillis(serverConfig.getConsumerPollTrackerStaleThresholdSeconds()),
              stuckConsumerStats,
              killIngestionTaskRunnable),
          intervalInSeconds,
          intervalInSeconds,
          TimeUnit.SECONDS);
      LOGGER.info("Started stuck consumer repair service with checking interval: {} seconds", intervalInSeconds);
    } else {
      this.stuckConsumerStats = null;
      LOGGER.info("Stuck consumer repair service is disabled");
    }
    this.pubSubPropertiesSupplier = pubSubPropertiesSupplier;
    this.pubSubContext = pubSubContext;

    // Create single shared thread pool for cross-TP parallel processing if enabled
    if (serverConfig.isCrossTpParallelProcessingEnabled()) {
      int poolSize = serverConfig.getCrossTpParallelProcessingThreadPoolSize();
      this.crossTpProcessingPool = new ThreadPoolExecutor(
          poolSize,
          poolSize,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(),
          new DaemonThreadFactory("cross-tp-parallel-processing", serverConfig.getLogContext()));
      this.crossTpProcessingStats = new ThreadPoolStats(metricsRepository, crossTpProcessingPool, "CrossTpProcessing");
      LOGGER.info("Cross-TP parallel processing enabled with shared thread pool size: {}", poolSize);
    } else {
      this.crossTpProcessingPool = null;
      this.crossTpProcessingStats = null;
    }
    LOGGER.info("Successfully initialized AggKafkaConsumerService");
  }

  /**
   * IMPORTANT: All newly created KafkaConsumerService are already started in {@link #createKafkaConsumerService(Properties)},
   * if this is no longer the case in future, make sure to update the startInner logic here.
   */
  @Override
  public boolean startInner() {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.stop();
    }
    if (this.stuckConsumerRepairExecutorService != null) {
      this.stuckConsumerRepairExecutorService.shutdownNow();
    }
    // Shutdown cross-TP processing pool if it was created
    if (crossTpProcessingPool != null) {
      crossTpProcessingPool.shutdown();
      try {
        if (!crossTpProcessingPool.awaitTermination(1, TimeUnit.SECONDS)) {
          crossTpProcessingPool.shutdownNow();
        }
        LOGGER.info("crossTpProcessingPool shutdown completed.");
      } catch (InterruptedException e) {
        crossTpProcessingPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  protected static Runnable getStuckConsumerDetectionAndRepairRunnable(
      Logger logger,
      Time time,
      Map<?, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap,
      Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping,
      long stuckConsumerRepairThresholdMs,
      long nonExistingTopicIngestionTaskKillThresholdMs,
      long nonExistingTopicRetryIntervalMs,
      long consumerPollTrackerStaleThresholdMs,
      StuckConsumerRepairStats stuckConsumerRepairStats,
      Consumer<String> killIngestionTaskRunnable) {
    return () -> {
      /**
       * The following logic can be further optimized in the following way:
       * 1. If the max delay of previous run is much smaller than the threshold.
       * 2. In the next run, the max possible delay will be schedule interval + previous max delay ms, and if it is
       * still below the threshold, this function can return directly.
       * We are not adopting such optimization right now because:
       * 1. Extra state to maintain.
       * 2. The schedule interval is supposed to be high.
       * 3. The check is cheap when there is no stuck consumer.
       */
      boolean scanStoreIngestionTaskToFixStuckConsumer = false;
      for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
        long maxDelayMs = consumerService.getMaxElapsedTimeMSSinceLastPollInConsumerPool();
        if (maxDelayMs >= stuckConsumerRepairThresholdMs) {
          scanStoreIngestionTaskToFixStuckConsumer = true;
          logger.warn("Found some consumer has stuck for {} ms, will start the repairing procedure", maxDelayMs);
          break;
        }
      }
      if (scanStoreIngestionTaskToFixStuckConsumer) {
        stuckConsumerRepairStats.recordStuckConsumerFound();

        /**
         * Collect a list of SITs, whose version topic doesn't exist by checking {@link StoreIngestionTask#isProducingVersionTopicHealthy()},
         * and this function will continue to check the version topic healthiness for a period of {@link nonExistingTopicIngestionTaskKillThresholdMs}
         * to tolerate transient topic discovery issue.
         */
        Map<String, StoreIngestionTask> versionTopicIngestionTaskMappingForNonExistingTopic = new HashMap<>();
        versionTopicStoreIngestionTaskMapping.forEach((vt, sit) -> {
          try {
            if (!sit.isProducingVersionTopicHealthy()) {
              versionTopicIngestionTaskMappingForNonExistingTopic.put(vt, sit);
              logger.warn("The producing version topic:{} is not healthy", vt);
            }
          } catch (Exception e) {
            logger.error("Got exception while checking topic existence for version topic: {}", vt, e);
          }
        });
        int maxAttempts =
            (int) Math.ceil((double) nonExistingTopicIngestionTaskKillThresholdMs / nonExistingTopicRetryIntervalMs);
        for (int cnt = 0; cnt < maxAttempts; ++cnt) {
          Iterator<Map.Entry<String, StoreIngestionTask>> iterator =
              versionTopicIngestionTaskMappingForNonExistingTopic.entrySet().iterator();
          while (iterator.hasNext()) {
            Map.Entry<String, StoreIngestionTask> entry = iterator.next();
            String versionTopic = entry.getKey();
            StoreIngestionTask sit = entry.getValue();
            try {
              if (sit.isProducingVersionTopicHealthy()) {
                /**
                 * If the version topic becomes available after retries, remove it from the tracking map.
                 */
                iterator.remove();
                logger.info("The producing version topic:{} becomes healthy", versionTopic);
              }
            } catch (Exception e) {
              logger.error("Got exception while checking topic existence for version topic: {}", versionTopic, e);
            } finally {
              Utils.sleep(nonExistingTopicRetryIntervalMs);
            }
          }
        }

        AtomicBoolean hasRepairedSomeIngestionTask = new AtomicBoolean(false);
        versionTopicIngestionTaskMappingForNonExistingTopic.forEach((vt, sit) -> {
          try {
            logger.warn(
                "The ingestion topics (version topic) are not healthy for "
                    + "store version: {}, will kill the ingestion task to try to unblock shared consumer",
                vt);
            /**
             * The following function call will interrupt all the stuck {@link org.apache.kafka.clients.producer.KafkaProducer#send} call
             * to non-existing topics.
             */
            sit.closeVeniceWriters(false);
            killIngestionTaskRunnable.accept(vt);
            hasRepairedSomeIngestionTask.set(true);
            stuckConsumerRepairStats.recordIngestionTaskRepair();
          } catch (Exception e) {
            logger.error("Hit exception while killing the stuck ingestion task for store version: {}", vt, e);
          }
        });
        if (!hasRepairedSomeIngestionTask.get()) {
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(STUCK_CONSUMER_MSG)) {
            logger.warn(STUCK_CONSUMER_MSG);
          }
          stuckConsumerRepairStats.recordRepairFailure();
        }
      }

      reportStaleTopicPartitions(logger, time, kafkaServerToConsumerServiceMap, consumerPollTrackerStaleThresholdMs);
    };
  }

  private static void reportStaleTopicPartitions(
      Logger logger,
      Time time,
      Map<?, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap,
      long consumerPollTrackerStaleThresholdMs) {
    StringBuilder stringBuilder = new StringBuilder();
    long now = time.getMilliseconds();
    // Detect and log any subscribed topic partitions that are not polling records
    for (Map.Entry<?, AbstractKafkaConsumerService> consumerService: kafkaServerToConsumerServiceMap.entrySet()) {
      Map<PubSubTopicPartition, Long> staleTopicPartitions =
          consumerService.getValue().getStaleTopicPartitions(now - consumerPollTrackerStaleThresholdMs);
      if (!staleTopicPartitions.isEmpty()) {
        stringBuilder.append(CONSUMER_POLL_WARNING_MESSAGE_PREFIX);
        stringBuilder.append(consumerService.getKey());
        for (Map.Entry<PubSubTopicPartition, Long> staleTopicPartition: staleTopicPartitions.entrySet()) {
          PubSubTopicPartition topicPartition = staleTopicPartition.getKey();
          // Get consumer name for this topic partition
          SharedKafkaConsumer consumer = consumerService.getValue()
              .getConsumerAssignedToVersionTopicPartition(topicPartition.getPubSubTopic(), topicPartition);
          String consumerName = "unknown";
          if (consumer != null) {
            Map<PubSubTopicPartition, TopicPartitionIngestionInfo> ingestionInfoMap =
                consumerService.getValue().getIngestionInfoFor(topicPartition.getPubSubTopic(), topicPartition, true);
            if (!ingestionInfoMap.isEmpty()) {
              TopicPartitionIngestionInfo info = ingestionInfoMap.get(topicPartition);
              if (info != null) {
                consumerName = info.getConsumerIdStr();
              }
            }
          }
          stringBuilder.append(", replica: ");
          stringBuilder
              .append(Utils.getReplicaId(topicPartition.getPubSubTopic(), topicPartition.getPartitionNumber()));
          stringBuilder.append(" consumer: ");
          stringBuilder.append(consumerName);
          stringBuilder.append(" stale for: ");
          stringBuilder.append(now - staleTopicPartition.getValue());
          stringBuilder.append("ms");
        }
        logger.warn(stringBuilder.toString());
        // clear the StringBuilder to be reused for next reporting cycle
        stringBuilder.setLength(0);
      }
    }
  }

  /**
   * @return the {@link KafkaConsumerService} for a specific Kafka bootstrap url,
   *         or null if there isn't any.
   */
  AbstractKafkaConsumerService getKafkaConsumerService(final String kafkaURL) {
    AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL, false);
    if (consumerService == null) {
      consumerService = getKafkaConsumerService(kafkaURL, true);
    }
    return consumerService;
  }

  AbstractKafkaConsumerService getKafkaConsumerService(final String kafkaURL, boolean decryptionEnabled) {
    String resolvedKafkaUrl = kafkaClusterUrlResolver == null ? kafkaURL : kafkaClusterUrlResolver.apply(kafkaURL);
    return kafkaServerToConsumerServiceMap.get(new ConsumerServiceKey(resolvedKafkaUrl, decryptionEnabled));
  }

  /**
   * Create a new {@link KafkaConsumerService} given consumerProperties which must contain a value for "bootstrap.servers".
   * If a {@link KafkaConsumerService} for the given "bootstrap.servers" (Kafka URL) has already been created, this method
   * returns the created {@link KafkaConsumerService}.
   *
   * @param consumerProperties consumer properties that are used to create {@link KafkaConsumerService}
   */
  public synchronized AbstractKafkaConsumerService createKafkaConsumerService(final Properties consumerProperties) {
    String kafkaUrl = consumerProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    Properties properties = pubSubPropertiesSupplier.get(kafkaUrl).toProperties();
    consumerProperties.putAll(properties);
    if (kafkaUrl == null || kafkaUrl.isEmpty()) {
      throw new IllegalArgumentException("Kafka URL must be set in the consumer properties config. Got: " + kafkaUrl);
    }
    String resolvedKafkaUrl = kafkaClusterUrlResolver == null ? kafkaUrl : kafkaClusterUrlResolver.apply(kafkaUrl);
    boolean decryptionEnabled = new VeniceProperties(consumerProperties).getBoolean(CLUSTER_ENCRYPTION_ENABLED, false);
    ConsumerServiceKey consumerServiceKey = new ConsumerServiceKey(resolvedKafkaUrl, decryptionEnabled);
    final AbstractKafkaConsumerService alreadyCreatedConsumerService =
        kafkaServerToConsumerServiceMap.get(consumerServiceKey);
    if (alreadyCreatedConsumerService != null) {
      LOGGER.info("KafkaConsumerService has already been created for {}", consumerServiceKey);
      return alreadyCreatedConsumerService;
    }

    // getOrDefault returns the stored value (even if null) when the key IS present.
    // kafkaClusterUrlToAliasMap can have null values when a kafka cluster config entry
    // has a URL but no "name" field. Fall back to the resolved URL in that case.
    String rawAlias = kafkaClusterUrlToAliasMap.getOrDefault(resolvedKafkaUrl, resolvedKafkaUrl);
    String regionAlias = (rawAlias == null || rawAlias.isEmpty()) ? resolvedKafkaUrl : rawAlias;
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (poolSize, poolType) -> sharedConsumerAssignmentStrategy.constructor.construct(
            poolType,
            consumerProperties,
            readCycleDelayMs,
            poolSize,
            ingestionThrottler,
            kafkaClusterBasedRecordThrottler,
            metricsRepository,
            regionAlias,
            sharedConsumerNonExistingTopicCleanupDelayMS,
            staleTopicChecker,
            liveConfigBasedKafkaThrottlingEnabled,
            SystemTime.INSTANCE,
            null,
            isKafkaConsumerOffsetCollectionEnabled,
            metadataRepository,
            serverConfig.isUnregisterMetricForDeletedStoreEnabled(),
            serverConfig,
            pubSubContext,
            getCrossTpProcessingPoolForPoolType(poolType));

    AbstractKafkaConsumerService consumerService =
        kafkaServerToConsumerServiceMap.computeIfAbsent(consumerServiceKey, key -> {
          if (serverConfig
              .getConsumerPoolStrategyType() == KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION) {
            return new KafkaConsumerServiceDelegator(serverConfig, consumerServiceBuilder);
          } else {
            return consumerServiceBuilder
                .apply(serverConfig.getConsumerPoolSizePerKafkaCluster(), ConsumerPoolType.REGULAR_POOL);
          }
        });

    if (!consumerService.isRunning()) {
      consumerService.start();
    }
    return consumerService;
  }

  /**
   * Returns the cross-TP processing pool for the given pool type based on configuration.
   * If {@code crossTpParallelProcessingCurrentVersionAAWCLeaderOnly} is true, only
   * {@code ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL} will receive the pool.
   * Otherwise, all pool types receive the pool when cross-TP parallel processing is enabled.
   *
   * @param poolType the consumer pool type
   * @return the cross-TP processing pool if applicable, null otherwise
   */
  private ThreadPoolExecutor getCrossTpProcessingPoolForPoolType(ConsumerPoolType poolType) {
    if (crossTpProcessingPool == null) {
      return null;
    }
    if (serverConfig.isCrossTpParallelProcessingCurrentVersionAAWCLeaderOnly()) {
      // Only enable for CURRENT_VERSION_AA_WC_LEADER_POOL
      return poolType == ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL ? crossTpProcessingPool : null;
    }
    // Enable for all pool types
    return crossTpProcessingPool;
  }

  public boolean hasConsumerAssignedFor(
      final String kafkaURL,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    for (boolean decryptionEnabled: new boolean[] { false, true }) {
      AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL, decryptionEnabled);
      if (consumerService != null) {
        SharedKafkaConsumer consumer =
            consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
        if (consumer != null && consumer.hasSubscription(pubSubTopicPartition)) {
          return true;
        }
      }
    }
    return false;
  }

  boolean hasConsumerAssignedFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      SharedKafkaConsumer consumer =
          consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
      if (consumer != null && consumer.hasSubscription(pubSubTopicPartition)) {
        return true;
      }
    }
    return false;
  }

  boolean hasAnyConsumerAssignedForVersionTopic(PubSubTopic versionTopic) {
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      if (consumerService.hasAnySubscriptionFor(versionTopic)) {
        return true;
      }
    }
    return false;
  }

  void resetOffsetFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    PubSubConsumerAdapter consumer;
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
      if (consumer != null) {
        consumer.resetOffset(pubSubTopicPartition);
      }
    }
  }

  public void unsubscribeConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    unsubscribeConsumerFor(versionTopic, pubSubTopicPartition, SharedKafkaConsumer.DEFAULT_MAX_WAIT_MS);
  }

  public void unsubscribeConsumerFor(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      long timeoutMs) {
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.unSubscribe(versionTopic, pubSubTopicPartition, timeoutMs);
    }
  }

  void batchUnsubscribeConsumerFor(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionSet) {
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.batchUnsubscribe(versionTopic, topicPartitionSet);
    }
  }

  public ConsumedDataReceiver<List<DefaultPubSubMessage>> subscribeConsumerFor(
      final String kafkaURL,
      StoreIngestionTask storeIngestionTask,
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      PubSubPosition lastOffset,
      boolean inclusive) {
    PubSubTopic versionTopic = storeIngestionTask.getVersionTopic();
    boolean decryptionEnabled = StoreIngestionTask.resolveConsumerEncryptionEnabled(
        serverConfig.getClusterProperties(),
        metadataRepository.getStore(versionTopic.getStoreName()));
    AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL, decryptionEnabled);
    return subscribeConsumerFor(
        consumerService,
        kafkaURL,
        storeIngestionTask,
        partitionReplicaIngestionContext,
        lastOffset,
        inclusive);
  }

  ConsumedDataReceiver<List<DefaultPubSubMessage>> subscribeConsumerFor(
      AbstractKafkaConsumerService consumerService,
      final String kafkaURL,
      StoreIngestionTask storeIngestionTask,
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      PubSubPosition lastOffset,
      boolean inclusive) {
    PubSubTopic versionTopic = storeIngestionTask.getVersionTopic();
    PubSubTopicPartition pubSubTopicPartition = partitionReplicaIngestionContext.getPubSubTopicPartition();
    if (consumerService == null) {
      throw new VeniceException(
          "Kafka consumer service must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaURL);
    }

    ConsumedDataReceiver<List<DefaultPubSubMessage>> dataReceiver = new StorePartitionDataReceiver(
        storeIngestionTask,
        pubSubTopicPartition,
        kafkaURL, // do not resolve and let it pass downstream for offset tracking purpose
        kafkaClusterUrlToIdMap.getOrDefault(kafkaURL, -1)); // same pubsub url but different id for sep topic

    versionTopicStoreIngestionTaskMapping.put(storeIngestionTask.getVersionTopic().getName(), storeIngestionTask);
    consumerService
        .startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, lastOffset, dataReceiver, inclusive);
    TopicManager topicManager = storeIngestionTask.getTopicManager(kafkaURL);

    /*
     * Prefetches and caches the latest offset for the specified partition. This optimization aims to prevent
     * the consumption/metric thread from blocking on the first cache miss while waiting for the latest offset
     * to be fetched from PubSub.
     */
    if (topicManager != null) {
      topicManager.prefetchAndCacheLatestOffset(pubSubTopicPartition);
    }
    return dataReceiver;
  }

  public long getLatestOffsetBasedOnMetrics(
      final String kafkaURL,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    for (boolean decryptionEnabled: new boolean[] { false, true }) {
      AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL, decryptionEnabled);
      if (consumerService != null
          && consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition) != null) {
        return consumerService.getLatestOffsetBasedOnMetrics(versionTopic, pubSubTopicPartition);
      }
    }
    return -1;
  }

  /**
   * This will be called when an ingestion task is killed. {@link AggKafkaConsumerService}
   * will try to stop all subscription associated with the given version topic.
   */
  void unsubscribeAll(PubSubTopic versionTopic) {
    kafkaServerToConsumerServiceMap.values().forEach(consumerService -> consumerService.unsubscribeAll(versionTopic));
    versionTopicStoreIngestionTaskMapping.remove(versionTopic.getName());
  }

  /**
   * @return {@code true} if a consumer was assigned for the topic-partition and physically paused;
   *         {@code false} if no consumer is currently assigned (nothing to pause).
   */
  boolean pauseConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    boolean applied = false;
    PubSubConsumerAdapter consumer;
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
      if (consumer != null) {
        consumer.pause(pubSubTopicPartition);
        applied = true;
      }
    }
    return applied;
  }

  void resumeConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    PubSubConsumerAdapter consumer;
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
      if (consumer != null) {
        consumer.resume(pubSubTopicPartition);
      }
    }
  }

  /**
   * It returns all Kafka URLs of the consumers assigned for {@param versionTopic}.
   */
  Set<String> getKafkaUrlsFor(PubSubTopic versionTopic) {
    Set<String> kafkaUrls = new HashSet<>(kafkaServerToConsumerServiceMap.size());
    for (Map.Entry<ConsumerServiceKey, AbstractKafkaConsumerService> entry: kafkaServerToConsumerServiceMap
        .entrySet()) {
      if (entry.getValue().hasAnySubscriptionFor(versionTopic)) {
        kafkaUrls.add(entry.getKey().getResolvedKafkaUrl());
      }
    }
    return kafkaUrls;
  }

  byte[] getIngestionInfoFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) throws IOException {
    Map<String, Map<String, TopicPartitionIngestionInfo>> topicPartitionIngestionContext = new HashMap<>();
    for (Map.Entry<ConsumerServiceKey, AbstractKafkaConsumerService> consumerServiceEntry: kafkaServerToConsumerServiceMap
        .entrySet()) {
      String kafkaUrl = consumerServiceEntry.getKey().getResolvedKafkaUrl();
      AbstractKafkaConsumerService consumerService = consumerServiceEntry.getValue();
      Map<PubSubTopicPartition, TopicPartitionIngestionInfo> topicPartitionIngestionInfoMap =
          consumerService.getIngestionInfoFor(versionTopic, pubSubTopicPartition, false);
      for (Map.Entry<PubSubTopicPartition, TopicPartitionIngestionInfo> entry: topicPartitionIngestionInfoMap
          .entrySet()) {
        PubSubTopicPartition topicPartition = entry.getKey();
        TopicPartitionIngestionInfo topicPartitionIngestionInfo = entry.getValue();
        topicPartitionIngestionContext.computeIfAbsent(kafkaUrl, k -> new HashMap<>())
            .put(topicPartition.toString(), topicPartitionIngestionInfo);
      }
    }
    return topicPartitionIngestionContextJsonSerializer.serialize(topicPartitionIngestionContext, "");
  }

  public String getIngestionInfoFor(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      String regionName) {
    String kafkaUrl = getKafkaUrlFromRegionName(regionName);
    if (kafkaUrl == null) {
      return "kafkaUrl is not found for region: " + regionName;
    }
    Map<PubSubTopicPartition, TopicPartitionIngestionInfo> topicPartitionIngestionInfoMap = new HashMap<>();
    boolean consumerServiceFound = false;
    for (boolean decryptionEnabled: new boolean[] { false, true }) {
      AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaUrl, decryptionEnabled);
      if (consumerService != null) {
        consumerServiceFound = true;
        topicPartitionIngestionInfoMap
            .putAll(consumerService.getIngestionInfoFor(versionTopic, pubSubTopicPartition, true));
      }
    }
    if (!consumerServiceFound) {
      return "Kafka consumer service is not found for kafkaUrl: " + kafkaUrl + ", region: " + regionName;
    }
    return KafkaConsumerService.convertTopicPartitionIngestionInfoMapToStr(topicPartitionIngestionInfoMap);
  }

  private String getKafkaUrlFromRegionName(String regionName) {
    for (Map.Entry<String, String> entry: kafkaClusterUrlToAliasMap.entrySet()) {
      if (entry.getValue().equals(regionName)) {
        return entry.getKey();
      }
    }
    return null;
  }

  public static int getKeyLevelLockMaxPoolSizeBasedOnServerConfig(VeniceServerConfig serverConfig, int partitionCount) {
    int consumerPoolSizeForLeaderConsumption = 0;
    if (serverConfig.getConsumerPoolStrategyType()
        .equals(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION)) {
      consumerPoolSizeForLeaderConsumption = serverConfig.getConsumerPoolSizeForCurrentVersionAAWCLeader()
          + serverConfig.getConsumerPoolSizeForCurrentVersionSepRTLeader()
          + serverConfig.getConsumerPoolSizeForNonCurrentVersionAAWCLeader();
    } else {
      consumerPoolSizeForLeaderConsumption = serverConfig.getConsumerPoolSizePerKafkaCluster();
    }
    int multiplier = 1;
    if (serverConfig.isAAWCWorkloadParallelProcessingEnabled()) {
      multiplier = serverConfig.getAAWCWorkloadParallelProcessingThreadPoolSize();
    }
    return Math.min(partitionCount, consumerPoolSizeForLeaderConsumption)
        * serverConfig.getKafkaClusterIdToUrlMap().size() * multiplier + 1;
  }

}
