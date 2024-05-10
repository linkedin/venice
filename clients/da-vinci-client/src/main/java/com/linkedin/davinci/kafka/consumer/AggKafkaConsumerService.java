package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.StuckConsumerRepairStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext.PubSubPropertiesSupplier;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link AggKafkaConsumerService} supports Kafka consumer pool for multiple Kafka clusters from different data centers;
 * for each Kafka bootstrap server url, {@link AggKafkaConsumerService} will create one {@link KafkaConsumerService}.
 */
public class AggKafkaConsumerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(AggKafkaConsumerService.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final PubSubConsumerAdapterFactory consumerFactory;
  private final VeniceServerConfig serverConfig;
  private final long readCycleDelayMs;
  private final long sharedConsumerNonExistingTopicCleanupDelayMS;
  private final EventThrottler bandwidthThrottler;
  private final EventThrottler recordsThrottler;
  private final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private final MetricsRepository metricsRepository;
  private final TopicExistenceChecker topicExistenceChecker;
  private final boolean liveConfigBasedKafkaThrottlingEnabled;
  private final boolean isKafkaConsumerOffsetCollectionEnabled;
  private final KafkaConsumerService.ConsumerAssignmentStrategy sharedConsumerAssignmentStrategy;
  private final Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap =
      new VeniceConcurrentHashMap<>();
  private final Map<String, String> kafkaClusterUrlToAliasMap;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  private final PubSubMessageDeserializer pubSubDeserializer;
  private final Function<String, String> kafkaClusterUrlResolver;
  private final PubSubPropertiesSupplier pubSubPropertiesSupplier;

  private final Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping = new VeniceConcurrentHashMap<>();
  private ScheduledExecutorService stuckConsumerRepairExecutorService;
  private final Function<String, Boolean> isAAOrWCEnabledFunc;
  private final ReadOnlyStoreRepository metadataRepository;

  private final VeniceJsonSerializer<Map<String, Map<String, TopicPartitionIngestionInfo>>> topicPartitionIngestionContextJsonSerializer =
      new VeniceJsonSerializer<>(new TypeReference<Map<String, Map<String, TopicPartitionIngestionInfo>>>() {
      });

  public AggKafkaConsumerService(
      final PubSubConsumerAdapterFactory consumerFactory,
      final PubSubPropertiesSupplier pubSubPropertiesSupplier,
      final VeniceServerConfig serverConfig,
      final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler,
      KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      TopicExistenceChecker topicExistenceChecker,
      final PubSubMessageDeserializer pubSubDeserializer,
      Consumer<String> killIngestionTaskRunnable,
      Function<String, Boolean> isAAOrWCEnabledFunc,
      ReadOnlyStoreRepository metadataRepository) {
    this.serverConfig = serverConfig;
    this.consumerFactory = consumerFactory;
    this.readCycleDelayMs = serverConfig.getKafkaReadCycleDelayMs();
    this.sharedConsumerNonExistingTopicCleanupDelayMS = serverConfig.getSharedConsumerNonExistingTopicCleanupDelayMS();
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.kafkaClusterBasedRecordThrottler = kafkaClusterBasedRecordThrottler;
    this.metricsRepository = metricsRepository;
    this.topicExistenceChecker = topicExistenceChecker;
    this.liveConfigBasedKafkaThrottlingEnabled = serverConfig.isLiveConfigBasedKafkaThrottlingEnabled();
    this.sharedConsumerAssignmentStrategy = serverConfig.getSharedConsumerAssignmentStrategy();
    this.kafkaClusterUrlToAliasMap = serverConfig.getKafkaClusterUrlToAliasMap();
    this.kafkaClusterUrlToIdMap = serverConfig.getKafkaClusterUrlToIdMap();
    this.isKafkaConsumerOffsetCollectionEnabled = serverConfig.isKafkaConsumerOffsetCollectionEnabled();
    this.pubSubDeserializer = pubSubDeserializer;
    this.kafkaClusterUrlResolver = serverConfig.getKafkaClusterUrlResolver();
    this.metadataRepository = metadataRepository;
    if (serverConfig.isStuckConsumerRepairEnabled()) {
      this.stuckConsumerRepairExecutorService = Executors.newSingleThreadScheduledExecutor(
          new DaemonThreadFactory(this.getClass().getName() + "-StuckConsumerRepair"));
      int intervalInSeconds = serverConfig.getStuckConsumerRepairIntervalSecond();
      this.stuckConsumerRepairExecutorService.scheduleAtFixedRate(
          getStuckConsumerDetectionAndRepairRunnable(
              versionTopicStoreIngestionTaskMapping,
              TimeUnit.SECONDS.toMillis(serverConfig.getStuckIngestionTaskKillThresholdMs()),
              TimeUnit.SECONDS.toMillis(serverConfig.getStuckIngestionTaskKillRetryIntervalMs()),
              new StuckConsumerRepairStats(metricsRepository),
              killIngestionTaskRunnable),
          intervalInSeconds,
          intervalInSeconds,
          TimeUnit.SECONDS);
      LOGGER.info("Started stuck consumer repair service with checking interval: {} seconds", intervalInSeconds);
    }
    this.isAAOrWCEnabledFunc = isAAOrWCEnabledFunc;
    this.pubSubPropertiesSupplier = pubSubPropertiesSupplier;
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
  }

  /**
   * This task is scheduled at fixed rate (1-min) to detect and repair stuck consumers.
   * Once it detects stuck consumers, it would try to repair them by killing the ingestion task within configured threshold.
   * The following logic can be further optimized in the following way:
   * 1. If the max delay of previous run is much smaller than the threshold.
   * 2. In the next run, the max possible delay will be schedule interval + previous max delay ms, and if it is
   * still below the threshold, this function can return directly.
   * We are not adopting such optimization right now because:
   * 1. Extra state to maintain.
   * 2. The schedule interval is supposed to be high.
   * 3. The check is cheap when there is no stuck consumer.
   */
  protected static Runnable getStuckConsumerDetectionAndRepairRunnable(
      Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping,
      long stuckIngestionTaskKillThresholdMs,
      long stuckIngestionTaskKillRetryIntervalMs,
      StuckConsumerRepairStats stuckConsumerRepairStats,
      Consumer<String> killIngestionTaskRunnable) {
    return () -> {
      Map<String, StoreIngestionTask> versionTopicMappingForStuckIngestionTasks = new HashMap<>();
      versionTopicStoreIngestionTaskMapping.forEach((vt, sit) -> {
        /**
         * There are 2 cases where ingestion can be stuck:
         * 1. version topic doesn't exist. The issue can be transient so it's self-recoverable.
         * 2. replica is online, but it encounters one exception during ingestion.
         * Collect a list of SITs, whose version topic doesn't exist by checking {@link StoreIngestionTask#isProducingVersionTopicHealthy()},
         * and this function will continue to check the version topic healthiness for a period of {@link stuckIngestionTaskKillThresholdMs}
         * to tolerate transient topic discovery issue.
         */
        if (!sit.isProducingVersionTopicHealthy() || sit.isIngestionStuckPostOnline()) {
          versionTopicMappingForStuckIngestionTasks.put(vt, sit);
          LOGGER.warn("The producing version topic:{} is not healthy", vt);
        }
      });

      if (versionTopicMappingForStuckIngestionTasks.isEmpty()) {
        return;
      }

      stuckConsumerRepairStats.recordStuckConsumerFound();
      LOGGER.warn("Found some consumer in stuck state, will start the repairing procedure");

      int maxAttempts =
          (int) Math.ceil((double) stuckIngestionTaskKillThresholdMs / stuckIngestionTaskKillRetryIntervalMs);
      for (int cnt = 0; cnt < maxAttempts; ++cnt) {
        Iterator<Map.Entry<String, StoreIngestionTask>> iterator =
            versionTopicMappingForStuckIngestionTasks.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<String, StoreIngestionTask> entry = iterator.next();
          String versionTopic = entry.getKey();
          StoreIngestionTask sit = entry.getValue();
          if (sit.isIngestionStuckPostOnline()) {
            // If the ingestion task is stuck post online, it's not recoverable by waiting.
            continue;
          }
          if (sit.isProducingVersionTopicHealthy()) {
            /**
             * If the version topic becomes available after retries, remove it from the tracking map.
             */
            iterator.remove();
            LOGGER.info("The producing version topic:{} becomes healthy", versionTopic);
          }
          Utils.sleep(stuckIngestionTaskKillRetryIntervalMs);
        }
      }

      versionTopicMappingForStuckIngestionTasks.forEach((vt, sit) -> {
        try {
          LOGGER.warn(
              "The ingestion topics (version topic) are not healthy for "
                  + "store version: {}, will kill the ingestion task to try to unblock shared consumer",
              vt);
          /**
           * The following function call will interrupt all the stuck {@link org.apache.kafka.clients.producer.KafkaProducer#send} call
           * to non-existing topics.
           */
          sit.closeVeniceWriters(false);
          killIngestionTaskRunnable.accept(vt);
          stuckConsumerRepairStats.recordIngestionTaskRepair();
        } catch (Exception e) {
          LOGGER.error("Hit exception while killing the stuck ingestion task for store version: {}", vt, e);
        }
      });
    };
  }

  /**
   * @return the {@link KafkaConsumerService} for a specific Kafka bootstrap url,
   *         or null if there isn't any.
   */
  AbstractKafkaConsumerService getKafkaConsumerService(final String kafkaURL) {
    AbstractKafkaConsumerService consumerService = kafkaServerToConsumerServiceMap.get(kafkaURL);
    if (consumerService == null && kafkaClusterUrlResolver != null) {
      consumerService = kafkaServerToConsumerServiceMap.get(kafkaClusterUrlResolver.apply(kafkaURL));
    }
    return consumerService;
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
    final AbstractKafkaConsumerService alreadyCreatedConsumerService = getKafkaConsumerService(resolvedKafkaUrl);
    if (alreadyCreatedConsumerService != null) {
      LOGGER.warn("KafkaConsumerService has already been created for Kafka cluster with URL: {}", resolvedKafkaUrl);
      return alreadyCreatedConsumerService;
    }

    AbstractKafkaConsumerService consumerService = kafkaServerToConsumerServiceMap.computeIfAbsent(
        resolvedKafkaUrl,
        url -> new KafkaConsumerServiceDelegator(
            serverConfig,
            (consumerPoolSize, statsSuffix) -> sharedConsumerAssignmentStrategy.constructor.construct(
                consumerFactory,
                consumerProperties,
                readCycleDelayMs,
                consumerPoolSize,
                bandwidthThrottler,
                recordsThrottler,
                kafkaClusterBasedRecordThrottler,
                metricsRepository,
                kafkaClusterUrlToAliasMap.getOrDefault(url, url) + statsSuffix,
                sharedConsumerNonExistingTopicCleanupDelayMS,
                topicExistenceChecker,
                liveConfigBasedKafkaThrottlingEnabled,
                pubSubDeserializer,
                SystemTime.INSTANCE,
                null,
                isKafkaConsumerOffsetCollectionEnabled,
                metadataRepository,
                serverConfig.isUnregisterMetricForDeletedStoreEnabled()),
            isAAOrWCEnabledFunc));

    if (!consumerService.isRunning()) {
      consumerService.start();
    }
    return consumerService;
  }

  public boolean hasConsumerAssignedFor(
      final String kafkaURL,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      return false;
    }
    SharedKafkaConsumer consumer =
        consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
    return consumer != null && consumer.hasSubscription(pubSubTopicPartition);
  }

  boolean hasConsumerAssignedFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    for (String kafkaUrl: kafkaServerToConsumerServiceMap.keySet()) {
      if (hasConsumerAssignedFor(kafkaUrl, versionTopic, pubSubTopicPartition)) {
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
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.unSubscribe(versionTopic, pubSubTopicPartition);
    }
  }

  void batchUnsubscribeConsumerFor(PubSubTopic versionTopic, Set<PubSubTopicPartition> topicPartitionSet) {
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.batchUnsubscribe(versionTopic, topicPartitionSet);
    }
  }

  public ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> subscribeConsumerFor(
      final String kafkaURL,
      StoreIngestionTask storeIngestionTask,
      PubSubTopicPartition pubSubTopicPartition,
      long lastOffset) {
    PubSubTopic versionTopic = storeIngestionTask.getVersionTopic();
    AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      throw new VeniceException(
          "Kafka consumer service must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaURL);
    }

    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> dataReceiver =
        new StorePartitionDataReceiver(
            storeIngestionTask,
            pubSubTopicPartition,
            kafkaURL,
            kafkaClusterUrlToIdMap.getOrDefault(kafkaURL, -1));

    versionTopicStoreIngestionTaskMapping.put(storeIngestionTask.getVersionTopic().getName(), storeIngestionTask);
    consumerService.startConsumptionIntoDataReceiver(pubSubTopicPartition, lastOffset, dataReceiver);
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

  public long getOffsetLagBasedOnMetrics(
      final String kafkaURL,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    return consumerService == null
        ? -1
        : consumerService.getOffsetLagBasedOnMetrics(versionTopic, pubSubTopicPartition);
  }

  public long getLatestOffsetBasedOnMetrics(
      final String kafkaURL,
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition) {
    AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    return consumerService == null
        ? -1
        : consumerService.getLatestOffsetBasedOnMetrics(versionTopic, pubSubTopicPartition);
  }

  /**
   * This will be called when an ingestion task is killed. {@link AggKafkaConsumerService}
   * will try to stop all subscription associated with the given version topic.
   */
  void unsubscribeAll(PubSubTopic versionTopic) {
    kafkaServerToConsumerServiceMap.values().forEach(consumerService -> consumerService.unsubscribeAll(versionTopic));
    versionTopicStoreIngestionTaskMapping.remove(versionTopic.getName());
  }

  void pauseConsumerFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    PubSubConsumerAdapter consumer;
    for (AbstractKafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, pubSubTopicPartition);
      if (consumer != null) {
        consumer.pause(pubSubTopicPartition);
      }
    }
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
    for (Map.Entry<String, AbstractKafkaConsumerService> entry: kafkaServerToConsumerServiceMap.entrySet()) {
      if (entry.getValue().hasAnySubscriptionFor(versionTopic)) {
        kafkaUrls.add(entry.getKey());
      }
    }
    return kafkaUrls;
  }

  byte[] getIngestionInfoFor(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) throws IOException {
    Map<String, Map<String, TopicPartitionIngestionInfo>> topicPartitionIngestionContext = new HashMap<>();
    for (String kafkaUrl: kafkaServerToConsumerServiceMap.keySet()) {
      AbstractKafkaConsumerService consumerService = getKafkaConsumerService(kafkaUrl);
      Map<PubSubTopicPartition, TopicPartitionIngestionInfo> topicPartitionIngestionInfoMap =
          consumerService.getIngestionInfoFromConsumer(versionTopic, pubSubTopicPartition);
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
}
