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
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final IngestionThrottler ingestionThrottler;
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

  private final static String STUCK_CONSUMER_MSG =
      "Didn't find any suspicious ingestion task, and please contact developers to investigate it further";

  private final VeniceJsonSerializer<Map<String, Map<String, TopicPartitionIngestionInfo>>> topicPartitionIngestionContextJsonSerializer =
      new VeniceJsonSerializer<>(new TypeReference<Map<String, Map<String, TopicPartitionIngestionInfo>>>() {
      });

  public AggKafkaConsumerService(
      final PubSubConsumerAdapterFactory consumerFactory,
      final PubSubPropertiesSupplier pubSubPropertiesSupplier,
      final VeniceServerConfig serverConfig,
      final IngestionThrottler ingestionThrottler,
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
    this.ingestionThrottler = ingestionThrottler;
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
              kafkaServerToConsumerServiceMap,
              versionTopicStoreIngestionTaskMapping,
              TimeUnit.SECONDS.toMillis(serverConfig.getStuckConsumerDetectionRepairThresholdSecond()),
              TimeUnit.SECONDS.toMillis(serverConfig.getNonExistingTopicIngestionTaskKillThresholdSecond()),
              TimeUnit.SECONDS.toMillis(serverConfig.getNonExistingTopicCheckRetryIntervalSecond()),
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

  protected static Runnable getStuckConsumerDetectionAndRepairRunnable(
      Map<String, AbstractKafkaConsumerService> kafkaServerToConsumerServiceMap,
      Map<String, StoreIngestionTask> versionTopicStoreIngestionTaskMapping,
      long stuckConsumerRepairThresholdMs,
      long nonExistingTopicIngestionTaskKillThresholdMs,
      long nonExistingTopicRetryIntervalMs,
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
          LOGGER.warn("Found some consumer has stuck for {} ms, will start the repairing procedure", maxDelayMs);
          break;
        }
      }
      if (!scanStoreIngestionTaskToFixStuckConsumer) {
        return;
      }
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
            LOGGER.warn("The producing version topic:{} is not healthy", vt);
          }
        } catch (Exception e) {
          LOGGER.error("Got exception while checking topic existence for version topic: {}", vt, e);
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
              LOGGER.info("The producing version topic:{} becomes healthy", versionTopic);
            }
          } catch (Exception e) {
            LOGGER.error("Got exception while checking topic existence for version topic: {}", versionTopic, e);
          } finally {
            Utils.sleep(nonExistingTopicRetryIntervalMs);
          }
        }
      }

      AtomicBoolean hasRepairedSomeIngestionTask = new AtomicBoolean(false);
      versionTopicIngestionTaskMappingForNonExistingTopic.forEach((vt, sit) -> {
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
          hasRepairedSomeIngestionTask.set(true);
          stuckConsumerRepairStats.recordIngestionTaskRepair();
        } catch (Exception e) {
          LOGGER.error("Hit exception while killing the stuck ingestion task for store version: {}", vt, e);
        }
      });
      if (!hasRepairedSomeIngestionTask.get()) {
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(STUCK_CONSUMER_MSG)) {
          LOGGER.warn(STUCK_CONSUMER_MSG);
        }
        stuckConsumerRepairStats.recordRepairFailure();
      }
    };
  }

  /**
   * @return the {@link KafkaConsumerService} for a specific Kafka bootstrap url,
   *         or null if there isn't any.
   */
  AbstractKafkaConsumerService getKafkaConsumerService(final String kafkaURL) {
    AbstractKafkaConsumerService consumerService = kafkaServerToConsumerServiceMap.get(kafkaURL);
    if (consumerService == null && kafkaClusterUrlResolver != null) {
      // The resolver is needed to resolve a special format of kafka URL to the original kafka URL
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
            (poolSize, poolType) -> sharedConsumerAssignmentStrategy.constructor.construct(
                poolType,
                consumerFactory,
                consumerProperties,
                readCycleDelayMs,
                poolSize,
                ingestionThrottler,
                kafkaClusterBasedRecordThrottler,
                metricsRepository,
                kafkaClusterUrlToAliasMap.getOrDefault(url, url) + poolType.getStatSuffix(),
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

  public ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> subscribeConsumerFor(
      final String kafkaURL,
      StoreIngestionTask storeIngestionTask,
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      long lastOffset) {
    PubSubTopic versionTopic = storeIngestionTask.getVersionTopic();
    PubSubTopicPartition pubSubTopicPartition = partitionReplicaIngestionContext.getPubSubTopicPartition();
    AbstractKafkaConsumerService consumerService =
        getKafkaConsumerService(kafkaClusterUrlResolver == null ? kafkaURL : kafkaClusterUrlResolver.apply(kafkaURL));
    if (consumerService == null) {
      throw new VeniceException(
          "Kafka consumer service must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaURL);
    }

    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> dataReceiver =
        new StorePartitionDataReceiver(
            storeIngestionTask,
            pubSubTopicPartition,
            kafkaURL, // do not resolve and let it pass downstream for offset tracking purpose
            kafkaClusterUrlToIdMap.getOrDefault(kafkaURL, -1)); // same pubsub url but different id for sep topic

    versionTopicStoreIngestionTaskMapping.put(storeIngestionTask.getVersionTopic().getName(), storeIngestionTask);
    consumerService.startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, lastOffset, dataReceiver);
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
          consumerService.getIngestionInfoFor(versionTopic, pubSubTopicPartition);
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
