package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.notifier.MetaSystemStoreReplicaStatusNotifier;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.stats.ParticipantStoreConsumptionStats;
import com.linkedin.davinci.stats.StoreBufferServiceStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.MetadataRetriever;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriterFactory;

import io.tehuti.metrics.MetricsRepository;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static com.linkedin.venice.ConfigConstants.*;
import static java.lang.Thread.*;
import static org.apache.kafka.common.config.SslConfigs.*;


/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * Manages Kafka topics and partitions that need to be consumed for the stores on this node.
 *
 * Launches {@link StoreIngestionTask} for each store version to consume and process messages.
 *
 * Uses the "new" Kafka Consumer.
 */
public class KafkaStoreIngestionService extends AbstractVeniceService implements StoreIngestionService, MetadataRetriever {
  private static final String GROUP_ID_FORMAT = "%s_%s";

  private static final Logger logger = Logger.getLogger(KafkaStoreIngestionService.class);

  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> onlineOfflineNotifiers = new ConcurrentLinkedQueue<>();
  private final Queue<VeniceNotifier> leaderFollowerNotifiers = new ConcurrentLinkedQueue<>();

  private final StorageMetadataService storageMetadataService;

  private final ReadOnlyStoreRepository metadataRepo;

  private final AggStoreIngestionStats ingestionStats;

  private final AggVersionedStorageIngestionStats versionedStorageIngestionStats;

  /**
   * Store buffer service to persist data into local bdb for all the stores.
   */
  private final StoreBufferService storeBufferService;

  private final AggKafkaConsumerService aggKafkaConsumerService;

  /**
   * A repository mapping each Kafka Topic to it corresponding Ingestion task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final NavigableMap<String, StoreIngestionTask> topicNameToIngestionTaskMap;
  private final Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  private final ExecutorService participantStoreConsumerExecutorService = Executors.newSingleThreadExecutor();

  private ExecutorService ingestionExecutorService;

  // Need to make sure that the service has started before start running KafkaConsumptionTask.
  private final AtomicBoolean isRunning;

  private ParticipantStoreConsumptionTask participantStoreConsumptionTask = null;

  private StoreIngestionTaskFactory ingestionTaskFactory;

  private final int retryStoreRefreshIntervalInMs = 5000;
  private final int retryStoreRefreshAttempt = 10;

  private final ExecutorService cacheWarmingExecutorService;

  private final MetaStoreWriter metaStoreWriter;
  private final MetaSystemStoreReplicaStatusNotifier metaSystemStoreReplicaStatusNotifier;
  private boolean metaSystemStoreReplicaStatusNotifierQueued = false;

  public KafkaStoreIngestionService(StorageEngineRepository storageEngineRepository,
      VeniceConfigLoader veniceConfigLoader,
      StorageMetadataService storageMetadataService,
      ClusterInfoProvider clusterInfoProvider,
      ReadOnlyStoreRepository metadataRepo,
      ReadOnlySchemaRepository schemaRepo,
      MetricsRepository metricsRepository,
      RocksDBMemoryStats rocksDBMemoryStats,
      Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader,
      Optional<ClientConfig> clientConfig,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this(storageEngineRepository, veniceConfigLoader, storageMetadataService, clusterInfoProvider, metadataRepo, schemaRepo,
        metricsRepository, rocksDBMemoryStats, kafkaMessageEnvelopeSchemaReader, clientConfig, partitionStateSerializer,
        Optional.empty());
  }

  public KafkaStoreIngestionService(StorageEngineRepository storageEngineRepository,
                                    VeniceConfigLoader veniceConfigLoader,
                                    StorageMetadataService storageMetadataService,
                                    ClusterInfoProvider clusterInfoProvider,
                                    ReadOnlyStoreRepository metadataRepo,
                                    ReadOnlySchemaRepository schemaRepo,
                                    MetricsRepository metricsRepository,
                                    RocksDBMemoryStats rocksDBMemoryStats,
                                    Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader,
                                    Optional<ClientConfig> clientConfig,
                                    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
                                    Optional<HelixReadOnlyZKSharedSchemaRepository> zkSharedSchemaRepository) {
    this.storageMetadataService = storageMetadataService;
    this.metadataRepo = metadataRepo;
    this.partitionStateSerializer = partitionStateSerializer;

    this.topicNameToIngestionTaskMap = new ConcurrentSkipListMap<>();
    this.isRunning = new AtomicBoolean(false);

    this.veniceConfigLoader = veniceConfigLoader;

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    VeniceServerConsumerFactory veniceConsumerFactory = new VeniceServerConsumerFactory(serverConfig);

    Properties veniceWriterProperties = veniceConfigLoader.getVeniceClusterConfig().getClusterProperties().toProperties();
    if (serverConfig.isKafkaOpenSSLEnabled()) {
      veniceWriterProperties.setProperty(SSL_CONTEXT_PROVIDER_CLASS_CONFIG, DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME);
    }
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(veniceWriterProperties);

    EventThrottler bandwidthThrottler = new EventThrottler(
        serverConfig.getKafkaFetchQuotaBytesPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(),
        "kafka_consumption_bandwidth",
        false,
        EventThrottler.BLOCK_STRATEGY);

    EventThrottler recordsThrottler = new EventThrottler(
        serverConfig.getKafkaFetchQuotaRecordPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(),
        "kafka_consumption_records_count",
        false,
        EventThrottler.BLOCK_STRATEGY);

    EventThrottler unorderedBandwidthThrottler = new EventThrottler(
        serverConfig.getKafkaFetchQuotaUnorderedBytesPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(),
        "kafka_consumption_unordered_bandwidth",
        false,
        EventThrottler.BLOCK_STRATEGY);

    EventThrottler unorderedRecordsThrottler = new EventThrottler(
        serverConfig.getKafkaFetchQuotaUnorderedRecordPerSecond(),
        serverConfig.getKafkaFetchQuotaTimeWindow(),
        "kafka_consumption_unordered_records_count",
        false,
        EventThrottler.BLOCK_STRATEGY);

    TopicManagerRepository topicManagerRepository = new TopicManagerRepository(
        veniceConsumerFactory.getKafkaBootstrapServers(),
        veniceConsumerFactory.getKafkaZkAddress(),
        veniceConsumerFactory);

    VeniceNotifier notifier = new LogNotifier();
    this.onlineOfflineNotifiers.add(notifier);
    this.leaderFollowerNotifiers.add(notifier);

    /**
     * Only Venice SN will pass this param: {@param zkSharedSchemaRepository} since {@link metaStoreWriter} is
     * used to report the replica status from Venice SN.
     */
    if (zkSharedSchemaRepository.isPresent()) {
      this.metaStoreWriter = new MetaStoreWriter(topicManagerRepository.getTopicManager(), veniceWriterFactory, zkSharedSchemaRepository.get());
      this.metaSystemStoreReplicaStatusNotifier =
          new MetaSystemStoreReplicaStatusNotifier(serverConfig.getClusterName(), metaStoreWriter, metadataRepo,
              Instance.fromHostAndPort(Utils.getHostName(), serverConfig.getListenerPort()));
      logger.info("MetaSystemStoreReplicaStatusNotifier was initialized");
    } else {
      this.metaStoreWriter = null;
      this.metaSystemStoreReplicaStatusNotifier = null;
    }

    this.ingestionStats = new AggStoreIngestionStats(metricsRepository);
    AggVersionedDIVStats versionedDIVStats = new AggVersionedDIVStats(metricsRepository, metadataRepo);
    this.versionedStorageIngestionStats =
        new AggVersionedStorageIngestionStats(metricsRepository, metadataRepo);
    this.storeBufferService = new StoreBufferService(
        serverConfig.getStoreWriterNumber(),
        serverConfig.getStoreWriterBufferMemoryCapacity(),
        serverConfig.getStoreWriterBufferNotifyDelta());
    this.kafkaMessageEnvelopeSchemaReader = kafkaMessageEnvelopeSchemaReader;
    /**
     * Collect metrics for {@link #storeBufferService}.
     * Since all the metrics will be collected passively, there is no need to
     * keep the reference of this {@link StoreBufferServiceStats} variable.
     */
    new StoreBufferServiceStats(metricsRepository, this.storeBufferService);

    if (clientConfig.isPresent()) {
      String clusterName = veniceConfigLoader.getVeniceClusterConfig().getClusterName();
      participantStoreConsumptionTask = new ParticipantStoreConsumptionTask(
          this,
          clusterInfoProvider,
          new ParticipantStoreConsumptionStats(metricsRepository, clusterName),
          ClientConfig.cloneConfig(clientConfig.get()).setMetricsRepository(metricsRepository),
          serverConfig.getParticipantMessageConsumptionDelayMs());
    } else {
      logger.info("Unable to start participant store consumption task because client config is not provided, jobs "
          + "may not be killed if admin helix messaging channel is disabled");
    }

    if (serverConfig.isSharedConsumerPoolEnabled()) {
      aggKafkaConsumerService = new AggKafkaConsumerService(
          veniceConsumerFactory,
          serverConfig,
          bandwidthThrottler,
          recordsThrottler,
          metricsRepository);
      /**
       * After initializing a {@link AggKafkaConsumerService} service, it doesn't contain any consumer pool yet until
       * a new Kafka cluster is registered; here we explicitly register the local Kafka cluster by invoking
       * {@link AggKafkaConsumerService#getKafkaConsumerService(Properties)}
       *
       * Pass through all the customized Kafka consumer configs into the consumer as long as the customized config key
       * starts with {@link ConfigKeys#SERVER_LOCAL_CONSUMER_CONFIG_PREFIX}; the clipping and filtering work is done
       * in {@link VeniceServerConfig} already.
       *
       * Here, we only pass through the customized Kafka consumer configs for local consumption, if an ingestion task
       * creates a dedicated consumer or a new consumer service for a remote Kafka URL, we will passthrough the configs
       * for remote consumption inside the ingestion task.
       */
      Properties commonKafkaConsumerConfigs = getCommonKafkaConsumerProperties(serverConfig);
      if (!serverConfig.getKafkaConsumerConfigsForLocalConsumption().isEmpty()) {
        commonKafkaConsumerConfigs.putAll(serverConfig.getKafkaConsumerConfigsForLocalConsumption().toProperties());
      }
      aggKafkaConsumerService.getKafkaConsumerService(commonKafkaConsumerConfigs);
      logger.info("Shared consumer pool for ingestion is enabled");
    } else {
      aggKafkaConsumerService = null;
      logger.info("Shared consumer pool for ingestion is disabled");
    }

    if (serverConfig.isCacheWarmingBeforeReadyToServeEnabled()) {
      cacheWarmingExecutorService = Executors.newFixedThreadPool(serverConfig.getCacheWarmingThreadPoolSize(), new DaemonThreadFactory("Cache_Warming"));
      logger.info("Cache warming is enabled");
    } else {
      cacheWarmingExecutorService = null;
      logger.info("Cache warming is disabled");
    }

    /**
     * Use the same diskUsage instance for all ingestion tasks; so that all the ingestion tasks can update the same
     * remaining disk space state to provide a more accurate alert.
     */
    DiskUsage diskUsage = new DiskUsage(
        veniceConfigLoader.getVeniceServerConfig().getDataBasePath(),
        veniceConfigLoader.getVeniceServerConfig().getDiskFullThreshold());

    ingestionTaskFactory = StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(veniceWriterFactory)
        .setKafkaClientFactory(veniceConsumerFactory)
        .setStorageEngineRepository(storageEngineRepository)
        .setStorageMetadataService(storageMetadataService)
        .setOnlineOfflineNotifiersQueue(onlineOfflineNotifiers)
        .setLeaderFollowerNotifiersQueue(leaderFollowerNotifiers)
        .setBandwidthThrottler(bandwidthThrottler)
        .setRecordsThrottler(recordsThrottler)
        .setUnorderedBandwidthThrottler(unorderedBandwidthThrottler)
        .setUnorderedRecordsThrottler(unorderedRecordsThrottler)
        .setSchemaRepository(schemaRepo)
        .setMetadataRepository(metadataRepo)
        .setTopicManagerRepository(topicManagerRepository)
        .setStoreIngestionStats(ingestionStats)
        .setVersionedDIVStats(versionedDIVStats)
        .setVersionedStorageIngestionStats(versionedStorageIngestionStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(serverConfig)
        .setDiskUsage(diskUsage)
        .setAggKafkaConsumerService(aggKafkaConsumerService)
        .setRocksDBMemoryStats(rocksDBMemoryStats)
        .setCacheWarmingThreadPool(cacheWarmingExecutorService)
        .setStartReportingReadyToServeTimestamp(System.currentTimeMillis() + serverConfig.getDelayReadyToServeMS())
        .setPartitionStateSerializer(partitionStateSerializer)
        .build();
  }

  /**
   * This function should only be triggered in classical Venice since replica status reporting is only valid
   * in classical Venice for meta system store.
   */
  public synchronized void addMetaSystemStoreReplicaStatusNotifier() {
    if (metaSystemStoreReplicaStatusNotifierQueued) {
      throw new VeniceException("MetaSystemStoreReplicaStatusNotifier should NOT be added twice");
    }
    if (null == this.metaSystemStoreReplicaStatusNotifier) {
      throw new VeniceException("MetaSystemStoreReplicaStatusNotifier wasn't initialized properly");
    }
    addCommonNotifier(this.metaSystemStoreReplicaStatusNotifier);
    metaSystemStoreReplicaStatusNotifierQueued = true;
  }

  @Override
  public synchronized Optional<MetaSystemStoreReplicaStatusNotifier> getMetaSystemStoreReplicaStatusNotifier() {
    if (metaSystemStoreReplicaStatusNotifierQueued) {
      return Optional.of(metaSystemStoreReplicaStatusNotifier);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Starts the Kafka consumption tasks for already subscribed partitions.
   */
  @Override
  public boolean startInner() {
    logger.info("Enabling ingestion executor service and ingestion tasks.");
    ingestionExecutorService = Executors.newCachedThreadPool();
    topicNameToIngestionTaskMap.values().forEach(ingestionExecutorService::submit);
    isRunning.set(true);

    storeBufferService.start();
    if (null != aggKafkaConsumerService) {
      aggKafkaConsumerService.start();
    }
    if (participantStoreConsumptionTask != null) {
      participantStoreConsumerExecutorService.submit(participantStoreConsumptionTask);
    }
    // Although the StoreConsumptionTasks are now running in their own threads, there is no async
    // process that needs to finish before the KafkaStoreIngestionService can be considered
    // started, so we are done with the start up process.

    return true;
  }

  private StoreIngestionTask createConsumerTask(VeniceStoreConfig veniceStoreConfig, boolean isLeaderFollowerModel, int partitionId) {
    String storeName = Version.parseStoreFromKafkaTopicName(veniceStoreConfig.getStoreName());
    int storeVersion = Version.parseVersionFromKafkaTopicName(veniceStoreConfig.getStoreName());
    Store store = metadataRepo.getStoreOrThrow(storeName);
    Optional<Version> version = store.getVersion(storeVersion);

    int attempt = 0;

    // In theory, the version should exist since the corresponding store ingestion is ready to start.
    // The issue could be caused by race condition that the in-memory metadata hasn't been refreshed yet,
    // So here will refresh that store explicitly up to getRefreshAttemptsForZkReconnect times
    if (!version.isPresent()) {
      while (attempt < retryStoreRefreshAttempt) {
        store = metadataRepo.refreshOneStore(storeName);
        version = store.getVersion(storeVersion);
        if (version.isPresent()) {
          break;
        }
        attempt++;
        Utils.sleep(retryStoreRefreshIntervalInMs);
      }
    }
    // Error out if no store version found even after retries
    if (!version.isPresent()) {
      throw new VeniceException("Version: " + storeVersion + " doesn't exist in store: " + storeName);
    }

    VenicePartitioner venicePartitioner;
    PartitionerConfig partitionerConfig = version.get().getPartitionerConfig();
    if (partitionerConfig == null) {
      venicePartitioner = new DefaultVenicePartitioner();
    } else {
      venicePartitioner = PartitionUtils.getVenicePartitioner(partitionerConfig);
    }

    final Store finalStore = store;
    BooleanSupplier isStoreVersionCurrent = () -> finalStore.getCurrentVersion() == storeVersion;
    Optional<HybridStoreConfig> hybridStoreConfig = Optional.ofNullable(store.getHybridStoreConfig());

    boolean bufferReplayEnabledForHybrid = version.get().isBufferReplayEnabledForHybrid();
    Properties kafkaProperties = getKafkaConsumerProperties(veniceStoreConfig);

    boolean nativeReplicationEnabled = version.get().isNativeReplicationEnabled();
    boolean isIncrementalPushEnabled = version.get().isIncrementalPushEnabled();
    String nativeReplicationSourceAddress = version.get().getPushStreamSourceAddress();
    return ingestionTaskFactory.getNewIngestionTask(isLeaderFollowerModel, kafkaProperties, isStoreVersionCurrent,
        hybridStoreConfig, isIncrementalPushEnabled, veniceStoreConfig, bufferReplayEnabledForHybrid,
        nativeReplicationEnabled, nativeReplicationSourceAddress, partitionId, store.isWriteComputationEnabled(), venicePartitioner);
  }

  private void shutdownExecutorService(ExecutorService executorService, boolean force) {
    if (null == executorService) {
      return;
    }
    if (force) {
      executorService.shutdownNow();
    } else {
      executorService.shutdown();
    }
    try {
      executorService.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() throws Exception {
    logger.info("Shutting down Kafka consumer service");
    isRunning.set(false);

    shutdownExecutorService(participantStoreConsumerExecutorService, true);
    if (participantStoreConsumptionTask != null) {
      participantStoreConsumptionTask.close();
    }
    topicNameToIngestionTaskMap.values().forEach(StoreIngestionTask::close);
    /**
     * We would like to gracefully shutdown {@link #ingestionExecutorService}, so that it
     * will have an opportunity to checkpoint the processed offset.
     */
    shutdownExecutorService(ingestionExecutorService, false);
    shutdownExecutorService(cacheWarmingExecutorService, true);

    if (null != aggKafkaConsumerService) {
      aggKafkaConsumerService.stop();
    }

    if (null != storeBufferService) {
      storeBufferService.stop();
    }

    // N.B close() should be idempotent
    for (VeniceNotifier notifier : onlineOfflineNotifiers) {
      notifier.close();
    }

    for (VeniceNotifier notifier : leaderFollowerNotifiers) {
      notifier.close();
    }

    if (null != metaStoreWriter) {
      metaStoreWriter.close();
    }

    kafkaMessageEnvelopeSchemaReader.ifPresent(sr -> sr.close());
    logger.info("Shut down complete");
  }

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * Subscribes to partition if required.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public synchronized void startConsumption(VeniceStoreConfig veniceStore, int partitionId, boolean isLeaderFollowerModel) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = createConsumerTask(veniceStore, isLeaderFollowerModel, partitionId);
      topicNameToIngestionTaskMap.put(topic, consumerTask);
      versionedStorageIngestionStats.setIngestionTask(topic, consumerTask);
      if(!isRunning.get()) {
        logger.info("Ignoring Start consumption message as service is stopping. Topic " + topic + " Partition " + partitionId);
        return;
      }
      ingestionExecutorService.submit(consumerTask);
    }

    /**
     * Since Venice metric is store-level and it would have multiply topics tasks exist in the same time.
     * Only the task with largest version would emit it stats. That being said, relying on the {@link #metadataRepo}
     * to get the max version may be unreliable, since the information in this object is not guaranteed
     * to be up to date. As a sanity check, we will also look at the version in the topic name, and
     * pick whichever number is highest as the max version number.
     */
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int maxVersionNumberFromTopicName = Version.parseVersionFromKafkaTopicName(topic);
    int maxVersionNumberFromMetadataRepo = getStoreMaximumVersionNumber(storeName);
    if (maxVersionNumberFromTopicName > maxVersionNumberFromMetadataRepo) {
      logger.warn("Got stale info from metadataRepo. maxVersionNumberFromTopicName: " + maxVersionNumberFromTopicName +
          ", maxVersionNumberFromMetadataRepo: " + maxVersionNumberFromMetadataRepo +
          ". Will rely on the topic name's version.");
    }
    /**
     * Notice that the version push for maxVersionNumberFromMetadataRepo might be killed already (this code path will
     * also be triggered after server restarts).
     */
    int maxVersionNumber = Math.max(maxVersionNumberFromMetadataRepo, maxVersionNumberFromTopicName);
    updateStatsEmission(topicNameToIngestionTaskMap, storeName, maxVersionNumber);

    consumerTask.subscribePartition(topic, partitionId);
    logger.info("Started Consuming - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  @Override
  public synchronized void promoteToLeader(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    String topic = veniceStoreConfig.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.promoteToLeader(topic, partitionId, checker);
    } else {
      logger.warn("Ignoring standby to leader transition message for Topic " + topic + " Partition " + partitionId);
    }
  }

  @Override
  public synchronized void demoteToStandby(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    String topic = veniceStoreConfig.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.demoteToStandby(topic, partitionId, checker);
    } else {
      logger.warn("Ignoring leader to standby transition message for Topic " + topic + " Partition " + partitionId);
    }
  }

  /**
   * Find the task that matches both the storeName and maximumVersion number, enable metrics emission for this task and
   * update ingestion stats with this task; disable metric emission for all the task that doesn't max version.
   */
  protected void updateStatsEmission(NavigableMap<String, StoreIngestionTask> taskMap, String storeName, int maximumVersion) {
    String knownMaxVersionTopic = Version.composeKafkaTopic(storeName, maximumVersion);
    if (taskMap.containsKey(knownMaxVersionTopic)) {
      taskMap.forEach((topicName, task) -> {
        if (Version.parseStoreFromKafkaTopicName(topicName).equals(storeName)) {
          if (Version.parseVersionFromKafkaTopicName(topicName) < maximumVersion) {
            task.disableMetricsEmission();
          } else {
            task.enableMetricsEmission();
          }
        }
      });
    } else {
      /**
       * The version push doesn't exist in this server node at all; it's possible the push for largest version has
       * already been killed, so instead, emit metrics for the largest known batch push in this node.
       */
      updateStatsEmission(taskMap, storeName);
    }
  }

  /**
   * This function will go through all known ingestion task in this server node, find the task that matches the
   * storeName and has the largest version number; if the task doesn't enable metric emission, enable it and
   * update store ingestion stats.
   */
  protected void updateStatsEmission(NavigableMap<String, StoreIngestionTask> taskMap, String storeName) {
    int maxVersion = -1;
    StoreIngestionTask latestOngoingIngestionTask = null;
    for (Map.Entry<String, StoreIngestionTask> entry : taskMap.entrySet()) {
      String topic = entry.getKey();
      if (Version.parseStoreFromKafkaTopicName(topic).equals(storeName)) {
        int version = Version.parseVersionFromKafkaTopicName(topic);
        if (version > maxVersion) {
          maxVersion = version;
          latestOngoingIngestionTask = entry.getValue();
        }
      }
    }
    if (latestOngoingIngestionTask != null && !latestOngoingIngestionTask.isMetricsEmissionEnabled()) {
      latestOngoingIngestionTask.enableMetricsEmission();
      /**
       * Disable the metrics emission for all lower version pushes.
       */
      Map.Entry<String, StoreIngestionTask> lowerVersionPush = taskMap.lowerEntry(Version.composeKafkaTopic(storeName, maxVersion));
      while (lowerVersionPush != null && Version.parseStoreFromKafkaTopicName(lowerVersionPush.getKey()).equals(storeName)) {
        lowerVersionPush.getValue().disableMetricsEmission();
        lowerVersionPush = taskMap.lowerEntry(lowerVersionPush.getKey());
      }
    }
  }

  private int getStoreMaximumVersionNumber(String storeName) {
    Store store = metadataRepo.getStore(storeName);
    if (store == null) {
      throw new VeniceException("Could not find store " + storeName + " info in ZK");
    }

    int maxVersionNumber = store.getLargestUsedVersionNumber();
    if (maxVersionNumber == 0) {
      throw new VeniceException("No version has been created yet for store " + storeName);
    }

    return maxVersionNumber;
  }

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public synchronized void stopConsumption(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.unSubscribePartition(topic, partitionId);
    } else {
      logger.warn("Ignoring stop consumption message for Topic " + topic + " Partition " + partitionId);
    }
  }

  /**
   * Stops consuming messages from Kafka Partition corresponding to Venice Partition and wait up to
   * (sleepSeconds * numRetires) to make sure partition consumption is stopped.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   * @param sleepSeconds
   * @param numRetries
   */
  @Override
  public synchronized void stopConsumptionAndWait(VeniceStoreConfig veniceStore, int partitionId, int sleepSeconds, int numRetries) {
    stopConsumption(veniceStore, partitionId);
    try {
      for (int i = 0; i < numRetries; i++) {
        if (!isPartitionConsuming(veniceStore, partitionId)) {
          logger.info("Partition: " + partitionId + " of store: " + veniceStore.getStoreName() + " has stopped consumption.");
          return;
        }
        sleep(sleepSeconds * Time.MS_PER_SECOND);
      }
      logger.error("Partition: " + partitionId + " of store: " + veniceStore.getStoreName()
          + " is still consuming after waiting for it to stop for " + numRetries * sleepSeconds + " seconds.");
    } catch (InterruptedException e) {
      logger.warn("Waiting for partition to stop consumption was interrupted", e);
      currentThread().interrupt();
    }
  }

  /**
   * Resets Offset to beginning for Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public void resetConsumptionOffset(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if(consumerTask != null && consumerTask.isRunning()) {
      consumerTask.resetPartitionConsumptionOffset(topic, partitionId);
    }
    logger.info("Offset reset to beginning - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  /**
   * @param topicName Venice topic (store and version number) for the corresponding consumer task that needs to be killed.
   *                  No action is taken for invocations of killConsumptionTask on topics that are not in the map. This
   *                  includes logging.
   * @return true if a kill is needed and called, otherwise false
   */
  @Override
  public synchronized boolean killConsumptionTask(String topicName) {
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topicName);
    boolean killed = false;
    if (consumerTask != null) {
      if (consumerTask.isRunning()) {
        consumerTask.kill();
        killed = true;
        logger.info("Killed consumption task for Topic " + topicName);
      } else {
        logger.warn("Ignoring kill signal for Topic " + topicName);
      }
      // cleanup the map regardless if the task was running or not to prevent mem leak where errored tasks will linger
      // in the map since isRunning is set to false already.
      topicNameToIngestionTaskMap.remove(topicName);
      if (null != aggKafkaConsumerService) {
        aggKafkaConsumerService.detach(consumerTask);
      }

      /**
       * For the same store, there will be only one task emitting metrics, if this is the only task that is emitting
       * metrics, it means the latest ongoing push job is killed. In such case, find the largest version in the task
       * map and enable metric emission.
       */
      if (consumerTask.isMetricsEmissionEnabled()) {
        consumerTask.disableMetricsEmission();
        updateStatsEmission(topicNameToIngestionTaskMap, Version.parseStoreFromKafkaTopicName(topicName));
      }
    }
    return killed;
  }

  @Override
  public void addCommonNotifier(VeniceNotifier notifier) {
    onlineOfflineNotifiers.add(notifier);
    leaderFollowerNotifiers.add(notifier);
  }

  @Override
  public void addOnlineOfflineModelNotifier(VeniceNotifier notifier) {
    onlineOfflineNotifiers.add(notifier);
  }

  @Override
  public void addLeaderFollowerModelNotifier(VeniceNotifier notifier) {
    leaderFollowerNotifiers.add(notifier);
  }

  @Override
  public synchronized boolean containsRunningConsumption(VeniceStoreConfig veniceStore) {
    return containsRunningConsumption(veniceStore.getStoreName());
  }

  @Override
  public synchronized boolean containsRunningConsumption(String topic) {
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean isPartitionConsuming(VeniceStoreConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    return consumerTask != null && consumerTask.isRunning() && consumerTask.isPartitionConsuming(partitionId);
  }

  @Override
  public Set<String> getIngestingTopicsWithVersionStatusNotOnline() {
    Set<String> result = new HashSet<>();
    for (String topic : topicNameToIngestionTaskMap.keySet()) {
      Store store = metadataRepo.getStore(Version.parseStoreFromKafkaTopicName(topic));
      int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
      if (store == null || !store.getVersion(versionNumber).isPresent()
          || store.getVersion(versionNumber).get().getStatus() != VersionStatus.ONLINE) {
        result.add(topic);
      }
    }
    return result;
  }

  @Override
  public AggStoreIngestionStats getAggStoreIngestionStats() {
    return ingestionStats;
  }

  @Override
  public AggVersionedStorageIngestionStats getAggVersionedStorageIngestionStats() {
    return versionedStorageIngestionStats;
  }

  /**
   * @return Group Id for kafka consumer.
   */
  private static String getGroupId(String topic) {
    return String.format(GROUP_ID_FORMAT, topic, Utils.getHostName());
  }

  /**
   * So far, this function is only targeted to be used by shared consumer.
   * @param serverConfig
   * @return
   */
  private Properties getCommonKafkaConsumerProperties(VeniceServerConfig serverConfig) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, serverConfig.getKafkaBootstrapServers());
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Venice is persisting offset in local offset db.
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    //This is a temporary fix for the issue described here
    //https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    //In our case "com.linkedin.venice.serialization.KafkaKeySerializer" and
    //"com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer" classes can not be found
    //because class loader has no venice-common in class path. This can be only reproduced on JDK11
    //Trying to avoid class loading via Kafka's ConfigDef class
    kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class);
    kafkaConsumerProperties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
        String.valueOf(serverConfig.getKafkaFetchMinSizePerSecond()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
        String.valueOf(serverConfig.getKafkaFetchMaxSizePerSecond()));
    /**
     * The following setting is used to control the maximum number of records to returned in one poll request.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(serverConfig.getKafkaMaxPollRecords()));
    kafkaConsumerProperties
        .setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(serverConfig.getKafkaFetchMaxTimeMS()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        String.valueOf(serverConfig.getKafkaFetchPartitionMaxSizePerSecond()));
    kafkaConsumerProperties.setProperty(ApacheKafkaConsumer.CONSUMER_POLL_RETRY_TIMES_CONFIG,
        String.valueOf(serverConfig.getKafkaPollRetryTimes()));
    kafkaConsumerProperties.setProperty(ApacheKafkaConsumer.CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG,
        String.valueOf(serverConfig.getKafkaPollRetryBackoffMs()));
    if (kafkaMessageEnvelopeSchemaReader.isPresent()) {
      kafkaConsumerProperties.put(InternalAvroSpecificSerializer.VENICE_SCHEMA_READER_CONFIG, kafkaMessageEnvelopeSchemaReader.get());
    }

    return kafkaConsumerProperties;
  }

  /**
   * @return Properties Kafka properties corresponding to the venice store.
   */
  private Properties getKafkaConsumerProperties(VeniceStoreConfig storeConfig) {
    Properties kafkaConsumerProperties = getCommonKafkaConsumerProperties(storeConfig);
    String groupId = getGroupId(storeConfig.getStoreName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    /**
     * Temporarily we are going to use group_id as client_id as well since it is unique in cluster level.
     * With unique client_id, it will be easier for us to check Kafka consumer related metrics through JMX.
     * TODO: Kafka is throttling based on client_id, need to investigate whether we should use Kafka throttling or not.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
    return kafkaConsumerProperties;
  }

  @Override
  public Optional<Long> getOffset(String topicName, int partitionId) {
    StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topicName);
    if (null == ingestionTask) {
      return Optional.empty();
    }
    return ingestionTask.getCurrentOffset(partitionId);
  }

  @Override
  public boolean isStoreVersionChunked(String topicName) {
    return storageMetadataService.isStoreVersionChunked(topicName);
  }

  @Override
  public CompressionStrategy getStoreVersionCompressionStrategy(String topicName) {
    return storageMetadataService.getStoreVersionCompressionStrategy(topicName);
  }

  @Override
  public ByteBuffer getStoreVersionCompressionDictionary(String topicName) {
    return storageMetadataService.getStoreVersionCompressionDictionary(topicName);
  }

  public StoreIngestionTask getStoreIngestionTask(String topicName) {
    return topicNameToIngestionTaskMap.get(topicName);
  }
}
