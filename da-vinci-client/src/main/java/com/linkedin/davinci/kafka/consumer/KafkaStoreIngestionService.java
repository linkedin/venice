package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.MetaSystemStoreReplicaStatusNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggLagStats;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.stats.ParticipantStoreConsumptionStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.stats.StoreBufferServiceStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.KafkaClientStats;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.SharedKafkaProducerService;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

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
public class KafkaStoreIngestionService extends AbstractVeniceService implements StoreIngestionService {
  private static final String GROUP_ID_FORMAT = "%s_%s";

  private static final Logger logger = Logger.getLogger(KafkaStoreIngestionService.class);

  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> onlineOfflineNotifiers = new ConcurrentLinkedQueue<>();
  private final Queue<VeniceNotifier> leaderFollowerNotifiers = new ConcurrentLinkedQueue<>();

  private final StorageMetadataService storageMetadataService;

  private final ReadOnlyStoreRepository metadataRepo;

  private final AggStoreIngestionStats ingestionStats;

  private final AggVersionedStorageIngestionStats versionedStorageIngestionStats;

  private final AggLagStats aggLagStats;

  /**
   * Store buffer service to persist data into local bdb for all the stores.
   */
  private final AbstractStoreBufferService storeBufferService;

  private final AggKafkaConsumerService aggKafkaConsumerService;

  /**
   * A repository mapping each Kafka Topic to it corresponding Ingestion task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final NavigableMap<String, StoreIngestionTask> topicNameToIngestionTaskMap;

  private final Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader;

  private final ExecutorService cacheWarmingExecutorService;

  private final MetaStoreWriter metaStoreWriter;

  private final MetaSystemStoreReplicaStatusNotifier metaSystemStoreReplicaStatusNotifier;

  private final StoreIngestionTaskFactory ingestionTaskFactory;

  private final boolean isIsolatedIngestion;

  private final TopicManagerRepository topicManagerRepository;
  private final TopicManagerRepository topicManagerRepositoryJavaBased;

  private ExecutorService participantStoreConsumerExecutorService;

  private ExecutorService ingestionExecutorService;

  private ParticipantStoreConsumptionTask participantStoreConsumptionTask;

  private boolean metaSystemStoreReplicaStatusNotifierQueued = false;
  // TODO: This could be a composite storage engine which keeps secondary storage engines updated in lockstep with a primary
  // source.  This could be a view of the data, or in our case a cache, or both potentially.
  private Optional<ObjectCacheBackend> cacheBackend = Optional.empty();

  private final SharedKafkaProducerService sharedKafkaProducerService;

  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  private final StorageEngineBackedCompressorFactory compressorFactory;


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
      StorageEngineBackedCompressorFactory compressorFactory) {
    this(storageEngineRepository, veniceConfigLoader, storageMetadataService, clusterInfoProvider, metadataRepo, schemaRepo,
        metricsRepository, rocksDBMemoryStats, kafkaMessageEnvelopeSchemaReader, clientConfig, partitionStateSerializer,
        Optional.empty(), null, false, compressorFactory, Optional.empty());
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
                                    Optional<HelixReadOnlyZKSharedSchemaRepository> zkSharedSchemaRepository,
                                    ICProvider icProvider,
                                    boolean isIsolatedIngestion,
                                    StorageEngineBackedCompressorFactory compressorFactory,
                                    Optional<ObjectCacheBackend> cacheBackend) {
    this.cacheBackend = cacheBackend;
    this.storageMetadataService = storageMetadataService;
    this.metadataRepo = metadataRepo;
    this.topicNameToIngestionTaskMap = new ConcurrentSkipListMap<>();
    this.veniceConfigLoader = veniceConfigLoader;
    this.isIsolatedIngestion = isIsolatedIngestion;
    this.partitionStateSerializer = partitionStateSerializer;
    this.compressorFactory = compressorFactory;

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    VeniceServerConsumerFactory veniceConsumerFactory = new VeniceServerConsumerFactory(serverConfig);

    /**
     * This new veniceConsumerJavaBasedFactory (underneath it works with java based admin client only) is needed for leader_offset_lag metrics to work.
     * TODO: This should be removed once the VeniceServerConsumerFactory uses java based admin client in production reliably.
     */
    VeniceServerConsumerJavaBasedFactory veniceConsumerJavaBasedFactory = new VeniceServerConsumerJavaBasedFactory(serverConfig);

    Properties veniceWriterProperties = veniceConfigLoader.getVeniceClusterConfig().getClusterProperties().toProperties();
    if (serverConfig.isKafkaOpenSSLEnabled()) {
      veniceWriterProperties.setProperty(SSL_CONTEXT_PROVIDER_CLASS_CONFIG, DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME);
    }

    if (serverConfig.isSharedKafkaProducerEnabled()) {
      sharedKafkaProducerService = new SharedKafkaProducerService(veniceWriterProperties, serverConfig.getSharedProducerPoolSizePerKafkaCluster(), new SharedKafkaProducerService.KafkaProducerSupplier() {
        @Override
        public KafkaProducerWrapper getNewProducer(VeniceProperties props) {
          return new ApacheKafkaProducer(props);
        }
      }, metricsRepository, serverConfig.getKafkaProducerMetrics());
      logger.info("Shared kafka producer service is enabled");
    } else {
      sharedKafkaProducerService = null;
      logger.info("Shared kafka producer service is disabled");
    }

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(veniceWriterProperties, Optional.ofNullable(sharedKafkaProducerService));
    VeniceWriterFactory veniceWriterFactoryForMetaStoreWriter = new VeniceWriterFactory(veniceWriterProperties);

    // Create Kafka client stats
    KafkaClientStats.registerKafkaClientStats(metricsRepository, "KafkaClientStats", Optional.ofNullable(sharedKafkaProducerService));

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

    this.topicManagerRepository = new TopicManagerRepository(
        veniceConsumerFactory.getKafkaBootstrapServers(),
        veniceConsumerFactory.getKafkaZkAddress(),
        veniceConsumerFactory, metricsRepository);

    this.topicManagerRepositoryJavaBased = new TopicManagerRepository(
        veniceConsumerFactory.getKafkaBootstrapServers(),
        veniceConsumerFactory.getKafkaZkAddress(),
        veniceConsumerJavaBasedFactory, metricsRepository);

    VeniceNotifier notifier = new LogNotifier();
    this.onlineOfflineNotifiers.add(notifier);
    this.leaderFollowerNotifiers.add(notifier);

    /**
     * Only Venice SN will pass this param: {@param zkSharedSchemaRepository} since {@link metaStoreWriter} is
     * used to report the replica status from Venice SN.
     */
    if (zkSharedSchemaRepository.isPresent()) {
      this.metaStoreWriter = new MetaStoreWriter(topicManagerRepository.getTopicManager(), veniceWriterFactoryForMetaStoreWriter, zkSharedSchemaRepository.get());
      this.metaSystemStoreReplicaStatusNotifier =
          new MetaSystemStoreReplicaStatusNotifier(serverConfig.getClusterName(), metaStoreWriter, metadataRepo,
              Instance.fromHostAndPort(Utils.getHostName(), serverConfig.getListenerPort()));
      logger.info("MetaSystemStoreReplicaStatusNotifier was initialized");
      metadataRepo.registerStoreDataChangedListener(new StoreDataChangedListener() {
        @Override
        public void handleStoreDeleted(Store store) {
          String storeName = store.getName();
          if (VeniceSystemStoreType.META_STORE.equals(VeniceSystemStoreType.getSystemStoreType(storeName))) {
            metaStoreWriter.removeMetaStoreWriter(storeName);
            logger.info("MetaSystemWriter for meta store: " + storeName + " got removed.");
          }
        }
      });
    } else {
      this.metaStoreWriter = null;
      this.metaSystemStoreReplicaStatusNotifier = null;
    }

    this.ingestionStats = new AggStoreIngestionStats(metricsRepository);
    AggVersionedDIVStats versionedDIVStats = new AggVersionedDIVStats(metricsRepository, metadataRepo);
    this.versionedStorageIngestionStats =
        new AggVersionedStorageIngestionStats(metricsRepository, metadataRepo);
    if (serverConfig.isDedicatedDrainerQueueEnabled()) {
      this.storeBufferService = new SeparatedStoreBufferService(serverConfig);
    } else {
      this.storeBufferService = new StoreBufferService(serverConfig.getStoreWriterNumber(),
          serverConfig.getStoreWriterBufferMemoryCapacity(), serverConfig.getStoreWriterBufferNotifyDelta());
    }
    this.kafkaMessageEnvelopeSchemaReader = kafkaMessageEnvelopeSchemaReader;
    /**
     * Collect metrics for {@link #storeBufferService}.
     * Since all the metrics will be collected passively, there is no need to
     * keep the reference of this {@link StoreBufferServiceStats} variable.
     */
    new StoreBufferServiceStats(metricsRepository, this.storeBufferService);

    this.aggLagStats = new AggLagStats(this, metricsRepository);

    if (clientConfig.isPresent()) {
      String clusterName = veniceConfigLoader.getVeniceClusterConfig().getClusterName();
      participantStoreConsumptionTask = new ParticipantStoreConsumptionTask(
          this,
          clusterInfoProvider,
          new ParticipantStoreConsumptionStats(metricsRepository, clusterName),
          ClientConfig.cloneConfig(clientConfig.get()).setMetricsRepository(metricsRepository),
          serverConfig.getParticipantMessageConsumptionDelayMs(), icProvider);
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
          metricsRepository,
          new MetadataRepoBasedTopicExistingCheckerImpl(this.getMetadataRepo()));
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
        .setTopicManagerRepositoryJavaBased(topicManagerRepositoryJavaBased)
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
        .setIsIsolatedIngestion(isIsolatedIngestion)
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
    ingestionExecutorService = Executors.newCachedThreadPool();
    topicNameToIngestionTaskMap.values().forEach(ingestionExecutorService::submit);

    storeBufferService.start();
    if (null != aggKafkaConsumerService) {
      aggKafkaConsumerService.start();
    }
    if (null != sharedKafkaProducerService) {
      sharedKafkaProducerService.start();
    }
    if (participantStoreConsumptionTask != null) {
      participantStoreConsumerExecutorService = Executors.newSingleThreadExecutor();
      participantStoreConsumerExecutorService.submit(participantStoreConsumptionTask);
    }
    // Although the StoreConsumptionTasks are now running in their own threads, there is no async
    // process that needs to finish before the KafkaStoreIngestionService can be considered
    // started, so we are done with the start up process.
    return true;
  }

  private StoreIngestionTask createConsumerTask(VeniceStoreConfig veniceStoreConfig, int partitionId) {
    String storeName = Version.parseStoreFromKafkaTopicName(veniceStoreConfig.getStoreName());
    int versionNumber = Version.parseVersionFromKafkaTopicName(veniceStoreConfig.getStoreName());

    Pair<Store, Version> storeVersionPair = metadataRepo.waitVersion(storeName, versionNumber, Duration.ofSeconds(30));
    Store store = storeVersionPair.getFirst();
    Version version = storeVersionPair.getSecond();
    if (store == null) {
      throw new VeniceException("Store " + storeName + " does not exist.");
    }
    if (version == null) {
      throw new VeniceException("Version " + veniceStoreConfig.getStoreName() + " does not exist.");
    }
    int amplificationFactor;
    VenicePartitioner partitioner;
    PartitionerConfig partitionerConfig = version.getPartitionerConfig();
    if (partitionerConfig == null) {
      partitioner = new DefaultVenicePartitioner();
      amplificationFactor = 1;
    } else {
      partitioner = PartitionUtils.getVenicePartitioner(partitionerConfig);
      amplificationFactor = partitionerConfig.getAmplificationFactor();
    }

    BooleanSupplier isVersionCurrent = () -> {
      try {
        return versionNumber == metadataRepo.getStoreOrThrow(storeName).getCurrentVersion();
      } catch (VeniceNoStoreException e) {
        logger.warn("Unable to find store meta-data for " + veniceStoreConfig.getStoreName(), e);
        return false;
      }
    };

    return ingestionTaskFactory.getNewIngestionTask(
        version.isLeaderFollowerModelEnabled(),
        getKafkaConsumerProperties(veniceStoreConfig),
        isVersionCurrent,
        Optional.ofNullable(version.isUseVersionLevelHybridConfig() ? version.getHybridStoreConfig() : store.getHybridStoreConfig()),
        version.isUseVersionLevelIncrementalPushEnabled() ? version.isIncrementalPushEnabled() : store.isIncrementalPushEnabled(),
        version.getIncrementalPushPolicy(),
        veniceStoreConfig,
        version.isNativeReplicationEnabled(),
        version.getPushStreamSourceAddress(),
        partitionId,
        store.isWriteComputationEnabled(),
        partitioner,
        version.getPartitionCount(),
        isIsolatedIngestion,
        amplificationFactor,
        compressorFactory,
        cacheBackend);
  }

  private static void shutdownExecutorService(ExecutorService executorService, boolean force, String name) {
    if (null == executorService) {
      return;
    }
    if (force) {
      executorService.shutdownNow();
    } else {
      executorService.shutdown();
    }
    try {
      long startTimeMs = System.currentTimeMillis();
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS) && !force) {
        logger.error("Couldn't stop the thread pool: " + name + " after " + LatencyUtils.getElapsedTimeInMs(startTimeMs) + "(ms)");
        executorService.shutdownNow();
        if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
          logger.error("Still couldn't stop the thread pool: " + name + " after " + LatencyUtils.getElapsedTimeInMs(startTimeMs) + "(ms)");
        }
      }
    } catch (InterruptedException e) {
      logger.warn("Received interruptedException in stopping thread pool: " + name);
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() throws Exception {
    Utils.closeQuietlyWithErrorLogged(participantStoreConsumptionTask);
    shutdownExecutorService(participantStoreConsumerExecutorService, true, "participantStoreConsumerExecutorService");

    /*
     * We would like to gracefully shutdown {@link #ingestionExecutorService},
     * so that it will have an opportunity to checkpoint the processed offset.
     */
    topicNameToIngestionTaskMap.values().forEach(StoreIngestionTask::close);
    shutdownExecutorService(ingestionExecutorService, false, "ingestionExecutorService");
    shutdownExecutorService(cacheWarmingExecutorService, true, "cacheWarmingExecutorService");

    Utils.closeQuietlyWithErrorLogged(aggKafkaConsumerService);

    onlineOfflineNotifiers.forEach(VeniceNotifier::close);
    leaderFollowerNotifiers.forEach(VeniceNotifier::close);
    Utils.closeQuietlyWithErrorLogged(metaStoreWriter);

    kafkaMessageEnvelopeSchemaReader.ifPresent(SchemaReader::close);

    //close it the very end to make sure all ingestion task have released the shared producers.
    Utils.closeQuietlyWithErrorLogged(sharedKafkaProducerService);

    //close drainer service at the very end as it does not depend on any other service.
    Utils.closeQuietlyWithErrorLogged(storeBufferService);
    Utils.closeQuietlyWithErrorLogged(topicManagerRepository);
    Utils.closeQuietlyWithErrorLogged(topicManagerRepositoryJavaBased);
  }

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * Subscribes to partition if required.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   * @param leaderState
   */
  @Override
  public synchronized void startConsumption(VeniceStoreConfig veniceStore, int partitionId,
      Optional<LeaderFollowerStateType> leaderState) {
    String topic = veniceStore.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask == null || !consumerTask.isRunning()) {
      consumerTask = createConsumerTask(veniceStore, partitionId);
      topicNameToIngestionTaskMap.put(topic, consumerTask);
      versionedStorageIngestionStats.setIngestionTask(topic, consumerTask);
      if (!isRunning()) {
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

    consumerTask.subscribePartition(topic, partitionId, leaderState);
    logger.info("Started Consuming - Kafka Partition: " + topic + "-" + partitionId + ".");
  }

  @Override
  public synchronized void promoteToLeader(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    String topic = veniceStoreConfig.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      consumerTask.promoteToLeader(topic, partitionId, checker);
    } else {
      logger.warn("Ignoring standby to leader transition message for Topic " + topic + " Partition " + partitionId);
    }
  }

  @Override
  public synchronized void demoteToStandby(VeniceStoreConfig veniceStoreConfig, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    String topic = veniceStoreConfig.getStoreName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
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
    int maxVersionNumber = metadataRepo.getStoreOrThrow(storeName).getLargestUsedVersionNumber();
    if (maxVersionNumber == Store.NON_EXISTING_VERSION) {
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
    if (consumerTask != null && consumerTask.isRunning()) {
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
  public void stopConsumptionAndWait(VeniceStoreConfig veniceStore, int partitionId, int sleepSeconds, int numRetries) {
    stopConsumption(veniceStore, partitionId);
    try {
      for (int i = 0; i < numRetries; i++) {
        if (!isPartitionConsuming(veniceStore, partitionId)) {
          logger.info("Partition: " + partitionId + " of store: " + veniceStore.getStoreName() + " has stopped consumption.");
          return;
        }
        sleep((long) sleepSeconds * Time.MS_PER_SECOND);
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
    if (consumerTask != null && consumerTask.isRunning()) {
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
  public boolean killConsumptionTask(String topicName) {
    if (!isRunning()) {
      throw new VeniceException("KafkaStoreIngestionService is not running.");
    }

    synchronized (this) {
      StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topicName);
      if (consumerTask == null) {
        logger.info("Ignoring kill request for not-existing consumption task " + topicName);
        return false;
      }

      boolean killed = false;
      if (consumerTask.isRunning()) {
        consumerTask.kill();
        compressorFactory.removeVersionSpecificCompressor(topicName);
        killed = true;
        logger.info("Killed consumption task for topic " + topicName);
      } else {
        logger.warn("Ignoring kill request for stopped consumption task " + topicName);
      }
      // cleanup the map regardless if the task was running or not to prevent mem leak when failed tasks lingers
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
      return killed;
    }
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
    return consumerTask != null && consumerTask.isRunning();
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
      try {
        Store store = metadataRepo.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(topic));
        int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
        if (store == null || !store.getVersion(versionNumber).isPresent() || store.getVersion(versionNumber).get().getStatus() != VersionStatus.ONLINE) {
          result.add(topic);
        }
      } catch (VeniceNoStoreException e) {
        // Include topics that may have their corresponding store deleted already
        result.add(topic);
      } catch (Exception e) {
        logger.error("Unexpected exception while fetching ongoing ingestion topics, topic: " + topic, e);
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

  @Override
  public AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions) {
    AdminResponse response = new AdminResponse();
    StoreIngestionTask ingestionTask = getStoreIngestionTask(topicName);
    if (ingestionTask != null) {
      ingestionTask.dumpPartitionConsumptionStates(response, partitions);
      ingestionTask.dumpStoreVersionState(response);
    } else {
      String msg = "Ingestion task for " + topicName + " doesn't exist for " + ServerAdminAction.DUMP_INGESTION_STATE + " admin command";
      logger.warn(msg);
      response.setMessage(msg);
    }
    return response;
  }

  @Override
  public void traverseAllIngestionTasksAndApply(Consumer<StoreIngestionTask> consumer) {
    topicNameToIngestionTaskMap.values().forEach(consumer);
  }

  public AggLagStats getAggLagStats() {
    return aggLagStats;
  }

  public LeaderFollowerStateType getLeaderStateFromPartitionConsumptionState(String topicName, int partitionId) {
    return getStoreIngestionTask(topicName).getLeaderState(partitionId);
  }

  /**
   * updatePartitionOffsetRecords updates all sub-partitions latest offset records fetched from isolated ingestion process
   * in main process, so main process's in-memory storage metadata service could be aware of the latest updates and will
   * not re-start the ingestion from scratch.
   */
  public void updatePartitionOffsetRecords(String topicName, int partition, List<ByteBuffer> offsetRecordArray) {
    int amplificationFactor = PartitionUtils.getAmplificationFactor(metadataRepo, topicName);
    int offset = amplificationFactor * partition;
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      OffsetRecord offsetRecord = new OffsetRecord(offsetRecordArray.get(subPartition - offset).array(), partitionStateSerializer);
      storageMetadataService.put(topicName, subPartition, offsetRecord);
    }
  }

  /**
   * This method should only be called when the forked ingestion process is handing over ingestion task to main process.
   * It will collect the user partition's latest offsetRecords from partition consumption states.
   * In theory, PCS should be available in this situation as we haven't unsubscribe from topic. If it is not available,
   * we will throw exception as this is not as expected.
   */
  public List<ByteBuffer> getPartitionOffsetRecords(String topicName, int partition) {
    int amplificationFactor = PartitionUtils.getAmplificationFactor(metadataRepo, topicName);
    List<ByteBuffer> offsetRecordArray = new ArrayList<>();
    for (int subPartition : PartitionUtils.getSubPartitions(partition, amplificationFactor)) {
      if (getStoreIngestionTask(topicName) != null && getStoreIngestionTask(topicName).getPartitionConsumptionState(subPartition).isPresent()) {
        OffsetRecord offsetRecord = getStoreIngestionTask(topicName).getPartitionConsumptionState(subPartition).get().getOffsetRecord();
        offsetRecordArray.add(ByteBuffer.wrap(offsetRecord.toBytes()));
      } else {
        throw new VeniceException("StoreIngestionTask or PartitionConsumptionState does not exist for topic: " + topicName + ", partition: " + subPartition);
      }
    }
    return offsetRecordArray;
  }

  public ReadOnlyStoreRepository getMetadataRepo() {
    return metadataRepo;
  }
}
