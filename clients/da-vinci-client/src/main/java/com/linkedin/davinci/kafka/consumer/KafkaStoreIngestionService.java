package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLIENT_ID_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MAX_BYTES_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MAX_WAIT_MS_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MIN_BYTES_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_GROUP_ID_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG;
import static com.linkedin.venice.ConfigKeys.KAFKA_MAX_POLL_RECORDS_CONFIG;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.ReplicaIngestionResponse;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.PushStatusNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.ParticipantStoreConsumptionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.view.VeniceViewWriterFactory;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.system.store.ControllerClientBackedSystemSchemaInitializer;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ResourceAutoClosableLockManager;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import org.apache.avro.Schema;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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

  private static final Logger LOGGER = LogManager.getLogger(KafkaStoreIngestionService.class);
  private final StorageService storageService;

  private final VeniceConfigLoader veniceConfigLoader;

  private final Queue<VeniceNotifier> leaderFollowerNotifiers = new ConcurrentLinkedQueue<>();

  private final StorageMetadataService storageMetadataService;

  private final ReadOnlyStoreRepository metadataRepo;
  private final AggHostLevelIngestionStats hostLevelIngestionStats;
  private final AggVersionedIngestionStats versionedIngestionStats;

  /**
   * Store buffer service to persist data into local bdb for all the stores.
   */
  private final AbstractStoreBufferService storeBufferService;

  private final AggKafkaConsumerService aggKafkaConsumerService;

  /**
   * A repository mapping each Version Topic to it corresponding Ingestion task responsible
   * for consuming messages and making changes to the local store accordingly.
   */
  private final NavigableMap<String, StoreIngestionTask> topicNameToIngestionTaskMap;

  private final Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader;

  private final MetaStoreWriter metaStoreWriter;

  private final StoreIngestionTaskFactory ingestionTaskFactory;

  private final boolean isIsolatedIngestion;

  private final TopicManagerRepository topicManagerRepository;
  private ExecutorService participantStoreConsumerExecutorService;

  private ExecutorService ingestionExecutorService;

  private ParticipantStoreConsumptionTask participantStoreConsumptionTask;

  // TODO: This could be a composite storage engine which keeps secondary storage engines updated in lockstep with a
  // primary
  // source. This could be a view of the data, or in our case a cache, or both potentially.
  private final Optional<ObjectCacheBackend> cacheBackend;

  private final DaVinciRecordTransformerConfig recordTransformerConfig;

  private final PubSubProducerAdapterFactory producerAdapterFactory;

  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  private final StorageEngineBackedCompressorFactory compressorFactory;

  private final ResourceAutoClosableLockManager<String> topicLockManager;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final KafkaValueSerializer kafkaValueSerializer;
  private final IngestionThrottler ingestionThrottler;
  private final ExecutorService aaWCWorkLoadProcessingThreadPool;
  private final AdaptiveThrottlerSignalService adaptiveThrottlerSignalService;

  private final VeniceServerConfig serverConfig;

  private final Lazy<ZKHelixAdmin> zkHelixAdmin;

  private final ExecutorService aaWCIngestionStorageLookupThreadPool;

  private final ScheduledExecutorService idleStoreIngestionTaskKillerExecutor;

  private final VeniceWriterFactory veniceWriterFactory;

  public KafkaStoreIngestionService(
      StorageService storageService,
      VeniceConfigLoader veniceConfigLoader,
      StorageMetadataService storageMetadataService,
      ClusterInfoProvider clusterInfoProvider,
      ReadOnlyStoreRepository metadataRepo,
      ReadOnlySchemaRepository schemaRepo,
      ReadOnlyLiveClusterConfigRepository liveClusterConfigRepository,
      MetricsRepository metricsRepository,
      Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader,
      Optional<ClientConfig> clientConfig,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      Optional<HelixReadOnlyZKSharedSchemaRepository> zkSharedSchemaRepository,
      ICProvider icProvider,
      boolean isIsolatedIngestion,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ObjectCacheBackend> cacheBackend,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      boolean isDaVinciClient,
      RemoteIngestionRepairService remoteIngestionRepairService,
      PubSubClientsFactory pubSubClientsFactory,
      Optional<SSLFactory> sslFactory,
      HeartbeatMonitoringService heartbeatMonitoringService,
      Lazy<ZKHelixAdmin> zkHelixAdmin,
      AdaptiveThrottlerSignalService adaptiveThrottlerSignalService) {
    this.storageService = storageService;
    this.cacheBackend = cacheBackend;
    this.recordTransformerConfig = recordTransformerConfig;
    this.storageMetadataService = storageMetadataService;
    this.metadataRepo = metadataRepo;
    this.topicNameToIngestionTaskMap = new ConcurrentSkipListMap<>();
    this.veniceConfigLoader = veniceConfigLoader;
    this.isIsolatedIngestion = isIsolatedIngestion;
    this.partitionStateSerializer = partitionStateSerializer;
    this.compressorFactory = compressorFactory;
    this.zkHelixAdmin = zkHelixAdmin;
    // Each topic that has any partition ingested by this class has its own lock.
    this.topicLockManager = new ResourceAutoClosableLockManager<>(ReentrantLock::new);
    this.serverConfig = veniceConfigLoader.getVeniceServerConfig();
    Properties veniceWriterProperties =
        veniceConfigLoader.getVeniceClusterConfig().getClusterProperties().toProperties();

    veniceWriterProperties.put(PubSubConstants.PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS, "true");
    producerAdapterFactory = pubSubClientsFactory.getProducerAdapterFactory();
    this.veniceWriterFactory = new VeniceWriterFactory(
        veniceWriterProperties,
        producerAdapterFactory,
        metricsRepository,
        serverConfig.getPubSubPositionTypeRegistry());
    this.adaptiveThrottlerSignalService = adaptiveThrottlerSignalService;
    this.ingestionThrottler = new IngestionThrottler(
        isDaVinciClient,
        serverConfig,
        () -> Collections.unmodifiableMap(topicNameToIngestionTaskMap),
        adaptiveThrottlerSignalService);

    final Map<String, EventThrottler> kafkaUrlToRecordsThrottler;
    if (liveClusterConfigRepository != null) {
      Set<String> regionNames = serverConfig.getRegionNames();
      kafkaUrlToRecordsThrottler = new HashMap<>(regionNames.size());
      regionNames.forEach(region -> {
        kafkaUrlToRecordsThrottler.put(
            region,
            new EventThrottler(
                () -> (long) liveClusterConfigRepository.getConfigs()
                    .getServerKafkaFetchQuotaRecordsPerSecondForRegion(region),
                serverConfig.getKafkaFetchQuotaTimeWindow(),
                "kafka_consumption_records_count_" + region,
                true, // Check quota before recording since we buffer throttled records and don't send them to disk or
                      // kafka
                EventThrottler.REJECT_STRATEGY) // We want exceptions to be thrown when quota is exceeded
        );
      });
    } else {
      kafkaUrlToRecordsThrottler = Collections.emptyMap();
    }

    KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler =
        new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubTopicRepository(pubSubTopicRepository)
            .setMetricsRepository(metricsRepository)
            .setTopicOffsetCheckIntervalMs(serverConfig.getTopicOffsetCheckIntervalMs())
            .setPubSubPropertiesSupplier(this::getPubSubSSLPropertiesFromServerConfig)
            .setPubSubPositionTypeRegistry(serverConfig.getPubSubPositionTypeRegistry())
            .setPubSubAdminAdapterFactory(pubSubClientsFactory.getAdminAdapterFactory())
            .setPubSubConsumerAdapterFactory(pubSubClientsFactory.getConsumerAdapterFactory())
            .setTopicMetadataFetcherThreadPoolSize(serverConfig.getTopicManagerMetadataFetcherThreadPoolSize())
            .setTopicMetadataFetcherConsumerPoolSize(serverConfig.getTopicManagerMetadataFetcherConsumerPoolSize())
            .setVeniceComponent(VeniceComponent.SERVER)
            .build();
    this.topicManagerRepository =
        new TopicManagerRepository(topicManagerContext, serverConfig.getKafkaBootstrapServers());

    VeniceNotifier notifier = new LogNotifier();
    this.leaderFollowerNotifiers.add(notifier);

    /**
     * Only Venice SN will pass this param: {@param zkSharedSchemaRepository} since {@link metaStoreWriter} is
     * used to report the replica status from Venice SN.
     */
    if (zkSharedSchemaRepository.isPresent()) {
      this.metaStoreWriter = new MetaStoreWriter(
          topicManagerRepository.getLocalTopicManager(),
          veniceWriterFactory,
          zkSharedSchemaRepository.get(),
          pubSubTopicRepository,
          serverConfig.getMetaStoreWriterCloseTimeoutInMS(),
          serverConfig.getMetaStoreWriterCloseConcurrency());
      metadataRepo.registerStoreDataChangedListener(new StoreDataChangedListener() {
        @Override
        public void handleStoreDeleted(Store store) {
          String storeName = store.getName();
          if (VeniceSystemStoreType.META_STORE.equals(VeniceSystemStoreType.getSystemStoreType(storeName))) {
            metaStoreWriter.removeMetaStoreWriter(storeName);
            LOGGER.info("MetaSystemWriter for meta store: {} got removed.", storeName);
          }
        }
      });
    } else {
      this.metaStoreWriter = null;
    }

    this.hostLevelIngestionStats = new AggHostLevelIngestionStats(
        metricsRepository,
        serverConfig,
        topicNameToIngestionTaskMap,
        metadataRepo,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled(),
        SystemTime.INSTANCE);
    AggVersionedDIVStats versionedDIVStats = new AggVersionedDIVStats(
        metricsRepository,
        metadataRepo,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
    this.versionedIngestionStats = new AggVersionedIngestionStats(metricsRepository, metadataRepo, serverConfig);
    if (serverConfig.isDedicatedDrainerQueueEnabled()) {
      this.storeBufferService = new SeparatedStoreBufferService(serverConfig, metricsRepository);
    } else {
      this.storeBufferService = new StoreBufferService(
          serverConfig.getStoreWriterNumber(),
          serverConfig.getStoreWriterBufferMemoryCapacity(),
          serverConfig.getStoreWriterBufferNotifyDelta(),
          serverConfig.isStoreWriterBufferAfterLeaderLogicEnabled(),
          serverConfig.getRegionName(),
          metricsRepository,
          true);
    }
    this.kafkaMessageEnvelopeSchemaReader = kafkaMessageEnvelopeSchemaReader;

    this.participantStoreConsumptionTask = initializeParticipantStoreConsumptionTask(
        serverConfig,
        clientConfig,
        clusterInfoProvider,
        metricsRepository,
        icProvider);

    /**
     * Register a callback function to handle the case when a new KME value schema is encountered when the server
     * consumes messages from Kafka.
     */
    BiConsumer<Integer, Schema> newSchemaEncountered = (schemaId, schema) -> {
      LOGGER.info("Encountered a new KME value schema (id = {}), proceed to register", schemaId);
      try (ControllerClientBackedSystemSchemaInitializer schemaInitializer =
          new ControllerClientBackedSystemSchemaInitializer(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE,
              serverConfig.getSystemSchemaClusterName(),
              null,
              null,
              false,
              sslFactory,
              serverConfig.getLocalControllerUrl(),
              serverConfig.getLocalControllerD2ServiceName(),
              serverConfig.getLocalD2ZkHost(),
              false)) {
        schemaInitializer.execute(Collections.singletonMap(schemaId, schema));
      } catch (VeniceException e) {
        LOGGER.error(
            "Exception in registering '{}' schema version '{}'",
            AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.name(),
            schemaId,
            e);
        throw e;
      }
    };

    // Don't apply newSchemaEncountered callbacks for da vinci client.
    kafkaValueSerializer = (!isDaVinciClient && serverConfig.isKMERegistrationFromMessageHeaderEnabled())
        ? new OptimizedKafkaValueSerializer(newSchemaEncountered)
        : new OptimizedKafkaValueSerializer();

    kafkaMessageEnvelopeSchemaReader.ifPresent(kafkaValueSerializer::setSchemaReader);
    PubSubMessageDeserializer pubSubDeserializer = new PubSubMessageDeserializer(
        kafkaValueSerializer,
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));

    aggKafkaConsumerService = new AggKafkaConsumerService(
        pubSubClientsFactory.getConsumerAdapterFactory(),
        this::getPubSubSSLPropertiesFromServerConfig,
        serverConfig,
        ingestionThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        new MetadataRepoBasedStaleTopicCheckerImpl(this.getMetadataRepo()),
        pubSubDeserializer,
        (topicName) -> this.killConsumptionTask(topicName),
        vt -> {
          String storeName = Version.parseStoreFromKafkaTopicName(vt);
          int versionNumber = Version.parseVersionFromKafkaTopicName(vt);
          Store store = metadataRepo.getStore(storeName);
          if (null == store) {
            return false;
          }
          Version version = store.getVersion(versionNumber);
          if (version == null) {
            return false;
          }
          return version.isActiveActiveReplicationEnabled() || store.isWriteComputationEnabled();
        },
        metadataRepo);
    /**
     * After initializing a {@link AggKafkaConsumerService} service, it doesn't contain KafkaConsumerService yet until
     * a new Kafka cluster is registered; here we explicitly create KafkaConsumerService for the local Kafka cluster.
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
    aggKafkaConsumerService.createKafkaConsumerService(commonKafkaConsumerConfigs);

    /**
     * Use the same diskUsage instance for all ingestion tasks; so that all the ingestion tasks can update the same
     * remaining disk space state to provide a more accurate alert.
     */
    DiskUsage diskUsage = new DiskUsage(serverConfig.getDataBasePath(), serverConfig.getDiskFullThreshold());

    VeniceViewWriterFactory viewWriterFactory = new VeniceViewWriterFactory(veniceConfigLoader, veniceWriterFactory);

    if (serverConfig.isAAWCWorkloadParallelProcessingEnabled()) {
      this.aaWCWorkLoadProcessingThreadPool = Executors.newFixedThreadPool(
          serverConfig.getAAWCWorkloadParallelProcessingThreadPoolSize(),
          new DaemonThreadFactory("AA_WC_PARALLEL_PROCESSING", serverConfig.getRegionName()));
    } else {
      this.aaWCWorkLoadProcessingThreadPool = null;
    }

    this.idleStoreIngestionTaskKillerExecutor = serverConfig.getIdleIngestionTaskCleanupIntervalInSeconds() > 0
        ? Executors.newScheduledThreadPool(
            1,
            new DaemonThreadFactory("idle-store-ingestion-task-clean-up-thread", serverConfig.getRegionName()))
        : null;

    this.aaWCIngestionStorageLookupThreadPool = Executors.newFixedThreadPool(
        serverConfig.getAaWCIngestionStorageLookupThreadPoolSize(),
        new DaemonThreadFactory("AA_WC_INGESTION_STORAGE_LOOKUP", serverConfig.getRegionName()));
    LOGGER.info(
        "Enabled a thread pool for AA/WC ingestion lookup with {} threads.",
        serverConfig.getAaWCIngestionStorageLookupThreadPoolSize());

    AggVersionedDaVinciRecordTransformerStats recordTransformerStats = null;
    if (recordTransformerConfig != null) {
      recordTransformerStats =
          new AggVersionedDaVinciRecordTransformerStats(metricsRepository, metadataRepo, serverConfig);
    }

    ingestionTaskFactory = StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(veniceWriterFactory)
        .setStorageMetadataService(storageMetadataService)
        .setLeaderFollowerNotifiersQueue(leaderFollowerNotifiers)
        .setSchemaRepository(schemaRepo)
        .setMetadataRepository(metadataRepo)
        .setTopicManagerRepository(topicManagerRepository)
        .setHostLevelIngestionStats(hostLevelIngestionStats)
        .setVersionedDIVStats(versionedDIVStats)
        .setDaVinciRecordTransformerStats(recordTransformerStats)
        .setVersionedIngestionStats(versionedIngestionStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(serverConfig)
        .setDiskUsage(diskUsage)
        .setAggKafkaConsumerService(aggKafkaConsumerService)
        .setPartitionStateSerializer(partitionStateSerializer)
        .setIsDaVinciClient(isDaVinciClient)
        .setRemoteIngestionRepairService(remoteIngestionRepairService)
        .setMetaStoreWriter(metaStoreWriter)
        .setCompressorFactory(compressorFactory)
        .setVeniceViewWriterFactory(viewWriterFactory)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setRunnableForKillIngestionTasksForNonCurrentVersions(
            serverConfig.getIngestionMemoryLimit() > 0 ? () -> killConsumptionTaskForNonCurrentVersions() : null)
        .setHeartbeatMonitoringService(heartbeatMonitoringService)
        .setAAWCWorkLoadProcessingThreadPool(aaWCWorkLoadProcessingThreadPool)
        .setAAWCIngestionStorageLookupThreadPool(aaWCIngestionStorageLookupThreadPool)
        .build();
  }

  @VisibleForTesting
  ParticipantStoreConsumptionTask initializeParticipantStoreConsumptionTask(
      VeniceServerConfig serverConfig,
      Optional<ClientConfig> clientConfig,
      ClusterInfoProvider clusterInfoProvider,
      MetricsRepository metricsRepository,
      ICProvider icProvider) {

    if (!serverConfig.isParticipantMessageStoreEnabled() || !clientConfig.isPresent()) {
      LOGGER.warn(
          "Unable to start participant store consumption task because {}. Jobs may not be killed if the "
              + "admin Helix messaging channel is disabled.",
          clientConfig.isPresent() ? "participant message store is disabled" : "client config is missing");
      return null;
    }

    return new ParticipantStoreConsumptionTask(
        this,
        clusterInfoProvider,
        new ParticipantStoreConsumptionStats(metricsRepository, serverConfig.getClusterName()),
        ClientConfig.cloneConfig(clientConfig.get()).setMetricsRepository(metricsRepository),
        serverConfig.getParticipantMessageConsumptionDelayMs(),
        icProvider);
  }

  /**
   * Starts the Kafka consumption tasks for already subscribed partitions.
   */
  @Override
  public boolean startInner() {
    ingestionExecutorService = Executors.newCachedThreadPool();
    topicNameToIngestionTaskMap.values().forEach(ingestionExecutorService::submit);

    storeBufferService.start();
    if (aggKafkaConsumerService != null) {
      aggKafkaConsumerService.start();
    }
    if (participantStoreConsumptionTask != null) {
      participantStoreConsumerExecutorService = Executors.newSingleThreadExecutor(
          new DaemonThreadFactory("ParticipantStoreConsumptionTask", serverConfig.getRegionName()));
      participantStoreConsumerExecutorService.submit(participantStoreConsumptionTask);
    }
    final int idleIngestionTaskCleanupIntervalInSeconds = serverConfig.getIdleIngestionTaskCleanupIntervalInSeconds();
    if (idleStoreIngestionTaskKillerExecutor != null) {
      this.idleStoreIngestionTaskKillerExecutor.scheduleWithFixedDelay(
          this::scanAndCloseIdleConsumptionTasks,
          idleIngestionTaskCleanupIntervalInSeconds,
          idleIngestionTaskCleanupIntervalInSeconds,
          TimeUnit.SECONDS);
    }
    // Although the StoreConsumptionTasks are now running in their own threads, there is no async
    // process that needs to finish before the KafkaStoreIngestionService can be considered
    // started, so we are done with the start up process.
    return true;
  }

  private StoreIngestionTask createStoreIngestionTask(
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      int partitionId) {
    String storeName = Version.parseStoreFromKafkaTopicName(veniceStoreVersionConfig.getStoreVersionName());
    int versionNumber = Version.parseVersionFromKafkaTopicName(veniceStoreVersionConfig.getStoreVersionName());

    StoreVersionInfo storeVersionPair =
        Utils.waitStoreVersionOrThrow(veniceStoreVersionConfig.getStoreVersionName(), metadataRepo);
    Store store = storeVersionPair.getStore();
    Version version = storeVersionPair.getVersion();

    BooleanSupplier isVersionCurrent = () -> {
      try {
        return versionNumber == metadataRepo.getStoreOrThrow(storeName).getCurrentVersion();
      } catch (VeniceNoStoreException e) {
        LOGGER.warn("Unable to find store meta-data for {}", veniceStoreVersionConfig.getStoreVersionName(), e);
        return false;
      }
    };

    return ingestionTaskFactory.getNewIngestionTask(
        storageService,
        store,
        version,
        getKafkaConsumerProperties(veniceStoreVersionConfig),
        isVersionCurrent,
        veniceStoreVersionConfig,
        partitionId,
        isIsolatedIngestion,
        cacheBackend,
        recordTransformerConfig,
        zkHelixAdmin);
  }

  private static void shutdownExecutorService(ExecutorService executor, String name, boolean force) {
    if (executor == null) {
      return;
    }

    long startTime = System.currentTimeMillis();
    try {
      executor.shutdown();
      if (force || !executor.awaitTermination(60, TimeUnit.SECONDS)) {
        if (!force) {
          LOGGER.warn("Failed to gracefully shutdown executor {}", name);
        }
        executor.shutdownNow();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
          LOGGER.error("Failed to shutdown executor {}", name);
        }
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Executor shutdown is interrupted");
      executor.shutdownNow();
      currentThread().interrupt();
    } finally {
      LOGGER.info("{} shutdown took {} ms.", name, System.currentTimeMillis() - startTime);
    }
  }

  public boolean hasCurrentVersionBootstrapping() {
    return hasCurrentVersionBootstrapping(topicNameToIngestionTaskMap);
  }

  public static boolean hasCurrentVersionBootstrapping(Map<String, StoreIngestionTask> ingestionTaskMap) {
    for (Map.Entry<String, StoreIngestionTask> entry: ingestionTaskMap.entrySet()) {
      StoreIngestionTask task = entry.getValue();
      if (task.isCurrentVersion() && !task.hasAllPartitionReportedCompleted()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Stops all the Kafka consumption tasks.
   * Closes all the Kafka clients.
   */
  @Override
  public void stopInner() {
    Utils.closeQuietlyWithErrorLogged(ingestionThrottler);
    Utils.closeQuietlyWithErrorLogged(participantStoreConsumptionTask);
    shutdownExecutorService(participantStoreConsumerExecutorService, "participantStoreConsumerExecutorService", true);
    shutdownExecutorService(idleStoreIngestionTaskKillerExecutor, "idleStoreIngestionTaskKillerExecutor", true);
    /*
     * We would like to gracefully shutdown {@link #ingestionExecutorService},
     * so that it will have an opportunity to checkpoint the processed offset.
     */
    topicNameToIngestionTaskMap.values().forEach(StoreIngestionTask::close);
    shutdownExecutorService(ingestionExecutorService, "ingestionExecutorService", false);

    Utils.closeQuietlyWithErrorLogged(aggKafkaConsumerService);

    leaderFollowerNotifiers.forEach(VeniceNotifier::close);
    Utils.closeQuietlyWithErrorLogged(metaStoreWriter);

    shutdownExecutorService(aaWCWorkLoadProcessingThreadPool, "aaWCWorkLoadProcessingThreadPool", true);
    shutdownExecutorService(aaWCIngestionStorageLookupThreadPool, "aaWCIngestionStorageLookupThreadPool", true);

    kafkaMessageEnvelopeSchemaReader.ifPresent(Utils::closeQuietlyWithErrorLogged);

    // close it the very end to make sure all ingestion task have released the shared producers.
    Utils.closeQuietlyWithErrorLogged(producerAdapterFactory);

    // close drainer service at the very end as it does not depend on any other service.
    Utils.closeQuietlyWithErrorLogged(storeBufferService);
    Utils.closeQuietlyWithErrorLogged(topicManagerRepository);
    topicLockManager.removeAllLocks();
  }

  /**
   * Starts consuming messages from Kafka Partition corresponding to Venice Partition.
   * Subscribes to partition if required.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  @Override
  public void startConsumption(VeniceStoreVersionConfig veniceStore, int partitionId) {

    final String topic = veniceStore.getStoreVersionName();

    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      // Create new store ingestion task atomically.
      AtomicBoolean createNewStoreIngestionTask = new AtomicBoolean(false);
      StoreIngestionTask storeIngestionTask = topicNameToIngestionTaskMap.compute(topic, (k, v) -> {
        if ((v == null) || (!v.isIngestionTaskActive())) {
          createNewStoreIngestionTask.set(true);
          return createStoreIngestionTask(veniceStore, partitionId);
        }
        return v;
      });
      if (createNewStoreIngestionTask.get()) {
        versionedIngestionStats.setIngestionTask(topic, storeIngestionTask);
        if (!isRunning()) {
          LOGGER.info(
              "Ignore start consumption for topic: {}, partition: {} as service is stopping.",
              topic,
              partitionId);
          return;
        }
        ingestionExecutorService.submit(storeIngestionTask);
      }

      /**
       * Since Venice metric is store-level, and it would have multiply topics tasks exist in the same time.
       * Only the task with the largest version would emit it stats. That being said, relying on the {@link #metadataRepo}
       * to get the max version may be unreliable, since the information in this object is not guaranteed
       * to be up-to-date. As a sanity check, we will also look at the version in the topic name, and
       * pick whichever number is highest as the max version number.
       */
      String storeName = Version.parseStoreFromKafkaTopicName(topic);
      int maxVersionNumberFromTopicName = Version.parseVersionFromKafkaTopicName(topic);
      int maxVersionNumberFromMetadataRepo = getStoreMaximumVersionNumber(storeName);
      if (maxVersionNumberFromTopicName > maxVersionNumberFromMetadataRepo) {
        LOGGER.warn(
            "Got stale info from metadataRepo. maxVersionNumberFromTopicName: {}, maxVersionNumberFromMetadataRepo: {}. "
                + "Will rely on the topic name's version.",
            maxVersionNumberFromTopicName,
            maxVersionNumberFromMetadataRepo);
      }
      /**
       * Notice that the version push for maxVersionNumberFromMetadataRepo might be killed already (this code path will
       * also be triggered after server restarts).
       */
      int maxVersionNumber = Math.max(maxVersionNumberFromMetadataRepo, maxVersionNumberFromTopicName);
      updateStatsEmission(topicNameToIngestionTaskMap, storeName, maxVersionNumber);

      storeIngestionTask
          .subscribePartition(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId));
    }
    LOGGER.info("Started Consuming - Kafka Partition: {}-{}.", topic, partitionId);
  }

  /**
   * This method closes the specified {@link StoreIngestionTask} and wait for up to 10 seconds for fully shutdown.
   * @param topicName Topic name of the ingestion task to be shutdown.
   */
  public void shutdownStoreIngestionTask(String topicName) {
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topicName)) {
      StoreIngestionTask storeIngestionTask = topicNameToIngestionTaskMap.remove(topicName);
      if (storeIngestionTask != null) {
        storeIngestionTask.shutdownAndWait(30);
        LOGGER.info("Successfully shut down ingestion task for {}", topicName);
      } else {
        LOGGER.info("Ignoring close request for not-existing consumption task {}", topicName);
      }
    }
    topicLockManager.removeLockForResource(topicName);
  }

  @Override
  public void promoteToLeader(
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      int partitionId,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    final String topic = veniceStoreVersionConfig.getStoreVersionName();
    LOGGER.info("Promoting partition: {} of topic: {} to leader.", partitionId, topic);
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
      if (consumerTask != null && consumerTask.isRunning()) {
        consumerTask
            .promoteToLeader(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId), checker);
      } else {
        LOGGER.warn("Ignoring standby to leader transition message for Topic {} Partition {}", topic, partitionId);
      }
    }
  }

  @Override
  public void demoteToStandby(
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      int partitionId,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker) {
    final String topic = veniceStoreVersionConfig.getStoreVersionName();
    LOGGER.info("Demoting partition: {} of topic: {} to standby.", partitionId, topic);
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
      if (consumerTask != null && consumerTask.isRunning()) {
        consumerTask
            .demoteToStandby(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId), checker);
      } else {
        LOGGER.warn("Ignoring leader to standby transition message for Topic {} Partition {}", topic, partitionId);
      }
    }
  }

  public void waitIngestionTaskToCompleteAllPartitionPendingActions(
      String topicName,
      int partition,
      long retryIntervalInMs,
      int numRetries) {
    LOGGER.info("Waiting all ingestion action to complete for topic: {}, partition: {}", topicName, partition);
    if (!topicPartitionHasAnyPendingActions(topicName, partition)) {
      LOGGER.info("Topic: {}, partition: {} has no pending ingestion action.", topicName, partition);
      return;
    }

    try {
      long startTimeInMs = System.currentTimeMillis();
      for (int i = 0; i < numRetries; i++) {
        if (!topicPartitionHasAnyPendingActions(topicName, partition)) {
          LOGGER.info(
              "Partition: {} of topic: {} has stopped consumption in {} ms.",
              partition,
              topicName,
              LatencyUtils.getElapsedTimeFromMsToMs(startTimeInMs));
          return;
        }
        sleep(retryIntervalInMs);
      }
      LOGGER.warn(
          "Topic: {}, partition: {} is still having pending ingestion action for it to stop for {} ms.",
          topicName,
          partition,
          numRetries * retryIntervalInMs);
    } catch (InterruptedException e) {
      LOGGER.warn("Waiting for partition to clear up pending ingestion action was interrupted", e);
      currentThread().interrupt();
    }
  }

  public boolean topicPartitionHasAnyPendingActions(String topic, int partition) {
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topic);
      return ingestionTask != null && ingestionTask.isRunning()
          && ingestionTask.hasPendingPartitionIngestionAction(partition);
    }
  }

  public boolean isLiveUpdateSuppressionEnabled() {
    return serverConfig.freezeIngestionIfReadyToServeOrLocalDataExists();
  }

  @Override
  public VeniceConfigLoader getVeniceConfigLoader() {
    return veniceConfigLoader;
  }

  @Override
  public VeniceWriterFactory getVeniceWriterFactory() {
    return veniceWriterFactory;
  }

  /**
   * Find the task that matches both the storeName and maximumVersion number, enable metrics emission for this task and
   * update ingestion stats with this task; disable metric emission for all the task that doesn't max version.
   */
  protected void updateStatsEmission(
      NavigableMap<String, StoreIngestionTask> taskMap,
      String storeName,
      int maximumVersion) {
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
    for (Map.Entry<String, StoreIngestionTask> entry: taskMap.entrySet()) {
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
      Map.Entry<String, StoreIngestionTask> lowerVersionPush =
          taskMap.lowerEntry(Version.composeKafkaTopic(storeName, maxVersion));
      while (lowerVersionPush != null
          && Version.parseStoreFromKafkaTopicName(lowerVersionPush.getKey()).equals(storeName)) {
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
  public CompletableFuture<Void> stopConsumption(VeniceStoreVersionConfig veniceStore, int partitionId) {
    final String topic = veniceStore.getStoreVersionName();

    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topic);
      if (ingestionTask != null && ingestionTask.isRunning()) {
        return ingestionTask
            .unSubscribePartition(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId));
      } else {
        LOGGER.warn("Ignoring stop consumption message for Topic {} Partition {}", topic, partitionId);
        return CompletableFuture.completedFuture(null);
      }
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
  public void stopConsumptionAndWait(
      VeniceStoreVersionConfig veniceStore,
      int partitionId,
      int sleepSeconds,
      int numRetries,
      boolean whetherToResetOffset) {
    String topicName = veniceStore.getStoreVersionName();
    if (isPartitionConsuming(topicName, partitionId)) {
      stopConsumption(veniceStore, partitionId);
      try {
        long startTimeInMs = System.currentTimeMillis();
        for (int i = 0; i < numRetries; i++) {
          if (!isPartitionConsuming(topicName, partitionId)) {
            LOGGER.info(
                "Partition: {} of topic: {} has stopped consumption in {} ms.",
                partitionId,
                topicName,
                LatencyUtils.getElapsedTimeFromMsToMs(startTimeInMs));
            return;
          }
          sleep((long) sleepSeconds * Time.MS_PER_SECOND);
        }
        LOGGER.warn(
            "Partition: {} of store: {} is still consuming after waiting for it to stop for {} seconds.",
            partitionId,
            topicName,
            numRetries * sleepSeconds);
      } catch (InterruptedException e) {
        LOGGER.warn("Waiting for partition to stop consumption was interrupted", e);
        currentThread().interrupt();
      }
    } else {
      LOGGER.warn("Partition: {} of topic: {} is not consuming, skipped the stop consumption.", partitionId, topicName);
    }
    if (whetherToResetOffset) {
      resetConsumptionOffset(veniceStore, partitionId);
      LOGGER.info("Reset consumption offset for topic: {}, partition: {}", topicName, partitionId);
    }
    if (!ingestionTaskHasAnySubscription(topicName)) {
      shutdownIdleIngestionTask(topicName);
    }
  }

  /**
   * A helper function which checks the idles counter inside StoreIngestionTask; if the counter is higher than the idle
   * threshold, treat the task as idle and shutdown the task.
   */
  private void shutdownIdleIngestionTask(String topicName) {
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topicName)) {
      // Must check the idle status again after acquiring the lock.
      StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topicName);
      if (ingestionTask != null && !ingestionTask.hasAnySubscription()) {
        if (isIsolatedIngestion) {
          LOGGER.info(
              "Ingestion task for topic {} will be kept open for the access from main process even though it has no subscription now.",
              topicName);
        } else {
          LOGGER.info("Shutting down ingestion task of topic {}", topicName);
          shutdownStoreIngestionTask(topicName);
        }
      }
    }
  }

  /**
   * Drops the corresponding Venice Partition gracefully.
   * This should only be called after {@link #stopConsumptionAndWait} has been called
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   * @return a future for the drop partition action.
   */
  public CompletableFuture<Void> dropStoragePartitionGracefully(VeniceStoreVersionConfig veniceStore, int partitionId) {
    final String topic = veniceStore.getStoreVersionName();

    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topic);
      if (ingestionTask != null) {
        return ingestionTask.dropStoragePartitionGracefully(
            new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId));
      } else {
        LOGGER.info(
            "Ingestion task for Topic {} is null. Dropping partition {} synchronously",
            veniceStore.getStoreVersionName(),
            partitionId);
        this.storageService.dropStorePartition(veniceStore, partitionId, true);
        return CompletableFuture.completedFuture(null);
      }
    }
  }

  /**
   * This function will try to kill the ingestion tasks belonging to non-current versions.
   * And this is mainly being used by memory limiter feature to free up resources when encountering memory
   * exhausting issue.
   *
   */
  private void killConsumptionTaskForNonCurrentVersions() {
    // Find out all non-current versions
    Set<String> topicNameSet = topicNameToIngestionTaskMap.keySet();
    List<String> nonCurrentVersions = new ArrayList<>();
    topicNameSet.forEach(topic -> {
      String storeName = Version.parseStoreFromKafkaTopicName(topic);
      int version = Version.parseVersionFromKafkaTopicName(topic);
      Store store = metadataRepo.getStore(storeName);
      if (store == null || version != store.getCurrentVersion()) {
        nonCurrentVersions.add(topic);
      }
    });
    if (nonCurrentVersions.isEmpty()) {
      LOGGER.info("No ingestion task belonging to non-current version");
      return;
    }
    LOGGER.info("Start killing the following ingestion tasks: {}", nonCurrentVersions);
    nonCurrentVersions.forEach(topic -> killConsumptionTask(topic));
    LOGGER.info("Finished killing the following ingestion tasks: {}", nonCurrentVersions);
  }

  private void scanAndCloseIdleConsumptionTasks() {
    try {
      LOGGER.info("Number of ingestion tasks before cleaning: {}", topicNameToIngestionTaskMap.size());
      for (Map.Entry<String, StoreIngestionTask> entry: topicNameToIngestionTaskMap.entrySet()) {
        String topicName = entry.getKey();
        StoreIngestionTask task = entry.getValue();
        if (task.isIdleOverThreshold()) {
          LOGGER.info("Found idle task for topic {}, shutting it down.", topicName);
          shutdownIdleIngestionTask(topicName);
        }
      }
      LOGGER.info("Number of active ingestion tasks after cleaning: {}", topicNameToIngestionTaskMap.size());
    } catch (Exception e) {
      LOGGER.error("Error when attempting to shutdown idle store ingestion tasks", e);
    }
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

    boolean killed = false;
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topicName)) {
      StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topicName);
      if (consumerTask == null) {
        LOGGER.info("Ignoring kill request for not-existing consumption task {}", topicName);
        return false;
      }

      if (consumerTask.isRunning()) {
        consumerTask.kill();
        killed = true;
        LOGGER.info("Killed consumption task for topic {}", topicName);
      } else {
        LOGGER.warn("Ignoring kill request for stopped consumption task {}", topicName);
      }
      // Always remove the version level compressor regardless if the task was running or not.
      compressorFactory.removeVersionSpecificCompressor(topicName);
      // cleanup the map regardless if the task was running or not to prevent mem leak when failed tasks lingers
      // in the map since isRunning is set to false already.
      topicNameToIngestionTaskMap.remove(topicName);
      if (aggKafkaConsumerService != null) {
        aggKafkaConsumerService.unsubscribeAll(consumerTask.getVersionTopic());
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
    topicLockManager.removeLockForResource(topicName);
    return killed;
  }

  @Override
  public void addIngestionNotifier(VeniceNotifier notifier) {
    leaderFollowerNotifiers.add(notifier);
  }

  // test only
  public void replaceAndAddTestNotifier(VeniceNotifier notifier) {
    leaderFollowerNotifiers.removeIf(veniceNotifier -> veniceNotifier instanceof PushStatusNotifier);
    leaderFollowerNotifiers.add(notifier);
  }

  @Override
  public boolean containsRunningConsumption(VeniceStoreVersionConfig veniceStore) {
    return containsRunningConsumption(veniceStore.getStoreVersionName());
  }

  @Override
  public boolean containsRunningConsumption(String topic) {
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
      return consumerTask != null && consumerTask.isRunning();
    }
  }

  @Override
  public boolean isPartitionConsuming(String topic, int partitionId) {
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask ingestionTask = topicNameToIngestionTaskMap.get(topic);
      return ingestionTask != null && ingestionTask.isRunning()
          && ingestionTask.isPartitionConsumingOrHasPendingIngestionAction(partitionId);
    }
  }

  @Override
  public Set<String> getIngestingTopicsWithVersionStatusNotOnline() {
    Set<String> result = new HashSet<>();
    for (String topic: topicNameToIngestionTaskMap.keySet()) {
      try {
        Store store = metadataRepo.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(topic));
        int versionNumber = Version.parseVersionFromKafkaTopicName(topic);
        Version version = store.getVersion(versionNumber);
        if (version == null || version.getStatus() != VersionStatus.ONLINE) {
          result.add(topic);
        }
      } catch (VeniceNoStoreException e) {
        // Include topics that may have their corresponding store deleted already
        result.add(topic);
      } catch (Exception e) {
        LOGGER.error("Unexpected exception while fetching ongoing ingestion topics, topic: {}", topic, e);
      }
    }
    return result;
  }

  public void recordIngestionFailure(String storeName) {
    hostLevelIngestionStats.getStoreStats(storeName).recordIngestionFailure();
  }

  @Override
  public AggVersionedIngestionStats getAggVersionedIngestionStats() {
    return versionedIngestionStats;
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
    Properties kafkaConsumerProperties = serverConfig.getClusterProperties().getPropertiesCopy();
    kafkaConsumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig.getKafkaBootstrapServers());
    kafkaConsumerProperties
        .setProperty(KAFKA_FETCH_MIN_BYTES_CONFIG, String.valueOf(serverConfig.getKafkaFetchMinSizePerSecond()));
    kafkaConsumerProperties
        .setProperty(KAFKA_FETCH_MAX_BYTES_CONFIG, String.valueOf(serverConfig.getKafkaFetchMaxSizePerSecond()));
    /**
     * The following setting is used to control the maximum number of records to returned in one poll request.
     */
    kafkaConsumerProperties
        .setProperty(KAFKA_MAX_POLL_RECORDS_CONFIG, Integer.toString(serverConfig.getKafkaMaxPollRecords()));
    kafkaConsumerProperties
        .setProperty(KAFKA_FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(serverConfig.getKafkaFetchMaxTimeMS()));
    kafkaConsumerProperties.setProperty(
        KAFKA_MAX_PARTITION_FETCH_BYTES_CONFIG,
        String.valueOf(serverConfig.getKafkaFetchPartitionMaxSizePerSecond()));
    kafkaConsumerProperties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_TIMES,
        String.valueOf(serverConfig.getPubSubConsumerPollRetryTimes()));
    kafkaConsumerProperties.setProperty(
        PubSubConstants.PUBSUB_CONSUMER_POLL_RETRY_BACKOFF_MS,
        String.valueOf(serverConfig.getPubSubConsumerPollRetryBackoffMs()));

    return kafkaConsumerProperties;
  }

  private VeniceProperties getPubSubSSLPropertiesFromServerConfig(String kafkaBootstrapUrls) {
    final VeniceServerConfig serverConfigForPubSubCluster;
    if (kafkaBootstrapUrls.equals(serverConfig.getKafkaBootstrapServers())) {
      serverConfigForPubSubCluster = serverConfig;
    } else {
      Properties clonedProperties = serverConfig.getClusterProperties().toProperties();
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrls);
      serverConfigForPubSubCluster =
          new VeniceServerConfig(new VeniceProperties(clonedProperties), serverConfig.getKafkaClusterMap());
    }

    Properties properties = serverConfigForPubSubCluster.getClusterProperties().getPropertiesCopy();
    kafkaBootstrapUrls = serverConfigForPubSubCluster.getKafkaBootstrapServers();
    String resolvedKafkaUrl = serverConfigForPubSubCluster.getKafkaClusterUrlResolver().apply(kafkaBootstrapUrls);
    if (resolvedKafkaUrl != null) {
      kafkaBootstrapUrls = resolvedKafkaUrl;
    }
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrls);
    PubSubSecurityProtocol securityProtocol =
        serverConfigForPubSubCluster.getPubSubSecurityProtocol(kafkaBootstrapUrls);
    if (PubSubUtil.isPubSubSslProtocol(securityProtocol)) {
      Optional<SSLConfig> sslConfig = serverConfigForPubSubCluster.getSslConfig();
      if (!sslConfig.isPresent()) {
        throw new VeniceException("SSLConfig should be present when Kafka SSL is enabled");
      }
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
    }
    properties.setProperty(PUBSUB_SECURITY_PROTOCOL, securityProtocol.name());
    return new VeniceProperties(properties);
  }

  /**
   * @return Properties Kafka properties corresponding to the venice store.
   */
  private Properties getKafkaConsumerProperties(VeniceStoreVersionConfig storeConfig) {
    Properties kafkaConsumerProperties = getCommonKafkaConsumerProperties(storeConfig);
    String groupId = getGroupId(storeConfig.getStoreVersionName());
    kafkaConsumerProperties.setProperty(KAFKA_GROUP_ID_CONFIG, groupId);
    /**
     * Temporarily we are going to use group_id as client_id as well since it is unique in cluster level.
     * With unique client_id, it will be easier for us to check Kafka consumer related metrics through JMX.
     * TODO: Kafka is throttling based on client_id, need to investigate whether we should use Kafka throttling or not.
     */
    kafkaConsumerProperties.setProperty(KAFKA_CLIENT_ID_CONFIG, groupId);
    return kafkaConsumerProperties;
  }

  public ByteBuffer getStoreVersionCompressionDictionary(String topicName) {
    return storageMetadataService.getStoreVersionCompressionDictionary(topicName);
  }

  public StoreIngestionTask getStoreIngestionTask(String topicName) {
    return topicNameToIngestionTaskMap.get(topicName);
  }

  public AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions) {
    AdminResponse response = new AdminResponse();
    StoreIngestionTask ingestionTask = getStoreIngestionTask(topicName);
    if (ingestionTask != null) {
      ingestionTask.dumpPartitionConsumptionStates(response, partitions);
      ingestionTask.dumpStoreVersionState(response);
    } else {
      String msg = "Ingestion task for " + topicName + " doesn't exist for " + ServerAdminAction.DUMP_INGESTION_STATE
          + " admin command";
      LOGGER.warn(msg);
      response.setMessage(msg);
    }
    return response;
  }

  public ReplicaIngestionResponse getTopicPartitionIngestionContext(
      String versionTopic,
      String topicName,
      int partitionId) {
    ReplicaIngestionResponse replicaIngestionResponse = new ReplicaIngestionResponse();
    PubSubTopic pubSubVersionTopic = pubSubTopicRepository.getTopic(versionTopic);
    PubSubTopic requestTopic = pubSubTopicRepository.getTopic(topicName);
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(requestTopic, partitionId);
    try {
      byte[] topicPartitionInfo = aggKafkaConsumerService.getIngestionInfoFor(pubSubVersionTopic, pubSubTopicPartition);
      replicaIngestionResponse.setPayload(topicPartitionInfo);
    } catch (Exception e) {
      replicaIngestionResponse.setError(true);
      replicaIngestionResponse.setMessage(e.getMessage());
      LOGGER.error(
          "Error on get topic partition ingestion context for resource: " + Utils.getReplicaId(topicName, partitionId),
          e);
    }
    return replicaIngestionResponse;
  }

  /**
   * This method updates all sub-partitions' latest offset records fetched from isolated ingestion process
   * in main process, so main process's in-memory storage metadata service could be aware of the latest updates and will
   * not re-start the ingestion from scratch.
   */
  public void updatePartitionOffsetRecords(String topicName, int partition, ByteBuffer offsetRecordByteBuffer) {
    byte[] offsetRecordByteArray = offsetRecordByteBuffer.array();
    OffsetRecord offsetRecord;
    try {
      offsetRecord = new OffsetRecord(offsetRecordByteArray, partitionStateSerializer);
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception when deserializing offset record byte array: {} for replica: {}",
          Arrays.toString(offsetRecordByteArray),
          Utils.getReplicaId(topicName, partition),
          e);
      throw e;
    }
    storageMetadataService.put(topicName, partition, offsetRecord);
    LOGGER.info(
        "Updated OffsetRecord: {} for resource: {} ",
        offsetRecord.toString(),
        Utils.getReplicaId(topicName, partition));
  }

  /**
   * This method should only be called when the forked ingestion process is handing over ingestion task to main process.
   * It collects the user partition's latest OffsetRecords from partition consumption states (PCS).
   * In theory, PCS should be available in this situation as we haven't unsubscribed from topic. If it is not available,
   * we will throw exception as this is not as expected.
   */
  public ByteBuffer getPartitionOffsetRecords(String topicName, int partition) {
    StoreIngestionTask storeIngestionTask = getStoreIngestionTask(topicName);
    PartitionConsumptionState pcs = storeIngestionTask.getPartitionConsumptionState(partition);
    return ByteBuffer.wrap(pcs.getOffsetRecord().toBytes());
  }

  /**
   * Updates offset metadata and sync to storage for specified topic partition.
   * This method is invoked only when isolated ingestion process is reporting topic partition completion to make sure
   * ingestion process is persisted.
   */
  public void syncTopicPartitionOffset(String topicName, int partition) {
    StoreIngestionTask storeIngestionTask = getStoreIngestionTask(topicName);
    storeIngestionTask.updateOffsetMetadataAndSync(topicName, partition);
  }

  public final ReadOnlyStoreRepository getMetadataRepo() {
    return metadataRepo;
  }

  private boolean ingestionTaskHasAnySubscription(String topic) {
    try (AutoCloseableLock ignore = topicLockManager.getLockForResource(topic)) {
      StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
      return consumerTask != null && consumerTask.hasAnySubscription();
    }
  }

  /**
   * Resets Offset to beginning for Kafka Partition corresponding to Venice Partition.
   * @param veniceStore Venice Store for the partition.
   * @param partitionId Venice partition's id.
   */
  private void resetConsumptionOffset(VeniceStoreVersionConfig veniceStore, int partitionId) {
    String topic = veniceStore.getStoreVersionName();
    StoreIngestionTask consumerTask = topicNameToIngestionTaskMap.get(topic);
    if (consumerTask != null && consumerTask.isRunning()) {
      consumerTask.resetPartitionConsumptionOffset(
          new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId));
    }
    LOGGER.info("Offset reset to beginning - Kafka Partition: {}-{}.", topic, partitionId);
  }

  // For testing purpose only.
  public KafkaValueSerializer getKafkaValueSerializer() {
    return kafkaValueSerializer;
  }
}
