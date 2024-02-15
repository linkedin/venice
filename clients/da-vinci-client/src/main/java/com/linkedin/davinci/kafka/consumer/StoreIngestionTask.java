package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.ingestion.LagType.OFFSET_LAG;
import static com.linkedin.davinci.ingestion.LagType.TIME_LAG;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.RESET_OFFSET;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.SUBSCRIBE;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.UNSUBSCRIBE;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.LogMessages.KILLED_JOB_MESSAGE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.ingestion.LagType;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.utils.ChunkAssembler;
import com.linkedin.davinci.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceInconsistentStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public abstract class StoreIngestionTask implements Runnable, Closeable {
  // TODO: Make this LOGGER prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger LOGGER = LogManager.getLogger(StoreIngestionTask.class);

  private static final String CONSUMER_TASK_ID_FORMAT = StoreIngestionTask.class.getSimpleName() + " for [ Topic: %s ]";
  public static long SCHEMA_POLLING_DELAY_MS = SECONDS.toMillis(5);
  public static long STORE_VERSION_POLLING_DELAY_MS = MINUTES.toMillis(1);

  private static final long SCHEMA_POLLING_TIMEOUT_MS = MINUTES.toMillis(5);
  private static final long SOP_POLLING_TIMEOUT_MS = HOURS.toMillis(1);

  private static final int MAX_CONSUMER_ACTION_ATTEMPTS = 5;
  private static final int CONSUMER_ACTION_QUEUE_INIT_CAPACITY = 11;
  protected static final long KILL_WAIT_TIME_MS = 5000L;
  private static final int MAX_KILL_CHECKING_ATTEMPTS = 10;

  protected static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  /**
   * Speed up DaVinci shutdown by closing partitions concurrently.
   */
  private static final ExecutorService SHUTDOWN_EXECUTOR_FOR_DVC =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

  /** storage destination for consumption */
  protected final StorageEngineRepository storageEngineRepository;
  protected final AbstractStorageEngine storageEngine;

  /** Topics used for this topic consumption
   * TODO: Using a PubSubVersionTopic and PubSubRealTimeTopic extending PubSubTopic for type safety.
   * */
  protected final String kafkaVersionTopic;
  protected final PubSubTopic versionTopic;
  protected final PubSubTopic realTimeTopic;
  protected final String storeName;
  private final boolean isUserSystemStore;
  protected final int versionNumber;
  protected final ReadOnlySchemaRepository schemaRepository;
  protected final ReadOnlyStoreRepository storeRepository;
  protected final String ingestionTaskName;
  protected final Properties kafkaProps;
  protected final AtomicBoolean isRunning;
  protected final AtomicBoolean emitMetrics; // TODO: remove this once we migrate to versioned stats
  protected final AtomicInteger consumerActionSequenceNumber = new AtomicInteger(0);
  protected final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;
  protected final Map<Integer, AtomicInteger> partitionToPendingConsumerActionCountMap;
  protected final StorageMetadataService storageMetadataService;
  protected final TopicManagerRepository topicManagerRepository;
  /** Per-partition consumption state map */
  protected final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  protected final AbstractStoreBufferService storeBufferService;

  /**
   * Persists partitions that encountered exceptions in other threads. i.e. consumer, producer and drainer.
   *
   * The exception for the corresponding partition will only been cleaned up when resetting the partition,
   * or re-subscribing the partition.
   */
  private final List<PartitionExceptionInfo> partitionIngestionExceptionList = new SparseConcurrentList<>();
  /**
   * Do not remove partition from this set unless Helix explicitly sends an unsubscribe action for the partition to
   * remove it from the node, or Helix sends a subscribe action for the partition to re-subscribe it to the node.
   */
  private final Set<Integer> failedPartitions = new ConcurrentSkipListSet<>();

  /** Persists the exception thrown by {@link KafkaConsumerService}. */
  private Exception lastConsumerException = null;
  /** Persists the last exception thrown by any asynchronous component that should terminate the entire ingestion task */
  private final AtomicReference<Exception> lastStoreIngestionException = new AtomicReference<>();
  /**
   * Keeps track of producer states inside version topic that drainer threads have processed so far. Producers states in this validator will be
   * flushed to the metadata partition of the storage engine regularly in {@link #syncOffset(String, PartitionConsumptionState)}
   */
  private final KafkaDataIntegrityValidator kafkaDataIntegrityValidator;
  protected final HostLevelIngestionStats hostLevelIngestionStats;
  protected final AggVersionedDIVStats versionedDIVStats;
  protected final AggVersionedIngestionStats versionedIngestionStats;
  protected final BooleanSupplier isCurrentVersion;
  protected final Optional<HybridStoreConfig> hybridStoreConfig;
  protected final Consumer<DataValidationException> divErrorMetricCallback;
  private final ExecutorService missingSOPCheckExecutor = Executors.newSingleThreadExecutor();

  protected final long readCycleDelayMs;
  protected final long emptyPollSleepMs;

  protected final DiskUsage diskUsage;

  /** Message bytes consuming interval before persisting offset in offset db for transactional mode database. */
  protected final long databaseSyncBytesIntervalForTransactionalMode;
  /** Message bytes consuming interval before persisting offset in offset db for deferred-write database. */
  protected final long databaseSyncBytesIntervalForDeferredWriteMode;
  protected final VeniceServerConfig serverConfig;

  private final int ingestionTaskMaxIdleCount;

  /** Used for reporting error when the {@link #partitionConsumptionStateMap} is empty */
  protected final int errorPartitionId;

  // use this checker to check whether ingestion completion can be reported for a partition
  protected final ReadyToServeCheck defaultReadyToServeChecker;

  protected final SparseConcurrentList<Object> availableSchemaIds = new SparseConcurrentList<>();
  protected final SparseConcurrentList<Object> deserializedSchemaIds = new SparseConcurrentList<>();
  protected int idleCounter = 0;

  private final StorageUtilizationManager storageUtilizationManager;

  protected final AggKafkaConsumerService aggKafkaConsumerService;

  protected int writeComputeFailureCode = 0;

  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  // Do not convert it to a local variable because it is used in test.
  private boolean purgeTransientRecordBuffer = true;

  protected final boolean isWriteComputationEnabled;

  /**
   * Freeze ingestion if ready to serve or local data exists
   */
  private final boolean suppressLiveUpdates;

  private final boolean isActiveActiveReplicationEnabled;

  /**
   * This would be the number of partitions in the StorageEngine and in version topics
   */
  protected final int subPartitionCount;

  protected final int amplificationFactor;

  // Used to construct VenicePartitioner
  protected final VenicePartitioner venicePartitioner;

  // Total number of partition for this store version
  protected final int storeVersionPartitionCount;

  private int subscribedCount = 0;
  private int forceUnSubscribedCount = 0;

  // Push timeout threshold for the store
  protected final long bootstrapTimeoutInMs;

  protected final boolean isIsolatedIngestion;

  protected final AmplificationFactorAdapter amplificationFactorAdapter;

  protected final StatusReportAdapter statusReportAdapter;

  protected final ChunkAssembler chunkAssembler;
  private final Optional<ObjectCacheBackend> cacheBackend;
  private final DaVinciRecordTransformer recordTransformer;
  private final Runnable runnableForKillIngestionTasksForMissingSOP;

  protected final String localKafkaServer;
  protected final int localKafkaClusterId;
  protected final Set<String> localKafkaServerSingletonSet;
  private int valueSchemaId = -1;

  protected final boolean isDaVinciClient;

  private final boolean offsetLagDeltaRelaxEnabled;
  private final boolean ingestionCheckpointDuringGracefulShutdownEnabled;

  protected boolean isDataRecovery;
  protected int dataRecoverySourceVersionNumber;
  protected final MetaStoreWriter metaStoreWriter;
  protected final Function<String, String> kafkaClusterUrlResolver;
  /** TODO Get rid of this map once we delete the dedicated consumer mode */
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  protected final boolean readOnlyForBatchOnlyStoreEnabled;
  protected final CompressionStrategy compressionStrategy;
  protected final StorageEngineBackedCompressorFactory compressorFactory;
  protected final Lazy<VeniceCompressor> compressor;
  protected final boolean isChunked;
  protected final PubSubTopicRepository pubSubTopicRepository;
  private final String[] msgForLagMeasurement;
  private final Runnable runnableForKillIngestionTasksForNonCurrentVersions;

  public StoreIngestionTask(
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend,
      DaVinciRecordTransformer recordTransformer,
      Queue<VeniceNotifier> notifiers) {
    this.readCycleDelayMs = storeConfig.getKafkaReadCycleDelayMs();
    this.emptyPollSleepMs = storeConfig.getKafkaEmptyPollSleepMs();
    this.databaseSyncBytesIntervalForTransactionalMode = storeConfig.getDatabaseSyncBytesIntervalForTransactionalMode();
    this.databaseSyncBytesIntervalForDeferredWriteMode = storeConfig.getDatabaseSyncBytesIntervalForDeferredWriteMode();
    this.kafkaProps = kafkaConsumerProperties;
    this.storageEngineRepository = builder.getStorageEngineRepository();
    this.storageMetadataService = builder.getStorageMetadataService();
    this.storeRepository = builder.getMetadataRepo();
    this.schemaRepository = builder.getSchemaRepo();
    this.kafkaVersionTopic = storeConfig.getStoreVersionName();
    this.pubSubTopicRepository = builder.getPubSubTopicRepository();
    this.versionTopic = pubSubTopicRepository.getTopic(kafkaVersionTopic);
    this.storeName = versionTopic.getStoreName();
    this.isUserSystemStore = VeniceSystemStoreUtils.isUserSystemStore(storeName);
    this.realTimeTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
    this.versionNumber = Version.parseVersionFromKafkaTopicName(kafkaVersionTopic);
    this.consumerActionsQueue = new PriorityBlockingQueue<>(CONSUMER_ACTION_QUEUE_INIT_CAPACITY);
    this.partitionToPendingConsumerActionCountMap = new VeniceConcurrentHashMap<>();

    // partitionConsumptionStateMap could be accessed by multiple threads: consumption thread and the thread handling
    // kill message
    this.partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    // Could be accessed from multiple threads since there are multiple worker threads.
    this.kafkaDataIntegrityValidator = new KafkaDataIntegrityValidator(
        this.kafkaVersionTopic,
        KafkaDataIntegrityValidator.DISABLED,
        builder.getServerConfig().getDivProducerStateMaxAgeMs());
    this.ingestionTaskName = String.format(CONSUMER_TASK_ID_FORMAT, kafkaVersionTopic);
    this.topicManagerRepository = builder.getTopicManagerRepository();
    this.hostLevelIngestionStats = builder.getIngestionStats().getStoreStats(storeName);
    this.versionedDIVStats = builder.getVersionedDIVStats();
    this.versionedIngestionStats = builder.getVersionedStorageIngestionStats();
    this.isRunning = new AtomicBoolean(true);
    this.emitMetrics = new AtomicBoolean(true);
    this.readOnlyForBatchOnlyStoreEnabled = storeConfig.isReadOnlyForBatchOnlyStoreEnabled();

    this.storeBufferService = builder.getStoreBufferService();
    this.isCurrentVersion = isCurrentVersion;
    this.hybridStoreConfig = Optional.ofNullable(
        version.isUseVersionLevelHybridConfig() ? version.getHybridStoreConfig() : store.getHybridStoreConfig());

    this.divErrorMetricCallback = e -> versionedDIVStats.recordException(storeName, versionNumber, e);

    this.diskUsage = builder.getDiskUsage();

    this.storageEngine = Objects.requireNonNull(storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic));

    this.serverConfig = builder.getServerConfig();

    this.defaultReadyToServeChecker = getDefaultReadyToServeChecker();

    this.aggKafkaConsumerService = Objects.requireNonNull(builder.getAggKafkaConsumerService());

    this.errorPartitionId = errorPartitionId;

    this.isWriteComputationEnabled = store.isWriteComputationEnabled();

    this.partitionStateSerializer = builder.getPartitionStateSerializer();

    this.suppressLiveUpdates = serverConfig.freezeIngestionIfReadyToServeOrLocalDataExists();

    this.storeVersionPartitionCount = version.getPartitionCount();

    long pushTimeoutInMs;
    try {
      pushTimeoutInMs = HOURS.toMillis(store.getBootstrapToOnlineTimeoutInHours());
    } catch (Exception e) {
      LOGGER.warn(
          "Error when getting bootstrap to online timeout config for store {}. Will use default timeout threshold which is 24 hours",
          storeName,
          e);
      pushTimeoutInMs = HOURS.toMillis(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);
    }
    this.bootstrapTimeoutInMs = pushTimeoutInMs;
    this.isIsolatedIngestion = isIsolatedIngestion;

    PartitionerConfig partitionerConfig = version.getPartitionerConfig();
    if (partitionerConfig == null) {
      this.venicePartitioner = new DefaultVenicePartitioner();
      this.amplificationFactor = 1;
    } else {
      this.venicePartitioner = PartitionUtils.getVenicePartitioner(partitionerConfig);
      this.amplificationFactor = partitionerConfig.getAmplificationFactor();
    }

    this.subPartitionCount = storeVersionPartitionCount * amplificationFactor;
    this.amplificationFactorAdapter = new AmplificationFactorAdapter(amplificationFactor, partitionConsumptionStateMap);
    this.statusReportAdapter = new StatusReportAdapter(
        new IngestionNotificationDispatcher(notifiers, kafkaVersionTopic, isCurrentVersion),
        amplificationFactorAdapter);

    this.runnableForKillIngestionTasksForMissingSOP = () -> waitForStateVersion(kafkaVersionTopic);
    this.missingSOPCheckExecutor.execute(runnableForKillIngestionTasksForMissingSOP);
    this.chunkAssembler = new ChunkAssembler(storeName);
    this.cacheBackend = cacheBackend;
    this.recordTransformer = recordTransformer;
    if (recordTransformer != null) {
      versionedIngestionStats.registerTransformerLatencySensor(storeName, versionNumber);
    }
    this.localKafkaServer = this.kafkaProps.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    this.localKafkaServerSingletonSet = Collections.singleton(localKafkaServer);
    this.isDaVinciClient = builder.isDaVinciClient();
    this.isActiveActiveReplicationEnabled = version.isActiveActiveReplicationEnabled();
    this.offsetLagDeltaRelaxEnabled = serverConfig.getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart() > 0;
    this.ingestionCheckpointDuringGracefulShutdownEnabled =
        serverConfig.isServerIngestionCheckpointDuringGracefulShutdownEnabled();
    this.metaStoreWriter = builder.getMetaStoreWriter();

    this.storageUtilizationManager = new StorageUtilizationManager(
        storageEngine,
        store,
        kafkaVersionTopic,
        subPartitionCount,
        Collections.unmodifiableMap(partitionConsumptionStateMap),
        serverConfig.isHybridQuotaEnabled(),
        serverConfig.isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled(),
        statusReportAdapter,
        this::pauseConsumption,
        this::resumeConsumption);
    this.storeRepository.registerStoreDataChangedListener(this.storageUtilizationManager);
    this.kafkaClusterUrlResolver = serverConfig.getKafkaClusterUrlResolver();
    this.kafkaClusterUrlToIdMap = serverConfig.getKafkaClusterUrlToIdMap();
    this.localKafkaClusterId = kafkaClusterUrlToIdMap.getOrDefault(localKafkaServer, Integer.MIN_VALUE);
    this.compressionStrategy = version.getCompressionStrategy();
    this.compressorFactory = builder.getCompressorFactory();
    this.compressor = Lazy.of(() -> compressorFactory.getCompressor(compressionStrategy, kafkaVersionTopic));
    this.isChunked = version.isChunkingEnabled();
    this.msgForLagMeasurement = new String[subPartitionCount];
    for (int i = 0; i < this.msgForLagMeasurement.length; i++) {
      this.msgForLagMeasurement[i] = kafkaVersionTopic + "_" + i;
    }
    this.runnableForKillIngestionTasksForNonCurrentVersions =
        builder.getRunnableForKillIngestionTasksForNonCurrentVersions();
    this.ingestionTaskMaxIdleCount = serverConfig.getIngestionTaskMaxIdleCount();
  }

  /** Package-private on purpose, only intended for tests. Do not use for production use cases. */
  void setPurgeTransientRecordBuffer(boolean purgeTransientRecordBuffer) {
    this.purgeTransientRecordBuffer = purgeTransientRecordBuffer;
  }

  public AbstractStorageEngine getStorageEngine() {
    return storageEngine;
  }

  public String getIngestionTaskName() {
    return ingestionTaskName;
  }

  public int getVersionNumber() {
    return versionNumber;
  }

  public boolean isFutureVersion() {
    return versionedIngestionStats.isFutureVersion(storeName, versionNumber);
  }

  protected void throwIfNotRunning() {
    if (!isRunning()) {
      throw new VeniceException(" Topic " + kafkaVersionTopic + " is shutting down, no more messages accepted");
    }
  }

  /**
   * Apply an unique and increasing sequence number for each consumer action, so if there are multiple consumer actions
   * in the queue and they have the same priority, whichever be added first into the queue will be polled out first
   * from the queue (FIFO).
   * @return an unique and increasing sequence number for a new consumer action.
   */
  protected int nextSeqNum() {
    return consumerActionSequenceNumber.incrementAndGet();
  }

  private void waitForStateVersion(String kafkaTopic) {
    long startTime = System.currentTimeMillis();

    for (;;) {
      StoreVersionState state = storageEngine.getStoreVersionState();
      long elapsedTime = System.currentTimeMillis() - startTime;
      if (state != null || !isRunning()) {
        break;
      }

      if (elapsedTime > SOP_POLLING_TIMEOUT_MS) {
        LOGGER.warn("Killing the ingestion as Version state is not available for {} after {}", kafkaTopic, elapsedTime);
        kill();
      }
      try {
        Thread.sleep(STORE_VERSION_POLLING_DELAY_MS);
      } catch (InterruptedException e) {
        LOGGER.info("Received interruptedException while waiting for store version.");
        break;
      }
    }
  }

  public synchronized void subscribePartition(
      PubSubTopicPartition topicPartition,
      Optional<LeaderFollowerStateType> leaderState) {
    subscribePartition(topicPartition, leaderState, true);
  }

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public synchronized void subscribePartition(
      PubSubTopicPartition topicPartition,
      Optional<LeaderFollowerStateType> leaderState,
      boolean isHelixTriggeredAction) {
    throwIfNotRunning();
    statusReportAdapter.initializePartitionReportStatus(topicPartition.getPartitionNumber());
    amplificationFactorAdapter.execute(topicPartition.getPartitionNumber(), subPartition -> {
      partitionToPendingConsumerActionCountMap.computeIfAbsent(subPartition, x -> new AtomicInteger(0))
          .incrementAndGet();
      consumerActionsQueue.add(
          new ConsumerAction(
              SUBSCRIBE,
              new PubSubTopicPartitionImpl(topicPartition.getPubSubTopic(), subPartition),
              nextSeqNum(),
              amplificationFactorAdapter.isLeaderSubPartition(subPartition) ? leaderState : Optional.empty(),
              isHelixTriggeredAction));
    });
  }

  public synchronized CompletableFuture<Void> unSubscribePartition(PubSubTopicPartition topicPartition) {
    return unSubscribePartition(topicPartition, true);
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public synchronized CompletableFuture<Void> unSubscribePartition(
      PubSubTopicPartition topicPartition,
      boolean isHelixTriggeredAction) {
    throwIfNotRunning();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    amplificationFactorAdapter.execute(topicPartition.getPartitionNumber(), subPartition -> {
      partitionToPendingConsumerActionCountMap.computeIfAbsent(subPartition, x -> new AtomicInteger(0))
          .incrementAndGet();
      ConsumerAction consumerAction = new ConsumerAction(
          UNSUBSCRIBE,
          new PubSubTopicPartitionImpl(topicPartition.getPubSubTopic(), subPartition),
          nextSeqNum(),
          isHelixTriggeredAction);

      consumerActionsQueue.add(consumerAction);
      futures.add(consumerAction.getFuture());
    });

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  public boolean hasAnySubscription() {
    return !partitionConsumptionStateMap.isEmpty();
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public synchronized void resetPartitionConsumptionOffset(PubSubTopicPartition topicPartition) {
    throwIfNotRunning();
    amplificationFactorAdapter.execute(topicPartition.getPartitionNumber(), subPartition -> {
      partitionToPendingConsumerActionCountMap.computeIfAbsent(subPartition, x -> new AtomicInteger(0))
          .incrementAndGet();
      consumerActionsQueue.add(
          new ConsumerAction(
              RESET_OFFSET,
              new PubSubTopicPartitionImpl(topicPartition.getPubSubTopic(), subPartition),
              nextSeqNum(),
              false));
    });
  }

  public String getStoreName() {
    return storeName;
  }

  public boolean isUserSystemStore() {
    return isUserSystemStore;
  }

  public abstract void promoteToLeader(
      PubSubTopicPartition topicPartition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker);

  public abstract void demoteToStandby(
      PubSubTopicPartition topicPartition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker);

  public boolean hasPendingPartitionIngestionAction(int userPartition) {
    return amplificationFactorAdapter.meetsAny(userPartition, subPartition -> {
      AtomicInteger atomicInteger = partitionToPendingConsumerActionCountMap.get(subPartition);
      if (atomicInteger == null) {
        return false;
      }
      return atomicInteger.get() > 0;
    });
  }

  public void kill() {
    synchronized (this) {
      throwIfNotRunning();
      consumerActionsQueue.add(ConsumerAction.createKillAction(versionTopic, nextSeqNum()));
    }

    try (Timer ignored = Timer.run(
        elapsedTimeInMs -> LOGGER
            .info("Completed waiting for kill action to take effect. Total elapsed time: {} ms", elapsedTimeInMs))) {
      for (int attempt = 0; isRunning() && attempt < MAX_KILL_CHECKING_ATTEMPTS; ++attempt) {
        MILLISECONDS.sleep(KILL_WAIT_TIME_MS / MAX_KILL_CHECKING_ATTEMPTS);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("StoreIngestionTask::kill was interrupted.", e);
    }

    synchronized (this) {
      if (isRunning()) {
        // If task is still running, force close it.
        statusReportAdapter.reportError(
            partitionConsumptionStateMap.values(),
            KILLED_JOB_MESSAGE + kafkaVersionTopic,
            new VeniceException("Kill the consumer"));
        /*
         * close can not stop the consumption synchronously, but the status of helix would be set to ERROR after
         * reportError. This push is being killed by controller, so this version is abandoned, it will not have
         * chances to serve traffic; forced kill all resources in this push.
         * N.B.: if we start seeing alerts from forced killed resource, consider whether we should keep those alerts
         *       if they are useful, or refactor them.
         */
        closeVeniceWriters(false);
        close();
      }
    }
  }

  /**
   * This method checks if there was a previous ingestion and what its state was. If there is a mismatch
   * between the checkpointed information and the current state, this method returns false. This implies
   * that the process crashed during or after the ingestion but before syncing the OffsetRecord with EOP.
   * In this case, the upstream should restart the ingestion from scratch.
   */
  private boolean checkDatabaseIntegrity(
      int partitionId,
      String topic,
      OffsetRecord offsetRecord,
      PartitionConsumptionState partitionConsumptionState) {
    boolean returnStatus = true;
    if (offsetRecord.getLocalVersionTopicOffset() > 0) {
      StoreVersionState storeVersionState = storageEngine.getStoreVersionState();
      if (storeVersionState != null) {
        LOGGER.info(
            "storeVersionState found for {}, partition: {}: checkDatabaseIntegrity will proceed",
            topic,
            partitionId);
        returnStatus = storageEngine.checkDatabaseIntegrity(
            partitionId,
            offsetRecord.getDatabaseInfo(),
            getStoragePartitionConfig(partitionId, storeVersionState.sorted, partitionConsumptionState));
        LOGGER.info(
            "checkDatabaseIntegrity {} for {}, partition: {}",
            returnStatus ? "succeeded" : "failed",
            topic,
            partitionId);
      } else {
        LOGGER.info(
            "storeVersionState not found for {}, partition: {}: checkDatabaseIntegrity will be skipped",
            topic,
            partitionId);
      }
    } else {
      LOGGER.info(
          "Local topic offset not found for {}, partition: {}: checkDatabaseIntegrity will be skipped",
          topic,
          partitionId);
    }
    return returnStatus;
  }

  private void beginBatchWrite(int partitionId, boolean sorted, PartitionConsumptionState partitionConsumptionState) {
    Map<String, String> checkpointedDatabaseInfo = partitionConsumptionState.getOffsetRecord().getDatabaseInfo();
    StoragePartitionConfig storagePartitionConfig =
        getStoragePartitionConfig(partitionId, sorted, partitionConsumptionState);
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());

    Optional<Supplier<byte[]>> partitionChecksumSupplier = Optional.empty();
    /**
     * In rocksdb Plain Table mode or in non deferredWrite mode, we can't use rocksdb SSTFileWriter to verify the checksum.
     * So there is no point keep calculating the running checksum here.
     */
    if (serverConfig.isDatabaseChecksumVerificationEnabled() && partitionConsumptionState.isDeferredWrite()
        && !serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()) {
      partitionConsumptionState.initializeExpectedChecksum();
      partitionChecksumSupplier = Optional.ofNullable(() -> {
        byte[] checksum = partitionConsumptionState.getExpectedChecksum();
        partitionConsumptionState.resetExpectedChecksum();
        return checksum;
      });
    }
    storageEngine.beginBatchWrite(storagePartitionConfig, checkpointedDatabaseInfo, partitionChecksumSupplier);
    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get()
            .getStorageEngine(kafkaVersionTopic)
            .beginBatchWrite(storagePartitionConfig, checkpointedDatabaseInfo, partitionChecksumSupplier);
      }
    }
  }

  private StoragePartitionConfig getStoragePartitionConfig(
      int partitionId,
      boolean sorted,
      PartitionConsumptionState partitionConsumptionState) {
    StoragePartitionConfig storagePartitionConfig = new StoragePartitionConfig(kafkaVersionTopic, partitionId);
    boolean deferredWrites;
    boolean readOnly = false;
    if (partitionConsumptionState.isEndOfPushReceived()) {
      // After EOP, we never enable deferred writes.
      // No matter what the sorted config was before the EOP message, it doesn't matter anymore.
      deferredWrites = false;
      if (partitionConsumptionState.isBatchOnly() && readOnlyForBatchOnlyStoreEnabled) {
        readOnly = true;
      }
    } else {
      // Prior to the EOP, we can optimize the storage if the data is sorted.
      deferredWrites = sorted;
    }
    storagePartitionConfig.setDeferredWrite(deferredWrites);
    storagePartitionConfig.setReadOnly(readOnly);
    return storagePartitionConfig;
  }

  protected abstract boolean isHybridFollower(PartitionConsumptionState partitionConsumptionState);

  /**
   * Checks whether the lag is acceptable for hybrid stores
   */
  protected abstract boolean checkAndLogIfLagIsAcceptableForHybridStore(
      PartitionConsumptionState partitionConsumptionState,
      long offsetLag,
      long offsetThreshold,
      boolean shouldLogLag,
      LagType lagType,
      long latestConsumedProducerTimestamp);

  /**
   * This function checks various conditions to verify if a store is ready to serve.
   * Lag = (Source Max Offset - SOBR Source Offset) - (Current Offset - SOBR Destination Offset)
   * @return true if EOP was received and (for hybrid stores) if lag <= threshold
   */
  protected boolean isReadyToServe(PartitionConsumptionState partitionConsumptionState) {
    // Check various short-circuit conditions first.
    if (!partitionConsumptionState.isEndOfPushReceived()) {
      // If the EOP has not been received yet, then for sure we aren't ready
      return false;
    }

    if (partitionConsumptionState.isComplete()) {
      // Since we know the EOP has been received, regular batch store is ready to go!
      return true;
    }

    if (!partitionConsumptionState.isWaitingForReplicationLag()) {
      // If we have already crossed the acceptable lag threshold in the past, then we will stick to that,
      // rather than possibly flip flopping
      return true;
    }

    if (partitionConsumptionState.isHybrid() && !isRealTimeBufferReplayStarted(partitionConsumptionState)) {
      return false;
    }

    int partitionId = partitionConsumptionState.getPartition();
    boolean isLagAcceptable = false;

    try {
      // Looks like none of the short-circuitry fired, so we need to measure lag!
      long offsetThreshold =
          getOffsetToOnlineLagThresholdPerPartition(hybridStoreConfig, storeRepository, storeName, subPartitionCount);
      long producerTimeLagThresholdInSeconds =
          hybridStoreConfig.get().getProducerTimestampLagThresholdToGoOnlineInSeconds();
      String msg = msgForLagMeasurement[partitionId];

      // Log only once a minute per partition.
      boolean shouldLogLag = !REDUNDANT_LOGGING_FILTER.isRedundantException(msg);
      /**
       * If offset lag threshold is set to -1, time lag threshold will be the only criterion for going online.
       */
      if (offsetThreshold >= 0) {
        isLagAcceptable = checkAndLogIfLagIsAcceptableForHybridStore(
            partitionConsumptionState,
            measureHybridOffsetLag(partitionConsumptionState, shouldLogLag),
            offsetThreshold,
            shouldLogLag,
            OFFSET_LAG,
            0);
      }

      /**
       * If the hybrid producer time lag threshold is positive, check the difference between current time and latest
       * producer timestamp; ready-to-serve will not be reported until the diff is smaller than the defined time lag threshold.
       *
       * If timestamp lag threshold is set to -1, offset lag threshold will be the only criterion for going online.
       */
      if (producerTimeLagThresholdInSeconds > 0) {
        long producerTimeLagThresholdInMS = TimeUnit.SECONDS.toMillis(producerTimeLagThresholdInSeconds);
        long latestConsumedProducerTimestamp =
            partitionConsumptionState.getOffsetRecord().getLatestProducerProcessingTimeInMs();
        if (amplificationFactor != 1) {
          latestConsumedProducerTimestamp = getLatestConsumedProducerTimestampWithSubPartition(
              latestConsumedProducerTimestamp,
              partitionConsumptionState);
        }
        boolean timestampLagIsAcceptable = checkAndLogIfLagIsAcceptableForHybridStore(
            partitionConsumptionState,
            LatencyUtils.getElapsedTimeInMs(latestConsumedProducerTimestamp),
            producerTimeLagThresholdInMS,
            shouldLogLag,
            TIME_LAG,
            latestConsumedProducerTimestamp);
        /**
         * If time lag is not acceptable but the producer timestamp of the last message of RT is smaller or equal than
         * the known latest producer timestamp in server, it means ingestion task has reached the end of RT, so it's
         * safe to ignore the time lag.
         *
         * Notice that if EOP is not received, this function will be short circuit before reaching here, so there is
         * no risk of meeting the time lag earlier than expected.
         */
        if (!timestampLagIsAcceptable) {
          String msgIdentifier = msg + "_ignore_time_lag";
          String realTimeTopicKafkaURL;
          Set<String> realTimeTopicKafkaURLs = getRealTimeDataSourceKafkaAddress(partitionConsumptionState);
          if (realTimeTopicKafkaURLs.isEmpty()) {
            throw new VeniceException("Expect a real-time topic Kafka URL for store " + storeName);
          } else if (realTimeTopicKafkaURLs.size() == 1) {
            realTimeTopicKafkaURL = realTimeTopicKafkaURLs.iterator().next();
          } else if (realTimeTopicKafkaURLs.contains(localKafkaServer)) {
            realTimeTopicKafkaURL = localKafkaServer;
          } else {
            throw new VeniceException(
                String.format(
                    "Expect source RT Kafka URLs contains local Kafka URL. Got local "
                        + "Kafka URL %s and RT source Kafka URLs %s",
                    localKafkaServer,
                    realTimeTopicKafkaURLs));
          }

          final PubSubTopic lagMeasurementTopic = pubSubTopicRepository.getTopic(realTimeTopic.getName());
          final PubSubTopicPartition pubSubTopicPartition =
              new PubSubTopicPartitionImpl(lagMeasurementTopic, partitionId);

          // DaVinci and STANDBY checks the local consumption and leaderCompleteState status
          final String lagMeasurementKafkaUrl =
              (isHybridFollower(partitionConsumptionState)) ? localKafkaServer : realTimeTopicKafkaURL;
          TopicManager topicManager = getTopicManager(lagMeasurementKafkaUrl);
          if (!topicManager.containsTopicCached(realTimeTopic)) {
            timestampLagIsAcceptable = true;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
              LOGGER.info(
                  "{} [Time lag] Topic {} doesn't exist; ignoring time lag.",
                  ingestionTaskName,
                  lagMeasurementTopic);
            }
          } else {
            long latestProducerTimestampInTopic =
                topicManager.getProducerTimestampOfLastDataMessageCached(pubSubTopicPartition);
            if (latestProducerTimestampInTopic < 0
                || latestProducerTimestampInTopic <= latestConsumedProducerTimestamp) {
              timestampLagIsAcceptable = true;
              if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
                if (latestProducerTimestampInTopic < 0) {
                  LOGGER.info(
                      "{} [Time lag] Topic {} is empty or all messages have been truncated; ignoring time lag.",
                      ingestionTaskName,
                      lagMeasurementTopic);
                } else {
                  LOGGER.info(
                      "{} [Time lag] Producer timestamp of last message in topic {} "
                          + "partition {}: {}, which is smaller or equal than the known latest producer time: {}. "
                          + "Consumption lag is caught up already.",
                      ingestionTaskName,
                      lagMeasurementTopic,
                      partitionId,
                      latestProducerTimestampInTopic,
                      latestConsumedProducerTimestamp);
                }
              }
            }
          }
        }
        if (offsetThreshold >= 0) {
          /**
           * If both threshold configs are on, both offset lag and time lag must be within thresholds before online.
           */
          isLagAcceptable &= timestampLagIsAcceptable;
        } else {
          isLagAcceptable = timestampLagIsAcceptable;
        }
      }
    } catch (Exception e) {
      String exceptionMsgIdentifier =
          new StringBuilder().append(kafkaVersionTopic).append("_isReadyToServe").toString();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(exceptionMsgIdentifier)) {
        LOGGER.info("Exception when trying to determine if hybrid store is ready to serve: {}", storeName, e);
      }
      isLagAcceptable = false;
    }

    if (isLagAcceptable) {
      amplificationFactorAdapter.executePartitionConsumptionState(
          partitionConsumptionState.getUserPartition(),
          PartitionConsumptionState::lagHasCaughtUp);
    }
    return isLagAcceptable;
  }

  public boolean isReadyToServeAnnouncedWithRTLag() {
    return false;
  }

  protected long getLatestConsumedProducerTimestampWithSubPartition(
      long consumedProducerTimestamp,
      PartitionConsumptionState partitionConsumptionState) {
    long latestConsumedProducerTimestamp = consumedProducerTimestamp;

    IntList subPartitions =
        PartitionUtils.getSubPartitions(partitionConsumptionState.getUserPartition(), amplificationFactor);
    PartitionConsumptionState subPartitionConsumptionState;
    for (int i = 0; i < subPartitions.size(); i++) {
      subPartitionConsumptionState = partitionConsumptionStateMap.get(subPartitions.getInt(i));
      if (subPartitionConsumptionState != null && subPartitionConsumptionState.isEndOfPushReceived()) {
        latestConsumedProducerTimestamp = Math.max(
            latestConsumedProducerTimestamp,
            subPartitionConsumptionState.getOffsetRecord().getLatestProducerProcessingTimeInMs());
      }
    }
    return latestConsumedProducerTimestamp;
  }

  protected abstract boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState);

  /**
   * Measure the hybrid offset lag for partition being tracked in `partitionConsumptionState`.
   */
  protected abstract long measureHybridOffsetLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag);

  /**
   * Check if the ingestion progress has reached to the end of the version topic. This is currently only
   * used {@link LeaderFollowerStoreIngestionTask}.
   */
  protected abstract void reportIfCatchUpVersionTopicOffset(PartitionConsumptionState partitionConsumptionState);

  /**
   * This function will produce a pair of consumer record and a it's derived produced record to the writer buffers maintained by {@link StoreBufferService}.
   * @param consumedRecord : received consumer record
   * @param leaderProducedRecordContext : derived leaderProducedRecordContext
   * @param subPartition
   * @param kafkaUrl
   * @throws InterruptedException
   */
  protected void produceToStoreBufferService(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumedRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs) throws InterruptedException {
    boolean measureTime = emitMetrics.get();
    long queuePutStartTimeInNS = measureTime ? System.nanoTime() : 0;
    storeBufferService.putConsumerRecord(
        consumedRecord,
        this,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs); // blocking call

    if (measureTime) {
      hostLevelIngestionStats.recordConsumerRecordsQueuePutLatency(
          LatencyUtils.getLatencyInMS(queuePutStartTimeInNS),
          currentTimeForMetricsMs);
    }
  }

  /**
   * This function is in charge of producing the consumer records to the writer buffers maintained by {@link StoreBufferService}.
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
   *
   * @param records : received consumer records
   * @param topicPartition
   * @throws InterruptedException
   */
  protected void produceToStoreBufferServiceOrKafka(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    long totalBytesRead = 0;
    double elapsedTimeForPuttingIntoQueue = 0;
    int subPartition = PartitionUtils.getSubPartition(topicPartition, amplificationFactor);
    boolean metricsEnabled = emitMetrics.get();
    long currentTimeForMetricsMs = System.currentTimeMillis();
    for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record: records) {
      long beforeProcessingRecordTimestampNs = System.nanoTime();
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLatestPolledMessageTimestampInMs(currentTimeForMetricsMs);
      }
      if (!shouldProcessRecord(record, subPartition)) {
        if (partitionConsumptionState != null) {
          partitionConsumptionState.updateLatestIgnoredUpstreamRTOffset(kafkaUrl, record.getOffset());
        }
        continue;
      }

      if (record.getKey().isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) record.getValue().payloadUnion;
        if (ControlMessageType.valueOf(controlMessage.controlMessageType) == ControlMessageType.START_OF_PUSH) {
          /**
           * N.B.: The rest of the {@link ControlMessage} types are handled by:
           * {@link #processControlMessage(KafkaMessageEnvelope, ControlMessage, int, long, PartitionConsumptionState)}
           *
           * But for the SOP in particular, we want to process it here, at the start of the pipeline, to ensure that the
           * {@link StoreVersionState} is properly primed, as other functions below this point, but prior to being
           * enqueued into the {@link StoreBufferService} rely on this state to be there.
           */
          processStartOfPush(
              record.getValue(),
              controlMessage,
              record.getTopicPartition().getPartitionNumber(),
              partitionConsumptionStateMap.get(subPartition));
        }
      }

      // Check schema id availability before putting consumer record to drainer queue
      waitReadyToProcessRecord(record);
      // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
      // this call.
      DelegateConsumerRecordResult delegateConsumerRecordResult = delegateConsumerRecord(
          record,
          subPartition,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingRecordTimestampNs,
          currentTimeForMetricsMs);
      switch (delegateConsumerRecordResult) {
        case QUEUED_TO_DRAINER:
          long queuePutStartTimeInNS = metricsEnabled ? System.nanoTime() : 0;

          // blocking call
          storeBufferService
              .putConsumerRecord(record, this, null, subPartition, kafkaUrl, beforeProcessingRecordTimestampNs);

          if (metricsEnabled) {
            elapsedTimeForPuttingIntoQueue += LatencyUtils.getLatencyInMS(queuePutStartTimeInNS);
          }
          break;
        case PRODUCED_TO_KAFKA:
        case SKIPPED_MESSAGE:
        case DUPLICATE_MESSAGE:
          break;
        default:
          throw new VeniceException(
              ingestionTaskName + " received unknown DelegateConsumerRecordResult enum for "
                  + record.getTopicPartition());
      }
      totalBytesRead += record.getPayloadSize();
      // Update the latest message consumed time
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLatestMessageConsumedTimestampInMs(currentTimeForMetricsMs);
      }
    }

    /**
     * Even if the records list is empty, we still need to check quota to potentially resume partition
     */
    storageUtilizationManager.enforcePartitionQuota(topicPartition.getPartitionNumber(), totalBytesRead);

    if (metricsEnabled) {
      if (totalBytesRead > 0) {
        hostLevelIngestionStats.recordTotalBytesReadFromKafkaAsUncompressedSize(totalBytesRead);
      }
      if (elapsedTimeForPuttingIntoQueue > 0) {
        hostLevelIngestionStats
            .recordConsumerRecordsQueuePutLatency(elapsedTimeForPuttingIntoQueue, currentTimeForMetricsMs);
      }

      hostLevelIngestionStats
          .recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage(), currentTimeForMetricsMs);
    }
  }

  // For testing purpose
  List<PartitionExceptionInfo> getPartitionIngestionExceptionList() {
    return this.partitionIngestionExceptionList;
  }

  // For testing purpose
  Set<Integer> getFailedPartitions() {
    return this.failedPartitions;
  }

  private void processIngestionException() {
    partitionIngestionExceptionList.forEach(partitionExceptionInfo -> {
      int exceptionPartition = partitionExceptionInfo.getPartitionId();
      Exception partitionException = partitionExceptionInfo.getException();
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(exceptionPartition);
      if (partitionConsumptionState == null || !partitionConsumptionState.isSubscribed()) {
        LOGGER.warn(
            "Ignoring exception for partition {} for store version {} since this partition has been unsubscribed already.",
            exceptionPartition,
            kafkaVersionTopic,
            partitionException);
        /**
         * Since the partition is already unsubscribed, we will clear the exception to avoid excessive logging, and in theory,
         * this shouldn't happen since {@link #processCommonConsumerAction} will clear the exception list during un-subscribing.
         */
        partitionIngestionExceptionList.set(exceptionPartition, null);
      } else {
        PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, exceptionPartition);
        /**
         * Special handling for current version when encountering {@link MemoryLimitExhaustedException}.
         */
        if (partitionException instanceof MemoryLimitExhaustedException
            || partitionException.getCause() instanceof MemoryLimitExhaustedException
                && isCurrentVersion.getAsBoolean()) {
          LOGGER.warn(
              "Encountered MemoryLimitExhaustedException, and ingestion task will try to reopen the database and"
                  + " resume the consumption after killing ingestion tasks for non current versions");
          /**
           * Pause topic consumption to avoid more damage.
           * We can't unsubscribe it since in some scenario, all the partitions can be unsubscribed, and the ingestion task
           * will end. Even later on, there are avaiable memory space, we can't resume the ingestion task.
           */
          pauseConsumption(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
          LOGGER.info(
              "Pausing consumption of topic: {}, partition: {} because of hitting memory limit",
              pubSubTopicPartition.getPubSubTopic().getName(),
              pubSubTopicPartition.getPartitionNumber());
          runnableForKillIngestionTasksForNonCurrentVersions.run();
          if (storageEngine.hasMemorySpaceLeft()) {
            unSubscribePartition(pubSubTopicPartition, false);
            /**
             * DaVinci ingestion hits memory limit and we would like to retry it in the following way:
             * 1. Kill the ingestion tasks for non-current versions.
             * 2. Reopen the database since the current database in a bad state, where it can't write or sync even
             *    there are rooms (bug in SSTFileManager implementation in RocksDB). Reopen will drop the not-yet-synced
             *    memtable unfortunately.
             * 3. Resubscribe the affected partition.
             */
            LOGGER.info(
                "Ingestion for topic: {}, partition: {} can resume since there are more space reclaimed",
                kafkaVersionTopic,
                exceptionPartition);
            storageEngine.reopenStoragePartition(exceptionPartition);
            // DaVinci is always a follower.
            subscribePartition(pubSubTopicPartition, Optional.empty(), false);
          }
        } else {
          if (!partitionConsumptionState.isCompletionReported()) {
            reportError(partitionException.getMessage(), exceptionPartition, partitionException);

          } else {
            LOGGER.error(
                "Ignoring exception for partition {} for store version {} since this partition is already online. "
                    + "Please engage Venice DEV team immediately.",
                exceptionPartition,
                kafkaVersionTopic,
                partitionException);
          }
          // Unsubscribe the partition to avoid more damages.
          if (partitionConsumptionStateMap.containsKey(exceptionPartition)) {
            // This is not an unsubscribe action from Helix
            unSubscribePartition(new PubSubTopicPartitionImpl(versionTopic, exceptionPartition), false);
          }
        }
      }
    });
  }

  protected void checkIngestionProgress(Store store) throws InterruptedException {
    Exception ingestionTaskException = lastStoreIngestionException.get();
    if (ingestionTaskException != null) {
      throw new VeniceException(
          "Unexpected store ingestion task level exception, will error out the entire"
              + " ingestion task and all its partitions",
          ingestionTaskException);
    }
    if (lastConsumerException != null) {
      throw new VeniceException("Exception thrown by shared consumer", lastConsumerException);
    }

    /**
     * We will unsubscribe all the errored partitions without killing the ingestion task.
     */
    processIngestionException();

    /**
     * Check whether current consumer has any subscription or not since 'poll' function will throw
     * {@link IllegalStateException} with empty subscription.
     */
    if (!consumerHasAnySubscription()) {
      if (++idleCounter <= getMaxIdleCounter()) {
        String message = ingestionTaskName + " Not subscribed to any partitions ";
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
          LOGGER.info(message);
        }

        Thread.sleep(readCycleDelayMs);
      } else {
        if (!hybridStoreConfig.isPresent() && serverConfig.isUnsubscribeAfterBatchpushEnabled() && subscribedCount != 0
            && subscribedCount == forceUnSubscribedCount) {
          String msg =
              ingestionTaskName + " Going back to sleep as consumption has finished and topics are unsubscribed";
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.info(msg);
          }
          Thread.sleep(readCycleDelayMs * 20);
          idleCounter = 0;
        } else {
          maybeCloseInactiveIngestionTask();
        }
      }
      return;
    }
    idleCounter = 0;
    maybeUnsubscribeCompletedPartitions(store);
    recordQuotaMetrics(store);
    recordMaxIdleTime();

    /**
     * While using the shared consumer, we still need to check hybrid quota here since the actual disk usage could change
     * because of compaction or the disk quota could be adjusted even there is no record write.
     * Since {@link #produceToStoreBufferServiceOrKafka} is only being invoked by {@link KafkaConsumerService} when there
     * are available records, this function needs to check whether we need to resume the consumption when there are
     * paused consumption because of hybrid quota violation.
     */
    if (storageUtilizationManager.hasPausedPartitionIngestion()) {
      storageUtilizationManager.checkAllPartitionsQuota();
    }
    Thread.sleep(readCycleDelayMs);
  }

  private void maybeUnsubscribeCompletedPartitions(Store store) {
    if (hybridStoreConfig.isPresent() || (!serverConfig.isUnsubscribeAfterBatchpushEnabled())) {
      return;
    }
    // unsubscribe completed backup version and batch-store versions.
    if (versionNumber <= store.getCurrentVersion()) {
      Set<PubSubTopicPartition> topicPartitionsToUnsubscribe = new HashSet<>();
      for (PartitionConsumptionState state: partitionConsumptionStateMap.values()) {
        if (state.isCompletionReported() && consumerHasSubscription(versionTopic, state)) {
          LOGGER.info(
              "Unsubscribing completed partitions {} of store : {} version : {} current version: {}",
              state.getPartition(),
              store.getName(),
              versionNumber,
              store.getCurrentVersion());
          topicPartitionsToUnsubscribe.add(new PubSubTopicPartitionImpl(versionTopic, state.getPartition()));
          forceUnSubscribedCount++;
        }
      }
      if (topicPartitionsToUnsubscribe.size() != 0) {
        consumerBatchUnsubscribe(topicPartitionsToUnsubscribe);
      }
    }
  }

  private void recordMaxIdleTime() {
    if (emitMetrics.get()) {
      long curTime = System.currentTimeMillis(), oldest = curTime;
      for (PartitionConsumptionState state: partitionConsumptionStateMap.values()) {
        if (state != null) {
          oldest = Math.min(oldest, state.getLatestPolledMessageTimestampInMs());
        }
      }
      versionedIngestionStats.recordMaxIdleTime(storeName, versionNumber, curTime - oldest);
    }
  }

  private void recordQuotaMetrics(Store store) {
    if (emitMetrics.get()) {
      long currentQuota = store.getStorageQuotaInByte();
      long currentTimeForMetricsMs = System.currentTimeMillis();
      hostLevelIngestionStats.recordDiskQuotaAllowed(currentQuota, currentTimeForMetricsMs);
      hostLevelIngestionStats
          .recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage(), currentTimeForMetricsMs);
    }
  }

  public boolean isIngestionTaskActive() {
    return maybeSetIngestionTaskActiveState(true);
  }

  /**
   * Polls the producer for new messages in an infinite loop by a dedicated consumer thread
   * and processes the new messages by current thread.
   */
  @Override
  public void run() {
    boolean doFlush = true;
    try {
      // Update thread name to include topic to make it easy debugging
      Thread.currentThread().setName("venice-SIT-" + kafkaVersionTopic);
      LOGGER.info("Running {}", ingestionTaskName);
      versionedIngestionStats.resetIngestionTaskPushTimeoutGauge(storeName, versionNumber);

      while (isRunning()) {
        Store store = storeRepository.getStoreOrThrow(storeName);
        processConsumerActions(store);
        checkLongRunningTaskState();
        checkIngestionProgress(store);
        maybeSendIngestionHeartbeat();
      }

      List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>(partitionConsumptionStateMap.size());
      // If the ingestion task is stopped gracefully (server stops), persist processed offset to disk
      for (Map.Entry<Integer, PartitionConsumptionState> entry: partitionConsumptionStateMap.entrySet()) {
        /**
         * Now, there are two threads, which could potentially trigger {@link #syncOffset(String, PartitionConsumptionState)}:
         * 1. {@link #processConsumerRecord} that will sync offset based on the consumed byte size.
         * 2. The main thread of ingestion task here, which will checkpoint when gracefully shutting down;
         *
         * We would like to make sure the syncOffset invocation is sequential with the message processing, so here
         * will try to drain all the messages before checkpointing.
         * Here is the detail::
         * If the checkpointing happens in different threads concurrently, there is no guarantee the atomicity of
         * offset and checksum, since the checksum could change in another thread, but the corresponding offset change
         * hasn't been applied yet, when checkpointing happens in current thread.
         */

        Runnable shutdownRunnable = () -> {
          int partition = entry.getKey();
          PartitionConsumptionState partitionConsumptionState = entry.getValue();
          consumerUnSubscribeAllTopics(partitionConsumptionState);

          if (ingestionCheckpointDuringGracefulShutdownEnabled) {
            try {
              waitForAllMessageToBeProcessedFromTopicPartition(
                  new PubSubTopicPartitionImpl(versionTopic, partitionConsumptionState.getPartition()),
                  partitionConsumptionState);
            } catch (InterruptedException e) {
              throw new VeniceException(e);
            }

            this.kafkaDataIntegrityValidator
                .updateOffsetRecordForPartition(partition, partitionConsumptionState.getOffsetRecord());
            updateOffsetMetadataInOffsetRecord(partitionConsumptionState);
            syncOffset(kafkaVersionTopic, partitionConsumptionState);
          }
        };

        if (isDaVinciClient) {
          shutdownFutures.add(CompletableFuture.runAsync(shutdownRunnable, SHUTDOWN_EXECUTOR_FOR_DVC));
        } else {
          /**
           * TODO: evaluate whether we need to apply concurrent shutdown in Venice Server or not.
           */
          shutdownRunnable.run();
        }
      }
      if (isDaVinciClient) {
        /**
         * DaVinci shutdown shouldn't take that long because of high concurrency, and it is fine to specify a high timeout here
         * to avoid infinite wait in case there is some regression.
         */
        CompletableFuture.allOf(shutdownFutures.toArray(new CompletableFuture[0])).get(60, SECONDS);
      }
    } catch (VeniceIngestionTaskKilledException e) {
      LOGGER.info("{} has been killed.", ingestionTaskName);
      statusReportAdapter.reportKilled(partitionConsumptionStateMap.values(), e);
      doFlush = false;
      if (isCurrentVersion.getAsBoolean()) {
        /**
         * Current version can be killed if {@link AggKafkaConsumerService} discovers there are some issues with
         * the producing topics, and here will report metrics for such case.
         */
        handleIngestionException(e);
      }
    } catch (VeniceChecksumException e) {
      /**
       * It's possible to receive checksum verification failure exception here from the above syncOffset() call.
       * If this task is getting closed anyway (most likely due to SN being shut down), we should not report this replica
       * as ERROR. just record the relevant metrics.
       */
      recordChecksumVerificationFailure();
      if (!isRunning()) {
        LOGGER.info(
            "{} encountered checksum verification failure, skipping error reporting because server is shutting down",
            ingestionTaskName,
            e);
      } else {
        handleIngestionException(e);
      }
    } catch (VeniceTimeoutException e) {
      versionedIngestionStats.setIngestionTaskPushTimeoutGauge(storeName, versionNumber);
      handleIngestionException(e);
    } catch (Exception e) {
      // After reporting error to controller, controller will ignore the message from this replica if job is aborted.
      // So even this storage node recover eventually, controller will not confused.
      // If job is not aborted, controller is open to get the subsequent message from this replica(if storage node was
      // recovered, it will send STARTED message to controller again)

      if (!isRunning() && ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
        // Known exceptions during graceful shutdown of storage server. Report error only if the server is still
        // running.
        LOGGER.info("{} interrupted, skipping error reporting because server is shutting down", ingestionTaskName, e);
        return;
      }
      handleIngestionException(e);
    } catch (Throwable t) {
      handleIngestionThrowable(t);
    } finally {
      internalClose(doFlush);
    }
  }

  private void handleIngestionException(Exception e) {
    LOGGER.error("{} has failed.", ingestionTaskName, e);
    reportError(partitionConsumptionStateMap.values(), errorPartitionId, "Caught Exception during ingestion.", e);
    hostLevelIngestionStats.recordIngestionFailure();
  }

  private void handleIngestionThrowable(Throwable t) {
    LOGGER.error("{} has failed.", ingestionTaskName, t);
    reportError(
        partitionConsumptionStateMap.values(),
        errorPartitionId,
        "Caught non-exception Throwable during ingestion.",
        new VeniceException(t));
    hostLevelIngestionStats.recordIngestionFailure();
  }

  private void reportError(
      Collection<PartitionConsumptionState> pcsList,
      int partitionId,
      String message,
      Exception consumerEx) {
    if (pcsList.isEmpty()) {
      statusReportAdapter.reportError(partitionId, message, consumerEx);
    } else {
      statusReportAdapter.reportError(pcsList, message, consumerEx);
    }
  }

  private void internalClose(boolean doFlush) {
    // Only reset Offset Messages are important, subscribe/unsubscribe will be handled
    // on the restart by Helix Controller notifications on the new StoreIngestionTask.
    try {
      this.storeRepository.unregisterStoreDataChangedListener(this.storageUtilizationManager);
      for (ConsumerAction message: consumerActionsQueue) {
        ConsumerActionType opType = message.getType();
        if (opType == ConsumerActionType.RESET_OFFSET) {
          String topic = message.getTopic();
          int partition = message.getPartition();
          LOGGER.info("{} Cleanup Reset OffSet : Topic {} Partition Id {}", ingestionTaskName, topic, partition);
          storageMetadataService.clearOffset(topic, partition);
        } else {
          LOGGER.info("{} Cleanup ignoring the Message {}", ingestionTaskName, message);
        }
      }
    } catch (Exception e) {
      LOGGER.error("{} Error while resetting offset.", ingestionTaskName, e);
    }
    // Unsubscribe any topic partitions related to this version topic from the shared consumer.
    aggKafkaConsumerService.unsubscribeAll(versionTopic);
    LOGGER.info("Detached Kafka consumer(s) for version topic: {}", kafkaVersionTopic);
    try {
      partitionConsumptionStateMap.values().parallelStream().forEach(PartitionConsumptionState::unsubscribe);
      partitionConsumptionStateMap.clear();
    } catch (Exception e) {
      LOGGER.error("{} Error while unsubscribing topic.", ingestionTaskName, e);
    }
    try {
      closeVeniceWriters(doFlush);
    } catch (Exception e) {
      LOGGER.error("Error while closing venice writers", e);
    }

    try {
      closeVeniceViewWriters();
    } catch (Exception e) {
      LOGGER.error("Error while closing venice view writer", e);
    }

    if (topicManagerRepository != null) {
      topicManagerRepository.invalidateTopicManagerCaches(versionTopic);
    }

    close();

    synchronized (this) {
      notifyAll();
    }

    LOGGER.info("Store ingestion task for store: {} is closed", kafkaVersionTopic);
  }

  public void closeVeniceWriters(boolean doFlush) {
  }

  protected void closeVeniceViewWriters() {
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processConsumerActions(Store store) throws InterruptedException {
    Instant startTime = Instant.now();
    for (;;) {
      // Do not want to remove a message from the queue unless it has been processed.
      ConsumerAction action = consumerActionsQueue.peek();
      if (action == null) {
        break;
      }
      final long actionProcessStartTimeInMs = System.currentTimeMillis();
      try {
        LOGGER.info(
            "Starting consumer action {}. Latency from creating action to starting action {}ms",
            action,
            LatencyUtils.getElapsedTimeInMs(action.getCreateTimestampInMs()));
        action.incrementAttempt();
        processConsumerAction(action, store);
        action.getFuture().complete(null);
        // Remove the action that is processed recently (not necessarily the head of consumerActionsQueue).
        if (consumerActionsQueue.remove(action)) {
          partitionToPendingConsumerActionCountMap.get(action.getPartition()).decrementAndGet();
        }
        LOGGER.info(
            "Finished consumer action {} in {}ms",
            action,
            LatencyUtils.getElapsedTimeInMs(actionProcessStartTimeInMs));
      } catch (VeniceIngestionTaskKilledException | InterruptedException e) {
        action.getFuture().completeExceptionally(e);
        throw e;
      } catch (Throwable e) {
        if (action.getAttemptsCount() <= MAX_CONSUMER_ACTION_ATTEMPTS) {
          LOGGER.warn("Failed to process consumer action {}, will retry later.", action, e);
          return;
        }
        LOGGER.error(
            "Failed to execute consumer action {} after {} attempts. Total elapsed time: {}ms",
            action,
            action.getAttemptsCount(),
            LatencyUtils.getElapsedTimeInMs(actionProcessStartTimeInMs),
            e);
        // Mark action as failed since it has exhausted all the retries.
        action.getFuture().completeExceptionally(e);
        // After MAX_CONSUMER_ACTION_ATTEMPTS retries we should give up and error the ingestion task.
        PartitionConsumptionState state = partitionConsumptionStateMap.get(action.getPartition());

        // Remove the action that is failed to execute recently (not necessarily the head of consumerActionsQueue).
        if (consumerActionsQueue.remove(action)) {
          partitionToPendingConsumerActionCountMap.get(action.getPartition()).decrementAndGet();
        }
        if (state != null && !state.isCompletionReported()) {
          reportError(
              "Error when processing consumer action: " + action,
              action.getPartition(),
              new VeniceException(e));
        }
      }
    }
    if (emitMetrics.get()) {
      hostLevelIngestionStats.recordProcessConsumerActionLatency(Duration.between(startTime, Instant.now()).toMillis());
    }
  }

  /**
   * Applies name resolution to all Kafka URLs in the provided TopicSwitch. Useful for translating URLs that came from
   * a different runtime (e.g. from the controller, or from state persisted by a previous run of the same server).
   *
   * @return the same TopicSwitch, mutated such that all Kafka URLs it contains are guaranteed to be usable by
   *         the current Venice server instance
   */
  protected TopicSwitch resolveSourceKafkaServersWithinTopicSwitch(TopicSwitch originalTopicSwitch) {
    if (originalTopicSwitch == null || originalTopicSwitch.sourceKafkaServers == null) {
      return originalTopicSwitch;
    }
    List<CharSequence> returnSet = new ArrayList<>(originalTopicSwitch.sourceKafkaServers.size());
    for (CharSequence url: originalTopicSwitch.sourceKafkaServers) {
      returnSet.add(kafkaClusterUrlResolver.apply(url.toString()));
    }
    originalTopicSwitch.sourceKafkaServers = returnSet;
    return originalTopicSwitch;
  }

  protected static long getOffsetToOnlineLagThresholdPerPartition(
      Optional<HybridStoreConfig> hybridStoreConfig,
      ReadOnlyStoreRepository storeRepository,
      String storeName,
      int subPartitionCount) {
    if (!hybridStoreConfig.isPresent()) {
      throw new VeniceException("This is not a hybrid store: " + storeName);
    }
    long lagThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
    if (lagThreshold < 0) {
      /**
       * Offset lag threshold is disabled, use time lag.
       */
      return lagThreshold;
    }

    return Math.max(lagThreshold / subPartitionCount, 1);
  }

  private void checkConsumptionStateWhenStart(
      OffsetRecord offsetRecord,
      PartitionConsumptionState newPartitionConsumptionState) {
    int partition = newPartitionConsumptionState.getPartition();
    // Once storage node restart, send the "START" status to controller to rebuild the task status.
    // If this storage node has never consumed data from this topic, instead of sending "START" here, we send it
    // once START_OF_PUSH message has been read.
    if (offsetRecord.getLocalVersionTopicOffset() > 0) {
      StoreVersionState storeVersionState = storageEngine.getStoreVersionState();
      if (storeVersionState != null) {
        boolean sorted = storeVersionState.sorted;
        /**
         * Put TopicSwitch message into in-memory state.
         */
        TopicSwitch resolvedTopicSwitch = resolveSourceKafkaServersWithinTopicSwitch(storeVersionState.topicSwitch);
        newPartitionConsumptionState.setTopicSwitch(
            resolvedTopicSwitch == null
                ? null
                : new TopicSwitchWrapper(
                    resolvedTopicSwitch,
                    pubSubTopicRepository.getTopic(resolvedTopicSwitch.sourceTopicName.toString())));

        /**
         * Notify the underlying store engine about starting batch push.
         */
        beginBatchWrite(partition, sorted, newPartitionConsumptionState);

        newPartitionConsumptionState.setStartOfPushTimestamp(storeVersionState.startOfPushTimestamp);
        newPartitionConsumptionState.setEndOfPushTimestamp(storeVersionState.endOfPushTimestamp);

        statusReportAdapter.reportRestarted(newPartitionConsumptionState);
      }
      /**
       * If StoreVersionState doesn't exist, we would create it when we process
       * START_OF_PUSH message for the first time.
       */
    }
    /**
     * TODO: The behavior for completed partition is not consistent here.
     *
     * When processing subscription action for restart scenario, {@link #consumer} won't subscribe the topic
     * partition if it is already completed.
     * In normal case (not completed right away), {@link #consumer} will continue subscribing the topic partition
     * even after receiving the 'EOP' control message (no auto-unsubscription happens).
     *
     * From my understanding, at least we should keep them consistent to avoid confusion.
     *
     * Possible proposals:
     * 1. (Preferred) Auto-unsubscription when receiving EOP for batch store. With this way,
     * the unused consumer thread (not processing any kafka message) will be collected.
     * 2. Always keep subscription no matter what happens.
     */

    // Second, take care of informing the controller about our status, and starting consumption
    /**
     * There could be two cases in this scenario:
     * 1. The job is completed, so Controller will ignore any status message related to the completed/archived job.
     * 2. The job is still running: some partitions are in 'ONLINE' state, but other partitions are still in
     * 'BOOTSTRAP' state.
     * In either case, StoreIngestionTask should report 'started' => ['progress' => ] 'completed' to accomplish
     * task state transition in Controller.
     */
    try {
      // Compare the offset lag is acceptable or not, if acceptable, report completed directly, otherwise rely on the
      // normal ready-to-server checker.
      boolean isCompletedReport = false;
      long offsetLagDeltaRelaxFactor = serverConfig.getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart();
      long previousOffsetLag = newPartitionConsumptionState.getOffsetRecord().getOffsetLag();
      if (hybridStoreConfig.isPresent() && newPartitionConsumptionState.isEndOfPushReceived()) {
        long offsetLagThreshold =
            getOffsetToOnlineLagThresholdPerPartition(hybridStoreConfig, storeRepository, storeName, subPartitionCount);
        // Only enable this feature with positive offset lag delta relax factor and offset lag threshold.
        if (offsetLagDeltaRelaxEnabled && offsetLagThreshold > 0) {
          long offsetLag = measureHybridOffsetLag(newPartitionConsumptionState, true);
          if (previousOffsetLag != OffsetRecord.DEFAULT_OFFSET_LAG) {
            LOGGER.info(
                "Checking offset Lag behavior: current offset lag: {}, previous offset lag: {}, offset lag threshold: {}",
                offsetLag,
                previousOffsetLag,
                offsetLagThreshold);
            if (offsetLag < previousOffsetLag + offsetLagDeltaRelaxFactor * offsetLagThreshold) {
              amplificationFactorAdapter.executePartitionConsumptionState(
                  newPartitionConsumptionState.getUserPartition(),
                  PartitionConsumptionState::lagHasCaughtUp);
              reportCompleted(newPartitionConsumptionState, true);
              isCompletedReport = true;
            }
            // Clear offset lag in metadata, it is only used in restart.
            newPartitionConsumptionState.getOffsetRecord().setOffsetLag(OffsetRecord.DEFAULT_OFFSET_LAG);
          }
        }
      }
      if (!isCompletedReport) {
        defaultReadyToServeChecker.apply(newPartitionConsumptionState);
      }
    } catch (VeniceInconsistentStoreMetadataException e) {
      hostLevelIngestionStats.recordInconsistentStoreMetadata();
      // clear the local store metadata and the replica will be rebuilt from scratch upon retry as part of
      // processConsumerActions.
      storageMetadataService.clearOffset(kafkaVersionTopic, partition);
      storageMetadataService.clearStoreVersionState(kafkaVersionTopic);
      kafkaDataIntegrityValidator.clearPartition(partition);
      throw e;
    }
  }

  protected void processCommonConsumerAction(ConsumerAction consumerAction) throws InterruptedException {
    PubSubTopicPartition topicPartition = consumerAction.getTopicPartition();
    LeaderFollowerStateType leaderState = consumerAction.getLeaderState();
    int partition = topicPartition.getPartitionNumber();
    String topic = topicPartition.getPubSubTopic().getName();
    ConsumerActionType operation = consumerAction.getType();
    switch (operation) {
      case SUBSCRIBE:
        // Clear the error partition tracking
        partitionIngestionExceptionList.set(partition, null);
        // Regardless of whether it's Helix action or not, remove the partition from alerts as long as server decides
        // to start or retry the ingestion.
        failedPartitions.remove(partition);
        // Drain the buffered message by last subscription.
        storeBufferService.drainBufferedRecordsFromTopicPartition(topicPartition);
        subscribedCount++;

        // Get the last persisted Offset record from metadata service
        OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topic, partition);

        // Let's try to restore the state retrieved from the OffsetManager
        PartitionConsumptionState newPartitionConsumptionState =
            new PartitionConsumptionState(partition, amplificationFactor, offsetRecord, hybridStoreConfig.isPresent());
        newPartitionConsumptionState.setLeaderFollowerState(leaderState);

        partitionConsumptionStateMap.put(partition, newPartitionConsumptionState);
        kafkaDataIntegrityValidator.setPartitionState(partition, offsetRecord);

        long consumptionStatePrepTimeStart = System.currentTimeMillis();
        if (!checkDatabaseIntegrity(partition, topic, offsetRecord, newPartitionConsumptionState)) {
          LOGGER.warn(
              "Restart ingestion from the beginning by resetting OffsetRecord for topic: {} and partition: {}",
              topic,
              partition);
          resetOffset(partition, topicPartition, true);
          newPartitionConsumptionState = partitionConsumptionStateMap.get(partition);
          newPartitionConsumptionState.setLeaderFollowerState(leaderState);
          offsetRecord = newPartitionConsumptionState.getOffsetRecord();
        }

        checkConsumptionStateWhenStart(offsetRecord, newPartitionConsumptionState);
        reportIfCatchUpVersionTopicOffset(newPartitionConsumptionState);
        versionedIngestionStats.recordSubscribePrepLatency(
            storeName,
            versionNumber,
            LatencyUtils.getElapsedTimeInMs(consumptionStatePrepTimeStart));
        /**
         * If it is already elected to LEADER, we should subscribe to leader topic in the offset record, instead of VT.
         * For now, this will only be triggered by ingestion isolation, as it is passing LEADER state from forked process
         * to main process when it completed ingestion in the forked process.
         */
        if (leaderState.equals(LeaderFollowerStateType.LEADER)) {
          startConsumingAsLeader(newPartitionConsumptionState);
        } else {
          updateLeaderTopicOnFollower(newPartitionConsumptionState);
          reportStoreVersionTopicOffsetRewindMetrics(newPartitionConsumptionState);

          // Subscribe to local version topic.
          consumerSubscribe(
              newPartitionConsumptionState.getSourceTopicPartition(topicPartition.getPubSubTopic()),
              offsetRecord.getLocalVersionTopicOffset(),
              localKafkaServer);
          LOGGER.info(
              "{} subscribed to: {} Offset {}",
              ingestionTaskName,
              topicPartition,
              offsetRecord.getLocalVersionTopicOffset());
        }
        storageUtilizationManager.initPartition(partition);
        break;
      case UNSUBSCRIBE:
        LOGGER.info("{} Unsubscribing to: {}", ingestionTaskName, topicPartition);
        PartitionConsumptionState consumptionState = partitionConsumptionStateMap.get(partition);
        forceUnSubscribedCount--;
        subscribedCount--;
        if (consumptionState != null) {
          consumerUnSubscribeAllTopics(consumptionState);
        }
        /**
         * The subscribed flag is turned off, so that drainer thread will skip messages for this partition.
         * Basically, now is the cut off time; all buffered messages in drainer queue will be dropped, to speed up
         * the unsubscribe process.
         */
        if (consumptionState != null) {
          consumptionState.unsubscribe();
        }

        // Drain the buffered message by last subscription.
        waitForAllMessageToBeProcessedFromTopicPartition(topicPartition, consumptionState);

        /**
         * If state transition model is still hanging on waiting for the released latch, but unsubscription happens,
         * we should make a call to state model to release the latch.
         *
         * In normal case, if a state model is pending on completing a transition message, no new transition message
         * will be sent, meaning UNSUBSCRIBE won't happen if the model is waiting for the latch to release (push complete
         * or fail); however, if ZK disconnection/reconnection happens, state transition will be reset while the latch
         * is still not released; in this case, reset should make sure to restore all internal states, including
         * releasing latch.
         */
        if (consumptionState != null) {
          statusReportAdapter.reportStopped(consumptionState);
        }

        /**
         * Since the processing of the buffered messages are using {@link #partitionConsumptionStateMap} and
         * {@link #kafkaDataValidationService}, we would like to drain all the buffered messages before cleaning up those
         * two variables to avoid the race condition.
         */
        partitionConsumptionStateMap.remove(partition);
        storageUtilizationManager.removePartition(partition);
        kafkaDataIntegrityValidator.clearPartition(partition);
        // Reset the error partition tracking
        PartitionExceptionInfo partitionExceptionInfo = partitionIngestionExceptionList.get(partition);
        if (partitionExceptionInfo != null) {
          partitionIngestionExceptionList.set(partition, null);
          LOGGER.info(
              "{} Unsubscribed to: {}, which has errored with exception",
              ingestionTaskName,
              topicPartition,
              partitionExceptionInfo.getException());
        } else {
          LOGGER.info("{} Unsubscribed to: {}", ingestionTaskName, topicPartition);
        }
        // Only remove the partition from the maintained set if it's triggered by Helix.
        // Otherwise, keep it in the set since it is used for alerts; alerts should be sent out if unsubscription
        // happens due to internal errors.
        if (consumerAction.isHelixTriggeredAction()) {
          failedPartitions.remove(partition);
        }
        break;
      case RESET_OFFSET:
        resetOffset(partition, topicPartition, false);
        break;
      case KILL:
        LOGGER.info("Kill this consumer task for Topic: {}", topic);
        // Throw the exception here to break the consumption loop, and then this task is marked as error status.
        throw new VeniceIngestionTaskKilledException(KILLED_JOB_MESSAGE + topic);
      default:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + getClass().getName());
    }
  }

  private void resetOffset(int partition, PubSubTopicPartition topicPartition, boolean restartIngestion) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);

    if (partitionConsumptionState != null
        && (restartIngestion || consumerHasSubscription(topicPartition.getPubSubTopic(), partitionConsumptionState))) {
      if (restartIngestion) {
        LOGGER.info("Reset offset to restart ingestion for: {}", topicPartition);
      } else {
        LOGGER.error(
            "This shouldn't happen since unsubscription should happen before reset offset for: {}",
            topicPartition);
      }
      /*
       * Only update the consumer and partitionConsumptionStateMap when consumer actually has
       * subscription to this topic/partition; otherwise, we would blindly update the StateMap
       * and mess up other operations on the StateMap.
       */
      try {
        consumerResetOffset(topicPartition.getPubSubTopic(), partitionConsumptionState);
        LOGGER.info("{} Reset OffSet : {}", ingestionTaskName, topicPartition);
      } catch (PubSubUnsubscribedTopicPartitionException e) {
        LOGGER.error(
            "{} Kafka consumer should have subscribed to the partition already but it fails "
                + "on resetting offset for: {}",
            ingestionTaskName,
            topicPartition);
      }
      partitionConsumptionStateMap.put(
          partition,
          new PartitionConsumptionState(
              partition,
              amplificationFactor,
              new OffsetRecord(partitionStateSerializer),
              hybridStoreConfig.isPresent()));
      storageUtilizationManager.initPartition(partition);
      // Reset the error partition tracking
      partitionIngestionExceptionList.set(partition, null);
      failedPartitions.remove(partition);
    } else {
      LOGGER.info(
          "{} No need to reset offset by Kafka consumer, since the consumer is not subscribing: {}",
          ingestionTaskName,
          topicPartition);
    }
    kafkaDataIntegrityValidator.clearPartition(partition);
    storageMetadataService.clearOffset(topicPartition.getPubSubTopic().getName(), partition);
  }

  /**
   * This function checks, for a store, if its persisted offset is greater than the end offset of its
   * corresponding Kafka topic (indicating an offset rewind event, thus a potential data loss in Kafka) and increases
   * related sensor counter values.
   */
  private void reportStoreVersionTopicOffsetRewindMetrics(PartitionConsumptionState pcs) {
    long offset = pcs.getLatestProcessedLocalVersionTopicOffset();
    if (offset == OffsetRecord.LOWEST_OFFSET) {
      return;
    }
    /**
     * N.B.: We do not want to use {@link #getTopicPartitionEndOffSet(String, PubSubTopic, int)} because it can return
     *       a cached value which will result in a false positive in the below check.
     */
    long endOffset = aggKafkaConsumerService.getLatestOffsetBasedOnMetrics(
        localKafkaServer,
        versionTopic,
        new PubSubTopicPartitionImpl(versionTopic, pcs.getPartition()));
    // Proceed if persisted OffsetRecord exists and has meaningful content.
    if (endOffset >= 0 && offset > endOffset) {
      // report offset rewind.
      LOGGER.warn(
          "Offset rewind for version topic: {}, partition: {}, persisted record offset: {}, Kafka topic partition end-offset: {}",
          kafkaVersionTopic,
          pcs.getPartition(),
          offset,
          endOffset);
      versionedIngestionStats.recordVersionTopicEndOffsetRewind(storeName, versionNumber);
    }
  }

  /**
   * @return the end offset in kafka for the topic partition in SIT, or a negative value if it failed to get it.
   *
   * N.B.: The returned end offset is the last successfully replicated message plus one. If the partition has never been
   * written to, the end offset is 0.
   */
  protected long getTopicPartitionEndOffSet(String kafkaUrl, PubSubTopic pubSubTopic, int partition) {
    long offsetFromConsumer = aggKafkaConsumerService
        .getLatestOffsetBasedOnMetrics(kafkaUrl, versionTopic, new PubSubTopicPartitionImpl(pubSubTopic, partition));
    if (offsetFromConsumer >= 0) {
      return offsetFromConsumer;
    }
    return getTopicManager(kafkaUrl).getLatestOffsetCached(pubSubTopic, partition);
  }

  protected long getPartitionOffsetLagBasedOnMetrics(String kafkaSourceAddress, PubSubTopic topic, int partition) {
    return aggKafkaConsumerService
        .getOffsetLagBasedOnMetrics(kafkaSourceAddress, versionTopic, new PubSubTopicPartitionImpl(topic, partition));
  }

  protected abstract void checkLongRunningTaskState() throws InterruptedException;

  protected abstract void processConsumerAction(ConsumerAction message, Store store) throws InterruptedException;

  protected abstract Set<String> getConsumptionSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState);

  protected void startConsumingAsLeader(PartitionConsumptionState partitionConsumptionState) {
    throw new UnsupportedOperationException("Leader consumption should only happen in L/F mode!");
  }

  protected Set<String> getRealTimeDataSourceKafkaAddress(PartitionConsumptionState partitionConsumptionState) {
    return localKafkaServerSingletonSet;
  }

  public PartitionConsumptionState getPartitionConsumptionState(int partitionId) {
    return partitionConsumptionStateMap.get(partitionId);
  }

  public boolean hasAnyPartitionConsumptionState(Predicate<PartitionConsumptionState> pcsPredicate) {
    for (Map.Entry<Integer, PartitionConsumptionState> partitionToConsumptionState: partitionConsumptionStateMap
        .entrySet()) {
      if (pcsPredicate.test(partitionToConsumptionState.getValue())) {
        return true;
      }
    }
    return false;
  }

  public int getFailedIngestionPartitionCount() {
    return failedPartitions.size();
  }

  /**
   * Common record check for different state models:
   * check whether server continues receiving messages after EOP for a batch-only store.
   */
  protected boolean shouldProcessRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record, int subPartition) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);

    if (partitionConsumptionState == null) {
      String msg = "Topic : " + kafkaVersionTopic + " Partition : " + subPartition
          + " has been unsubscribed, skip this record that has offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getOffset());
      }
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      String msg = "Topic : " + kafkaVersionTopic + " Partition : " + subPartition
          + " is already errored, skip this record that has offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getOffset());
      }
      return false;
    }

    if (partitionConsumptionState.isEndOfPushReceived() && partitionConsumptionState.isBatchOnly()) {
      KafkaKey key = record.getKey();
      KafkaMessageEnvelope value = record.getValue();
      if (key.isControlMessage()
          && ControlMessageType.valueOf((ControlMessage) value.payloadUnion) == ControlMessageType.END_OF_SEGMENT) {
        // Still allow END_OF_SEGMENT control message
        return true;
      }
      // emit metric for unexpected messages
      if (emitMetrics.get()) {
        hostLevelIngestionStats.recordUnexpectedMessage();
      }

      // Report such kind of message once per minute to reduce logging volume
      /*
       * TODO: right now, if we update a store to enable hybrid, {@link StoreIngestionTask} for the existing versions
       * won't know it since {@link #hybridStoreConfig} parameter is passed during construction.
       *
       * So far, to make hybrid store/incremental store work, customer needs to do a new push after enabling hybrid/
       * incremental push feature of the store.
       */
      String message = "The record was received after 'EOP', but the store: " + kafkaVersionTopic
          + " is neither hybrid nor incremental push enabled, so will skip it.";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
        LOGGER.warn(message);
      }
      return false;
    }
    return true;
  }

  protected boolean shouldPersistRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record,
      PartitionConsumptionState partitionConsumptionState) {
    int partitionId = record.getTopicPartition().getPartitionNumber();
    if (failedPartitions.contains(partitionId)) {
      String msg = "Errors already exist in partition: " + partitionId + " for resource: " + kafkaVersionTopic
          + ", skipping this record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info("{} that has offset {}", msg, record.getOffset());
      }
      return false;
    }
    if (partitionConsumptionState == null || !partitionConsumptionState.isSubscribed()) {
      String msg =
          "Topic " + kafkaVersionTopic + " Partition " + partitionId + " has been unsubscribed, skip this record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info("{} that has offset {}", msg, record.getOffset());
      }
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      String msg = "Topic " + kafkaVersionTopic + " Partition " + partitionId + " is already errored, skip this record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info("{} that has offset {}", msg, record.getOffset());
      }
      return false;
    }

    if (this.suppressLiveUpdates && partitionConsumptionState.isCompletionReported()) {
      String msg = "Skipping message as live update suppression is enabled and store: " + kafkaVersionTopic
          + " partition " + partitionId + " is already ready to serve, these are buffered records in the queue.";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg);
      }
      return false;
    }

    /*
     * If ingestion isolation is enabled, when completion is reported for a partition, we don't need to persist the remaining
     * records in the drainer queue, as per ingestion isolation design, we will unsubscribe topic partition in child process
     * and re-subscribe it in main process, thus these records can be processed in main process instead without slowing down
     * the unsubscribe action.
     */
    if (this.isIsolatedIngestion && partitionConsumptionState.isCompletionReported()) {
      String msg = "Skipping message as it is using ingestion isolation and store: " + kafkaVersionTopic + " partition "
          + partitionId + " is already ready to serve, these are buffered records in the queue.";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg);
      }
      return false;
    }

    return true;
  }

  /**
   * This function will be invoked in {@link StoreBufferService} to process buffered {@link PubSubMessage}.
   */
  public void processConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    // The partitionConsumptionStateMap can be modified by other threads during consumption (for example when
    // unsubscribing)
    // in order to maintain thread safety, we hold onto the reference to the partitionConsumptionState and pass that
    // reference to all downstream methods so that all offset persistence operations use the same
    // partitionConsumptionState
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
    if (!shouldPersistRecord(record, partitionConsumptionState)) {
      return;
    }

    int recordSize = 0;
    try {
      recordSize = internalProcessConsumerRecord(
          record,
          partitionConsumptionState,
          leaderProducedRecordContext,
          kafkaUrl,
          beforeProcessingRecordTimestampNs);
    } catch (FatalDataValidationException e) {
      int faultyPartition = record.getTopicPartition().getPartitionNumber();
      String errorMessage;
      if (amplificationFactor != 1 && record.getTopicPartition().getPubSubTopic().isRealTime()) {
        errorMessage = "Fatal data validation problem with in RT topic partition " + faultyPartition + ", offset "
            + record.getOffset() + ", leaderSubPartition: " + subPartition;
      } else {
        errorMessage =
            "Fatal data validation problem with partition " + faultyPartition + ", offset " + record.getOffset();
      }
      // TODO need a way to safeguard DIV errors from backup version that have once been current (but not anymore)
      // during re-balancing
      boolean needToUnsub = !(isCurrentVersion.getAsBoolean() || partitionConsumptionState.isEndOfPushReceived());
      if (needToUnsub) {
        errorMessage += ". Consumption will be halted.";
        statusReportAdapter.reportError(Collections.singletonList(partitionConsumptionState), errorMessage, e);
        unSubscribePartition(new PubSubTopicPartitionImpl(versionTopic, faultyPartition));
      } else {
        LOGGER.warn(
            "{}. However, {} is the current version or EOP is already received so consumption will continue. {}",
            errorMessage,
            kafkaVersionTopic,
            e.getMessage());
      }
    } catch (VeniceMessageException | UnsupportedOperationException e) {
      throw new VeniceException(
          ingestionTaskName + " : Received an exception for message at partition: "
              + record.getTopicPartition().getPartitionNumber() + ", offset: " + record.getOffset() + ". Bubbling up.",
          e);
    }

    if (diskUsage.isDiskFull(recordSize)) {
      throw new VeniceException(
          "Disk is full: throwing exception to error push: " + storeName + " version " + versionNumber + ". "
              + diskUsage.getDiskStatus());
    }

    /*
     * Report ingestion throughput metric based on the store version
     */
    if (!record.getKey().isControlMessage()) { // skip control messages
      versionedIngestionStats.recordBytesConsumed(storeName, versionNumber, recordSize);
      versionedIngestionStats.recordRecordsConsumed(storeName, versionNumber);

      /*
       * Meanwhile, contribute to the host-level ingestion throughput rate, which aggregates the consumption rate across
       * all store versions.
       */
      hostLevelIngestionStats.recordTotalBytesConsumed(recordSize);
      hostLevelIngestionStats.recordTotalRecordsConsumed();

      /*
       * Also update this stats separately for Leader and Follower.
       */
      recordProcessedRecordStats(partitionConsumptionState, recordSize);
      partitionConsumptionState.incrementProcessedRecordSizeSinceLastSync(recordSize);
    }
    reportIfCatchUpVersionTopicOffset(partitionConsumptionState);

    long syncBytesInterval = partitionConsumptionState.isDeferredWrite()
        ? databaseSyncBytesIntervalForDeferredWriteMode
        : databaseSyncBytesIntervalForTransactionalMode;
    boolean recordsProcessedAboveSyncIntervalThreshold = (syncBytesInterval > 0
        && (partitionConsumptionState.getProcessedRecordSizeSinceLastSync() >= syncBytesInterval));
    defaultReadyToServeChecker.apply(partitionConsumptionState, recordsProcessedAboveSyncIntervalThreshold);

    /**
     * Syncing offset checking in syncOffset() should be the very last step for processing a record.
     *
     * Check whether offset metadata checkpoint will happen; if so, update the producer states recorded in OffsetRecord
     * with the updated producer states maintained in {@link #kafkaDataIntegrityValidator}
     */
    boolean syncOffset =
        shouldSyncOffset(partitionConsumptionState, syncBytesInterval, record, leaderProducedRecordContext);

    if (syncOffset) {
      /**
       * Offset metadata and producer states must be updated at the same time in OffsetRecord; otherwise, one checkpoint
       * could be ahead of the other.
       */
      OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
      /**
       * The reason to transform the internal state only during checkpointing is that
       * the intermediate checksum generation is an expensive operation.
       */
      this.kafkaDataIntegrityValidator.updateOffsetRecordForPartition(subPartition, offsetRecord);
      // update the offset metadata in the OffsetRecord
      updateOffsetMetadataInOffsetRecord(partitionConsumptionState);
      syncOffset(kafkaVersionTopic, partitionConsumptionState);
    }
  }

  /**
   * Retrieve current LeaderFollowerState from partition's PCS. This method is used by IsolatedIngestionServer to sync
   * user-partition LeaderFollower status from child process to parent process in ingestion isolation.
   */
  public LeaderFollowerStateType getLeaderState(int partition) {
    return amplificationFactorAdapter.getLeaderState(partition);
  }

  /**
   * Update the offset metadata in OffsetRecord in the following cases:
   * 1. A ControlMessage other than Start_of_Segment and End_of_Segment is processed
   * 2. The size of total processed message has exceeded a threshold: {@link #databaseSyncBytesIntervalForDeferredWriteMode}
   *    for batch data and {@link #databaseSyncBytesIntervalForTransactionalMode} for real-time updates.
   * For RocksDB, too frequent sync will produce lots of small level-0 SST files, which makes the compaction very inefficient.
   * Hence, we want to avoid the database sync for the following cases:
   * 1. Every ControlMessage
   * 2. Record count based strategy, which doesn't work well for stores with very small key/value pairs.
   */
  private boolean shouldSyncOffset(
      PartitionConsumptionState pcs,
      long syncBytesInterval,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record,
      LeaderProducedRecordContext leaderProducedRecordContext) {
    boolean syncOffset = false;
    if (record.getKey().isControlMessage()) {
      ControlMessage controlMessage = (leaderProducedRecordContext == null
          ? (ControlMessage) record.getValue().payloadUnion
          : (ControlMessage) leaderProducedRecordContext.getValueUnion());
      final ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
      /**
       * We don't want to sync offset/database for every control message since it could trigger RocksDB to generate
       * a lot of small level-0 SST files, which will make the compaction very inefficient.
       * In hybrid mode, we know START_OF_SEGMENT and END_OF_SEGMENT could happen multiple times per partition since
       * each Samza job could produce to every partition, and the situation could become even worse if the Samza jobs have been
       * restarted many times.
       * But we still want to checkpoint for those known infrequent control messages:
       * 1. Easy testing;
       * 2. Avoid unnecessary offset rewind when ungraceful shutdown happens.
       *
       * TODO: if we know some other types of Control Messages are frequent as START_OF_SEGMENT and END_OF_SEGMENT in the future,
       * we need to consider to exclude them to avoid the issue described above.
       */
      if (controlMessageType != ControlMessageType.START_OF_SEGMENT
          && controlMessageType != ControlMessageType.END_OF_SEGMENT) {
        syncOffset = true;
      }
    } else {
      syncOffset = (syncBytesInterval > 0 && (pcs.getProcessedRecordSizeSinceLastSync() >= syncBytesInterval));
    }
    return syncOffset;
  }

  /**
   * This method flushes data partition on disk and syncs the underlying database with {@link OffsetRecord}.
   * Note that the updates for {@link OffsetRecord} is happened in {@link #updateOffsetMetadataInOffsetRecord}
   * @param topic, the given version topic(VT) for the store.
   * @param pcs, the corresponding {@link PartitionConsumptionState} to sync with.
   */
  private void syncOffset(String topic, PartitionConsumptionState pcs) {
    int partition = pcs.getPartition();
    AbstractStorageEngine storageEngineReloadedFromRepo = storageEngineRepository.getLocalStorageEngine(topic);
    if (storageEngineReloadedFromRepo == null) {
      LOGGER.warn(
          "Storage engine has been removed. Could not execute sync offset for topic: {} and partition: {}",
          topic,
          partition);
      return;
    }
    // Flush data partition
    final AtomicReference<Map<String, String>> dbCheckpointingInfoReference = new AtomicReference<>();
    executeStorageEngineRunnable(partition, () -> {
      Map<String, String> dbCheckpointingInfoFinal = storageEngineReloadedFromRepo.sync(partition);
      dbCheckpointingInfoReference.set(dbCheckpointingInfoFinal);
    });
    if (dbCheckpointingInfoReference.get() == null) {
      throw new VeniceException("The ingestion task has already stopped");
    }
    storageUtilizationManager.notifyFlushToDisk(pcs);

    // Update the partition key in metadata partition
    if (offsetLagDeltaRelaxEnabled) {
      // Try to persist offset lag to make partition online faster when restart.
      updateOffsetLagInMetadata(pcs);
    }
    OffsetRecord offsetRecord = pcs.getOffsetRecord();
    // Check-pointing info required by the underlying storage engine
    offsetRecord.setDatabaseInfo(dbCheckpointingInfoReference.get());
    storageMetadataService.put(this.kafkaVersionTopic, partition, offsetRecord);
    pcs.resetProcessedRecordSizeSinceLastSync();
    String msg = "Offset synced for partition " + partition + " of topic " + topic + ": ";
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
      LOGGER.info(msg + offsetRecord.getLocalVersionTopicOffset());
    }
  }

  private void updateOffsetLagInMetadata(PartitionConsumptionState ps) {
    // Measure and save real-time offset lag.
    long offsetLag = measureHybridOffsetLag(ps, false);
    ps.getOffsetRecord().setOffsetLag(offsetLag);
  }

  void setIngestionException(int partitionId, Exception e) {
    boolean replicaCompleted = false;
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
    if (partitionConsumptionState != null && partitionConsumptionState.isCompletionReported()) {
      replicaCompleted = true;
    }
    partitionIngestionExceptionList.set(partitionId, new PartitionExceptionInfo(e, partitionId, replicaCompleted));
    failedPartitions.add(partitionId);
  }

  public void setLastConsumerException(Exception e) {
    lastConsumerException = e;
  }

  // visible for testing
  Exception getLastConsumerException() {
    return lastConsumerException;
  }

  public void setLastStoreIngestionException(Exception e) {
    lastStoreIngestionException.set(e);
  }

  public void recordChecksumVerificationFailure() {
    hostLevelIngestionStats.recordChecksumVerificationFailure();
  }

  public abstract long getBatchReplicationLag();

  public abstract long getLeaderOffsetLag();

  public abstract long getBatchLeaderOffsetLag();

  public abstract long getHybridLeaderOffsetLag();

  /**
   * @param pubSubServerName Pub Sub deployment to interrogate
   * @param topic topic to measure
   * @param partition for which to measure lag
   * @return the lag, or {@value Long#MAX_VALUE} if it failed to measure it
   *
   * N.B.: Note that the returned lag can be negative since the end offset used in the calculation is cached.
   */
  protected long measureLagWithCallToPubSub(
      String pubSubServerName,
      PubSubTopic topic,
      int partition,
      long currentOffset) {
    if (currentOffset < 0) {
      return Long.MAX_VALUE;
    }
    TopicManager tm = getTopicManager(pubSubServerName);
    long endOffset = tm.getLatestOffsetCached(topic, partition) - 1;
    if (endOffset < 0) {
      return Long.MAX_VALUE;
    }
    return endOffset - currentOffset;
  }

  /**
   * Measure the offset lag between follower and leader
   */
  public abstract long getFollowerOffsetLag();

  public abstract long getBatchFollowerOffsetLag();

  public abstract long getHybridFollowerOffsetLag();

  public abstract long getRegionHybridOffsetLag(int regionId);

  public abstract int getWriteComputeErrorCode();

  public abstract void updateLeaderTopicOnFollower(PartitionConsumptionState partitionConsumptionState);

  /**
   * Because of timing considerations, it is possible that some lag metrics could compute negative
   * values. Negative lag does not make sense so the intent is to ease interpretation by applying a
   * lower bound of zero on these metrics...
   */
  protected long minZeroLag(long value) {
    if (value < 0) {
      LOGGER.debug("Got a negative value for a lag metric. Will report zero.");
      return 0;
    } else {
      return value;
    }
  }

  public boolean isHybridMode() {
    return hybridStoreConfig.isPresent();
  }

  private void syncEndOfPushTimestampToMetadataService(long endOfPushTimestamp) {
    storageMetadataService.computeStoreVersionState(kafkaVersionTopic, previousStoreVersionState -> {
      if (previousStoreVersionState != null) {
        previousStoreVersionState.endOfPushTimestamp = endOfPushTimestamp;

        // Sync latest store version level metadata to disk
        return previousStoreVersionState;
      } else {
        throw new VeniceException(
            "Unexpected: received some " + ControlMessageType.END_OF_PUSH.name()
                + " control message in a topic where we have not yet received a "
                + ControlMessageType.START_OF_PUSH.name() + " control message.");
      }
    });
  }

  private void processStartOfPush(
      KafkaMessageEnvelope startOfPushKME,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {
    StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
    /*
     * Notify the underlying store engine about starting batch push.
     */
    beginBatchWrite(partition, startOfPush.sorted, partitionConsumptionState);
    partitionConsumptionState.setStartOfPushTimestamp(startOfPushKME.producerMetadata.messageTimestamp);

    statusReportAdapter.reportStarted(partitionConsumptionState);
    storageMetadataService.computeStoreVersionState(kafkaVersionTopic, previousStoreVersionState -> {
      if (previousStoreVersionState == null) {
        // No other partition of the same topic has started yet, let's initialize the StoreVersionState
        StoreVersionState newStoreVersionState = new StoreVersionState();
        newStoreVersionState.sorted = startOfPush.sorted;
        newStoreVersionState.chunked = startOfPush.chunked;
        newStoreVersionState.compressionStrategy = startOfPush.compressionStrategy;
        newStoreVersionState.compressionDictionary = startOfPush.compressionDictionary;
        if (startOfPush.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT.getValue()) {
          if (startOfPush.compressionDictionary == null) {
            throw new VeniceException(
                "compression Dictionary should not be empty if CompressionStrategy is ZSTD_WITH_DICT");
          }
        }
        newStoreVersionState.batchConflictResolutionPolicy = startOfPush.timestampPolicy;
        newStoreVersionState.startOfPushTimestamp = startOfPushKME.producerMetadata.messageTimestamp;

        LOGGER.info(
            "Persisted {} for the first time following a SOP for topic {}.",
            StoreVersionState.class.getSimpleName(),
            kafkaVersionTopic);
        return newStoreVersionState;
      } else if (previousStoreVersionState.sorted != startOfPush.sorted) {
        // Something very wrong is going on ): ...
        throw new VeniceException(
            "Unexpected: received multiple " + ControlMessageType.START_OF_PUSH.name()
                + " control messages with inconsistent 'sorted' fields within the same topic!");
      } else if (previousStoreVersionState.chunked != startOfPush.chunked) {
        // Something very wrong is going on ): ...
        throw new VeniceException(
            "Unexpected: received multiple " + ControlMessageType.START_OF_PUSH.name()
                + " control messages with inconsistent 'chunked' fields within the same topic!");
      } else {
        // No need to mutate it, so we return it as is
        return previousStoreVersionState;
      }
    });
  }

  protected void processEndOfPush(
      KafkaMessageEnvelope endOfPushKME,
      ControlMessage controlMessage,
      int partition,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {

    // Do not process duplication EOP messages.
    if (partitionConsumptionState.getOffsetRecord().isEndOfPushReceived()) {
      LOGGER.warn(
          "{} Received duplicate EOP control message, ignoring it. Partition: {}, Offset: {}",
          ingestionTaskName,
          partition,
          offset);
      return;
    }

    // We need to keep track of when the EOP happened, as that is used within Hybrid Stores' lag measurement
    partitionConsumptionState.getOffsetRecord().endOfPushReceived(offset);
    /*
     * Right now, we assume there are no sorted message after EndOfPush control message.
     * TODO: if this behavior changes in the future, the logic needs to be adjusted as well.
     */
    StoragePartitionConfig storagePartitionConfig =
        getStoragePartitionConfig(partition, false, partitionConsumptionState);

    /**
     * Update the transactional/deferred mode of the partition.
     */
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());

    /**
     * Indicate the batch push is done, and the internal storage engine needs to do some cleanup.
     */
    storageEngine.endBatchWrite(storagePartitionConfig);

    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get().getStorageEngine(kafkaVersionTopic).endBatchWrite(storagePartitionConfig);
      }
    }

    /**
     * The checksum verification is not used after EOP, so completely reset it.
     */
    partitionConsumptionState.finalizeExpectedChecksum();

    // persist the EOP message producer's timestamp.
    partitionConsumptionState.setEndOfPushTimestamp(endOfPushKME.producerMetadata.messageTimestamp);
    syncEndOfPushTimestampToMetadataService(endOfPushKME.producerMetadata.messageTimestamp);

    /**
     * It's a bit of tricky here. Since the offset is not updated yet, it's actually previous offset reported
     * here.
     * TODO: sync up offset before invoking dispatcher
     */
    statusReportAdapter.reportEndOfPushReceived(partitionConsumptionState);

    if (isDataRecovery && partitionConsumptionState.isBatchOnly()) {
      partitionConsumptionState.setDataRecoveryCompleted(true);
      statusReportAdapter.reportDataRecoveryCompleted(partitionConsumptionState);
    }
  }

  protected void processStartOfIncrementalPush(
      ControlMessage startOfIncrementalPush,
      PartitionConsumptionState partitionConsumptionState) {
    CharSequence startVersion = ((StartOfIncrementalPush) startOfIncrementalPush.controlMessageUnion).version;
    statusReportAdapter.reportStartOfIncrementalPushReceived(partitionConsumptionState, startVersion.toString());
  }

  protected void processEndOfIncrementalPush(
      ControlMessage endOfIncrementalPush,
      PartitionConsumptionState partitionConsumptionState) {
    // TODO: it is possible that we could turn incremental store to be read-only when incremental push is done
    CharSequence endVersion = ((EndOfIncrementalPush) endOfIncrementalPush.controlMessageUnion).version;
    // Reset incremental push version
    statusReportAdapter.reportEndOfIncrementalPushReceived(partitionConsumptionState, endVersion.toString());
  }

  /**
   *  This isn't really used for ingestion outside of A/A, so we NoOp here and rely on the actual implementation in
   *  {@link ActiveActiveStoreIngestionTask}
   */
  protected void processVersionSwapMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {
    // NoOp
  }

  protected boolean processTopicSwitch(
      ControlMessage controlMessage,
      int partition,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {
    throw new VeniceException(
        ControlMessageType.TOPIC_SWITCH.name() + " control message should not be received in"
            + "Online/Offline state model. Topic " + kafkaVersionTopic + " partition " + partition);
  }

  /**
   * In this method, we pass both offset and partitionConsumptionState(ps). The reason behind it is that ps's
   * offset is stale and is not updated until the very end
   */
  private boolean processControlMessage(
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {
    boolean checkReadyToServeAfterProcess = false;
    /**
     * If leader consumes control messages from topics other than version topic, it should produce
     * them to version topic; however, START_OF_SEGMENT and END_OF_SEGMENT should not be forwarded
     * because the leader producer will keep track of its own segment state; besides, leader/follower
     * model will not encounter START_OF_BUFFER_REPLAY because TOPIC_SWITCH will replace it in L/F
     * model; incremental push is also a mutually exclusive feature with hybrid stores.
     */
    final ControlMessageType type = ControlMessageType.valueOf(controlMessage);
    if (!isSegmentControlMsg(type)) {
      LOGGER.info(
          "{} : Received {} control message. Partition: {}, Offset: {}",
          ingestionTaskName,
          type.name(),
          partition,
          offset);
    }
    switch (type) {
      case START_OF_PUSH:
        /**
         * N.B.: The processing for SOP happens at the very beginning of the pipeline, in:
         * {@link #produceToStoreBufferServiceOrKafka(Iterable, boolean, PubSubTopicPartition, String, int)}
         */
        break;
      case END_OF_PUSH:
        processEndOfPush(kafkaMessageEnvelope, controlMessage, partition, offset, partitionConsumptionState);
        break;
      case START_OF_SEGMENT:
      case END_OF_SEGMENT:
        /**
         * Nothing to do here as all the processing is being done in {@link StoreIngestionTask#delegateConsumerRecord(ConsumerRecord, int, String)}.
         */
        break;
      case START_OF_INCREMENTAL_PUSH:
        processStartOfIncrementalPush(controlMessage, partitionConsumptionState);
        break;
      case END_OF_INCREMENTAL_PUSH:
        processEndOfIncrementalPush(controlMessage, partitionConsumptionState);
        break;
      case TOPIC_SWITCH:
        checkReadyToServeAfterProcess =
            processTopicSwitch(controlMessage, partition, offset, partitionConsumptionState);
        break;
      case VERSION_SWAP:
        processVersionSwapMessage(controlMessage, partition, partitionConsumptionState);
        break;
      default:
        throw new UnsupportedMessageTypeException(
            "Unrecognized Control message type " + controlMessage.controlMessageType);
    }
    return checkReadyToServeAfterProcess;
  }

  /**
   * Sync the metadata about offset in {@link OffsetRecord}.
   * {@link PartitionConsumptionState} will pass through some information to {@link OffsetRecord} for persistence and
   * Offset rewind/split brain has been guarded in {@link #updateLatestInMemoryProcessedOffset}.
   *
   * @param partitionConsumptionState
   */
  protected abstract void updateOffsetMetadataInOffsetRecord(PartitionConsumptionState partitionConsumptionState);

  /**
   * Maintain the latest processed offsets by drainers in memory; in most of the time, these offsets are ahead of the
   * checkpoint offsets inside {@link OffsetRecord}. Prior to update the offset in memory, the underlying storage engine
   * should have persisted the given record.
   */
  protected abstract void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl);

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @return the size of the data written to persistent storage.
   */
  private int internalProcessConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    int sizeOfPersistedData = 0;
    boolean checkReadyToServeAfterProcess = false;
    try {
      // Assumes the timestamp on the record is the broker's timestamp when it received the message.
      long currentTimeMs = System.currentTimeMillis();
      long producerBrokerLatencyMs =
          Math.max(consumerRecord.getPubSubMessageTime() - kafkaValue.producerMetadata.messageTimestamp, 0);
      long brokerConsumerLatencyMs = Math.max(currentTimeMs - consumerRecord.getPubSubMessageTime(), 0);
      long producerConsumerLatencyMs = Math.max(currentTimeMs - kafkaValue.producerMetadata.messageTimestamp, 0);
      recordWriterStats(
          currentTimeMs,
          producerBrokerLatencyMs,
          brokerConsumerLatencyMs,
          producerConsumerLatencyMs,
          partitionConsumptionState);
      boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
      /**
       * DIV check will happen for every single message in drainer queues.
       * Leader replicas will run DIV check twice for every single message (one in leader consumption thread before
       * producing to local version topic; the other one is here); the extra overhead is mandatory for a few reasons:
       * 1. We need DIV check in consumption phase in order to filter out unneeded data from Kafka topic
       * 2. Consumption states are always ahead of drainer states, because there are buffering in between: kafka producing,
       *    drainer buffers; so we cannot persist the DIV check results from consumption phases
       * 3. The DIV info checkpoint on disk must match the actual data persistence which is done inside drainer threads.
       */
      try {
        if (leaderProducedRecordContext == null || leaderProducedRecordContext.hasCorrespondingUpstreamMessage()) {
          /**
           * N.B.: If a leader server is processing a chunk, then the {@link consumerRecord} is going to be the same for
           * every chunk, and we don't want to treat them as dupes, hence we skip DIV. The DIV state will get updated on
           * the last message of the sequence, which is not a chunk but rather the manifest.
           */
          validateMessage(
              this.kafkaDataIntegrityValidator,
              consumerRecord,
              endOfPushReceived,
              partitionConsumptionState);
        }
        versionedDIVStats.recordSuccessMsg(storeName, versionNumber);
      } catch (FatalDataValidationException fatalException) {
        if (!endOfPushReceived) {
          throw fatalException;
        } else {
          LOGGER.warn(
              "Encountered errors during updating metadata for 2nd round DIV validation "
                  + "after EOP consuming from: {} offset: {} ExMsg: {}",
              consumerRecord.getTopicPartition(),
              consumerRecord.getOffset(),
              fatalException.getMessage());
        }
      }
      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (leaderProducedRecordContext == null
            ? (ControlMessage) kafkaValue.payloadUnion
            : (ControlMessage) leaderProducedRecordContext.getValueUnion());
        checkReadyToServeAfterProcess = processControlMessage(
            kafkaValue,
            controlMessage,
            consumerRecord.getTopicPartition().getPartitionNumber(),
            consumerRecord.getOffset(),
            partitionConsumptionState);
      } else {
        sizeOfPersistedData = processKafkaDataMessage(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            currentTimeMs);
        recordNearlineLocalBrokerToReadyToServerLatency(
            storeName,
            versionNumber,
            partitionConsumptionState,
            kafkaValue,
            leaderProducedRecordContext);
      }
      versionedIngestionStats.recordConsumedRecordEndToEndProcessingLatency(
          storeName,
          versionNumber,
          LatencyUtils.getLatencyInMS(beforeProcessingRecordTimestampNs),
          currentTimeMs);
    } catch (DuplicateDataException e) {
      divErrorMetricCallback.accept(e);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("{} : Skipping a duplicate record at offset: {}", ingestionTaskName, consumerRecord.getOffset());
      }
    } catch (PersistenceFailureException ex) {
      if (partitionConsumptionStateMap.containsKey(consumerRecord.getTopicPartition().getPartitionNumber())) {
        // If we actually intend to be consuming this partition, then we need to bubble up the failure to persist.
        LOGGER.error(
            "Met PersistenceFailureException while processing record with offset: {}, topic: {}, meta data of the record: {}",
            consumerRecord.getOffset(),
            kafkaVersionTopic,
            consumerRecord.getValue().producerMetadata);
        throw ex;
      } else {
        /*
         * We can ignore this exception for unsubscribed partitions. The unsubscription of partitions in Kafka Consumer
         * is an asynchronous event. Thus, it is possible that the partition has been dropped from the local storage
         * engine but was not yet unsubscribed by the Kafka Consumer, therefore, leading to consumption of useless messages.
         */
        return 0;
      }

      /** N.B.: In either branches of the if, above, we don't want to update the {@link OffsetRecord} */
    }

    // We want to update offsets in all cases, except when we hit the PersistenceFailureException
    if (partitionConsumptionState == null) {
      LOGGER.info("{} has been unsubscribed, will skip offset update", consumerRecord.getTopicPartition());
    } else {
      OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
      /**
       * Only update the latest message timestamp when seeing data messages after EOP; the data messages after EOP
       * should come from real-time topics.
       */
      if (partitionConsumptionState.isEndOfPushReceived() && !kafkaKey.isControlMessage()) {
        offsetRecord.setLatestProducerProcessingTimeInMs(kafkaValue.producerMetadata.messageTimestamp);
      }
      // Update latest in-memory processed offset for every processed message
      updateLatestInMemoryProcessedOffset(
          partitionConsumptionState,
          consumerRecord,
          leaderProducedRecordContext,
          kafkaUrl);
      if (checkReadyToServeAfterProcess) {
        defaultReadyToServeChecker.apply(partitionConsumptionState);
      }
    }
    return sizeOfPersistedData;
  }

  protected void recordWriterStats(
      long consumerTimestampMs,
      long producerBrokerLatencyMs,
      long brokerConsumerLatencyMs,
      long producerConsumerLatencyMs,
      PartitionConsumptionState partitionConsumptionState) {

  }

  /**
   * Message validation using DIV. Leaders should pass in the validator instance from {@link LeaderFollowerStoreIngestionTask};
   * and drainers should pass in the validator instance from {@link StoreIngestionTask}
   *
   * 1. If valid DIV errors happen after EOP is received, no fatal exceptions will be thrown.
   *    But the errors will be recorded into the DIV metrics.
   * 2. For any DIV errors happened to unregistered producers && after EOP, the errors will be ignored.
   * 3. For any DIV errors happened to records which is after logCompactionDelayInMs, the errors will be ignored.
   **/
  protected void validateMessage(
      KafkaDataIntegrityValidator validator,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      boolean endOfPushReceived,
      PartitionConsumptionState partitionConsumptionState) {
    KafkaKey key = consumerRecord.getKey();
    if (key.isControlMessage() && Arrays.equals(KafkaKey.HEART_BEAT.getKey(), key.getKey())) {
      // Skip DIV for ingestion heartbeat records.
      return;
    }
    Lazy<Boolean> tolerateMissingMsgs = Lazy.of(() -> {
      TopicManager topicManager = topicManagerRepository.getLocalTopicManager();
      // Tolerate missing message if store version is data recovery + hybrid and TS not received yet (due to source
      // topic
      // data may have been log compacted) or log compaction is enabled and record is old enough for log compaction.
      PubSubTopic pubSubTopic = consumerRecord.getTopicPartition().getPubSubTopic();

      return (isDataRecovery && isHybridMode() && partitionConsumptionState.getTopicSwitch() == null)
          || (topicManager.isTopicCompactionEnabled(pubSubTopic)
              && LatencyUtils.getElapsedTimeInMs(consumerRecord.getPubSubMessageTime()) >= topicManager
                  .getTopicMinLogCompactionLagMs(pubSubTopic));
    });

    try {
      validator.validateMessage(consumerRecord, endOfPushReceived, tolerateMissingMsgs);
    } catch (FatalDataValidationException fatalException) {
      divErrorMetricCallback.accept(fatalException);
      /**
       * If DIV errors happens after EOP is received, we will not error out the replica.
       */
      if (!endOfPushReceived) {
        throw fatalException;
      }

      FatalDataValidationException warningException = fatalException;

      // TODO: remove this condition check after fixing the bug that drainer in leaders is validating RT DIV info
      if (consumerRecord.getValue().producerMetadata.messageSequenceNumber != 1) {
        LOGGER.warn(
            "Data integrity validation problem with: {} offset: {}, "
                + "but consumption will continue since EOP is already received. Msg: {}",
            consumerRecord.getTopicPartition(),
            consumerRecord.getOffset(),
            warningException.getMessage());
      }

      if (!(warningException instanceof ImproperlyStartedSegmentException)) {
        /**
         * Run a dummy validation to update DIV metadata.
         */
        validator.validateMessage(consumerRecord, endOfPushReceived, Lazy.TRUE);
      }
    }
  }

  /**
   * We should only allow {@link StoreIngestionTask} to access {@link #kafkaDataIntegrityValidator}; other components
   * like leaders in LeaderFollowerStoreIngestionTask should never access the DIV validator in drainer, because messages
   * consumption in leader is ahead of drainer, leaders and drainers are processing messages at different paces.
   */
  protected void cloneProducerStates(int partition, KafkaDataIntegrityValidator validator) {
    this.kafkaDataIntegrityValidator.cloneProducerStates(partition, validator);
  }

  /**
   * Write to the storage engine with the optimization that we leverage the padding in front of the {@param putValue}
   * in order to insert the {@param schemaId} there. This avoids a byte array copy, which can be beneficial in terms
   * of GC.
   */
  private void prependHeaderAndWriteToStorageEngine(int partition, byte[] keyBytes, Put put, long currentTimeMs) {
    ByteBuffer putValue = put.putValue;

    if ((putValue.remaining() == 0) && (put.replicationMetadataPayload.remaining() > 0)) {
      // For RMD chunk, it is already prepended with the schema ID, so we will just put to storage engine.
      writeToStorageEngine(partition, keyBytes, put, currentTimeMs);
    } else if (putValue.position() < ValueRecord.SCHEMA_HEADER_LENGTH) {
      throw new VeniceException(
          "Start position of 'putValue' ByteBuffer shouldn't be less than " + ValueRecord.SCHEMA_HEADER_LENGTH);
    } else {
      /*
       * Since {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer} reuses the original byte
       * array, which is big enough to pre-append schema id, so we just reuse it to avoid unnecessary byte array allocation.
       * This value encoding scheme is used in {@link PartitionConsumptionState#maybeUpdateExpectedChecksum(byte[], Put)} to
       * calculate checksum for all the kafka PUT messages seen so far. Any change here needs to be reflected in that function.
       */
      // Back up the original 4 bytes
      putValue.position(putValue.position() - ValueRecord.SCHEMA_HEADER_LENGTH);
      int backupBytes = putValue.getInt();
      putValue.position(putValue.position() - ValueRecord.SCHEMA_HEADER_LENGTH);
      ByteUtils.writeInt(putValue.array(), put.schemaId, putValue.position());
      try {
        writeToStorageEngine(partition, keyBytes, put, currentTimeMs);
      } finally {
        /* We still want to recover the original position to make this function idempotent. */
        putValue.putInt(backupBytes);
      }
    }
  }

  private void writeToStorageEngine(int partition, byte[] keyBytes, Put put, long currentTimeMs) {
    boolean metricsEnabled = emitMetrics.get();
    boolean traceEnabled = LOGGER.isTraceEnabled();
    long putStartTimeNs = (metricsEnabled || traceEnabled) ? System.nanoTime() : 0;
    putInStorageEngine(partition, keyBytes, put);
    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get().getStorageEngine(kafkaVersionTopic).put(partition, keyBytes, put.putValue);
      }
    }
    if (traceEnabled) {
      LOGGER.trace(
          "{} : Completed PUT to Store: {} in {} ns at {}",
          ingestionTaskName,
          kafkaVersionTopic,
          System.nanoTime() - putStartTimeNs,
          System.currentTimeMillis());
    }
    if (metricsEnabled) {
      hostLevelIngestionStats.recordStorageEnginePutLatency(LatencyUtils.getLatencyInMS(putStartTimeNs), currentTimeMs);
    }
  }

  private void executeStorageEngineRunnable(int partition, Runnable storageEngineRunnable) {
    try {
      storageEngineRunnable.run();
    } catch (VeniceException e) {
      throwOrLogStorageFailureDependingIfStillSubscribed(partition, e);
    }
  }

  /**
   * Persist Put record to storage engine.
   */
  protected void putInStorageEngine(int partition, byte[] keyBytes, Put put) {
    executeStorageEngineRunnable(partition, () -> storageEngine.put(partition, keyBytes, put.putValue));
  }

  protected void removeFromStorageEngine(int partition, byte[] keyBytes, Delete delete) {
    executeStorageEngineRunnable(partition, () -> storageEngine.delete(partition, keyBytes));
  }

  protected void throwOrLogStorageFailureDependingIfStillSubscribed(int partition, VeniceException e) {
    if (partitionConsumptionStateMap.containsKey(partition)) {
      throw new VeniceException(
          "Caught an exception while trying to interact with the storage engine for partition " + partition
              + " while it still appears to be subscribed.",
          e);
    } else {
      logStorageOperationWhileUnsubscribed(partition);
    }
  }

  protected void logStorageOperationWhileUnsubscribed(int partition) {
    // TODO Consider if this is going to be too noisy, in which case we could mute it.
    LOGGER.info(
        "Attempted to interact with the storage engine for partition {} while the "
            + "partitionConsumptionStateMap does not contain this partition. "
            + "Will ignore the operation as it probably indicates the partition was unsubscribed.",
        partition);
  }

  public boolean consumerHasAnySubscription() {
    return aggKafkaConsumerService.hasAnyConsumerAssignedForVersionTopic(versionTopic);
  }

  public boolean consumerHasSubscription(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    int partitionId = partitionConsumptionState.getSourceTopicPartitionNumber(topic);
    return aggKafkaConsumerService
        .hasConsumerAssignedFor(versionTopic, new PubSubTopicPartitionImpl(topic, partitionId));
  }

  public void consumerUnSubscribe(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    Instant startTime = Instant.now();
    int partitionId = partitionConsumptionState.getPartition();
    aggKafkaConsumerService.unsubscribeConsumerFor(versionTopic, new PubSubTopicPartitionImpl(topic, partitionId));
    LOGGER.info(
        "Consumer unsubscribed topic {} partition {}. Took {} ms",
        topic,
        partitionId,
        Instant.now().toEpochMilli() - startTime.toEpochMilli());
  }

  public void consumerBatchUnsubscribe(Set<PubSubTopicPartition> topicPartitionSet) {
    Instant startTime = Instant.now();
    aggKafkaConsumerService.batchUnsubscribeConsumerFor(versionTopic, topicPartitionSet);
    LOGGER.info(
        "Consumer unsubscribed {} partitions. Took {} ms",
        topicPartitionSet.size(),
        Instant.now().toEpochMilli() - startTime.toEpochMilli());
  }

  public abstract void consumerUnSubscribeAllTopics(PartitionConsumptionState partitionConsumptionState);

  public void consumerSubscribe(PubSubTopicPartition topicPartition, long startOffset, String kafkaURL) {
    final boolean consumeRemotely = !Objects.equals(kafkaURL, localKafkaServer);
    // TODO: Move remote KafkaConsumerService creating operations into the aggKafkaConsumerService.
    aggKafkaConsumerService
        .createKafkaConsumerService(createKafkaConsumerProperties(kafkaProps, kafkaURL, consumeRemotely));
    aggKafkaConsumerService.subscribeConsumerFor(kafkaURL, this, topicPartition, startOffset);
  }

  public void consumerResetOffset(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    int partitionId = partitionConsumptionState.getSourceTopicPartitionNumber(topic);
    aggKafkaConsumerService.resetOffsetFor(versionTopic, new PubSubTopicPartitionImpl(topic, partitionId));
  }

  private void pauseConsumption(String topic, int partitionId) {
    aggKafkaConsumerService.pauseConsumerFor(
        versionTopic,
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId));
  }

  private void resumeConsumption(String topic, int partitionId) {
    aggKafkaConsumerService.resumeConsumerFor(
        versionTopic,
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId));
  }

  /**
   * Write the kafka message to the underlying storage engine.
   * @param consumerRecord
   * @param partitionConsumptionState
   * @param leaderProducedRecordContext
   * @return the size of the data which was written to persistent storage.
   */
  private int processKafkaDataMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      long currentTimeMs) {
    int keyLen = 0;
    int valueLen = 0;
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    // partition being produced by VeniceWriter
    // when amplificationFactor != 1, consumedPartition might not equal to producedPartition since
    // RT has different #partitions compared to VTs and StorageEngine
    // when writing to local StorageEngine, use producedPartition.
    // #subPartitionins in StorageEngine should be consistent with #subPartitionins in VTs.
    int producedPartition = partitionConsumptionState.getPartition();
    byte[] keyBytes;

    MessageType messageType = (leaderProducedRecordContext == null
        ? MessageType.valueOf(kafkaValue)
        : leaderProducedRecordContext.getMessageType());
    switch (messageType) {
      case PUT:
        // If single-threaded, we can re-use (and clobber) the same Put instance. // TODO: explore GC tuning later.
        Put put;
        if (leaderProducedRecordContext == null) {
          keyBytes = kafkaKey.getKey();
          put = (Put) kafkaValue.payloadUnion;
        } else {
          keyBytes = leaderProducedRecordContext.getKeyBytes();
          put = (Put) leaderProducedRecordContext.getValueUnion();
        }
        valueLen = put.putValue.remaining();
        keyLen = keyBytes.length;
        // update checksum for this PUT message if needed.
        partitionConsumptionState.maybeUpdateExpectedChecksum(keyBytes, put);

        // Check if put.getSchemaId is positive, if not default to 1
        int putSchemaId = put.getSchemaId() > 0 ? put.getSchemaId() : 1;

        // Do transformation recompute key, value and partition
        if (recordTransformer != null) {
          long recordTransformStartTime = System.currentTimeMillis();
          ByteBuffer valueBytes = put.getPutValue();
          Schema valueSchema = schemaRepository.getValueSchema(storeName, putSchemaId).getSchema();

          // Decompress/assemble record
          Object assembledObject = chunkAssembler.bufferAndAssembleRecord(
              consumerRecord.getTopicPartition(),
              putSchemaId,
              keyBytes,
              valueBytes,
              consumerRecord.getOffset(),
              Lazy.of(() -> new AvroGenericDeserializer<>(valueSchema, valueSchema)),
              putSchemaId,
              compressorFactory.getCompressor(compressionStrategy, kafkaVersionTopic));

          // Current record is a chunk. We only write to the storage engine for fully assembled records
          if (assembledObject == null) {
            return 0;
          }

          SchemaEntry keySchema = schemaRepository.getKeySchema(storeName);
          Lazy<Object> lazyKey = Lazy.of(() -> deserializeAvroObjectAndReturn(ByteBuffer.wrap(keyBytes), keySchema));
          Lazy<Object> lazyValue = Lazy.of(() -> assembledObject);
          TransformedRecord transformedRecord = recordTransformer.put(lazyKey, lazyValue);
          ByteBuffer transformedBytes = transformedRecord.getValueBytes(recordTransformer.getValueOutputSchema());

          put.putValue = transformedBytes;
          versionedIngestionStats.recordTransformerLatency(
              storeName,
              versionNumber,
              LatencyUtils.getElapsedTimeInMs(recordTransformStartTime),
              currentTimeMs);
          writeToStorageEngine(producedPartition, keyBytes, put, currentTimeMs);
        } else {

          prependHeaderAndWriteToStorageEngine(
              // Leaders might consume from a RT topic and immediately write into StorageEngine,
              // so we need to re-calculate partition.
              // Followers are not affected since they are always consuming from VTs.
              producedPartition,
              keyBytes,
              put,
              currentTimeMs);
        }
        // grab the positive schema id (actual value schema id) to be used in schema warm-up value schema id.
        // for hybrid use case in read compute store in future we need revisit this as we can have multiple schemas.
        if (putSchemaId > 0) {
          valueSchemaId = putSchemaId;
        }
        break;

      case DELETE:
        Delete delete;
        if (leaderProducedRecordContext == null) {
          keyBytes = kafkaKey.getKey();
          delete = ((Delete) kafkaValue.payloadUnion);
        } else {
          keyBytes = leaderProducedRecordContext.getKeyBytes();
          delete = ((Delete) leaderProducedRecordContext.getValueUnion());
        }
        keyLen = keyBytes.length;
        removeFromStorageEngine(producedPartition, keyBytes, delete);
        if (cacheBackend.isPresent()) {
          if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
            cacheBackend.get().getStorageEngine(kafkaVersionTopic).delete(producedPartition, keyBytes);
          }
        }
        break;

      case UPDATE:
        throw new VeniceMessageException(
            ingestionTaskName + ": Not expecting UPDATE message from: " + consumerRecord.getTopicPartition()
                + ", Offset: " + consumerRecord.getOffset());
      default:
        throw new VeniceMessageException(
            ingestionTaskName + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    /*
     * Potentially clean the mapping from transient record map. consumedOffset may be -1 when individual chunks are getting
     * produced to drainer queue from kafka callback thread {@link LeaderFollowerStoreIngestionTask#LeaderProducerMessageCallback}
     */
    // This flag is introduced to help write integration test to exercise code path during UPDATE message processing in
    // {@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}. This must be always set to true
    // except
    // as needed in integration test.
    if (purgeTransientRecordBuffer && isTransientRecordBufferUsed() && partitionConsumptionState.isEndOfPushReceived()
        && leaderProducedRecordContext != null && leaderProducedRecordContext.getConsumedOffset() != -1) {
      partitionConsumptionState.mayRemoveTransientRecord(
          leaderProducedRecordContext.getConsumedKafkaClusterId(),
          leaderProducedRecordContext.getConsumedOffset(),
          kafkaKey.getKey());
    }

    if (emitMetrics.get()) {
      hostLevelIngestionStats.recordKeySize(keyLen, currentTimeMs);
      hostLevelIngestionStats.recordValueSize(valueLen, currentTimeMs);
    }

    return keyLen + valueLen;
  }

  /**
   * This method checks whether the given record needs to be checked schema availability. Only PUT and UPDATE message
   * needs to #checkValueSchemaAvail
   * @param record
   */
  private void waitReadyToProcessRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record)
      throws InterruptedException {
    KafkaMessageEnvelope kafkaValue = record.getValue();
    if (record.getKey().isControlMessage() || kafkaValue == null) {
      return;
    }

    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(put.schemaId);
        try {
          deserializeValue(put.schemaId, put.putValue, record);
        } catch (Exception e) {
          PartitionConsumptionState pcs =
              partitionConsumptionStateMap.get(record.getTopicPartition().getPartitionNumber());
          LeaderFollowerStateType state = pcs == null ? null : pcs.getLeaderFollowerState();
          throw new VeniceException(
              "Failed to deserialize PUT for: " + record.getTopicPartition() + ", offset: " + record.getOffset()
                  + ", schema id: " + put.schemaId + ", LF state: " + state,
              e);
        }
        break;
      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(update.schemaId);
        break;
      case DELETE:
        /* we don't need to check schema availability for DELETE */
        break;
      default:
        throw new VeniceMessageException(
            ingestionTaskName + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
  }

  /**
   * Check whether the given schema id is available for current store.
   * The function will bypass the check if schema id is -1 (VPJ job is still using it before we finishes t he integration with schema registry).
   * Right now, this function is maintaining a local cache for schema id of current store considering that the value schema is immutable;
   * If the schema id is not available, this function will polling until the schema appears or timeout: {@link #SCHEMA_POLLING_TIMEOUT_MS};
   *
   * @param schemaId
   */
  private void waitReadyToProcessDataRecord(int schemaId) throws InterruptedException {
    if (schemaId == -1) {
      // TODO: Once Venice Client (VeniceShellClient) finish the integration with schema registry,
      // we need to remove this check here.
      return;
    }

    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()
        || schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      StoreVersionState storeVersionState = waitVersionStateAvailable(kafkaVersionTopic);
      if (!storeVersionState.chunked) {
        throw new VeniceException(
            "Detected chunking in a store-version where chunking is NOT enabled. Will abort ingestion.");
      }
      return;
    }

    waitUntilValueSchemaAvailable(schemaId);
  }

  protected StoreVersionState waitVersionStateAvailable(String kafkaTopic) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long elapsedTime;
    StoreVersionState state;
    for (;;) {
      state = storageEngine.getStoreVersionState();
      elapsedTime = System.currentTimeMillis() - startTime;

      if (state != null) {
        return state;
      }

      if (elapsedTime > SCHEMA_POLLING_TIMEOUT_MS || !isRunning()) {
        LOGGER.warn("Version state is not available for {} after {}", kafkaTopic, elapsedTime);
        throw new VeniceException("Store version state is not available for " + kafkaTopic);
      }

      Thread.sleep(SCHEMA_POLLING_DELAY_MS);
    }
  }

  private void waitUntilValueSchemaAvailable(int schemaId) throws InterruptedException {
    // Considering value schema is immutable for an existing store, we can cache it locally
    if (availableSchemaIds.get(schemaId) != null) {
      return;
    }

    long startTime = System.currentTimeMillis();
    long elapsedTime;
    boolean schemaExists;
    for (;;) {
      // Since schema registration topic might be slower than data topic,
      // the consumer will be pending until the new schema arrives.
      // TODO: better polling policy
      // TODO: Need to add metrics to track this scenario
      // In the future, we might consider other polling policies,
      // such as throwing error after certain amount of time;
      // Or we might want to propagate our state to the Controller via the VeniceNotifier,
      // if we're stuck polling more than a certain threshold of time?
      schemaExists = schemaRepository.hasValueSchema(storeName, schemaId);
      elapsedTime = System.currentTimeMillis() - startTime;
      if (schemaExists) {
        LOGGER.info("Found new value schema [{}] for {} after {} ms", schemaId, storeName, elapsedTime);
        availableSchemaIds.set(schemaId, new Object());
        // TODO: Query metastore for existence of value schema id before doing an update. During bounce of large
        // cluster these metastore writes could be spiky
        if (metaStoreWriter != null && !VeniceSystemStoreType.META_STORE.isSystemStore(storeName)) {
          String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
          PubSubTopic metaStoreRT = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(metaStoreName));
          if (getTopicManager(localKafkaServer).containsTopic(metaStoreRT)) {
            metaStoreWriter.writeInUseValueSchema(storeName, versionNumber, schemaId);
          }
        }
        return;
      }

      if (elapsedTime > SCHEMA_POLLING_TIMEOUT_MS || !isRunning()) {
        LOGGER.warn("Value schema [{}] is not available for {} after {} ms", schemaId, storeName, elapsedTime);
        throw new VeniceException("Value schema [" + schemaId + "] is not available for " + storeName);
      }

      Thread.sleep(SCHEMA_POLLING_DELAY_MS);
    }
  }

  /**
   * Deserialize a value using the schema that serializes it. Exception will be thrown and ingestion will fail if the
   * value cannot be deserialized. Currently, the deserialization dry-run won't happen in the following cases:
   *
   * 1. Value is chunked. A single piece of value cannot be deserialized. In this case, the schema id is not added in
   *    availableSchemaIds by {@link StoreIngestionTask#waitUntilValueSchemaAvailable}.
   * 2. Ingestion isolation is enabled, in which case ingestion happens on forked process instead of this main process.
   */
  private void deserializeValue(
      int schemaId,
      ByteBuffer value,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) throws IOException {
    if (schemaId < 0 || deserializedSchemaIds.get(schemaId) != null || availableSchemaIds.get(schemaId) == null) {
      return;
    }
    if (!record.getTopicPartition().getPubSubTopic().isRealTime()) {
      value = compressor.get().decompress(value);
    }
    SchemaEntry valueSchema = schemaRepository.getValueSchema(storeName, schemaId);
    if (valueSchema != null) {
      Schema schema = valueSchema.getSchema();
      new AvroGenericDeserializer<>(schema, schema).deserialize(value);
      LOGGER.info("Value deserialization succeeded with schema id {} for: {}", schemaId, record.getTopicPartition());
      deserializedSchemaIds.set(schemaId, new Object());
    }
  }

  private Object deserializeAvroObjectAndReturn(ByteBuffer input, SchemaEntry schemaEntry) {
    return new AvroGenericDeserializer<>(schemaEntry.getSchema(), schemaEntry.getSchema()).deserialize(input);
  }

  private void maybeCloseInactiveIngestionTask() {
    LOGGER.warn("{} Has expired due to not being subscribed to any partitions for too long.", ingestionTaskName);
    if (!consumerActionsQueue.isEmpty()) {
      LOGGER.info("{} consumerActionsQueue is not empty, will not close ingestion task.", ingestionTaskName);
      return;
    }
    maybeSetIngestionTaskActiveState(false);
  }

  /**
   * This method tries to update ingestion task active state.
   * It is used in two place: (1) Subscribe new partition; (2) Close idle store ingestion task.
   * We use synchronized modifier to avoid the below race condition edge case:
   * 1. Thread 1: startConsumption() API fetch ingestion task and check its running state.
   * 2. Thread 2: Due to idleness, ingestion task call close() to change running state.
   * 3. Thread 1: startConsumption() API call ingestionTask#subscribePartition() API and throws exception.
   */
  synchronized boolean maybeSetIngestionTaskActiveState(boolean isNewStateActive) {
    if (isNewStateActive) {
      setIdleCounter(0);
    } else {
      if (getIdleCounter() > getMaxIdleCounter()) {
        close();
      }
    }
    return isRunning();
  }

  void setIdleCounter(int counter) {
    idleCounter = counter;
  }

  int getIdleCounter() {
    return idleCounter;
  }

  int getMaxIdleCounter() {
    return ingestionTaskMaxIdleCount;
  }

  AtomicBoolean getIsRunning() {
    return isRunning;
  }

  /**
   * Stops the consumer task.
   */
  public synchronized void close() {
    // Evict any pending repair tasks
    getIsRunning().set(false);
    // KafkaConsumer is closed at the end of the run method.
    // The operation is executed on a single thread in run method.
    // This method signals the run method to end, which closes the
    // resources before exiting.
  }

  /**
   * This method is a blocking call to wait for {@link StoreIngestionTask} for fully shutdown in the given time.
   * @param waitTime Maximum wait time for the shutdown operation.
   */
  public synchronized void shutdown(int waitTime) {
    long startTimeInMs = System.currentTimeMillis();
    close();
    try {
      wait(waitTime);
    } catch (Exception e) {
      LOGGER.error("Caught exception while waiting for ingestion task of topic: {} shutdown.", kafkaVersionTopic);
    }
    LOGGER.info(
        "Ingestion task of topic: {} is shutdown in {}ms",
        kafkaVersionTopic,
        LatencyUtils.getElapsedTimeInMs(startTimeInMs));
  }

  /**
   * A function to allow the service to get the current status of the task.
   * This would allow the service to create a new task if required.
   */
  public boolean isRunning() {
    return getIsRunning().get();
  }

  public PubSubTopic getVersionTopic() {
    return versionTopic;
  }

  public PubSubTopic getRealtimeTopic() {
    return realTimeTopic;
  }

  public boolean isMetricsEmissionEnabled() {
    return emitMetrics.get();
  }

  public void enableMetricsEmission() {
    emitMetrics.set(true);
  }

  public void disableMetricsEmission() {
    emitMetrics.set(false);
  }

  /**
   * To check whether the given partition is still consuming message from Kafka
   */
  public boolean isPartitionConsumingOrHasPendingIngestionAction(int userPartition) {
    boolean subPartitionConsumptionStateExist =
        amplificationFactorAdapter.meetsAny(userPartition, partitionConsumptionStateMap::containsKey);
    boolean pendingPartitionIngestionAction = hasPendingPartitionIngestionAction(userPartition);
    return pendingPartitionIngestionAction || subPartitionConsumptionStateExist;
  }

  /**
   * Override the {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} config with a remote Kafka bootstrap url.
   */
  protected Properties createKafkaConsumerProperties(
      Properties localConsumerProps,
      String remoteKafkaSourceAddress,
      boolean consumeRemotely) {
    Properties newConsumerProps = serverConfig.getClusterProperties().getPropertiesCopy();
    newConsumerProps.putAll(localConsumerProps);
    newConsumerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, remoteKafkaSourceAddress);
    VeniceProperties customizedConsumerConfigs = consumeRemotely
        ? serverConfig.getKafkaConsumerConfigsForRemoteConsumption()
        : serverConfig.getKafkaConsumerConfigsForLocalConsumption();
    if (!customizedConsumerConfigs.isEmpty()) {
      newConsumerProps.putAll(customizedConsumerConfigs.toProperties());
    }
    return newConsumerProps;
  }

  /**
   * A function that would apply on a specific partition to check whether the partition is ready to serve.
   */
  @FunctionalInterface
  interface ReadyToServeCheck {
    default void apply(PartitionConsumptionState partitionConsumptionState) {
      apply(partitionConsumptionState, false);
    }

    void apply(PartitionConsumptionState partitionConsumptionState, boolean extraDisjunctionCondition);
  }

  /**
   * @return the default way of checking whether a partition is ready to serve or not:
   *         i.  if completion has been reported, no need to report anything again unless extraDisjunctionCondition is true
   *         ii. if completion hasn't been reported and EndOfPush message has been received,
   *             call {@link StoreIngestionTask#isReadyToServe(PartitionConsumptionState)} to
   *             check whether we can report completion.
   */
  private ReadyToServeCheck getDefaultReadyToServeChecker() {
    return (partitionConsumptionState, extraDisjunctionCondition) -> {
      if (extraDisjunctionCondition
          || (partitionConsumptionState.isEndOfPushReceived() && !partitionConsumptionState.isCompletionReported())) {
        if (isReadyToServe(partitionConsumptionState)) {
          int partition = partitionConsumptionState.getPartition();
          Store store = storeRepository.getStoreOrThrow(storeName);

          AbstractStorageEngine storageEngineReloadedFromRepo =
              storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
          if (store.isHybrid()) {
            if (storageEngineReloadedFromRepo == null) {
              LOGGER.warn("Storage engine {} was removed before reopening", kafkaVersionTopic);
            } else {
              storageEngineReloadedFromRepo.preparePartitionForReading(partition);
            }
          }
          if (!partitionConsumptionState.isCompletionReported()) {
            reportCompleted(partitionConsumptionState);
            warmupSchemaCache(store);
          }
          if (suppressLiveUpdates) {
            // If live updates suppression is enabled, stop consuming any new messages once the partition is ready to
            // serve.
            String msg =
                ingestionTaskName + " Live update suppression is enabled. Stop consumption for partition " + partition;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
              LOGGER.info(msg);
            }
            unSubscribePartition(new PubSubTopicPartitionImpl(versionTopic, partition));
          }
        } else {
          statusReportAdapter.reportProgress(partitionConsumptionState);
        }
      }
    };
  }

  void reportCompleted(PartitionConsumptionState partitionConsumptionState) {
    reportCompleted(partitionConsumptionState, false);
  }

  void reportCompleted(PartitionConsumptionState partitionConsumptionState, boolean forceCompletion) {
    statusReportAdapter.reportCompleted(partitionConsumptionState, forceCompletion);
    LOGGER.info("{} Partition {} is ready to serve", ingestionTaskName, partitionConsumptionState.getPartition());
  }

  /**
   * Try to warm-up the schema repo cache before reporting completion as new value schema could cause latency degradation
   * while trying to compile it in the read-path.
   */
  private void warmupSchemaCache(Store store) {
    if (!store.isReadComputationEnabled()) {
      return;
    }
    // sanity check
    if (valueSchemaId < 1) {
      return;
    }
    try {
      waitUntilValueSchemaAvailable(valueSchemaId);
    } catch (InterruptedException e) {
      LOGGER.error("Got interrupted while trying to fetch value schema");
      return;
    }
    int numSchemaToGenerate = serverConfig.getNumSchemaFastClassWarmup();
    long warmUpTimeLimit = serverConfig.getFastClassSchemaWarmupTimeout();
    int endSchemaId = numSchemaToGenerate >= valueSchemaId ? 1 : valueSchemaId - numSchemaToGenerate;
    Schema writerSchema = schemaRepository.getValueSchema(storeName, valueSchemaId).getSchema();
    Set<Schema> schemaSet = new HashSet<>();

    for (int i = valueSchemaId; i >= endSchemaId; i--) {
      schemaSet.add(schemaRepository.getValueSchema(storeName, i).getSchema());
    }
    if (store.getLatestSuperSetValueSchemaId() > 0) {
      schemaSet.add(schemaRepository.getValueSchema(storeName, store.getLatestSuperSetValueSchemaId()).getSchema());
    }
    for (Schema schema: schemaSet) {
      FastSerializerDeserializerFactory.cacheFastAvroGenericDeserializer(writerSchema, schema, warmUpTimeLimit);
    }
  }

  public void reportError(String message, int userPartition, Exception e) {
    // this method is called by StateModelNotifier.waitConsumptionCompleted which is working on
    // userPartitions
    List<PartitionConsumptionState> pcsList = new ArrayList<>();
    for (int subPartition: PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      if (partitionConsumptionStateMap.containsKey(subPartition)) {
        pcsList.add(partitionConsumptionStateMap.get(subPartition));
      }
    }
    statusReportAdapter.reportError(pcsList, message, e);
  }

  public int getAmplificationFactor() {
    return amplificationFactor;
  }

  protected StatusReportAdapter getStatusReportAdapter() {
    return statusReportAdapter;
  }

  public boolean isActiveActiveReplicationEnabled() {
    return this.isActiveActiveReplicationEnabled;
  }

  /**
   * Invoked by admin request to dump the requested partition consumption states
   */
  public void dumpPartitionConsumptionStates(AdminResponse response, ComplementSet<Integer> partitions) {
    for (Map.Entry<Integer, PartitionConsumptionState> entry: partitionConsumptionStateMap.entrySet()) {
      try {
        if (partitions.contains(entry.getKey())) {
          response.addPartitionConsumptionState(entry.getValue());
        }
      } catch (Throwable e) {
        LOGGER
            .error("Error when dumping consumption state for store {} partition {}", this.storeName, entry.getKey(), e);
      }
    }
  }

  /**
   * Invoked by admin request to dump store version state metadata.
   */
  public void dumpStoreVersionState(AdminResponse response) {
    StoreVersionState storeVersionState = storageEngine.getStoreVersionState();
    if (storeVersionState != null) {
      response.addStoreVersionState(storeVersionState);
    }
  }

  public VeniceServerConfig getServerConfig() {
    return serverConfig;
  }

  public void updateOffsetMetadataAndSync(String topic, int partitionId) {
    PartitionConsumptionState pcs = getPartitionConsumptionState(partitionId);
    updateOffsetMetadataInOffsetRecord(pcs);
    syncOffset(topic, pcs);
  }

  /**
   * The function returns local or remote topic manager.
   * @param sourceKafkaServer The address of source kafka bootstrap server.
   * @return topic manager
   */
  protected TopicManager getTopicManager(String sourceKafkaServer) {
    if (sourceKafkaServer.equals(localKafkaServer)) {
      // Use default kafka admin client (could be scala or java based) to get local topic manager
      return topicManagerRepository.getLocalTopicManager();
    }
    // Use java-based kafka admin client to get remote topic manager
    return topicManagerRepository.getTopicManager(sourceKafkaServer);
  }

  /**
   * The purpose of this function is to wait for the complete processing (including persistence to disk) of all the messages
   * those were consumed from this kafka {topic, partition} prior to calling this function.
   * This will make the calling thread to block.
   * @param topicPartition for which to wait
   * @throws InterruptedException
   */
  protected void waitForAllMessageToBeProcessedFromTopicPartition(
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    storeBufferService.drainBufferedRecordsFromTopicPartition(topicPartition);
  }

  protected abstract DelegateConsumerRecordResult delegateConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      int subPartition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs);

  /**
   * This enum represents all potential results after calling {@link #delegateConsumerRecord(PubSubMessage, int, String, int, long, long)}.
   */
  protected enum DelegateConsumerRecordResult {
    /**
     * The consumer record has been produced to local version topic by leader.
     */
    PRODUCED_TO_KAFKA,
    /**
     * The consumer record has been put into drainer queue; the following cases will result in putting to drainer directly:
     * 1. Online/Offline ingestion task
     * 2. Follower replicas
     * 3. Leader is consuming from local version topics
     */
    QUEUED_TO_DRAINER,
    /**
     * The consumer record is a duplicated message.
     */
    DUPLICATE_MESSAGE,
    /**
     * The consumer record is skipped. e.g. remote VT's TS message during data recovery.
     */
    SKIPPED_MESSAGE
  }

  private void recordNearlineLocalBrokerToReadyToServerLatency(
      String storeName,
      int versionNumber,
      PartitionConsumptionState partitionConsumptionState,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      LeaderProducedRecordContext leaderProducedRecordContext) {
    /**
     * Record nearline latency only when it's a hybrid store and the lag has been caught up. Sometimes
     * the producerTimestamp can be -1 if the leaderProducedRecordContext had an error after callback
     * Don't record latency for invalid timestamps
     */
    if (!isUserSystemStore() && isHybridMode() && partitionConsumptionState.hasLagCaughtUp()) {
      long afterProcessingRecordTimestampMs = System.currentTimeMillis();
      long producerTimestamp = (leaderProducedRecordContext == null)
          ? kafkaMessageEnvelope.producerMetadata.messageTimestamp
          : leaderProducedRecordContext.getProducedTimestampMs();
      if (producerTimestamp > 0) {
        versionedIngestionStats.recordNearlineLocalBrokerToReadyToServeLatency(
            storeName,
            versionNumber,
            afterProcessingRecordTimestampMs - producerTimestamp,
            afterProcessingRecordTimestampMs);
      } else if (!REDUNDANT_LOGGING_FILTER.isRedundantException(storeName, "IllegalTimestamp")) {
        LOGGER.warn(
            "Illegal timestamp for storeName: {}, versionNumber: {}, partition: {}, "
                + "leaderProducedRecordContext: {}, producerTimestamp: {}",
            storeName,
            versionNumber,
            partitionConsumptionState.getPartition(),
            leaderProducedRecordContext == null ? "NA" : leaderProducedRecordContext,
            producerTimestamp);
      }
    }
  }

  protected void recordProcessedRecordStats(
      PartitionConsumptionState partitionConsumptionState,
      int processedRecordSize) {
  }

  protected boolean isSegmentControlMsg(ControlMessageType msgType) {
    return ControlMessageType.START_OF_SEGMENT.equals(msgType) || ControlMessageType.END_OF_SEGMENT.equals(msgType);
  }

  /**
   * This is not a per record state. Rather it's used to indicate if the transient record buffer is being used at all
   * for this ingestion task or not.
   * For L/F mode only WC ingestion task needs this buffer.
   */
  public boolean isTransientRecordBufferUsed() {
    return isWriteComputationEnabled;
  }

  // Visible for unit test.
  protected void setPartitionConsumptionState(int partition, PartitionConsumptionState pcs) {
    partitionConsumptionStateMap.put(partition, pcs);
  }

  protected AggVersionedDIVStats getVersionedDIVStats() {
    return versionedDIVStats;
  }

  protected AggVersionedIngestionStats getVersionIngestionStats() {
    return versionedIngestionStats;
  }

  protected CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  protected Lazy<VeniceCompressor> getCompressor() {
    return compressor;
  }

  protected boolean isChunked() {
    return isChunked;
  }

  protected ReadOnlySchemaRepository getSchemaRepo() {
    return schemaRepository;
  }

  protected HostLevelIngestionStats getHostLevelIngestionStats() {
    return hostLevelIngestionStats;
  }

  protected String getKafkaVersionTopic() {
    return kafkaVersionTopic;
  }

  public boolean isStuckByMemoryConstraint() {
    for (PartitionExceptionInfo ex: partitionIngestionExceptionList) {
      if (ex == null) {
        continue;
      }
      Exception partitionIngestionException = ex.getException();
      if (partitionIngestionException instanceof MemoryLimitExhaustedException
          || partitionIngestionException.getCause() instanceof MemoryLimitExhaustedException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Validate if the given consumerRecord has a valid upstream offset to update from.
   * @param consumerRecord
   * @return true, if the record is not null and contains a valid upstream offset, otherwise false.
   */
  protected boolean shouldUpdateUpstreamOffset(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord) {
    if (consumerRecord == null) {
      return false;
    }
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    return kafkaValue.leaderMetadataFooter != null && kafkaValue.leaderMetadataFooter.upstreamOffset >= 0;
  }

  /**
   * For L/F hybrid stores, the leader periodically writes a special SOS message to the RT topic.
   * Check {@link LeaderFollowerStoreIngestionTask#maybeSendIngestionHeartbeat()} for more details.
   */
  protected abstract Set<String> maybeSendIngestionHeartbeat();

  /**
   * This function is checking the following conditions:
   * 1. Whether the version topic exists or not.
   */
  public boolean isProducingVersionTopicHealthy() {
    if (isDaVinciClient) {
      /**
       * DaVinci doesn't produce to any topics.
       */
      return true;
    }
    if (!topicManagerRepository.getLocalTopicManager().containsTopic(this.versionTopic)) {
      return false;
    }
    return true;
  }
}
