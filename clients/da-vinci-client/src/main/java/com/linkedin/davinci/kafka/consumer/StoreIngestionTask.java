package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.ingestion.LagType.HEARTBEAT_LAG;
import static com.linkedin.davinci.ingestion.LagType.OFFSET_LAG;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.DROP_PARTITION;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.RESET_OFFSET;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.SUBSCRIBE;
import static com.linkedin.davinci.kafka.consumer.ConsumerActionType.UNSUBSCRIBE;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static com.linkedin.davinci.kafka.consumer.LeaderProducedRecordContext.NO_UPSTREAM_POSITION;
import static com.linkedin.davinci.validation.DataIntegrityValidator.DISABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.LogMessages.KILLED_JOB_MESSAGE;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static com.linkedin.venice.pubsub.PubSubConstants.UNKNOWN_LATEST_OFFSET;
import static com.linkedin.venice.utils.Utils.FATAL_DATA_VALIDATION_ERROR;
import static com.linkedin.venice.utils.Utils.closeQuietlyWithErrorLogged;
import static com.linkedin.venice.utils.Utils.getReplicaId;
import static java.util.Comparator.comparingInt;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.linkedin.davinci.client.BlockingDaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.ingestion.LagType;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.memory.InMemoryStorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.davinci.utils.InMemoryChunkAssembler;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
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
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.ValueHolder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public abstract class StoreIngestionTask implements Runnable, Closeable {
  // TODO: Make this LOGGER prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger LOGGER = LogManager.getLogger(StoreIngestionTask.class);

  private static final String CONSUMER_TASK_ID_FORMAT = "SIT-%s";
  private static final List<Class<? extends Throwable>> RETRY_FAILURE_TYPES =
      Collections.singletonList(VeniceException.class);
  private static final long POST_UNSUB_SLEEP_MS = 600L;
  public static long SCHEMA_POLLING_DELAY_MS = SECONDS.toMillis(5);
  public static long STORE_VERSION_POLLING_DELAY_MS = MINUTES.toMillis(1);

  private static final long SCHEMA_POLLING_TIMEOUT_MS = MINUTES.toMillis(5);
  private static final long SOP_POLLING_TIMEOUT_MS = HOURS.toMillis(1);
  private static final long DEFAULT_HEARTBEAT_LAG_THRESHOLD_MS = MINUTES.toMillis(2); // Default is 2 minutes.
  protected static final long WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED = MINUTES.toMillis(1); // 1 min

  static final int MAX_CONSUMER_ACTION_ATTEMPTS = 5;

  static final int MAX_OFFSET_FETCH_ATTEMPTS = 10;

  private static final int CONSUMER_ACTION_QUEUE_INIT_CAPACITY = 11;
  protected static final long KILL_WAIT_TIME_MS = 5000L;
  private static final int MAX_KILL_CHECKING_ATTEMPTS = 10;
  protected static final int CHUNK_SCHEMA_ID = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
  private static final int CHUNK_MANIFEST_SCHEMA_ID =
      AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();

  protected static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  /**
   * Speed up DaVinci shutdown by closing partitions concurrently.
   */
  private static final ExecutorService SHUTDOWN_EXECUTOR_FOR_DVC =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

  /** storage destination for consumption */
  protected final StorageService storageService;
  protected final StorageEngine storageEngine;

  /** Topics used for this topic consumption
   * TODO: Using a PubSubVersionTopic and PubSubRealTimeTopic extending PubSubTopic for type safety.
   * */
  protected final String kafkaVersionTopic;
  protected final PubSubTopic versionTopic;
  protected final PubSubTopic realTimeTopic;
  protected final PubSubTopic separateRealTimeTopic;
  protected final String storeName;
  protected final String storeVersionName;
  protected final boolean isSystemStore;
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
   * Keeps track of producer states inside version topic that drainer threads have processed so far.
   * Producers states in this validator will be flushed to the metadata partition of the storage engine regularly in
   * {@link #syncOffset(String, PartitionConsumptionState)}
   * NOTE: consumerDiv will be used in place of this when {@link #isGlobalRtDivEnabled()} is true.
   */
  private final DataIntegrityValidator drainerDiv;
  /**
   * The consumer and drainer DIV must remain separate. Since the consumer is always ahead of the drainer, the consumer
   * would be validating data ahead of the actual persisted data on the drainer.
   *
   * NOTE: Currently, the state clearing happens only in drainerDiv which persists its state to disk.
   * consumerDiv is transient, not persisted to disk, and its state is not expected to grow as large. Thus,
   * bouncing effectively clears it (which is not the case for drainerDiv). Later on, we could trigger state
   * cleaning for this consumer DIV as well, if deemed necessary.
   *
   * NOTE: When {@link #isGlobalRtDivEnabled()} is enabled, this will be used by leaders to produce Global RT DIV state
   * to local VT. This will also be used to send DIV snapshots to the drainer to persist the VT + RT DIV on-disk.
   */
  protected final DataIntegrityValidator consumerDiv;
  /** Map of broker URL to the total bytes consumed by ConsumptionTask since the last Global RT DIV sync */
  // TODO: clear it out when the sync is done
  protected final VeniceConcurrentHashMap<String, Long> consumedBytesSinceLastSync;
  protected final HostLevelIngestionStats hostLevelIngestionStats;
  protected final AggVersionedDIVStats versionedDIVStats;
  protected final AggVersionedIngestionStats versionedIngestionStats;
  protected AggVersionedDaVinciRecordTransformerStats daVinciRecordTransformerStats;
  protected final BooleanSupplier isCurrentVersion;
  protected final Optional<HybridStoreConfig> hybridStoreConfig;
  protected final Consumer<DataValidationException> divErrorMetricCallback;
  private final ExecutorService missingSOPCheckExecutor = Executors.newSingleThreadExecutor();
  private final VeniceStoreVersionConfig storeVersionConfig;
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
  protected AtomicInteger idleCounter = new AtomicInteger(0);

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
  protected final int partitionCount;

  // Total number of partition for this store version
  protected final int storeVersionPartitionCount;

  private AtomicInteger pendingSubscriptionActionCount = new AtomicInteger(0);
  private int subscribedCount = 0;
  private int forceUnSubscribedCount = 0;

  // Push timeout threshold for the store
  protected final long bootstrapTimeoutInMs;

  protected final boolean isIsolatedIngestion;

  protected final IngestionNotificationDispatcher ingestionNotificationDispatcher;

  protected final InMemoryChunkAssembler chunkAssembler;
  private final Optional<ObjectCacheBackend> cacheBackend;
  private final DaVinciRecordTransformerConfig recordTransformerConfig;
  private final Schema recordTransformerInputValueSchema;
  private final RecordDeserializer recordTransformerKeyDeserializer;
  private final Map<Integer, Schema> schemaIdToSchemaMap;
  private BlockingDaVinciRecordTransformer recordTransformer;

  protected final String localKafkaServer;
  protected final int localKafkaClusterId;
  protected final Set<String> localKafkaServerSingletonSet;
  private int valueSchemaId = -1;

  protected final boolean isDaVinciClient;

  private final boolean offsetLagDeltaRelaxEnabled;
  private final boolean ingestionCheckpointDuringGracefulShutdownEnabled;

  protected boolean isDataRecovery;
  protected int dataRecoverySourceVersionNumber;
  protected final boolean readOnlyForBatchOnlyStoreEnabled;
  protected final MetaStoreWriter metaStoreWriter;
  protected final Function<String, String> kafkaClusterUrlResolver;
  protected final boolean resetErrorReplicaEnabled;

  protected final CompressionStrategy compressionStrategy;
  protected final StorageEngineBackedCompressorFactory compressorFactory;
  protected final Lazy<VeniceCompressor> compressor;
  protected final boolean isChunked;
  protected final boolean isRmdChunked;
  protected final ChunkedValueManifestSerializer manifestSerializer;
  protected final PubSubTopicRepository pubSubTopicRepository;
  private final String[] msgForLagMeasurement;
  private final Runnable runnableForKillIngestionTasksForNonCurrentVersions;
  protected final AtomicBoolean recordLevelMetricEnabled;
  protected final boolean isGlobalRtDivEnabled;
  protected volatile PartitionReplicaIngestionContext.VersionRole versionRole;
  protected volatile PartitionReplicaIngestionContext.WorkloadType workloadType;
  protected final boolean batchReportIncPushStatusEnabled;

  protected final ExecutorService parallelProcessingThreadPool;

  protected Lazy<CountDownLatch> gracefulShutdownLatch = Lazy.of(() -> new CountDownLatch(1));
  protected Lazy<ZKHelixAdmin> zkHelixAdmin;
  protected final String hostName;
  private boolean skipAfterBatchPushUnsubEnabled = false;
  private final List<AutoCloseable> thingsToClose = new ArrayList<>();

  public StoreIngestionTask(
      StorageService storageService,
      StoreIngestionTaskFactory.Builder builder,
      Store store,
      Version version,
      Properties kafkaConsumerProperties,
      BooleanSupplier isCurrentVersion,
      VeniceStoreVersionConfig storeVersionConfig,
      int errorPartitionId,
      boolean isIsolatedIngestion,
      Optional<ObjectCacheBackend> cacheBackend,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      Queue<VeniceNotifier> notifiers,
      Lazy<ZKHelixAdmin> zkHelixAdmin) {
    this.storeVersionConfig = storeVersionConfig;
    this.readCycleDelayMs = storeVersionConfig.getKafkaReadCycleDelayMs();
    this.emptyPollSleepMs = storeVersionConfig.getKafkaEmptyPollSleepMs();
    this.databaseSyncBytesIntervalForTransactionalMode =
        storeVersionConfig.getDatabaseSyncBytesIntervalForTransactionalMode();
    this.databaseSyncBytesIntervalForDeferredWriteMode =
        storeVersionConfig.getDatabaseSyncBytesIntervalForDeferredWriteMode();
    this.kafkaProps = kafkaConsumerProperties;
    this.storageService = storageService;
    this.storageMetadataService = builder.getStorageMetadataService();
    this.storeRepository = builder.getMetadataRepo();
    this.schemaRepository = builder.getSchemaRepo();
    this.kafkaVersionTopic = storeVersionConfig.getStoreVersionName();
    this.pubSubTopicRepository = builder.getPubSubTopicRepository();
    this.versionTopic = pubSubTopicRepository.getTopic(kafkaVersionTopic);
    this.storeName = versionTopic.getStoreName();
    this.storeVersionName = storeVersionConfig.getStoreVersionName();
    this.isUserSystemStore = VeniceSystemStoreUtils.isUserSystemStore(storeName);
    this.isSystemStore = VeniceSystemStoreUtils.isSystemStore(storeName);
    // if version is not hybrid, it is not possible to create pub sub realTimeTopic, users of this field should do a
    // nullability check
    this.realTimeTopic = version.getHybridStoreConfig() != null
        ? Objects.requireNonNull(pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(version)))
        : null;
    this.separateRealTimeTopic = version.isSeparateRealTimeTopicEnabled() && version.getHybridStoreConfig() != null
        ? Objects.requireNonNull(pubSubTopicRepository.getTopic(Utils.getSeparateRealTimeTopicName(version)))
        : null;
    this.versionNumber = Version.parseVersionFromKafkaTopicName(kafkaVersionTopic);
    this.consumerActionsQueue = new PriorityBlockingQueue<>(CONSUMER_ACTION_QUEUE_INIT_CAPACITY);
    this.partitionToPendingConsumerActionCountMap = new VeniceConcurrentHashMap<>();

    // partitionConsumptionStateMap could be accessed by multiple threads: consumption thread and the thread handling
    // kill message
    this.partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    /**
     * Use the max of the two values to avoid the case where the producer state max age is set to a value that is less
     * than the rewind time. Otherwise, this would cause the corresponding segments in DIV partitionTracker to be
     * unnecessarily deleted (then created) during the time period between the rewind time and the producer state max age,
     * if the rewind time is larger.
     */
    long producerStateMaxAgeMs = builder.getServerConfig().getDivProducerStateMaxAgeMs();
    if (producerStateMaxAgeMs != DISABLED && version.getHybridStoreConfig() != null) {
      producerStateMaxAgeMs =
          Math.max(producerStateMaxAgeMs, version.getHybridStoreConfig().getRewindTimeInSeconds() * Time.MS_PER_SECOND);
    }
    // Could be accessed from multiple threads since there are multiple worker threads.
    this.drainerDiv = new DataIntegrityValidator(this.kafkaVersionTopic, DISABLED, producerStateMaxAgeMs);
    // Could be accessed from multiple threads since there are multiple worker threads.
    this.consumerDiv = new DataIntegrityValidator(kafkaVersionTopic);
    this.consumedBytesSinceLastSync = new VeniceConcurrentHashMap<>();
    this.ingestionTaskName = String.format(CONSUMER_TASK_ID_FORMAT, kafkaVersionTopic);
    this.topicManagerRepository = builder.getTopicManagerRepository();
    this.readOnlyForBatchOnlyStoreEnabled = storeVersionConfig.isReadOnlyForBatchOnlyStoreEnabled();
    this.hostLevelIngestionStats = builder.getIngestionStats().getStoreStats(storeName);
    this.versionedDIVStats = builder.getVersionedDIVStats();
    this.versionedIngestionStats = builder.getVersionedStorageIngestionStats();
    this.isRunning = new AtomicBoolean(true);
    this.emitMetrics = new AtomicBoolean(true);
    this.resetErrorReplicaEnabled = storeVersionConfig.isResetErrorReplicaEnabled();

    this.storeBufferService = builder.getStoreBufferService();
    this.isCurrentVersion = isCurrentVersion;
    this.hybridStoreConfig = Optional.ofNullable(
        version.isUseVersionLevelHybridConfig() ? version.getHybridStoreConfig() : store.getHybridStoreConfig());

    this.divErrorMetricCallback = e -> versionedDIVStats.recordException(storeName, versionNumber, e);

    this.diskUsage = builder.getDiskUsage();

    ReferenceCounted<? extends StorageEngine> refCountedStorageEngine =
        storageService.getRefCountedStorageEngine(storeVersionName);
    this.thingsToClose.add(refCountedStorageEngine);
    this.storageEngine = Objects.requireNonNull(refCountedStorageEngine.get());

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
    this.partitionCount = storeVersionPartitionCount;
    this.ingestionNotificationDispatcher =
        new IngestionNotificationDispatcher(notifiers, kafkaVersionTopic, isCurrentVersion);
    this.missingSOPCheckExecutor.execute(() -> waitForStateVersion(kafkaVersionTopic));
    this.cacheBackend = cacheBackend;

    this.recordTransformerConfig = recordTransformerConfig;
    if (recordTransformerConfig != null && recordTransformerConfig.getRecordTransformerFunction() != null) {
      this.chunkAssembler = new InMemoryChunkAssembler(new InMemoryStorageEngine(storeName));

      Schema keySchema = schemaRepository.getKeySchema(storeName).getSchema();
      if (recordTransformerConfig.useSpecificRecordKeyDeserializer()) {
        this.recordTransformerKeyDeserializer = FastSerializerDeserializerFactory
            .getFastAvroSpecificDeserializer(keySchema, recordTransformerConfig.getKeyClass());
      } else {
        this.recordTransformerKeyDeserializer =
            FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
      }

      this.recordTransformerInputValueSchema = schemaRepository.getSupersetOrLatestValueSchema(storeName).getSchema();
      Schema outputValueSchema = recordTransformerConfig.getOutputValueSchema();

      // User doesn't intend on transforming records. Use input value schema instead.
      if (outputValueSchema == null) {
        outputValueSchema = this.recordTransformerInputValueSchema;
      }

      DaVinciRecordTransformer clientRecordTransformer = recordTransformerConfig.getRecordTransformerFunction()
          .apply(
              versionNumber,
              keySchema,
              this.recordTransformerInputValueSchema,
              outputValueSchema,
              recordTransformerConfig);

      this.recordTransformer = new BlockingDaVinciRecordTransformer(
          clientRecordTransformer,
          keySchema,
          this.recordTransformerInputValueSchema,
          outputValueSchema,
          recordTransformerConfig);
      this.schemaIdToSchemaMap = new VeniceConcurrentHashMap<>();

      daVinciRecordTransformerStats = builder.getDaVinciRecordTransformerStats();

      // onStartVersionIngestion called here instead of run() because this needs to finish running
      // before bootstrapping starts
      long startTime = System.nanoTime();
      recordTransformer.onStartVersionIngestion(isCurrentVersion.getAsBoolean());
      LOGGER.info(
          "DaVinciRecordTransformer onStartVersionIngestion took {} ms for store version: {}",
          LatencyUtils.getElapsedTimeFromNSToMS(startTime),
          storeVersionName);
    } else {
      this.schemaIdToSchemaMap = null;
      this.recordTransformerKeyDeserializer = null;
      this.recordTransformerInputValueSchema = null;
      this.chunkAssembler = null;
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
        partitionCount,
        Collections.unmodifiableMap(partitionConsumptionStateMap),
        serverConfig.isHybridQuotaEnabled(),
        serverConfig.isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled(),
        version.isSeparateRealTimeTopicEnabled(),
        ingestionNotificationDispatcher,
        this::pauseConsumption,
        this::resumeConsumption);
    this.storeRepository.registerStoreDataChangedListener(this.storageUtilizationManager);
    this.versionRole =
        PartitionReplicaIngestionContext.determineStoreVersionRole(versionNumber, store.getCurrentVersion());
    this.workloadType = PartitionReplicaIngestionContext
        .determineWorkloadType(isActiveActiveReplicationEnabled, isWriteComputationEnabled);
    this.kafkaClusterUrlResolver = serverConfig.getKafkaClusterUrlResolver();
    Object2IntMap<String> kafkaClusterUrlToIdMap = serverConfig.getKafkaClusterUrlToIdMap();
    this.localKafkaClusterId = kafkaClusterUrlToIdMap.getOrDefault(localKafkaServer, Integer.MIN_VALUE);
    this.compressionStrategy = version.getCompressionStrategy();
    this.compressorFactory = builder.getCompressorFactory();
    this.compressor = Lazy.of(
        () -> compressorFactory
            .getCompressor(compressionStrategy, kafkaVersionTopic, serverConfig.getZstdDictCompressionLevel()));
    this.isChunked = version.isChunkingEnabled();
    this.isRmdChunked = version.isRmdChunkingEnabled();
    this.manifestSerializer = new ChunkedValueManifestSerializer(true);
    this.msgForLagMeasurement = new String[partitionCount];
    for (int i = 0; i < this.msgForLagMeasurement.length; i++) {
      this.msgForLagMeasurement[i] = kafkaVersionTopic + "_" + i;
    }
    this.runnableForKillIngestionTasksForNonCurrentVersions =
        builder.getRunnableForKillIngestionTasksForNonCurrentVersions();
    this.ingestionTaskMaxIdleCount = serverConfig.getIngestionTaskMaxIdleCount();
    this.recordLevelMetricEnabled = new AtomicBoolean(
        serverConfig.isRecordLevelMetricWhenBootstrappingCurrentVersionEnabled()
            || !this.isCurrentVersion.getAsBoolean());
    this.isGlobalRtDivEnabled = version.isGlobalRtDivEnabled();
    if (!this.recordLevelMetricEnabled.get()) {
      LOGGER.info("Disabled record-level metric when ingesting current version: {}", kafkaVersionTopic);
    }
    this.batchReportIncPushStatusEnabled = !isDaVinciClient && serverConfig.getBatchReportEOIPEnabled();
    this.parallelProcessingThreadPool = builder.getAAWCWorkLoadProcessingThreadPool();
    this.hostName = Utils.getHostName() + "_" + storeVersionConfig.getListenerPort();
    this.zkHelixAdmin = zkHelixAdmin;
  }

  /** Package-private on purpose, only intended for tests. Do not use for production use cases. */
  void setPurgeTransientRecordBuffer(boolean purgeTransientRecordBuffer) {
    this.purgeTransientRecordBuffer = purgeTransientRecordBuffer;
  }

  protected abstract IngestionBatchProcessor getIngestionBatchProcessor();

  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  public String getIngestionTaskName() {
    return ingestionTaskName;
  }

  public int getVersionNumber() {
    return versionNumber;
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

  public synchronized void subscribePartition(PubSubTopicPartition topicPartition) {
    subscribePartition(topicPartition, true);
  }

  void resubscribeForAllPartitions() throws InterruptedException {
    throwIfNotRunning();
    for (PartitionConsumptionState partitionConsumptionState: partitionConsumptionStateMap.values()) {
      resubscribe(partitionConsumptionState);
    }
  }

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public synchronized void subscribePartition(PubSubTopicPartition topicPartition, boolean isHelixTriggeredAction) {
    throwIfNotRunning();
    int partitionNumber = topicPartition.getPartitionNumber();

    partitionToPendingConsumerActionCountMap.computeIfAbsent(partitionNumber, x -> new AtomicInteger(0))
        .incrementAndGet();
    pendingSubscriptionActionCount.incrementAndGet();
    consumerActionsQueue.add(new ConsumerAction(SUBSCRIBE, topicPartition, nextSeqNum(), isHelixTriggeredAction));
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
    partitionToPendingConsumerActionCountMap
        .computeIfAbsent(topicPartition.getPartitionNumber(), x -> new AtomicInteger(0))
        .incrementAndGet();
    ConsumerAction consumerAction =
        new ConsumerAction(UNSUBSCRIBE, topicPartition, nextSeqNum(), isHelixTriggeredAction);

    consumerActionsQueue.add(consumerAction);
    return consumerAction.getFuture();
  }

  /**
   * Drops a storage partition gracefully.
   * This is always a Helix triggered action.
   */
  public CompletableFuture<Void> dropStoragePartitionGracefully(PubSubTopicPartition topicPartition) {
    int partitionId = topicPartition.getPartitionNumber();
    synchronized (this) {
      if (isRunning()) {
        LOGGER.info(
            "Ingestion task is still running for Topic {}. Dropping partition {} asynchronously",
            topicPartition.getTopicName(),
            partitionId);
        ConsumerAction consumerAction = new ConsumerAction(DROP_PARTITION, topicPartition, nextSeqNum(), true);
        consumerActionsQueue.add(consumerAction);
        return consumerAction.getFuture();
      }
    }

    LOGGER.info(
        "Ingestion task isn't running for Topic {}. Dropping partition {} synchronously",
        topicPartition.getTopicName(),
        partitionId);
    dropPartitionSynchronously(topicPartition);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Drops a partition synchrnously. This is invoked when processing a DROP_PARTITION message.
   */
  private void dropPartitionSynchronously(PubSubTopicPartition topicPartition) {
    LOGGER.info("{} Dropping partition: {}", ingestionTaskName, topicPartition);
    int partition = topicPartition.getPartitionNumber();
    this.storageService.dropStorePartition(storeVersionConfig, partition, true);
    LOGGER.info("{} Dropped partition: {}", ingestionTaskName, topicPartition);
  }

  public boolean hasAnySubscription() {
    return !partitionConsumptionStateMap.isEmpty() || hasAnyPendingSubscription();
  }

  public boolean hasAnyPendingSubscription() {
    return pendingSubscriptionActionCount.get() > 0;
  }

  /**
   * This helper function will check if the ingestion task has been idle for a long time.
   */
  public boolean isIdleOverThreshold() {
    return getIdleCounter() > getMaxIdleCounter();
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public synchronized void resetPartitionConsumptionOffset(PubSubTopicPartition topicPartition) {
    throwIfNotRunning();
    partitionToPendingConsumerActionCountMap
        .computeIfAbsent(topicPartition.getPartitionNumber(), x -> new AtomicInteger(0))
        .incrementAndGet();
    consumerActionsQueue.add(new ConsumerAction(RESET_OFFSET, topicPartition, nextSeqNum(), false));
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
    AtomicInteger atomicInteger = partitionToPendingConsumerActionCountMap.get(userPartition);
    if (atomicInteger == null) {
      return false;
    }
    return atomicInteger.get() > 0;
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
        ingestionNotificationDispatcher.reportError(
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
    String replicaId = getReplicaId(topic, partitionId);
    boolean returnStatus = true;
    if (offsetRecord.getLocalVersionTopicOffset() > 0) {
      StoreVersionState storeVersionState = storageEngine.getStoreVersionState();
      if (storeVersionState != null) {
        LOGGER.info("Found storeVersionState for replica: {}: checkDatabaseIntegrity will proceed", replicaId);
        returnStatus = storageEngine.checkDatabaseIntegrity(
            partitionId,
            offsetRecord.getDatabaseInfo(),
            getStoragePartitionConfig(storeVersionState.sorted, partitionConsumptionState));
        LOGGER.info("checkDatabaseIntegrity {} for replica: {}", returnStatus ? "succeeded" : "failed", replicaId);
      } else {
        LOGGER.info("storeVersionState not found for replica: {}: checkDatabaseIntegrity will be skipped", replicaId);
      }
    } else {
      LOGGER.info("Local topic offset not found for replica: {}: checkDatabaseIntegrity will be skipped", replicaId);
    }
    return returnStatus;
  }

  private void beginBatchWrite(boolean sorted, PartitionConsumptionState partitionConsumptionState) {
    Map<String, String> checkpointedDatabaseInfo = partitionConsumptionState.getOffsetRecord().getDatabaseInfo();
    StoragePartitionConfig storagePartitionConfig = getStoragePartitionConfig(sorted, partitionConsumptionState);
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

  protected StoragePartitionConfig getStoragePartitionConfig(PartitionConsumptionState partitionConsumptionState) {
    StoreVersionState storeVersionState = storageEngine.getStoreVersionState();
    if (storeVersionState == null) {
      throw new VeniceException("Can't find store version state for store version: " + kafkaVersionTopic);
    }
    return getStoragePartitionConfig(storeVersionState.sorted, partitionConsumptionState);
  }

  protected StoragePartitionConfig getStoragePartitionConfig(
      boolean sorted,
      PartitionConsumptionState partitionConsumptionState) {
    StoragePartitionConfig storagePartitionConfig =
        new StoragePartitionConfig(kafkaVersionTopic, partitionConsumptionState.getPartition());
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

    if (partitionConsumptionState.isCompletionReported()) {
      storagePartitionConfig.setWriteOnlyConfig(false);
    }

    if (partitionConsumptionState.isHybrid()) {
      if (partitionConsumptionState.getLeaderFollowerState().equals(STANDBY)) {
        storagePartitionConfig.setReadWriteLeaderForDefaultCF(false);
        storagePartitionConfig.setReadWriteLeaderForRMDCF(false);
      } else if (partitionConsumptionState.getLeaderFollowerState().equals(LEADER)) {
        if (isActiveActiveReplicationEnabled) {
          storagePartitionConfig.setReadWriteLeaderForRMDCF(true);
        }
        if (isWriteComputationEnabled) {
          storagePartitionConfig.setReadWriteLeaderForDefaultCF(true);
        }
      }
    }

    return storagePartitionConfig;
  }

  protected abstract boolean isHybridFollower(PartitionConsumptionState partitionConsumptionState);

  /**
   * Checks whether the lag is acceptable for hybrid stores
   */
  protected abstract boolean checkAndLogIfLagIsAcceptableForHybridStore(
      PartitionConsumptionState partitionConsumptionState,
      long lag,
      long threshold,
      boolean shouldLogLag,
      LagType lagType);

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
      String msg = msgForLagMeasurement[partitionId];

      // Log only once a minute per partition.
      boolean shouldLogLag = !REDUNDANT_LOGGING_FILTER.isRedundantException(msg);
      if (serverConfig.isUseHeartbeatLagForReadyToServeCheckEnabled()) {
        // Measure heartbeat lag for ready-to-serve check.
        isLagAcceptable = checkAndLogIfLagIsAcceptableForHybridStore(
            partitionConsumptionState,
            measureHybridHeartbeatLag(partitionConsumptionState, shouldLogLag),
            DEFAULT_HEARTBEAT_LAG_THRESHOLD_MS,
            shouldLogLag,
            HEARTBEAT_LAG);
      } else {
        long offsetThreshold = getOffsetToOnlineLagThresholdPerPartition(hybridStoreConfig, storeName, partitionCount);
        if (offsetThreshold >= 0) {
          isLagAcceptable = checkAndLogIfLagIsAcceptableForHybridStore(
              partitionConsumptionState,
              measureHybridOffsetLag(partitionConsumptionState, shouldLogLag),
              offsetThreshold,
              shouldLogLag,
              OFFSET_LAG);
        }
      }
    } catch (Exception e) {
      String exceptionMsgIdentifier =
          new StringBuilder().append(kafkaVersionTopic).append("_isReadyToServe").toString();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(exceptionMsgIdentifier)) {
        LOGGER.info(
            "Exception when trying to determine if hybrid store replica is ready to serve: {}",
            partitionConsumptionState.getReplicaId(),
            e);
      }
    }

    if (isLagAcceptable) {
      partitionConsumptionState.lagHasCaughtUp();
    }
    return isLagAcceptable;
  }

  public boolean isReadyToServeAnnouncedWithRTLag() {
    return false;
  }

  IngestionNotificationDispatcher getIngestionNotificationDispatcher() {
    return ingestionNotificationDispatcher;
  }

  protected abstract boolean isRealTimeBufferReplayStarted(PartitionConsumptionState partitionConsumptionState);

  /**
   * Measure the hybrid offset lag for partition being tracked in `partitionConsumptionState`.
   */
  protected abstract long measureHybridOffsetLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag);

  protected abstract long measureHybridHeartbeatLag(
      PartitionConsumptionState partitionConsumptionState,
      boolean shouldLogLag);

  /**
   * Check if the ingestion progress has reached to the end of the version topic. This is currently only
   * used {@link LeaderFollowerStoreIngestionTask}.
   */
  protected abstract void reportIfCatchUpVersionTopicOffset(PartitionConsumptionState partitionConsumptionState);

  /**
   * This function will produce a pair of consumer record and a it's derived produced record to the writer buffers
   * maintained by {@link StoreBufferService}.
   *
   * @param consumedRecord              : received consumer record
   * @param leaderProducedRecordContext : derived leaderProducedRecordContext
   * @param partition
   * @param kafkaUrl
   * @throws InterruptedException
   */
  protected void produceToStoreBufferService(
      DefaultPubSubMessage consumedRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs,
      long currentTimeForMetricsMs) throws InterruptedException {
    boolean measureTime = emitMetrics.get();
    long queuePutStartTimeInNS = measureTime ? System.nanoTime() : 0;
    storeBufferService.putConsumerRecord(
        consumedRecord,
        this,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs); // blocking call

    if (measureTime && recordLevelMetricEnabled.get()) {
      hostLevelIngestionStats.recordConsumerRecordsQueuePutLatency(
          LatencyUtils.getElapsedTimeFromNSToMS(queuePutStartTimeInNS),
          currentTimeForMetricsMs);
    }
  }

  protected abstract Iterable<DefaultPubSubMessage> validateAndFilterOutDuplicateMessagesFromLeaderTopic(
      Iterable<DefaultPubSubMessage> records,
      String kafkaUrl,
      PubSubTopicPartition topicPartition);

  private int handleSingleMessage(
      PubSubMessageProcessedResultWrapper consumerRecordWrapper,
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs,
      boolean metricsEnabled,
      ValueHolder<Double> elapsedTimeForPuttingIntoQueue) throws InterruptedException {
    DefaultPubSubMessage record = consumerRecordWrapper.getMessage();
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
            partitionConsumptionStateMap.get(topicPartition.getPartitionNumber()));
      }
    }

    // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
    // this call.
    DelegateConsumerRecordResult delegateConsumerRecordResult = delegateConsumerRecord(
        consumerRecordWrapper,
        topicPartition.getPartitionNumber(),
        kafkaUrl,
        kafkaClusterId,
        beforeProcessingPerRecordTimestampNs,
        beforeProcessingBatchRecordsTimestampMs);

    switch (delegateConsumerRecordResult) {
      case QUEUED_TO_DRAINER:
        long queuePutStartTimeInNS = metricsEnabled ? System.nanoTime() : 0;

        // blocking call
        storeBufferService.putConsumerRecord(
            record,
            this,
            null,
            topicPartition.getPartitionNumber(),
            kafkaUrl,
            beforeProcessingPerRecordTimestampNs);

        if (metricsEnabled) {
          elapsedTimeForPuttingIntoQueue.setValue(
              elapsedTimeForPuttingIntoQueue.getValue() + LatencyUtils.getElapsedTimeFromNSToMS(queuePutStartTimeInNS));
        }
        break;
      case PRODUCED_TO_KAFKA:
      case SKIPPED_MESSAGE:
        break;
      default:
        throw new VeniceException(
            ingestionTaskName + " received unknown DelegateConsumerRecordResult enum for "
                + record.getTopicPartition());
    }
    // Update the latest message consumed time
    partitionConsumptionState.setLatestMessageConsumedTimestampInMs(beforeProcessingBatchRecordsTimestampMs);

    return record.getPayloadSize();
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
      Iterable<DefaultPubSubMessage> records,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    PartitionConsumptionState partitionConsumptionState =
        partitionConsumptionStateMap.get(topicPartition.getPartitionNumber());
    if (partitionConsumptionState == null) {
      throw new VeniceException(
          "PartitionConsumptionState should present for store version: " + kafkaVersionTopic + ", partition: "
              + topicPartition.getPartitionNumber());
    }
    /**
     * Validate and filter out duplicate messages from the real-time topic as early as possible, so that
     * the following batch processing logic won't spend useless efforts on duplicate messages.
      */
    records = validateAndFilterOutDuplicateMessagesFromLeaderTopic(records, kafkaUrl, topicPartition);

    if ((isActiveActiveReplicationEnabled || isWriteComputationEnabled)
        && serverConfig.isAAWCWorkloadParallelProcessingEnabled()
        && IngestionBatchProcessor.isAllMessagesFromRTTopic(records)) {
      produceToStoreBufferServiceOrKafkaInBatch(
          records,
          topicPartition,
          partitionConsumptionState,
          kafkaUrl,
          kafkaClusterId);
      return;
    }

    long totalBytesRead = 0;
    ValueHolder<Double> elapsedTimeForPuttingIntoQueue = new ValueHolder<>(0d);
    boolean metricsEnabled = emitMetrics.get();
    long beforeProcessingBatchRecordsTimestampMs = System.currentTimeMillis();

    partitionConsumptionState = partitionConsumptionStateMap.get(topicPartition.getPartitionNumber());
    for (DefaultPubSubMessage record: records) {
      long beforeProcessingPerRecordTimestampNs = System.nanoTime();
      partitionConsumptionState.setLatestPolledMessageTimestampInMs(beforeProcessingBatchRecordsTimestampMs);
      if (!shouldProcessRecord(record)) {
        continue;
      }

      // Check schema id availability before putting consumer record to drainer queue
      waitReadyToProcessRecord(record);

      int recordSize = handleSingleMessage(
          new PubSubMessageProcessedResultWrapper(record),
          topicPartition,
          partitionConsumptionState,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingPerRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs,
          metricsEnabled,
          elapsedTimeForPuttingIntoQueue);
      totalBytesRead += recordSize;
      if (isGlobalRtDivEnabled()) {
        consumedBytesSinceLastSync.compute(kafkaUrl, (k, v) -> (v == null) ? recordSize : v + recordSize);
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
      if (elapsedTimeForPuttingIntoQueue.getValue() > 0) {
        hostLevelIngestionStats.recordConsumerRecordsQueuePutLatency(
            elapsedTimeForPuttingIntoQueue.getValue(),
            beforeProcessingBatchRecordsTimestampMs);
      }

      hostLevelIngestionStats.recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage());
    }
  }

  protected void produceToStoreBufferServiceOrKafkaInBatch(
      Iterable<DefaultPubSubMessage> records,
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    long totalBytesRead = 0;
    ValueHolder<Double> elapsedTimeForPuttingIntoQueue = new ValueHolder<>(0d);
    boolean metricsEnabled = emitMetrics.get();
    long beforeProcessingBatchRecordsTimestampMs = System.currentTimeMillis();
    /**
     * Split the records into mini batches.
     */
    int batchSize = serverConfig.getAAWCWorkloadParallelProcessingThreadPoolSize();
    List<List<DefaultPubSubMessage>> batches = new ArrayList<>();
    List<DefaultPubSubMessage> ongoingBatch = new ArrayList<>(batchSize);
    Iterator<DefaultPubSubMessage> iter = records.iterator();
    while (iter.hasNext()) {
      DefaultPubSubMessage record = iter.next();
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLatestPolledMessageTimestampInMs(beforeProcessingBatchRecordsTimestampMs);
      }
      if (!shouldProcessRecord(record)) {
        continue;
      }
      waitReadyToProcessRecord(record);
      ongoingBatch.add(record);
      if (ongoingBatch.size() == batchSize) {
        batches.add(ongoingBatch);
        ongoingBatch = new ArrayList<>(batchSize);
      }
    }
    if (!ongoingBatch.isEmpty()) {
      batches.add(ongoingBatch);
    }
    if (batches.isEmpty()) {
      return;
    }
    IngestionBatchProcessor ingestionBatchProcessor = getIngestionBatchProcessor();
    if (ingestionBatchProcessor == null) {
      throw new VeniceException(
          "IngestionBatchProcessor object should present for store version: " + kafkaVersionTopic);
    }
    /**
     * Process records batch by batch.
     */
    for (List<DefaultPubSubMessage> batch: batches) {
      NavigableMap<ByteArrayKey, ReentrantLock> keyLockMap = ingestionBatchProcessor.lockKeys(batch);
      try {
        long beforeProcessingPerRecordTimestampNs = System.nanoTime();
        List<PubSubMessageProcessedResultWrapper> processedResults = ingestionBatchProcessor.process(
            batch,
            partitionConsumptionState,
            topicPartition.getPartitionNumber(),
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingPerRecordTimestampNs,
            beforeProcessingBatchRecordsTimestampMs);

        for (PubSubMessageProcessedResultWrapper processedRecord: processedResults) {
          totalBytesRead += handleSingleMessage(
              processedRecord,
              topicPartition,
              partitionConsumptionState,
              kafkaUrl,
              kafkaClusterId,
              beforeProcessingPerRecordTimestampNs,
              beforeProcessingBatchRecordsTimestampMs,
              metricsEnabled,
              elapsedTimeForPuttingIntoQueue);
        }
      } finally {
        ingestionBatchProcessor.unlockKeys(keyLockMap);
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
      if (elapsedTimeForPuttingIntoQueue.getValue() > 0) {
        hostLevelIngestionStats.recordConsumerRecordsQueuePutLatency(
            elapsedTimeForPuttingIntoQueue.getValue(),
            beforeProcessingBatchRecordsTimestampMs);
      }

      hostLevelIngestionStats.recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage());
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
            "Ignoring exception for replica: {} since the topic-partition has been unsubscribed already.",
            getReplicaId(kafkaVersionTopic, exceptionPartition),
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
        if (isCurrentVersion.getAsBoolean()
            && ExceptionUtils.recursiveClassEquals(partitionException, MemoryLimitExhaustedException.class)) {
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
              "Memory limit reached. Pausing consumption of topic-partition: {}",
              getReplicaId(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber()));
          runnableForKillIngestionTasksForNonCurrentVersions.run();
          if (storageEngine.getStats().hasMemorySpaceLeft()) {
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
                "Ingestion for topic-partition: {} can resume since more space has been reclaimed.",
                getReplicaId(kafkaVersionTopic, exceptionPartition));
            storageEngine.reopenStoragePartition(exceptionPartition);
            // DaVinci is always a follower.
            subscribePartition(pubSubTopicPartition, false);
          }
        } else if (isCurrentVersion.getAsBoolean() && resetErrorReplicaEnabled && !isDaVinciClient) {
          // marking its replica status ERROR which will later be reset by the controller
          zkHelixAdmin.get()
              .setPartitionsToError(
                  serverConfig.getClusterName(),
                  hostName,
                  kafkaVersionTopic,
                  Collections.singletonList(HelixUtils.getPartitionName(kafkaVersionTopic, exceptionPartition)));
          LOGGER.error(
              "Marking current version replica status to ERROR for replica: {}",
              getReplicaId(kafkaVersionTopic, exceptionPartition),
              partitionException);
          // No need to reset again, clearing out the exception.
          partitionIngestionExceptionList.set(exceptionPartition, null);
        } else {
          if (!partitionConsumptionState.isCompletionReported()) {
            reportError(partitionException.getMessage(), exceptionPartition, partitionException);
          } else {
            LOGGER.error(
                "Ignoring exception for replica: {} since it is already online. The replica will continue serving reads, but the data may be stale as it is not actively ingesting data. Please engage the Venice DEV team immediately.",
                getReplicaId(kafkaVersionTopic, exceptionPartition),
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
    maybeUnsubscribeCompletedPartitions(store);

    // record before consumer unsub as it might lead to stale metrics after unsub
    if (emitMetrics.get()) {
      recordQuotaMetrics();
      recordMaxIdleTime();
    }

    /**
     * Check whether current consumer has any subscription or not since 'poll' function will throw
     * {@link IllegalStateException} with empty subscription.
     */
    if (!(consumerHasAnySubscription() || hasAnyPendingSubscription())) {
      if (idleCounter.incrementAndGet() <= getMaxIdleCounter()) {
        String message = ingestionTaskName + " Not subscribed to any partitions ";
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
          LOGGER.info(message);
        }
      } else {
        if (!hybridStoreConfig.isPresent() && serverConfig.isUnsubscribeAfterBatchpushEnabled() && subscribedCount != 0
            && subscribedCount == forceUnSubscribedCount) {
          String msg =
              ingestionTaskName + " Going back to sleep as consumption has finished and topics are unsubscribed";
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.info(msg);
          }
          // long sleep here in case there are more consumer action to perform like KILL/subscription etc.
          Thread.sleep(POST_UNSUB_SLEEP_MS);
          resetIdleCounter();
          if (serverConfig.isSkipChecksAfterUnSubEnabled()) {
            skipAfterBatchPushUnsubEnabled = true;
          }
        } else {
          maybeCloseInactiveIngestionTask();
        }
      }
      return;
    }
    resetIdleCounter();

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
  }

  protected void refreshIngestionContextIfChanged(Store store) throws InterruptedException {
    if (!serverConfig.isResubscriptionTriggeredByVersionIngestionContextChangeEnabled() || !isHybridMode()
        || isSystemStore) {
      return;
    }
    int currentVersionNumber = store.getCurrentVersion();
    // If the store having no current version, we do not need to resubscribe.
    if (currentVersionNumber == Store.NON_EXISTING_VERSION) {
      return;
    }

    boolean isWriteComputeEnabled = store.isWriteComputationEnabled();
    PartitionReplicaIngestionContext.VersionRole newVersionRole =
        PartitionReplicaIngestionContext.determineStoreVersionRole(versionNumber, currentVersionNumber);
    PartitionReplicaIngestionContext.WorkloadType newWorkloadType =
        PartitionReplicaIngestionContext.determineWorkloadType(isActiveActiveReplicationEnabled, isWriteComputeEnabled);
    if (newVersionRole.equals(versionRole) && newWorkloadType.equals(workloadType)) {
      return;
    }

    LOGGER.info(
        "Trigger for version topic: {} due to Previous: version role: {}, workload type: {} "
            + "changed to New: version role: {}, workload type: {}",
        versionTopic,
        versionRole,
        workloadType,
        newVersionRole,
        newWorkloadType);
    versionRole = newVersionRole;
    workloadType = newWorkloadType;
    try {
      resubscribeForAllPartitions();
    } catch (Exception e) {
      LOGGER.error("Error happened during resubscription when store version ingestion role changed.", e);
      hostLevelIngestionStats.recordResubscriptionFailure();
      throw e;
    }
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
              "Unsubscribing completed topic-partition: {}. Current version at this time: {}",
              getReplicaId(versionTopic, state.getPartition()),
              store.getCurrentVersion());
          topicPartitionsToUnsubscribe.add(new PubSubTopicPartitionImpl(versionTopic, state.getPartition()));
          forceUnSubscribedCount++;
        }
      }
      if (!topicPartitionsToUnsubscribe.isEmpty()) {
        consumerBatchUnsubscribe(topicPartitionsToUnsubscribe);
      }
    }
  }

  private void recordMaxIdleTime() {
    long curTime = System.currentTimeMillis();
    long oldest = curTime;
    for (PartitionConsumptionState state: partitionConsumptionStateMap.values()) {
      if (state != null) {
        oldest = Math.min(oldest, state.getLatestPolledMessageTimestampInMs());
      }
    }
    versionedIngestionStats.recordMaxIdleTime(storeName, versionNumber, curTime - oldest);
  }

  private void recordQuotaMetrics() {
    hostLevelIngestionStats.recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage());
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
    LogContext.setRegionLogContext(serverConfig.getRegionName());
    CountDownLatch shutdownLatch = gracefulShutdownLatch.get();
    boolean doFlush = true;
    try {
      // Update thread name to include topic to make it easy debugging
      Thread.currentThread().setName("venice-SIT-" + kafkaVersionTopic);
      LOGGER.info("Running {}", ingestionTaskName);
      versionedIngestionStats.resetIngestionTaskPushTimeoutGauge(storeName, versionNumber);

      Store store = null;
      while (isRunning()) {
        store = storeRepository.getStoreOrThrow(storeName);
        if (!skipAfterBatchPushUnsubEnabled) {
          refreshIngestionContextIfChanged(store);
          processConsumerActions(store);
          checkLongRunningTaskState();
          checkIngestionProgress(store);
          maybeSendIngestionHeartbeat();
          mayResumeRecordLevelMetricsForCurrentVersion();
        } else {
          processConsumerActions(store);
          checkIngestionProgress(store);
        }
        Thread.sleep(this.readCycleDelayMs);
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
            PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
            try {
              CompletableFuture<Void> cmdFuture = storeBufferService.execSyncOffsetCommandAsync(topicPartition, this);
              waitForSyncOffsetCmd(cmdFuture, topicPartition);
              waitForAllMessageToBeProcessedFromTopicPartition(topicPartition, partitionConsumptionState);
            } catch (InterruptedException e) {
              throw new VeniceException(e);
            }
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
      // Release the latch after all the shutdown completes in DVC/Server.
      shutdownLatch.countDown();
    } catch (VeniceIngestionTaskKilledException e) {
      LOGGER.info("{} has been killed.", ingestionTaskName);
      ingestionNotificationDispatcher.reportKilled(partitionConsumptionStateMap.values(), e);
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

  private void waitForSyncOffsetCmd(CompletableFuture<Void> cmdFuture, PubSubTopicPartition topicPartition)
      throws InterruptedException {
    try {
      cmdFuture.get(WAITING_TIME_FOR_LAST_RECORD_TO_BE_PROCESSED, MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn(
          "Got interrupted while waiting for the sync offset command for {}. Cancel command and throw the interrupt exception.",
          topicPartition,
          e);
      throw e;
    } catch (TimeoutException e) {
      LOGGER.warn("Timeout while waiting for the sync offset command for {}. Cancel command.", topicPartition, e);
    } catch (Exception e) {
      LOGGER
          .error("Got exception while waiting for the sync offset command for {}. Cancel command.", topicPartition, e);
    } finally {
      /**
       * If exception happens, async command has to be invalidated because the SIT is going to be closed in SIT thread
       * and its internal state is not reliable anymore. Notice that if the async command is already in the process of
       * execution, cancel will wait for command to finish and not affect its execution. It is also safe to
       * cancel an already finished command, so we can put it in finally block.
       */
      cmdFuture.cancel(true);
    }
  }

  protected void updateOffsetMetadataAndSyncOffset(@Nonnull PartitionConsumptionState pcs) {
    updateOffsetMetadataAndSyncOffset(getDataIntegrityValidator(), pcs);
  }

  protected void updateOffsetMetadataAndSyncOffset(DataIntegrityValidator div, @Nonnull PartitionConsumptionState pcs) {
    /**
     * Offset metadata and producer states must be updated at the same time in OffsetRecord; otherwise, one checkpoint
     * could be ahead of the other.
     *
     * The reason to transform the internal state only during checkpointing is that the intermediate checksum
     * generation is an expensive operation.
     */
    div.updateOffsetRecordForPartition(PartitionTracker.VERSION_TOPIC, pcs.getPartition(), pcs.getOffsetRecord());
    // update the offset metadata in the OffsetRecord.
    updateOffsetMetadataInOffsetRecord(pcs);
    syncOffset(kafkaVersionTopic, pcs);
  }

  /**
   * This version of the method syncs using a PartitionTracker object which contains the vtSegments and LCVO
   */
  protected void updateAndSyncOffsetFromSnapshot(PartitionTracker vtDivSnapshot, PubSubTopicPartition topicPartition) {
    PartitionConsumptionState pcs = getPartitionConsumptionState(topicPartition.getPartitionNumber());
    vtDivSnapshot.updateOffsetRecord(PartitionTracker.VERSION_TOPIC, pcs.getOffsetRecord());
    updateOffsetMetadataInOffsetRecord(pcs);
    syncOffset(kafkaVersionTopic, pcs);
  }

  private void handleIngestionException(Exception e) {
    LOGGER.error(
        "Ingestion failed due to {}. Will propagate to reporters.",
        ingestionTaskName,
        e.getClass().getSimpleName());
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
      ingestionNotificationDispatcher.reportError(partitionId, message, consumerEx);
    } else {
      ingestionNotificationDispatcher.reportError(pcsList, message, consumerEx);
    }
    // Set the replica state to ERROR so that the controller can attempt to reset the partition.
    if (isCurrentVersion.getAsBoolean() && !isDaVinciClient && resetErrorReplicaEnabled
        && !(consumerEx instanceof VeniceTimeoutException)) {
      zkHelixAdmin.get()
          .setPartitionsToError(
              serverConfig.getClusterName(),
              hostName,
              kafkaVersionTopic,
              Collections.singletonList(HelixUtils.getPartitionName(kafkaVersionTopic, partitionId)));
    }
  }

  private void internalClose(boolean doFlush) {
    // Set isRunning to false to prevent messages being added after we've already looped through consumerActionsQueue.
    // Wrapping in synchronized to prevent a race condition on methods reading the value of isRunning.
    synchronized (this) {
      getIsRunning().set(false);
    }

    this.missingSOPCheckExecutor.shutdownNow();

    // Only reset Offset and Drop Partition Messages are important, subscribe/unsubscribe will be handled
    // on the restart by Helix Controller notifications on the new StoreIngestionTask.
    this.storeRepository.unregisterStoreDataChangedListener(this.storageUtilizationManager);
    // Remove all the actions out of the queue in order
    while (!consumerActionsQueue.isEmpty()) {
      ConsumerAction message = consumerActionsQueue.poll();
      ConsumerActionType opType = message.getType();
      String topic = message.getTopic();
      int partition = message.getPartition();
      String replica = getReplicaId(message.getTopic(), message.getPartition());
      try {
        switch (opType) {
          case RESET_OFFSET:
            LOGGER.info("Cleanup Reset OffSet. Replica: {}", replica);
            storageMetadataService.clearOffset(topic, partition);
            message.getFuture().complete(null);
            break;
          case DROP_PARTITION:
            PubSubTopicPartition topicPartition = message.getTopicPartition();
            LOGGER.info("Processing DROP_PARTITION message for {} in internalClose", topicPartition);
            dropPartitionSynchronously(topicPartition);
            message.getFuture().complete(null);
            break;
          case SUBSCRIBE:
            pendingSubscriptionActionCount.decrementAndGet();
          default:
            LOGGER.info("Ignore and cleanup the Message: {} Replica: {}", message, replica);
            break;
        }
      } catch (Exception e) {
        LOGGER.error(
            "{} Error while handling consumer action: {} replica: {} in internalClose",
            ingestionTaskName,
            message,
            replica,
            e);
        message.getFuture().completeExceptionally(e);
      }
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
      closeVeniceViewWriters(doFlush);
    } catch (Exception e) {
      LOGGER.error("Error while closing venice view writer", e);
    }

    try {
      closeVeniceWriters(doFlush);
    } catch (Exception e) {
      LOGGER.error("Error while closing venice writers", e);
    }

    if (topicManagerRepository != null) {
      topicManagerRepository.invalidateTopicManagerCaches(versionTopic);
    }

    for (AutoCloseable closeable: this.thingsToClose) {
      closeQuietlyWithErrorLogged(closeable);
    }

    close();

    synchronized (this) {
      notifyAll();
    }

    LOGGER.info("Store ingestion task for store: {} is closed", kafkaVersionTopic);
  }

  public void closeVeniceWriters(boolean doFlush) {
  }

  protected void closeVeniceViewWriters(boolean doFlush) {
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  void processConsumerActions(Store store) throws InterruptedException {
    boolean metricsEnabled = emitMetrics.get();
    long startTime = metricsEnabled ? System.currentTimeMillis() : 0;
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
            LatencyUtils.getElapsedTimeFromMsToMs(action.getCreateTimestampInMs()));
        action.incrementAttempt();
        processConsumerAction(action, store);
        action.getFuture().complete(null);
        // Remove the action that is processed recently (not necessarily the head of consumerActionsQueue).
        if (consumerActionsQueue.remove(action)) {
          partitionToPendingConsumerActionCountMap.get(action.getPartition()).decrementAndGet();
          if (action.getType().equals(SUBSCRIBE)) {
            pendingSubscriptionActionCount.decrementAndGet();
          }
        }
        LOGGER.info(
            "Finished consumer action {} in {}ms",
            action,
            LatencyUtils.getElapsedTimeFromMsToMs(actionProcessStartTimeInMs));
      } catch (VeniceIngestionTaskKilledException | InterruptedException e) {
        action.getFuture().completeExceptionally(e);
        throw e;
      } catch (Throwable e) {
        if (!handleConsumerActionsError(e, action, actionProcessStartTimeInMs)) {
          return;
        }
      }
    }
    if (metricsEnabled) {
      hostLevelIngestionStats.recordProcessConsumerActionLatency(LatencyUtils.getElapsedTimeFromMsToMs(startTime));
    }
  }

  boolean handleConsumerActionsError(Throwable e, ConsumerAction action, long actionProcessStartTimeInMs) {
    if (action.getAttemptsCount() <= MAX_CONSUMER_ACTION_ATTEMPTS) {
      LOGGER.warn("Failed to process consumer action {}, will retry later.", action, e);
      return false;
    }
    LOGGER.error(
        "Failed to execute consumer action {} after {} attempts. Total elapsed time: {}ms",
        action,
        action.getAttemptsCount(),
        LatencyUtils.getElapsedTimeFromMsToMs(actionProcessStartTimeInMs),
        e);
    // Mark action as failed since it has exhausted all the retries.
    action.getFuture().completeExceptionally(e);
    // After MAX_CONSUMER_ACTION_ATTEMPTS retries we should give up and error the ingestion task.
    PartitionConsumptionState state = partitionConsumptionStateMap.get(action.getPartition());

    // Remove the action that is failed to execute recently (not necessarily the head of consumerActionsQueue).
    if (consumerActionsQueue.remove(action)) {
      partitionToPendingConsumerActionCountMap.get(action.getPartition()).decrementAndGet();
      if (action.getType().equals(SUBSCRIBE)) {
        pendingSubscriptionActionCount.decrementAndGet();
      }
    }
    /**
     * {@link state} can be null if the {@link OffsetRecord} from {@link storageMetadataService} was corrupted in
     * {@link #processCommonConsumerAction}, so the {@link PartitionConsumptionState} was never created
     */
    if (state == null || !state.isCompletionReported()) {
      reportError("Error when processing consumer action: " + action, action.getPartition(), new VeniceException(e));
    }
    return true;
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
      // For separate incremental topic URL, the original URL is not a valid URL, so need to resolve it.
      // There's no issue for TS, as we do topic switch for both real-time and separate incremental topic.
      returnSet.add(kafkaClusterUrlResolver.apply(url.toString()));
    }
    originalTopicSwitch.sourceKafkaServers = returnSet;
    return originalTopicSwitch;
  }

  protected static long getOffsetToOnlineLagThresholdPerPartition(
      Optional<HybridStoreConfig> hybridStoreConfig,
      String storeName,
      int partitionCount) {
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

    return Math.max(lagThreshold / partitionCount, 1);
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
        beginBatchWrite(sorted, newPartitionConsumptionState);

        newPartitionConsumptionState.setStartOfPushTimestamp(storeVersionState.startOfPushTimestamp);
        newPartitionConsumptionState.setEndOfPushTimestamp(storeVersionState.endOfPushTimestamp);

        ingestionNotificationDispatcher.reportRestarted(newPartitionConsumptionState);
      }
    }
    try {
      // Compare the offset lag is acceptable or not, if acceptable, report completed directly, otherwise rely on the
      // normal ready-to-server checker.
      boolean isCompletedReport = false;
      long offsetLagDeltaRelaxFactor = serverConfig.getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart();
      long previousOffsetLag = newPartitionConsumptionState.getOffsetRecord().getOffsetLag();
      if (hybridStoreConfig.isPresent() && newPartitionConsumptionState.isEndOfPushReceived()) {
        long offsetLagThreshold =
            getOffsetToOnlineLagThresholdPerPartition(hybridStoreConfig, storeName, partitionCount);
        // Only enable this feature with positive offset lag delta relax factor and offset lag threshold.
        if (offsetLagDeltaRelaxEnabled && offsetLagThreshold > 0) {
          long offsetLag = measureHybridOffsetLag(newPartitionConsumptionState, true);
          if (previousOffsetLag != OffsetRecord.DEFAULT_OFFSET_LAG) {
            LOGGER.info(
                "Checking offset lag behavior for replica: {}. Current offset lag: {}, previous offset lag: {}, offset lag threshold: {}",
                getReplicaId(versionTopic, partition),
                offsetLag,
                previousOffsetLag,
                offsetLagThreshold);
            if (newPartitionConsumptionState.getReadyToServeInOffsetRecord()
                && (offsetLag < previousOffsetLag + offsetLagDeltaRelaxFactor * offsetLagThreshold)) {
              newPartitionConsumptionState.lagHasCaughtUp();
              reportCompleted(newPartitionConsumptionState, true);
              isCompletedReport = true;
            }
            // Clear offset lag in metadata, it is only used in restart.
            newPartitionConsumptionState.getOffsetRecord().setOffsetLag(OffsetRecord.DEFAULT_OFFSET_LAG);
          }
        }
      }
      // This ready-to-serve check is acceptable in SIT thread as it happens before subscription.
      if (!isCompletedReport) {
        getDefaultReadyToServeChecker().apply(newPartitionConsumptionState);
      }
    } catch (VeniceInconsistentStoreMetadataException e) {
      hostLevelIngestionStats.recordInconsistentStoreMetadata();
      // clear the local store metadata and the replica will be rebuilt from scratch upon retry as part of
      // processConsumerActions.
      storageMetadataService.clearOffset(kafkaVersionTopic, partition);
      storageMetadataService.clearStoreVersionState(kafkaVersionTopic);
      getDataIntegrityValidator().clearPartition(partition);
      throw e;
    }
  }

  protected void processCommonConsumerAction(ConsumerAction consumerAction) throws InterruptedException {
    PubSubTopicPartition topicPartition = consumerAction.getTopicPartition();
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

        if (recordTransformer != null) {
          long startTime = System.nanoTime();
          recordTransformer.internalOnRecovery(storageEngine, partition, partitionStateSerializer, compressor);
          LOGGER.info(
              "DaVinciRecordTransformer onRecovery took {} ms for replica: {}",
              LatencyUtils.getElapsedTimeFromNSToMS(startTime),
              getReplicaId(topic, partition));
        }

        // Get the last persisted Offset record from metadata service
        OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topic, partition);

        // Let's try to restore the state retrieved from the OffsetManager
        PartitionConsumptionState newPartitionConsumptionState = new PartitionConsumptionState(
            getReplicaId(versionTopic, partition),
            partition,
            offsetRecord,
            hybridStoreConfig.isPresent());
        newPartitionConsumptionState.setCurrentVersionSupplier(isCurrentVersion);

        if (isCurrentVersion.getAsBoolean()) {
          // Latch creation is in StateModelIngestionProgressNotifier#startConsumption() from the Helix transition
          newPartitionConsumptionState.setLatchCreated();
        }

        partitionConsumptionStateMap.put(partition, newPartitionConsumptionState);

        // Load the VT segments from the offset record into the appropriate data integrity validator
        getDataIntegrityValidator().setPartitionState(PartitionTracker.VERSION_TOPIC, partition, offsetRecord);

        long consumptionStatePrepTimeStart = System.currentTimeMillis();
        if (!checkDatabaseIntegrity(partition, topic, offsetRecord, newPartitionConsumptionState)) {
          LOGGER.warn(
              "Restart ingestion from the beginning by resetting OffsetRecord for topic-partition: {}. Replica: {}",
              getReplicaId(topic, partition),
              newPartitionConsumptionState.getReplicaId());
          resetOffset(partition, topicPartition, true);
          newPartitionConsumptionState = partitionConsumptionStateMap.get(partition);
          offsetRecord = newPartitionConsumptionState.getOffsetRecord();
        }

        checkConsumptionStateWhenStart(offsetRecord, newPartitionConsumptionState);
        reportIfCatchUpVersionTopicOffset(newPartitionConsumptionState);
        versionedIngestionStats.recordSubscribePrepLatency(
            storeName,
            versionNumber,
            LatencyUtils.getElapsedTimeFromMsToMs(consumptionStatePrepTimeStart));
        updateLeaderTopicOnFollower(newPartitionConsumptionState);
        reportStoreVersionTopicOffsetRewindMetrics(newPartitionConsumptionState);

        // Subscribe to local version topic.
        long subscribeOffset = getLocalVtSubscribeOffset(newPartitionConsumptionState);
        consumerSubscribe(
            topicPartition.getPubSubTopic(),
            newPartitionConsumptionState,
            subscribeOffset,
            localKafkaServer);
        LOGGER.info("Subscribed to: {} Offset {}", topicPartition, subscribeOffset);
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
          ingestionNotificationDispatcher.reportStopped(consumptionState);
        }

        /**
         * Since the processing of the buffered messages are using {@link #partitionConsumptionStateMap} and
         * {@link #kafkaDataValidationService}, we would like to drain all the buffered messages before cleaning up those
         * two variables to avoid the race condition.
         */
        partitionConsumptionStateMap.remove(partition);
        storageUtilizationManager.removePartition(partition);
        getDataIntegrityValidator().clearPartition(partition);
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
      case DROP_PARTITION:
        dropPartitionSynchronously(topicPartition);
        break;
      default:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + getClass().getName());
    }
  }

  private void resetOffset(int partition, PubSubTopicPartition topicPartition, boolean restartIngestion) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);

    if (partitionConsumptionState != null
        && (restartIngestion || consumerHasSubscription(topicPartition.getPubSubTopic(), partitionConsumptionState))) {
      if (restartIngestion) {
        LOGGER.info(
            "Reset offset to restart ingestion for: {}. Replica: {}",
            topicPartition,
            partitionConsumptionState.getReplicaId());
      } else {
        LOGGER.error(
            "Unexpected situation: attempting to reset the offset for: {} without unsubscribing first. Replica: {}",
            topicPartition,
            partitionConsumptionState.getReplicaId());
      }
      /*
       * Only update the consumer and partitionConsumptionStateMap when consumer actually has
       * subscription to this topic/partition; otherwise, we would blindly update the StateMap
       * and mess up other operations on the StateMap.
       */
      try {
        consumerResetOffset(topicPartition.getPubSubTopic(), partitionConsumptionState);
        LOGGER.info(
            "Reset OffSet for topic-partition: {}. Replica: {}",
            topicPartition,
            partitionConsumptionState.getReplicaId());
      } catch (PubSubUnsubscribedTopicPartitionException e) {
        LOGGER.error(
            "Kafka consumer should have subscribed to the partition already but it fails "
                + "on resetting offset for: {}. Replica: {}",
            topicPartition,
            partitionConsumptionState.getReplicaId());
      }
      PartitionConsumptionState consumptionState = new PartitionConsumptionState(
          getReplicaId(versionTopic, partition),
          partition,
          new OffsetRecord(partitionStateSerializer),
          hybridStoreConfig.isPresent());
      consumptionState.setCurrentVersionSupplier(isCurrentVersion);
      partitionConsumptionStateMap.put(partition, consumptionState);
      storageUtilizationManager.initPartition(partition);
      // Reset the error partition tracking
      partitionIngestionExceptionList.set(partition, null);
      failedPartitions.remove(partition);
    } else {
      LOGGER.info(
          "{} No need to reset offset by Kafka consumer, since the consumer is not subscribed to: {}",
          ingestionTaskName,
          topicPartition);
    }
    getDataIntegrityValidator().clearPartition(partition);
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
          "Offset rewind for version topic-partition: {}, persisted record offset: {}, Kafka topic partition end-offset: {}",
          getReplicaId(kafkaVersionTopic, pcs.getPartition()),
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
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    long offsetFromConsumer =
        aggKafkaConsumerService.getLatestOffsetBasedOnMetrics(kafkaUrl, versionTopic, topicPartition);
    if (offsetFromConsumer >= 0) {
      return offsetFromConsumer;
    }
    try {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoffNoLog(() -> {
        long offset = getTopicManager(kafkaUrl).getLatestOffsetCachedNonBlocking(pubSubTopic, partition);
        if (offset == UNKNOWN_LATEST_OFFSET) {
          throw new VeniceException("Latest offset is unknown. Check if the topic: " + topicPartition + " exists.");
        }
        return offset;
      },
          MAX_OFFSET_FETCH_ATTEMPTS,
          Duration.ofMillis(10),
          Duration.ofMillis(500),
          Duration.ofSeconds(5),
          RETRY_FAILURE_TYPES);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to get end offset for topic-partition: {} with kafka url {} even after {} retries",
          topicPartition,
          kafkaUrl,
          MAX_OFFSET_FETCH_ATTEMPTS,
          e);
      return StatsErrorCode.LAG_MEASUREMENT_FAILURE.code;
    }
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
  protected boolean shouldProcessRecord(DefaultPubSubMessage record) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(record.getPartition());

    if (partitionConsumptionState == null) {
      String msg = "PCS for replica: " + getReplicaId(kafkaVersionTopic, record.getPartition())
          + " is null. Skipping incoming record with topic-partition: {} and offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getTopicPartition(), record.getPosition());
      }
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      String msg = "Replica:  " + partitionConsumptionState.getReplicaId()
          + " is already errored. Skipping incoming record with topic-partition: {} and offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getTopicPartition(), record.getPosition());
      }
      return false;
    }

    // Just a sanity check for something that shouldn't ever happen. Skip it and log a warning.
    if (record.getKey().isGlobalRtDiv() && record.getTopic().isRealTime()) {
      LOGGER.warn("Skipping Global RT DIV message from realtime topic partition: {}", record.getTopicPartition());
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
          + " is neither hybrid nor incremental push enabled, so will skip it. Current records replica: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
        LOGGER.warn(message, partitionConsumptionState.getReplicaId());
      }
      return false;
    }
    return true;
  }

  protected boolean shouldPersistRecord(
      DefaultPubSubMessage record,
      PartitionConsumptionState partitionConsumptionState) {
    int partitionId = record.getTopicPartition().getPartitionNumber();
    String replicaId = getReplicaId(kafkaVersionTopic, partitionId);
    if (failedPartitions.contains(partitionId)) {
      String msg = "Errors already exist for replica: " + replicaId + ", skipping incoming record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info("{} with topic-partition: {} and offset {}", msg, record.getTopicPartition(), record.getPosition());
      }
      return false;
    }
    if (partitionConsumptionState == null || !partitionConsumptionState.isSubscribed()) {
      String msg = "PCS for replica: " + replicaId
          + " is null or it is not subscribed to any topic-partition. Skipping incoming record with topic-partition: {} and offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getTopicPartition(), record.getPosition());
      }
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      String msg = "Replica: " + replicaId + " is already errored, skipping incoming record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info("{} with topic-partition: {} and offset {}", msg, record.getTopicPartition(), record.getPosition());
      }
      return false;
    }

    if (this.suppressLiveUpdates && partitionConsumptionState.isCompletionReported()) {
      String msg = "Skipping message as live update suppression is enabled and replica: " + replicaId
          + " is already ready to serve, these are buffered records in the queue. Incoming record with topic-partition: {} and offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getTopicPartition(), record.getPosition());
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
      String msg = "Skipping message as it is using ingestion isolation and replica: " + replicaId
          + " is already ready to serve, these are buffered records in the queue. Incoming record with topic-partition: {} and offset: {}";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        LOGGER.info(msg, record.getTopicPartition(), record.getPosition());
      }
      return false;
    }

    return true;
  }

  /**
   * This function will be invoked in {@link StoreBufferService} to process buffered {@link PubSubMessage}.
   */
  public void processConsumerRecord(
      DefaultPubSubMessage record,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    // The partitionConsumptionStateMap can be modified by other threads during consumption (for example when
    // unsubscribing)
    // in order to maintain thread safety, we hold onto the reference to the partitionConsumptionState and pass that
    // reference to all downstream methods so that all offset persistence operations use the same
    // partitionConsumptionState
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
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
      String replicaId = getReplicaId(versionTopic, faultyPartition);
      String errorMessage;
      errorMessage = FATAL_DATA_VALIDATION_ERROR + " for replica: " + replicaId + ". Incoming record topic-partition: "
          + record.getTopicPartition() + " offset: " + record.getPosition();
      // TODO need a way to safeguard DIV errors from backup version that have once been current (but not anymore)
      // during re-balancing
      boolean needToUnsub = !(isCurrentVersion.getAsBoolean() || partitionConsumptionState.isEndOfPushReceived());
      if (needToUnsub) {
        errorMessage += ". Consumption will be halted.";
        ingestionNotificationDispatcher
            .reportError(Collections.singletonList(partitionConsumptionState), errorMessage, e);
        unSubscribePartition(new PubSubTopicPartitionImpl(versionTopic, faultyPartition));
      } else {
        LOGGER.warn(
            "{}. Consumption will continue because it is either a current version replica or EOP has already been received. {}",
            errorMessage,
            e.getMessage());
      }
    } catch (VeniceMessageException | UnsupportedOperationException e) {
      throw new VeniceException(
          ingestionTaskName + " : Received an exception for message at partition: "
              + record.getTopicPartition().getPartitionNumber() + ", offset: " + record.getPosition()
              + ". Bubbling up.",
          e);
    }

    if (diskUsage.isDiskFull(recordSize)) {
      throw new DiskLimitExhaustedException(storeName, versionNumber, diskUsage.getDiskStatus());
    }

    /*
     * Report ingestion throughput metric based on the store version
     */
    if (!record.getKey().isControlMessage() && !record.getKey().isGlobalRtDiv()) { // skip control messages and DIV
      // Still track record throughput to understand the performance benefits of disabling other record-level metrics.
      hostLevelIngestionStats.recordTotalRecordsConsumed();
      if (recordLevelMetricEnabled.get()) {
        versionedIngestionStats.recordBytesConsumed(storeName, versionNumber, recordSize);
        versionedIngestionStats.recordRecordsConsumed(storeName, versionNumber);
        /*
         * Meanwhile, contribute to the host-level ingestion throughput rate, which aggregates the consumption rate across
         * all store versions.
         */
        hostLevelIngestionStats.recordTotalBytesConsumed(recordSize);
        /*
         * Also update this stats separately for Leader and Follower.
         */
        recordProcessedRecordStats(partitionConsumptionState, recordSize);
      }
      partitionConsumptionState.incrementProcessedRecordSizeSinceLastSync(recordSize);
    }
    reportIfCatchUpVersionTopicOffset(partitionConsumptionState);

    long syncBytesInterval = getSyncBytesInterval(partitionConsumptionState);
    boolean recordsProcessedAboveSyncIntervalThreshold = (syncBytesInterval > 0
        && (partitionConsumptionState.getProcessedRecordSizeSinceLastSync() >= syncBytesInterval));
    getDefaultReadyToServeChecker().apply(partitionConsumptionState, recordsProcessedAboveSyncIntervalThreshold);

    /**
     * Syncing offset checking in syncOffset() should be the very last step for processing a record.
     *
     * Check whether offset metadata checkpoint will happen; if so, update the producer states recorded in OffsetRecord
     * with the updated producer states maintained in {@link #drainerDiv}
     */
    if (shouldSyncOffset(partitionConsumptionState, record, leaderProducedRecordContext)) {
      updateOffsetMetadataAndSyncOffset(partitionConsumptionState);
      if (isHybridMode()) {
        hostLevelIngestionStats.recordTotalDuplicateKeys(storageEngine.getStats().getDuplicateKeyCountEstimate());
        hostLevelIngestionStats.recordTotalKeyCount(storageEngine.getStats().getKeyCountEstimate());
      }
    }
  }

  long getSyncBytesInterval(PartitionConsumptionState pcs) {
    return pcs.isDeferredWrite()
        ? databaseSyncBytesIntervalForDeferredWriteMode
        : databaseSyncBytesIntervalForTransactionalMode;
  }

  protected void recordHeartbeatReceived(
      PartitionConsumptionState partitionConsumptionState,
      DefaultPubSubMessage consumerRecord,
      String kafkaUrl) {
    // No Op
  }

  protected boolean shouldSendGlobalRtDiv(DefaultPubSubMessage record, PartitionConsumptionState pcs, String kafkaUrl) {
    if (!isGlobalRtDivEnabled()) {
      return false;
    }

    // The Global RT DIV is sent on a per-broker basis, so divide the size limit by the number of brokers
    final long syncBytesInterval = getSyncBytesInterval(pcs) / getConsumedBytesSinceLastSync().size();
    boolean shouldSync = false;
    if (!record.getKey().isControlMessage()) {
      shouldSync = syncBytesInterval > 0 && (getConsumedBytesSinceLastSync().get(kafkaUrl) >= syncBytesInterval);
    }
    return shouldSync;
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
   *
   * When the Global RT DIV feature is enabled, the size-based sync is no longer triggered by the drainer. The
   * condition is on the ConsumptionTask and bytes consumed rather than bytes processed. The VT DIV is still synced
   * by the drainer whenever a non-SOS/EOS control message is processed.
   */
  boolean shouldSyncOffset(
      PartitionConsumptionState pcs,
      DefaultPubSubMessage record,
      LeaderProducedRecordContext leaderProducedRecordContext) {
    boolean syncOffset = false;
    if (record.getKey().isControlMessage()) {
      ControlMessage controlMessage = (leaderProducedRecordContext == null
          ? (ControlMessage) record.getValue().getPayloadUnion()
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
      if (!controlMessageType.isSegmentControlMessage()) {
        syncOffset = true;
      }
    } else {
      if (isGlobalRtDivEnabled()) {
        return false; // for the Global RT DIV feature, size-based sync is by ConsumptionTask rather than Drainer
      }

      final long syncBytesInterval = getSyncBytesInterval(pcs);
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
    if (this.storageEngine.isClosed()) {
      LOGGER.warn("Storage engine has been closed. Could not execute sync offset for replica: {}", pcs.getReplicaId());
      return;
    }
    // Flush data partition
    final AtomicReference<Map<String, String>> dbCheckpointingInfoReference = new AtomicReference<>();
    executeStorageEngineRunnable(partition, () -> {
      Map<String, String> dbCheckpointingInfoFinal = this.storageEngine.sync(partition);
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
    String msg = "Offset synced for replica: " + pcs.getReplicaId() + " - localVtOffset: {}";
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
      LOGGER.info(msg, offsetRecord.getLocalVersionTopicOffset());
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

  /**
   * Records metrics for the original size of full-assembled records (key + value) and RMD by utilizing the field
   * {@link ChunkedValueManifest#size}.
   * Also records the ratio of assembled record size to maximum allowed size, which is intended to be used to alert
   * customers about how close they are to hitting the size limit.
   * @param keyLen The size of the record's key
   * @param valueBytes {@link Put#putValue} which is expected to be a serialized {@link ChunkedValueManifest}
   * @param rmdBytes {@link Put#replicationMetadataPayload} which can be a serialized {@link ChunkedValueManifest} if
   * RMD chunking was enabled or just the RMD payload otherwise
   */
  protected void recordAssembledRecordSize(int keyLen, ByteBuffer valueBytes, ByteBuffer rmdBytes, long currentTimeMs) {
    try {
      byte[] valueByteArray = ByteUtils.extractByteArray(valueBytes);
      ChunkedValueManifest valueManifest = manifestSerializer.deserialize(valueByteArray, CHUNK_MANIFEST_SCHEMA_ID);
      int recordSize = keyLen + valueManifest.getSize();
      hostLevelIngestionStats.recordAssembledRecordSize(recordSize, currentTimeMs);
      recordAssembledRecordSizeRatio(calculateAssembledRecordSizeRatio(recordSize), currentTimeMs);

      if (rmdBytes == null || rmdBytes.remaining() == 0) {
        return;
      }

      int rmdSize = rmdBytes.remaining();
      if (isRmdChunked) {
        byte[] rmdByteArray = ByteUtils.extractByteArray(rmdBytes);
        ChunkedValueManifest rmdManifest = manifestSerializer.deserialize(rmdByteArray, CHUNK_MANIFEST_SCHEMA_ID);
        rmdSize = rmdManifest.getSize();
      }
      hostLevelIngestionStats.recordAssembledRmdSize(rmdSize, currentTimeMs);
    } catch (VeniceException | IllegalArgumentException | AvroRuntimeException e) {
      LOGGER.error("Failed to deserialize ChunkedValueManifest to record the assembled record or RMD size", e);
    }
  }

  protected abstract void recordAssembledRecordSizeRatio(double ratio, long currentTimeMs);

  protected abstract double calculateAssembledRecordSizeRatio(long recordSize);

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
    return measureLagWithCallToPubSub(pubSubServerName, topic, partition, currentOffset, this::getTopicManager);
  }

  protected static long measureLagWithCallToPubSub(
      String pubSubServerName,
      PubSubTopic topic,
      int partition,
      long currentOffset,
      Function<String, TopicManager> topicManagerProvider) {
    if (currentOffset < OffsetRecord.LOWEST_OFFSET) {
      // -1 is a valid offset, which means that nothing was consumed yet, but anything below that is invalid.
      return Long.MAX_VALUE;
    }
    TopicManager tm = topicManagerProvider.apply(pubSubServerName);
    long endOffset = tm.getLatestOffsetCached(topic, partition);
    if (endOffset < 0) {
      // A negative value means there was a problem in measuring the end offset, and therefore we return "infinite lag"
      return Long.MAX_VALUE;
    } else if (endOffset == 0) {
      /**
       * Topics which were never produced to have an end offset of zero. Such topics are empty and therefore, by
       * definition, there cannot be any lag.
       *
       * Note that the reverse is not true: a topic can be currently empty and have an end offset above zero, if it had
       * messages produced to it before, which have since then disappeared (e.g. due to time-based retention).
       */
      return 0;
    }

    /**
     * A topic with an end offset of zero is empty. A topic with a single message in it will have an end offset of 1,
     * while that single message will have offset 0. In such single message topic, a consumer which fully scans the
     * topic would have a current offset of 0, while the topic has an end offset of 1, and therefore we need to subtract
     * 1 from the end offset in order to arrive at the correct lag of 0.
     */
    return endOffset - 1 - currentOffset;
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
      PartitionConsumptionState partitionConsumptionState) {
    StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
    if (partitionConsumptionState.isEndOfPushReceived()) {
      LOGGER.warn(
          "Received START_OF_PUSH after END_OF_PUSH for replica: {}. This may indicate out-of-order or "
              + "duplicate control messages. Details: Incoming SoP: {}, Previous SoP timestamp: {}, Previous "
              + "EoP timestamp: {}. The message will be ignored.",
          partitionConsumptionState.getReplicaId(),
          startOfPush,
          partitionConsumptionState.getStartOfPushTimestamp(),
          partitionConsumptionState.getEndOfPushTimestamp());
      return;
    }

    /*
     * Notify the underlying store engine about starting batch push.
     */
    final boolean sorted;
    if (serverConfig.getRocksDBServerConfig().isBlobFilesEnabled() && isHybridMode()) {
      /**
       * We would like to skip {@link RocksDBSstFileWriter} for hybrid stores
       * when RocksDB blob mode is enabled and here are the reasons:
       * 1. Hybrid stores will use the same amount of MemTables eventually even not in batch processing phase.
       * 2. SSTFileWriter + RocksDB blob mode will introduce additional space overhead in the following way:
       *    a. SSTFileWriter doesn't support RocksDB blob mode, which means even with blob enabled, SSTFileWriter
       *       will still write both key and value into the same SST file regardless of value size.
       *    b. When RocksDB ingests the generated SST files, it will put them in the bottom level.
       *    c. After finishing the batch portion, RocksDB won't use SSTFileWriter anymore and in the regular mode, when
       *       RocksDB blob mode is enabled, RocksDB will store the value in blob files when the value size is
       *       larger than the configured threshold, and this also means the LSM tree built by the real-time writes
       *       will be much smaller as it contains keys and smaller values/value pointers.
       *    d. As LSM tree is small, it is not easy to trigger a compaction in the bottom level (the bottom - 1 level
       *       needs to keep enough data to trigger the bottom-level compaction), so the staled entries in the bottom
       *       level will remain for a long time.
       *    e. RocksDB blob config tuning won't affect the large bottom-level SST files.
       * 3. If we disable SSTFileWriter for hybrid stores when RocksDB blob mode is enabled, all the writes will
       *    go through MemTable and key/value separation logic will apply all the time, and blob related configs
       *    will apply to all the writes.
       */
      sorted = false;
    } else {
      sorted = startOfPush.sorted;
    }

    StoreVersionState persistedStoreVersionState =
        storageMetadataService.computeStoreVersionState(kafkaVersionTopic, previousStoreVersionState -> {
          if (previousStoreVersionState == null) {
            // No other partition of the same topic has started yet, let's initialize the StoreVersionState
            StoreVersionState newStoreVersionState = new StoreVersionState();
            newStoreVersionState.sorted = sorted;
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
                "Persisted {} for the first time following a SOP for topic {} with sorted: {}.",
                StoreVersionState.class.getSimpleName(),
                kafkaVersionTopic,
                newStoreVersionState.sorted);
            return newStoreVersionState;
          } else if (previousStoreVersionState.chunked != startOfPush.chunked) {
            // Something very wrong is going on ): ...
            throw new VeniceException(
                "Unexpected: received multiple " + ControlMessageType.START_OF_PUSH.name()
                    + " control messages with inconsistent 'chunked' fields within the same topic!");
          } else if (previousStoreVersionState.sorted != sorted) {
            if (!isHybridMode()) {
              // Something very wrong is going on ): ...
              throw new VeniceException(
                  "Unexpected: received multiple " + ControlMessageType.START_OF_PUSH.name()
                      + " control messages with inconsistent 'sorted' fields within the same topic!"
                      + " And persisted sorted value: " + previousStoreVersionState.sorted
                      + " is different from the current one: " + sorted);
            }
            /**
             * Because of the blob-db integration, `SIT` will forcibly set `sorted` to `false` for hybrid stores (check the
             * above javadoc) and inconsistent `sorted` can happen during the rolling out/rolling back blob-db features.
             * Here is one case how it can happen during the rolling out of blob-db:
             * 1. P1 processes `SOP` with `sorted=true` and persist it in `StoreVersionState`.
             * 2. P2 hasn't started processing `SOP` yet.
             * 3. Restart the cluster and roll out blob-db feature.
             * 4. when P2 processes `SOP` with `sorted=true`, it will forcibly set `sorted=false`, and it will be different
             *    from the previously persisted `StoreVersionState`.
             * 5. This is fine as long as we just follow the previously persisted `StoreVersionState`.
             * 6. Here are the reasons why step 5 is true:
             *    a. If the input for a given partition is truly sorted, it can always be ingested as unsorted data.
             *    b. If the input for a given partition is not sorted, the underlying `SSTFileWriter` will throw exception
             *       when we try to ingest it as sorted data.
             */
            LOGGER.warn(
                "Store version state for topic {} has already been initialized with a different value of 'sorted': {}",
                kafkaVersionTopic,
                previousStoreVersionState.sorted);
          }
          // No need to mutate it, so we return it as is
          return previousStoreVersionState;
        });

    ingestionNotificationDispatcher.reportStarted(partitionConsumptionState);
    beginBatchWrite(persistedStoreVersionState.sorted, partitionConsumptionState);
    partitionConsumptionState.setStartOfPushTimestamp(startOfPushKME.producerMetadata.messageTimestamp);
  }

  protected void processEndOfPush(
      KafkaMessageEnvelope endOfPushKME,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {

    // Do not process duplication EOP messages.
    if (partitionConsumptionState.getOffsetRecord().isEndOfPushReceived()) {
      LOGGER.warn(
          "Received duplicate EOP control message, ignoring it. Replica: {}, Offset: {}",
          partitionConsumptionState.getReplicaId(),
          offset);
      return;
    }

    // We need to keep track of when the EOP happened, as that is used within Hybrid Stores' lag measurement
    partitionConsumptionState.getOffsetRecord().endOfPushReceived(offset);
    /*
     * Right now, we assume there are no sorted message after EndOfPush control message.
     * TODO: if this behavior changes in the future, the logic needs to be adjusted as well.
     */
    StoragePartitionConfig storagePartitionConfig = getStoragePartitionConfig(false, partitionConsumptionState);

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
    ingestionNotificationDispatcher.reportEndOfPushReceived(partitionConsumptionState);

    if (isDataRecovery && partitionConsumptionState.isBatchOnly()) {
      partitionConsumptionState.setDataRecoveryCompleted(true);
      ingestionNotificationDispatcher.reportDataRecoveryCompleted(partitionConsumptionState);
    }
  }

  protected void processStartOfIncrementalPush(
      ControlMessage startOfIncrementalPush,
      PartitionConsumptionState partitionConsumptionState) {
    CharSequence startVersion = ((StartOfIncrementalPush) startOfIncrementalPush.controlMessageUnion).version;
    if (!batchReportIncPushStatusEnabled || partitionConsumptionState.isComplete()) {
      ingestionNotificationDispatcher
          .reportStartOfIncrementalPushReceived(partitionConsumptionState, startVersion.toString());
    }
  }

  protected void processEndOfIncrementalPush(
      ControlMessage endOfIncrementalPush,
      PartitionConsumptionState partitionConsumptionState) {
    CharSequence endVersion = ((EndOfIncrementalPush) endOfIncrementalPush.controlMessageUnion).version;
    if (!batchReportIncPushStatusEnabled || partitionConsumptionState.isComplete()) {
      ingestionNotificationDispatcher
          .reportEndOfIncrementalPushReceived(partitionConsumptionState, endVersion.toString());
    } else {
      LOGGER.info(
          "Adding incremental push: {} to pending batch report list for replica: {}.",
          endVersion.toString(),
          partitionConsumptionState.getReplicaId());
      partitionConsumptionState.addIncPushVersionToPendingReportList(endVersion.toString());
    }
  }

  /**
   *  This isn't used for ingestion outside L/F, so we NoOp here and rely on the actual implementation in
   *  {@link LeaderFollowerStoreIngestionTask}
   */
  protected void processControlMessageForViews(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {
    // NoOp
  }

  protected void processTopicSwitch(
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
  private void processControlMessage(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      long offset,
      PartitionConsumptionState partitionConsumptionState) {
    /**
     * If leader consumes control messages from topics other than version topic, it should produce
     * them to version topic; however, START_OF_SEGMENT and END_OF_SEGMENT should not be forwarded
     * because the leader producer will keep track of its own segment state; besides, leader/follower
     * model will not encounter START_OF_BUFFER_REPLAY because TOPIC_SWITCH will replace it in L/F
     * model; incremental push is also a mutually exclusive feature with hybrid stores.
     */
    final ControlMessageType type = ControlMessageType.valueOf(controlMessage);
    if (!type.isSegmentControlMessage()) {
      LOGGER.info(
          "Received {} control message. Replica: {}, Offset: {}",
          type.name(),
          partitionConsumptionState.getReplicaId(),
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
        processEndOfPush(kafkaMessageEnvelope, offset, partitionConsumptionState);
        break;
      case START_OF_SEGMENT:
        break;
      case END_OF_SEGMENT:
        break;
      case VERSION_SWAP:
        if (recordTransformer != null) {
          VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
          int currentVersion =
              Version.parseVersionFromVersionTopicName(versionSwap.getOldServingVersionTopic().toString());
          int futureVersion =
              Version.parseVersionFromVersionTopicName(versionSwap.getNewServingVersionTopic().toString());
          recordTransformer.onVersionSwap(currentVersion, futureVersion, partition);
        }
        break;
      case START_OF_INCREMENTAL_PUSH:
        processStartOfIncrementalPush(controlMessage, partitionConsumptionState);
        break;
      case END_OF_INCREMENTAL_PUSH:
        processEndOfIncrementalPush(controlMessage, partitionConsumptionState);
        break;
      case TOPIC_SWITCH:
        TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;
        LOGGER.info(
            "Received {} control message. Replica: {}, Offset: {} NewSource: {}",
            type.name(),
            partitionConsumptionState.getReplicaId(),
            offset,
            topicSwitch.getSourceKafkaServers());
        processTopicSwitch(controlMessage, partition, offset, partitionConsumptionState);
        break;
      default:
        throw new UnsupportedMessageTypeException(
            "Unrecognized Control message type " + controlMessage.controlMessageType);
    }
    processControlMessageForViews(kafkaKey, kafkaMessageEnvelope, controlMessage, partition, partitionConsumptionState);
  }

  /**
   * Sync the metadata about offset in {@link OffsetRecord}.
   * {@link PartitionConsumptionState} will pass through some information to {@link OffsetRecord} for persistence and
   * Offset rewind/split brain has been guarded in {@link #updateLatestInMemoryProcessedOffset}.
   */
  protected abstract void updateOffsetMetadataInOffsetRecord(PartitionConsumptionState partitionConsumptionState);

  /**
   * Maintain the latest processed offsets by drainers in memory; in most of the time, these offsets are ahead of the
   * checkpoint offsets inside {@link OffsetRecord}. Prior to update the offset in memory, the underlying storage engine
   * should have persisted the given record.
   *
   * Dry-run mode will only do offset rewind check and it won't update the processed offset.
   */
  protected abstract void updateLatestInMemoryProcessedOffset(
      PartitionConsumptionState partitionConsumptionState,
      DefaultPubSubMessage consumerRecordWrapper,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl,
      boolean dryRun);

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @return the size of the data written to persistent storage.
   */
  private int internalProcessConsumerRecord(
      DefaultPubSubMessage consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    int sizeOfPersistedData = 0;
    try {
      long currentTimeMs = System.currentTimeMillis();
      if (recordLevelMetricEnabled.get()) {
        // Assumes the timestamp on the record is the broker's timestamp when it received the message.
        long producerBrokerLatencyMs =
            Math.max(consumerRecord.getPubSubMessageTime() - kafkaValue.producerMetadata.messageTimestamp, 0);
        long brokerConsumerLatencyMs = Math.max(currentTimeMs - consumerRecord.getPubSubMessageTime(), 0);
        recordWriterStats(currentTimeMs, producerBrokerLatencyMs, brokerConsumerLatencyMs, partitionConsumptionState);
      }

      // Only the ConsumptionTask validates messages if Global RT DIV is enabled, so we don't need to validate here
      if (!isGlobalRtDivEnabled()) {
        drainerValidateMessage(consumerRecord, partitionConsumptionState, leaderProducedRecordContext);
      }

      if (batchReportIncPushStatusEnabled) {
        maybeReportBatchEndOfIncPushStatus(partitionConsumptionState);
      }

      /**
       heavy internal processing starts here
       **/
      if (recordLevelMetricEnabled.get()) {
        versionedIngestionStats.recordInternalPreprocessingLatency(
            storeName,
            versionNumber,
            LatencyUtils.getElapsedTimeFromMsToMs(currentTimeMs),
            currentTimeMs);
      }

      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (leaderProducedRecordContext == null
            ? (ControlMessage) kafkaValue.payloadUnion
            : (ControlMessage) leaderProducedRecordContext.getValueUnion());
        processControlMessage(
            kafkaKey,
            kafkaValue,
            controlMessage,
            consumerRecord.getTopicPartition().getPartitionNumber(),
            consumerRecord.getPosition().getNumericOffset(),
            partitionConsumptionState);
        try {
          if (controlMessage.controlMessageType == START_OF_SEGMENT.getValue()
              && Arrays.equals(consumerRecord.getKey().getKey(), KafkaKey.HEART_BEAT.getKey())) {
            recordHeartbeatReceived(partitionConsumptionState, consumerRecord, kafkaUrl);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to record Record heartbeat with message: ", e);
        }
      } else {
        updateLatestInMemoryProcessedOffset(
            partitionConsumptionState,
            consumerRecord,
            leaderProducedRecordContext,
            kafkaUrl,
            true);
        sizeOfPersistedData = processKafkaDataMessage(
            consumerRecord,
            partitionConsumptionState,
            leaderProducedRecordContext,
            currentTimeMs);
        if (recordLevelMetricEnabled.get()) {
          recordNearlineLocalBrokerToReadyToServerLatency(
              storeName,
              versionNumber,
              partitionConsumptionState,
              kafkaValue,
              leaderProducedRecordContext);
        }
      }
      if (recordLevelMetricEnabled.get()) {
        versionedIngestionStats.recordConsumedRecordEndToEndProcessingLatency(
            storeName,
            versionNumber,
            LatencyUtils.getElapsedTimeFromNSToMS(beforeProcessingRecordTimestampNs),
            currentTimeMs);
      }
    } catch (DuplicateDataException e) {
      divErrorMetricCallback.accept(e);
      LOGGER.debug(
          "Skipping a duplicate record at offset: {} from topic-partition: {} for replica: {}",
          consumerRecord.getPosition(),
          consumerRecord.getTopicPartition(),
          partitionConsumptionState.getReplicaId());
    } catch (PersistenceFailureException ex) {
      if (partitionConsumptionStateMap.containsKey(consumerRecord.getTopicPartition().getPartitionNumber())) {
        // If we actually intend to be consuming this partition, then we need to bubble up the failure to persist.
        LOGGER.error(
            "Met PersistenceFailureException for replica: {} while processing record with offset: {}, topic-partition: {}, meta data of the record: {}",
            partitionConsumptionState.getReplicaId(),
            consumerRecord.getPosition(),
            consumerRecord.getTopicPartition(),
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
      LOGGER.info(
          "PCS for replica: {} is null, will skip offset update. Processed record was from topic-partition: {} offset: {}",
          getReplicaId(versionTopic, consumerRecord.getPartition()),
          consumerRecord.getTopicPartition(),
          consumerRecord.getPosition());
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
          kafkaUrl,
          false);
    }
    return sizeOfPersistedData;
  }

  /**
   * DIV check will happen for every single message in drainer queues.
   * Leader replicas will run DIV check twice for every single message (one in leader consumption thread before
   * producing to local version topic; the other one is here); the extra overhead is mandatory for a few reasons:
   * 1. We need DIV check in consumption phase in order to filter out unneeded data from Kafka topic
   * 2. Consumption states are always ahead of drainer states, because there are buffering in between: kafka producing,
   *    drainer buffers; so we cannot persist the DIV check results from consumption phases
   * 3. The DIV info checkpoint on disk must match the actual data persistence which is done inside drainer threads.
   */
  private void drainerValidateMessage(
      DefaultPubSubMessage consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext) {
    boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
    try {
      if (leaderProducedRecordContext == null || leaderProducedRecordContext.hasCorrespondingUpstreamMessage()) {
        /**
         * N.B.: If a leader server is processing a chunk, then the {@link consumerRecord} is going to be the same for
         * every chunk, and we don't want to treat them as dupes, hence we skip DIV. The DIV state will get updated on
         * the last message of the sequence, which is not a chunk but rather the manifest.
         *
         * TODO:
         * This function is called by drainer threads and we need to transfer its full responsibility to the
         * consumer DIV which resides in the consumer thread and then gradually retire the use of drainer DIV.
         * However, given that DIV heartbeat is yet implemented, so keep drainer DIV the way as is today and let the
         * VERSION_TOPIC to contain both rt and vt messages.
         */
        validateMessage(
            PartitionTracker.VERSION_TOPIC,
            this.drainerDiv,
            consumerRecord,
            endOfPushReceived,
            partitionConsumptionState,
            /**
             * N.B.: For A/A enabled stores, the drainer DIV is useless, since upstream of here we may have filtered
             * out any message due to DCR. So we'll still try to perform the DIV on a best-effort basis, but we pass
             * {@link #isActiveActiveReplicationEnabled} in the extraTolerateMissingMessageCondition param to indicate
             * that if the store is A/A, then we can't trust the DIV results.
             */
            this.isActiveActiveReplicationEnabled);
      }
      if (recordLevelMetricEnabled.get()) {
        versionedDIVStats.recordSuccessMsg(storeName, versionNumber);
      }
    } catch (FatalDataValidationException fatalException) {
      if (!endOfPushReceived) {
        throw fatalException;
      } else {
        LOGGER.warn(
            "Encountered errors during updating metadata for 2nd round DIV validation "
                + "after EOP consuming from: {} offset: {} replica: {} ExMsg: {}",
            consumerRecord.getTopicPartition(),
            consumerRecord.getPosition(),
            partitionConsumptionState.getReplicaId(),
            fatalException.getMessage());
      }
    }
  }

  protected abstract void recordWriterStats(
      long consumerTimestampMs,
      long producerBrokerLatencyMs,
      long brokerConsumerLatencyMs,
      PartitionConsumptionState partitionConsumptionState);

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
      PartitionTracker.TopicType type,
      DataIntegrityValidator validator,
      DefaultPubSubMessage consumerRecord,
      boolean endOfPushReceived,
      PartitionConsumptionState partitionConsumptionState,
      boolean tolerateMissingMessagesForRealTimeTopic) {
    KafkaKey key = consumerRecord.getKey();
    if (key.isControlMessage() && Arrays.equals(KafkaKey.HEART_BEAT.getKey(), key.getKey())) {
      return; // Skip validation for ingestion heartbeat records.
    } else if (key.isGlobalRtDiv()) {
      return; // Skip validation for Global RT DIV messages.
    }

    Lazy<Boolean> tolerateMissingMsgs = Lazy.of(() -> {
      PubSubTopic pubSubTopic = consumerRecord.getTopic();
      /** N.B.: In the switch/case, we only short-circuit if true, as there is one more condition to check below. */
      switch (pubSubTopic.getPubSubTopicType()) {
        case REALTIME_TOPIC:
          if (tolerateMissingMessagesForRealTimeTopic) {
            return true;
          }
          break;
        case VERSION_TOPIC:
          TopicManager topicManager = this.topicManagerRepository.getLocalTopicManager();
          if (topicManager.isTopicCompactionEnabled(pubSubTopic)) {
            long elapsedTime = LatencyUtils.getElapsedTimeFromMsToMs(consumerRecord.getPubSubMessageTime());

            /** If we are consuming a portion of topic beyond compaction threshold, then all bets are off */
            if (elapsedTime >= topicManager.getTopicMinLogCompactionLagMs(pubSubTopic)) {
              return true;
            }
          }
          break;
        default: // no-op
      }

      if (this.isDataRecovery && isHybridMode() && partitionConsumptionState.getTopicSwitch() == null) {
        /**
         * Tolerate missing message if store version is data recovery + hybrid and TS not received yet (due to source
         * topic data may have been log compacted)
         */
        return true;
      }

      return false;
    });

    try {
      validator.validateMessage(type, consumerRecord, endOfPushReceived, tolerateMissingMsgs);
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
        String regionName = RegionNameUtil.getRegionName(consumerRecord, serverConfig.getKafkaClusterIdToAliasMap());
        LOGGER.warn(
            "Data integrity validation problem with incoming record from topic-partition: {}{} and offset: {}, "
                + "but consumption will continue since EOP is already received for replica: {}. Msg: {}",
            consumerRecord.getTopicPartition(),
            regionName == null || regionName.isEmpty() ? "" : "/" + regionName,
            consumerRecord.getPosition(),
            partitionConsumptionState.getReplicaId(),
            warningException.getMessage());
      }

      if (!(warningException instanceof ImproperlyStartedSegmentException)) {
        /**
         * Run a dummy validation to update DIV metadata.
         */
        validator.validateMessage(type, consumerRecord, true, Lazy.TRUE);
      }
    }
  }

  /**
   * We should only allow {@link StoreIngestionTask} to access {@link #drainerDiv}; other components
   * like leaders in LeaderFollowerStoreIngestionTask should never access the DIV validator in drainer, because messages
   * consumption in leader is ahead of drainer, leaders and drainers are processing messages at different paces.
   */
  protected void cloneDrainerDivProducerStates(int partition, DataIntegrityValidator validator) {
    this.drainerDiv.cloneVtProducerStates(partition, validator);
  }

  /**
   * Write to the storage engine with the optimization that we leverage the padding in front of the {@param putValue}
   * in order to insert the {@param schemaId} there. This avoids a byte array copy, which can be beneficial in terms
   * of GC.
   */
  private void prependHeaderAndWriteToStorageEngine(int partition, byte[] keyBytes, Put put) {
    ByteBuffer putValue = put.putValue;

    if ((putValue.remaining() == 0) && (put.replicationMetadataPayload.remaining() > 0)) {
      // For RMD chunk, it is already prepended with the schema ID, so we will just put to storage engine.
      writeToStorageEngine(partition, keyBytes, put);
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
        writeToStorageEngine(partition, keyBytes, put);
      } finally {
        /* We still want to recover the original position to make this function idempotent. */
        putValue.putInt(backupBytes);
      }
    }
  }

  private void writeToStorageEngine(int partition, byte[] keyBytes, Put put) {
    putInStorageEngine(partition, keyBytes, put);
    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get().getStorageEngine(kafkaVersionTopic).put(partition, keyBytes, put.putValue);
      }
    }
  }

  private void deleteFromStorageEngine(int partition, byte[] keyBytes, Delete delete) {
    removeFromStorageEngine(partition, keyBytes, delete);
    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get().getStorageEngine(kafkaVersionTopic).delete(partition, keyBytes);
      }
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
        "Attempted to interact with the storage engine for partition: {} while the "
            + "partitionConsumptionStateMap does not contain this partition. "
            + "Will ignore the operation as it probably indicates the partition was unsubscribed.",
        getReplicaId(versionTopic, partition));
  }

  public boolean consumerHasAnySubscription() {
    return aggKafkaConsumerService.hasAnyConsumerAssignedForVersionTopic(versionTopic);
  }

  public boolean consumerHasSubscription(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    int partitionId = partitionConsumptionState.getPartition();
    return aggKafkaConsumerService
        .hasConsumerAssignedFor(versionTopic, new PubSubTopicPartitionImpl(topic, partitionId));
  }

  /**
   * This method unsubscribes topic-partition from the input.
   * If it is real-time topic and separate RT topic is enabled, it will also unsubscribe from separate real-time topic.
   */
  protected void unsubscribeFromTopic(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    consumerUnSubscribeForStateTransition(topic, partitionConsumptionState);
    if (isSeparatedRealtimeTopicEnabled() && topic.isRealTime()) {
      consumerUnSubscribeForStateTransition(separateRealTimeTopic, partitionConsumptionState);
    }
  }

  /**
   * It is important during a state transition to wait in {@link SharedKafkaConsumer#waitAfterUnsubscribe(long, Set, long)}
   * until all inflight messages have been processed by the consumer, otherwise there could be a mismatch in the PCS's
   * leader-follower state vs the intended state when the message was polled. Thus, we use an increased timeout of up to
   * 30 minutes according to the maximum value of the metric consumer_records_producing_to_write_buffer_latency.
   */
  void consumerUnSubscribeForStateTransition(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    long startTime = System.currentTimeMillis();
    int partitionId = partitionConsumptionState.getPartition();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, partitionId);
    aggKafkaConsumerService
        .unsubscribeConsumerFor(versionTopic, topicPartition, serverConfig.getMaxWaitAfterUnsubscribeMs());
    LOGGER.info(
        "Consumer unsubscribed to topic-partition: {} for replica: {}. Took {} ms",
        topicPartition,
        partitionConsumptionState.getReplicaId(),
        LatencyUtils.getElapsedTimeFromMsToMs(startTime));
  }

  public void consumerBatchUnsubscribe(Set<PubSubTopicPartition> topicPartitionSet) {
    long startTime = System.currentTimeMillis();
    aggKafkaConsumerService.batchUnsubscribeConsumerFor(versionTopic, topicPartitionSet);
    LOGGER.info(
        "Consumer unsubscribed {} partitions. Took {} ms",
        topicPartitionSet.size(),
        LatencyUtils.getElapsedTimeFromMsToMs(startTime));
  }

  public abstract void consumerUnSubscribeAllTopics(PartitionConsumptionState partitionConsumptionState);

  /**
   * This method will try to resolve actual topic-partition from input Kafka URL and subscribe to the resolved
   * topic-partition.
   */
  public void consumerSubscribe(
      PubSubTopic pubSubTopic,
      PartitionConsumptionState partitionConsumptionState,
      long startOffset,
      String kafkaURL) {
    PubSubTopicPartition resolvedTopicPartition =
        resolveTopicPartitionWithKafkaURL(pubSubTopic, partitionConsumptionState, kafkaURL);
    consumerSubscribe(resolvedTopicPartition, startOffset, kafkaURL);
  }

  void consumerSubscribe(PubSubTopicPartition pubSubTopicPartition, long startOffset, String kafkaURL) {
    String resolvedKafkaURL = kafkaClusterUrlResolver != null ? kafkaClusterUrlResolver.apply(kafkaURL) : kafkaURL;
    if (!Objects.equals(resolvedKafkaURL, kafkaURL) && !isSeparatedRealtimeTopicEnabled()
        && pubSubTopicPartition.getPubSubTopic().isRealTime()) {
      return;
    }
    final boolean consumeRemotely = !Objects.equals(resolvedKafkaURL, localKafkaServer);
    // TODO: Move remote KafkaConsumerService creating operations into the aggKafkaConsumerService.
    aggKafkaConsumerService
        .createKafkaConsumerService(createKafkaConsumerProperties(kafkaProps, resolvedKafkaURL, consumeRemotely));
    PartitionReplicaIngestionContext partitionReplicaIngestionContext =
        new PartitionReplicaIngestionContext(versionTopic, pubSubTopicPartition, versionRole, workloadType);
    // localKafkaServer doesn't have suffix but kafkaURL may have suffix,
    // and we don't want to pass the resolvedKafkaURL as it will be passed to data receiver for parsing cluster id
    aggKafkaConsumerService.subscribeConsumerFor(kafkaURL, this, partitionReplicaIngestionContext, startOffset);
  }

  public void consumerResetOffset(PubSubTopic topic, PartitionConsumptionState partitionConsumptionState) {
    int partitionId = partitionConsumptionState.getPartition();
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
      DefaultPubSubMessage consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      long currentTimeMs) {
    int keyLen = 0;
    int valueLen = 0;
    KafkaKey kafkaKey = consumerRecord.getKey();
    KafkaMessageEnvelope kafkaValue = consumerRecord.getValue();
    int producedPartition = partitionConsumptionState.getPartition();
    byte[] keyBytes;

    MessageType messageType = (leaderProducedRecordContext == null
        ? MessageType.valueOf(kafkaValue)
        : leaderProducedRecordContext.getMessageType());

    boolean metricsEnabled = emitMetrics.get();
    boolean traceEnabled = LOGGER.isTraceEnabled();
    long startTimeNs = (metricsEnabled || traceEnabled) ? System.nanoTime() : 0;
    boolean emitRecordLevelMetrics = metricsEnabled && recordLevelMetricEnabled.get();

    switch (messageType) {
      case PUT:
      case GLOBAL_RT_DIV:
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
        if (emitRecordLevelMetrics && put.getSchemaId() == CHUNK_MANIFEST_SCHEMA_ID && messageType == MessageType.PUT) {
          // This must be done before the recordTransformer modifies the putValue, otherwise the size will be incorrect.
          recordAssembledRecordSize(keyLen, put.getPutValue(), put.getReplicationMetadataPayload(), currentTimeMs);
        }

        int writerSchemaId = put.getSchemaId();

        if (recordTransformer != null && messageType == MessageType.PUT) {
          long recordTransformerStartTime = System.nanoTime();
          ByteBufferValueRecord<ByteBuffer> assembledRecord = chunkAssembler.bufferAndAssembleRecord(
              consumerRecord.getTopicPartition(),
              put.getSchemaId(),
              keyBytes,
              put.getPutValue(),
              consumerRecord.getPosition().getNumericOffset(),
              compressor.get());

          // Current record is a chunk. We only write to the storage engine for fully assembled records
          if (assembledRecord == null) {
            return 0;
          }

          ByteBuffer assembledObject = assembledRecord.value();
          writerSchemaId = assembledRecord.writerSchemaId();
          final int readerSchemaId = writerSchemaId;
          Lazy<Object> lazyKey = Lazy.of(() -> this.recordTransformerKeyDeserializer.deserialize(keyBytes));
          Lazy<Object> lazyValue = Lazy.of(() -> {
            try {
              ByteBuffer decompressedAssembledObject = compressor.get().decompress(assembledObject);
              Schema valueSchema = this.schemaIdToSchemaMap.computeIfAbsent(
                  readerSchemaId,
                  i -> schemaRepository.getValueSchema(storeName, readerSchemaId).getSchema());
              RecordDeserializer recordDeserializer;

              if (recordTransformerConfig.useSpecificRecordValueDeserializer()) {
                recordDeserializer = FastSerializerDeserializerFactory
                    .getFastAvroSpecificDeserializer(valueSchema, recordTransformerConfig.getOutputValueClass());
              } else {
                if (this.recordTransformer.useUniformInputValueSchema()) {
                  recordDeserializer = FastSerializerDeserializerFactory
                      .getFastAvroGenericDeserializer(valueSchema, this.recordTransformerInputValueSchema);
                } else {
                  recordDeserializer =
                      FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema);
                }
              }

              return recordDeserializer.deserialize(decompressedAssembledObject);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

          DaVinciRecordTransformerResult transformerResult;
          try {
            transformerResult = recordTransformer.transformAndProcessPut(lazyKey, lazyValue, producedPartition);
          } catch (Exception e) {
            daVinciRecordTransformerStats.recordPutError(storeName, versionNumber, currentTimeMs);
            String errorMessage =
                "DaVinciRecordTransformer experienced an error when processing value: " + assembledObject;

            throw new VeniceMessageException(errorMessage, e);
          }

          // Record was skipped, so don't write to storage engine
          if (transformerResult == null
              || transformerResult.getResult() == DaVinciRecordTransformerResult.Result.SKIP) {
            return 0;
          }

          ByteBuffer transformedBytes;
          if (transformerResult.getResult() == DaVinciRecordTransformerResult.Result.UNCHANGED) {
            // Use original value if the record wasn't transformed
            transformedBytes = recordTransformer.prependSchemaIdToHeader(assembledObject, writerSchemaId);
          } else {
            // Serialize and compress the new record if it was transformed
            transformedBytes = recordTransformer
                .prependSchemaIdToHeader(transformerResult.getValue(), writerSchemaId, compressor.get());
          }

          put.putValue = transformedBytes;
          daVinciRecordTransformerStats.recordPutLatency(
              storeName,
              versionNumber,
              LatencyUtils.getElapsedTimeFromNSToMS(recordTransformerStartTime),
              currentTimeMs);
          writeToStorageEngine(producedPartition, keyBytes, put);
        } else {
          prependHeaderAndWriteToStorageEngine(
              // Leaders might consume from a RT topic and immediately write into StorageEngine,
              // so we need to re-calculate partition.
              // Followers are not affected since they are always consuming from VTs.
              producedPartition,
              keyBytes,
              put);
        }
        // grab the positive schema id (actual value schema id) to be used in schema warm-up value schema id.
        // for hybrid use case in read compute store in future we need revisit this as we can have multiple schemas.
        if (writerSchemaId > 0) {
          valueSchemaId = writerSchemaId;
        }
        if (emitRecordLevelMetrics) {
          hostLevelIngestionStats
              .recordStorageEnginePutLatency(LatencyUtils.getElapsedTimeFromNSToMS(startTimeNs), currentTimeMs);
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

        if (recordTransformer != null) {
          Lazy<Object> lazyKey = Lazy.of(() -> this.recordTransformerKeyDeserializer.deserialize(keyBytes));

          long startTime = System.nanoTime();
          try {
            recordTransformer.processDelete(lazyKey, producedPartition);
          } catch (Exception e) {
            daVinciRecordTransformerStats.recordDeleteError(storeName, versionNumber, currentTimeMs);
            String errorMessage = "DaVinciRecordTransformer experienced an error when deleting key: " + lazyKey.get();

            throw new VeniceMessageException(errorMessage, e);
          }
          daVinciRecordTransformerStats.recordDeleteLatency(
              storeName,
              versionNumber,
              LatencyUtils.getElapsedTimeFromNSToMS(startTime),
              System.currentTimeMillis());

          // This is called here after processDelete because if the user stores their data somewhere other than
          // Da Vinci, this function needs to execute to allow them to delete the data from the appropriate store
          if (!recordTransformer.getStoreRecordsInDaVinci()) {
            // If we're not storing in Da Vinci, then no need to try to delete from the storageEngine
            break;
          }
        }

        keyLen = keyBytes.length;
        deleteFromStorageEngine(producedPartition, keyBytes, delete);
        if (metricsEnabled && recordLevelMetricEnabled.get()) {
          hostLevelIngestionStats
              .recordStorageEngineDeleteLatency(LatencyUtils.getElapsedTimeFromNSToMS(startTimeNs), currentTimeMs);
        }
        break;

      case UPDATE:
        throw new VeniceMessageException(
            ingestionTaskName + ": Not expecting UPDATE message from: " + consumerRecord.getTopicPartition()
                + ", Offset: " + consumerRecord.getPosition());
      default:
        throw new VeniceMessageException(
            ingestionTaskName + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    if (traceEnabled) {
      LOGGER.trace(
          "Completed {} to replica: {} in {} ns at {}",
          messageType,
          partitionConsumptionState.getReplicaId(),
          System.nanoTime() - startTimeNs,
          System.currentTimeMillis());
    }
    /*
     * Potentially clean the mapping from transient record map. consumedOffset may be -1 when individual chunks are getting
     * produced to drainer queue from kafka callback thread {@link LeaderFollowerStoreIngestionTask#LeaderProducerMessageCallback}
     */
    // This flag is introduced to help write integration test to exercise code path during UPDATE message processing in
    // {@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}. This must be always set to true
    // except
    // as needed in integration test.
    if (purgeTransientRecordBuffer && isTransientRecordBufferUsed(partitionConsumptionState)
        && leaderProducedRecordContext != null
        && leaderProducedRecordContext.getConsumedPosition() != NO_UPSTREAM_POSITION) {
      partitionConsumptionState.mayRemoveTransientRecord(
          leaderProducedRecordContext.getConsumedKafkaClusterId(),
          leaderProducedRecordContext.getConsumedPosition(),
          kafkaKey.getKey());
    }

    if (emitRecordLevelMetrics) {
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
  private void waitReadyToProcessRecord(DefaultPubSubMessage record) throws InterruptedException {
    KafkaMessageEnvelope kafkaValue = record.getValue();
    if (record.getKey().isControlMessage() || record.getKey().isGlobalRtDiv() || kafkaValue == null) {
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
              "Failed to deserialize PUT for: " + record.getTopicPartition() + ", offset: " + record.getPosition()
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
   * The function will bypass the check if schema id is -1 (VPJ job is still using it before we finishes the integration with schema registry).
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

    if (schemaId == CHUNK_SCHEMA_ID || schemaId == CHUNK_MANIFEST_SCHEMA_ID) {
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
          PubSubTopic metaStoreRT = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(metaStoreName));
          if (getTopicManager(localKafkaServer).containsTopicWithRetries(metaStoreRT, 5)) {
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
  private void deserializeValue(int schemaId, ByteBuffer value, DefaultPubSubMessage record) throws IOException {
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
      LOGGER.info(
          "Value deserialization succeeded with schema id {} for incoming record from: {} for replica: {}",
          schemaId,
          record.getTopicPartition(),
          getReplicaId(versionTopic, record.getPartition()));
      deserializedSchemaIds.set(schemaId, new Object());
    }
  }

  private void maybeCloseInactiveIngestionTask() {
    if (serverConfig.getIdleIngestionTaskCleanupIntervalInSeconds() > 0 && !isIsolatedIngestion) {
      // Ingestion task will not close by itself
      return;
    }
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
      resetIdleCounter();
    } else {
      if (getIdleCounter() > getMaxIdleCounter()) {
        close();
      }
    }
    return isRunning();
  }

  void resetIdleCounter() {
    idleCounter.set(0);
  }

  int getIdleCounter() {
    return idleCounter.get();
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

    if (recordTransformer != null) {
      long startTime = System.nanoTime();
      Store store = storeRepository.getStoreOrThrow(storeName);
      recordTransformer.onEndVersionIngestion(store.getCurrentVersion());
      LOGGER.info(
          "DaVinciRecordTransformer onEndVersionIngestion took {} ms for store version: {}",
          LatencyUtils.getElapsedTimeFromNSToMS(startTime),
          storeVersionName);
      Utils.closeQuietlyWithErrorLogged(this.recordTransformer);
    }
  }

  /**
   * This method is a blocking call to wait for {@link StoreIngestionTask} for fully shutdown in the given time.
   * @param waitTime Maximum wait time for the shutdown operation.
   * @return whether able to gracefully shut down within the waitTime
   */
  public boolean shutdownAndWait(int waitTime) {
    long startTimeInMs = System.currentTimeMillis();
    boolean timelyShutDown = true;
    close();
    try {
      if (getGracefulShutdownLatch().isPresent() && !getGracefulShutdownLatch().get().await(waitTime, SECONDS)) {
        LOGGER.warn(
            "Unable to shutdown ingestion task of topic: {} gracefully in {}ms",
            kafkaVersionTopic,
            SECONDS.toMillis(waitTime));
        timelyShutDown = false;
      } else {
        LOGGER.info(
            "Ingestion task of topic: {} is shutdown in {}ms",
            kafkaVersionTopic,
            LatencyUtils.getElapsedTimeFromMsToMs(startTimeInMs));
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while waiting for ingestion task of topic: {} shutdown.", kafkaVersionTopic);
    }
    return timelyShutDown;
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
    boolean partitionConsumptionStateExist = partitionConsumptionStateMap.containsKey(userPartition);
    boolean pendingPartitionIngestionAction = hasPendingPartitionIngestionAction(userPartition);
    return pendingPartitionIngestionAction || partitionConsumptionStateExist;
  }

  /**
   * Override the {@link ConfigKeys#KAFKA_BOOTSTRAP_SERVERS} config with a remote Kafka bootstrap url.
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
          if (!partitionConsumptionState.isCompletionReported()) {
            Store store = storeRepository.getStoreOrThrow(storeName);
            reportCompleted(partitionConsumptionState);
            if (store.isHybrid()) {
              if (this.storageEngine.isClosed()) {
                LOGGER.warn("Storage engine {} was closed", kafkaVersionTopic);
              } else {
                /**
                 * May adjust the underlying storage partition to optimize read perf.
                 */
                this.storageEngine.adjustStoragePartition(
                    partition,
                    StoragePartitionAdjustmentTrigger.PREPARE_FOR_READ,
                    getStoragePartitionConfig(partitionConsumptionState));
              }
            }

            warmupSchemaCache(store);
          }
          if (suppressLiveUpdates) {
            // If live updates suppression is enabled, stop consuming any new messages once the partition is ready to
            // serve.
            String msg = ingestionTaskName + " Live update suppression is enabled. Stop consumption for replica: "
                + partitionConsumptionState.getReplicaId();
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
              LOGGER.info(msg);
            }
            unSubscribePartition(new PubSubTopicPartitionImpl(versionTopic, partition));
          }
          partitionConsumptionState.recordReadyToServeInOffsetRecord();
        } else {
          ingestionNotificationDispatcher.reportProgress(partitionConsumptionState);
        }
      }
    };
  }

  void reportCompleted(PartitionConsumptionState partitionConsumptionState) {
    reportCompleted(partitionConsumptionState, false);
  }

  protected abstract void resubscribe(PartitionConsumptionState partitionConsumptionState) throws InterruptedException;

  void reportCompleted(PartitionConsumptionState partitionConsumptionState, boolean forceCompletion) {
    ingestionNotificationDispatcher.reportCompleted(partitionConsumptionState, forceCompletion);
    LOGGER.info("Replica: {} is ready to serve", partitionConsumptionState.getReplicaId());
  }

  // test only
  void setValueSchemaId(int id) {
    this.valueSchemaId = id;
  }

  /**
   * Try to warm-up the schema repo cache before reporting completion as new value schema could cause latency degradation
   * while trying to compile it in the read-path.
   */
  void warmupSchemaCache(Store store) {
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
    Schema writerSchema = schemaRepository.getValueSchema(storeName, valueSchemaId).getSchema();
    List<SchemaEntry> schemaEntries = new ArrayList<>(schemaRepository.getValueSchemas(storeName));
    schemaEntries.sort(comparingInt(SchemaEntry::getId).reversed());
    // Try to warm the schema cache by generating last `getNumSchemaFastClassWarmup` schemas.
    Set<Integer> schemaGenerated = new HashSet<>();
    for (SchemaEntry schemaEntry: schemaEntries) {
      schemaGenerated.add(schemaEntry.getId());
      if (schemaGenerated.size() > numSchemaToGenerate) {
        break;
      }
      cacheFastAvroGenericDeserializer(writerSchema, schemaEntry.getSchema(), warmUpTimeLimit);
    }
    LOGGER.info("Warmed up cache of value schema with ids {} of store {}", schemaGenerated, storeName);
  }

  void cacheFastAvroGenericDeserializer(Schema writerSchema, Schema readerSchema, long warmUpTimeLimit) {
    FastSerializerDeserializerFactory.cacheFastAvroGenericDeserializer(writerSchema, readerSchema, warmUpTimeLimit);
  }

  public void reportError(String message, int userPartition, Exception e) {
    List<PartitionConsumptionState> pcsList = new ArrayList<>();
    if (partitionConsumptionStateMap.containsKey(userPartition)) {
      pcsList.add(partitionConsumptionStateMap.get(userPartition));
    }
    reportError(pcsList, userPartition, message, e);
    ingestionNotificationDispatcher.reportError(pcsList, message, e);
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
    if (kafkaClusterUrlResolver != null) {
      sourceKafkaServer = kafkaClusterUrlResolver.apply(sourceKafkaServer);
    }
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
      PubSubMessageProcessedResultWrapper consumerRecordWrapper,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs);

  /**
   * This enum represents all potential results after calling {@link #delegateConsumerRecord(PubSubMessageProcessedResultWrapper, int, String, int, long, long)}.
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
     * The consumer record is skipped. e.g. remote VT's TS message during data recovery.
     */
    SKIPPED_MESSAGE
  }

  /**
   * The method measures the time between receiving the message from the local VT and when the message is committed in
   * the local db and ready to serve.
   * For a leader, it's the time when the callback to the version topic write returns.
   */
  private void recordNearlineLocalBrokerToReadyToServerLatency(
      String storeName,
      int versionNumber,
      PartitionConsumptionState partitionConsumptionState,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      LeaderProducedRecordContext leaderProducedRecordContext) {
    /**
     * Record nearline latency only when it's a hybrid store, the lag has been caught up and ignore
     * messages that are getting caughtup. Sometimes the producerTimestamp can be -1 if the
     * leaderProducedRecordContext had an error after callback. Don't record latency for invalid timestamps.
     */
    if (!isUserSystemStore() && isHybridMode() && partitionConsumptionState.hasLagCaughtUp()) {
      long producerTimestamp = (leaderProducedRecordContext == null)
          ? kafkaMessageEnvelope.producerMetadata.messageTimestamp
          : leaderProducedRecordContext.getProducedTimestampMs();
      if (producerTimestamp > 0) {
        if (partitionConsumptionState.isNearlineMetricsRecordingValid(producerTimestamp)) {
          long afterProcessingRecordTimestampMs = System.currentTimeMillis();
          versionedIngestionStats.recordNearlineLocalBrokerToReadyToServeLatency(
              storeName,
              versionNumber,
              afterProcessingRecordTimestampMs - producerTimestamp,
              afterProcessingRecordTimestampMs);
        }
      } else if (!REDUNDANT_LOGGING_FILTER.isRedundantException(storeName, "IllegalTimestamp")) {
        LOGGER.warn(
            "Illegal timestamp for storeName: {}, versionNumber: {}, replica: {}, "
                + "leaderProducedRecordContext: {}, producerTimestamp: {}",
            storeName,
            versionNumber,
            partitionConsumptionState.getReplicaId(),
            leaderProducedRecordContext == null ? "NA" : leaderProducedRecordContext,
            producerTimestamp);
      }
    }
  }

  protected void recordProcessedRecordStats(
      PartitionConsumptionState partitionConsumptionState,
      int processedRecordSize) {
  }

  /**
   * This is not a per record state. Rather it's used to indicate if the transient record buffer is being used at all
   * for this ingestion task/partition or not. The criterias are the following:
   *
   * 1. For L/F mode only WC ingestion task needs this buffer.
   * 2. The transient record buffer is only used post-EOP.
   */
  public boolean isTransientRecordBufferUsed(PartitionConsumptionState partitionConsumptionState) {
    return this.isWriteComputationEnabled && partitionConsumptionState.isEndOfPushReceived();
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
      if (ExceptionUtils.recursiveClassEquals(partitionIngestionException, MemoryLimitExhaustedException.class)) {
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
  protected boolean shouldUpdateUpstreamOffset(DefaultPubSubMessage consumerRecord) {
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

  void maybeReportBatchEndOfIncPushStatus(PartitionConsumptionState partitionConsumptionState) {
    if (partitionConsumptionState.getPendingReportIncPushVersionList().isEmpty()) {
      return;
    }
    // When PCS is completed but the pending report list is not empty, we should perform one last report to make sure
    // all EOIPs are reported.
    if (partitionConsumptionState.isComplete()) {
      getIngestionNotificationDispatcher().reportBatchEndOfIncrementalPushStatus(partitionConsumptionState);
      partitionConsumptionState.clearPendingReportIncPushVersionList();
    }
  }

  private void mayResumeRecordLevelMetricsForCurrentVersion() {
    if (recordLevelMetricEnabled.get()) {
      return;
    }
    if (partitionConsumptionStateMap.isEmpty()) {
      return;
    }
    for (PartitionConsumptionState partitionConsumptionState: partitionConsumptionStateMap.values()) {
      if (!partitionConsumptionState.isComplete()) {
        return;
      }
    }
    LOGGER
        .info("Resuming record-level metric emission for the current version as all the partitions are ready-to-serve");
    recordLevelMetricEnabled.set(true);
  }

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
    return topicManagerRepository.getLocalTopicManager().containsTopic(this.versionTopic);
  }

  public boolean isCurrentVersion() {
    return isCurrentVersion.getAsBoolean();
  }

  public boolean hasAllPartitionReportedCompleted() {
    for (Map.Entry<Integer, PartitionConsumptionState> entry: partitionConsumptionStateMap.entrySet()) {
      if (!entry.getValue().isCompletionReported()) {
        return false;
      }
    }

    return true;
  }

  public boolean isSeparatedRealtimeTopicEnabled() {
    return separateRealTimeTopic != null;
  }

  /**
   * For RT input topic with separate-RT kafka URL, this method will return topic-partition with separated-RT topic.
   * For other case, it will return topic-partition with input topic.
   */
  PubSubTopicPartition resolveTopicPartitionWithKafkaURL(
      PubSubTopic topic,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaURL) {
    PubSubTopic resolvedTopic = resolveTopicWithKafkaURL(topic, kafkaURL);
    PubSubTopicPartition pubSubTopicPartition = partitionConsumptionState.getSourceTopicPartition(resolvedTopic);
    LOGGER.info("Resolved topic-partition: {} from kafkaURL: {}", pubSubTopicPartition, kafkaURL);
    return pubSubTopicPartition;
  }

  /**
   * This method will return resolve topic from input Kafka URL. If it is a separated topic Kafka URL and input topic
   * is RT topic, it will return separate RT topic, otherwise it will return input topic.
   */
  PubSubTopic resolveTopicWithKafkaURL(PubSubTopic topic, String kafkaURL) {
    if (topic.isRealTime() && getKafkaClusterUrlResolver() != null
        && !kafkaURL.equals(getKafkaClusterUrlResolver().apply(kafkaURL))) {
      return separateRealTimeTopic;
    }
    return topic;
  }

  PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  Function<String, String> getKafkaClusterUrlResolver() {
    return kafkaClusterUrlResolver;
  }

  Lazy<CountDownLatch> getGracefulShutdownLatch() {
    return gracefulShutdownLatch;
  }

  // For unit test purpose.
  void setVersionRole(PartitionReplicaIngestionContext.VersionRole versionRole) {
    this.versionRole = versionRole;
  }

  // For testing only
  Map<Integer, PartitionConsumptionState> getPartitionConsumptionStateMap() {
    return partitionConsumptionStateMap;
  }

  boolean isDaVinciClient() {
    return isDaVinciClient;
  }

  ReadyToServeCheck getReadyToServeChecker() {
    return defaultReadyToServeChecker;
  }

  VeniceConcurrentHashMap<String, Long> getConsumedBytesSinceLastSync() {
    return consumedBytesSinceLastSync; // mainly for unit test mocks
  }

  boolean isGlobalRtDivEnabled() {
    return isGlobalRtDivEnabled; // mainly for unit test mocks
  }

  /** When Global RT DIV is enabled the ConsumptionTask's DIV is exclusively used to validate data integrity. */
  DataIntegrityValidator getDataIntegrityValidator() {
    return (isGlobalRtDivEnabled()) ? consumerDiv : drainerDiv;
  }

  /**
   * When Global RT DIV is enabled, the latest consumed VT offset (LCVO) should be used during subscription.
   * Otherwise, the drainer's latest processed VT offset is traditionally used.
   */
  long getLocalVtSubscribeOffset(PartitionConsumptionState pcs) {
    return (isGlobalRtDivEnabled()) ? pcs.getLatestConsumedVtOffset() : pcs.getLatestProcessedLocalVersionTopicOffset();
  }

}
