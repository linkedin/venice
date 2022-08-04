package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheBackend;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceChecksumException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceInconsistentStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.VeniceTimeoutException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Timer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.concurrent.TimeUnit.*;


/**
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public abstract class StoreIngestionTask implements Runnable, Closeable {
  // TODO: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = LogManager.getLogger();

  private static final String CONSUMER_TASK_ID_FORMAT = StoreIngestionTask.class.getSimpleName() + " for [ Topic: %s ]";
  public static long SCHEMA_POLLING_DELAY_MS = SECONDS.toMillis(5);
  private static final long SCHEMA_POLLING_TIMEOUT_MS = MINUTES.toMillis(5);

  /** After processing the following number of messages, Venice SN will report progress metrics. */
  public static int OFFSET_REPORTING_INTERVAL = 1000;
  private static final int MAX_CONSUMER_ACTION_ATTEMPTS = 5;
  private static final int MAX_IDLE_COUNTER  = 100;
  private static final int CONSUMER_ACTION_QUEUE_INIT_CAPACITY = 11;
  protected static final long KILL_WAIT_TIME_MS = 5000L;
  private static final int MAX_KILL_CHECKING_ATTEMPTS = 10;
  private static final int SLOPPY_OFFSET_CATCHUP_THRESHOLD = 100;
  private static final int EXCEPTION_BLOCKING_QUEUE_SIZE_IN_BYTE = 10000;
  private static final int EXCEPTION_BLOCKING_QUEUE_NOTIFY_IN_BYTE = 1000;
  private static final String EXCEPTION_GROUP_DRAINER = "drainer";
  private static final String EXCEPTION_GROUP_PRODUCER = "producer";
  private static final String EXCEPTION_GROUP_CONSUMER = "consumer";

  protected static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER = RedundantExceptionFilter.getRedundantExceptionFilter();

  /** storage destination for consumption */
  protected final StorageEngineRepository storageEngineRepository;
  protected final String kafkaVersionTopic;
  protected final String realTimeTopic;
  protected final String storeName;
  protected final int versionNumber;
  protected final ReadOnlySchemaRepository schemaRepository;
  protected final ReadOnlyStoreRepository storeRepository;
  protected final String consumerTaskId;
  protected final Properties kafkaProps;
  protected final KafkaClientFactory kafkaClientFactory;
  protected final AtomicBoolean isRunning;
  protected final AtomicBoolean emitMetrics; // TODO: remove this once we migrate to versioned stats
  protected final AtomicInteger consumerActionSequenceNumber = new AtomicInteger(0);
  protected final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;
  protected final StorageMetadataService storageMetadataService;
  protected final TopicManagerRepository topicManagerRepository;
  protected final TopicManagerRepository topicManagerRepositoryJavaBased;
  protected final CachedKafkaMetadataGetter cachedKafkaMetadataGetter;
  protected final EventThrottler bandwidthThrottler;
  protected final EventThrottler recordsThrottler;
  protected final EventThrottler unorderedBandwidthThrottler;
  protected final EventThrottler unorderedRecordsThrottler;
  /** Per-partition consumption state map */
  protected final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  protected final AbstractStoreBufferService storeBufferService;
  /** Persists partitions that encountered exceptions in other threads. i.e. consumer, producer and drainer */
  private final IntSet errorPartitions = IntSets.synchronize(new IntOpenHashSet());

  /** Persists the exceptions thrown by {@link StoreBufferService}. */
  private final BlockingQueue<PartitionExceptionInfo> drainerExceptions =
      new MemoryBoundBlockingQueue<>(EXCEPTION_BLOCKING_QUEUE_SIZE_IN_BYTE, EXCEPTION_BLOCKING_QUEUE_NOTIFY_IN_BYTE);
  /** Persists the exception thrown by kafka producer callback for L/F mode */
  private final BlockingQueue<PartitionExceptionInfo> producerExceptions =
      new MemoryBoundBlockingQueue<>(EXCEPTION_BLOCKING_QUEUE_SIZE_IN_BYTE, EXCEPTION_BLOCKING_QUEUE_NOTIFY_IN_BYTE);
  /**
   * Current it captures some of the exceptions that are thrown after consumption but before producing to local version topic.
   */
  private final BlockingQueue<PartitionExceptionInfo> consumerExceptions =
      new MemoryBoundBlockingQueue<>(EXCEPTION_BLOCKING_QUEUE_SIZE_IN_BYTE, EXCEPTION_BLOCKING_QUEUE_NOTIFY_IN_BYTE);
  /** Persists the exception thrown by {@link KafkaConsumerService}. */
  private Exception lastConsumerException = null;
  /** Persists the last exception thrown by any asynchronous component that should terminate the entire ingestion task */
  private final AtomicReference<Exception> lastStoreIngestionException = new AtomicReference<>();
  /**
   * Keeps track of producer states inside version topic that drainer threads have processed so far. Producers states in this validator will be
   * flushed to the metadata partition of the storage engine regularly in {@link #syncOffset(String, PartitionConsumptionState)}
   */
  private final KafkaDataIntegrityValidator kafkaDataIntegrityValidator;
  protected final AggStoreIngestionStats storeIngestionStats;
  protected final AggVersionedDIVStats versionedDIVStats;
  protected final AggVersionedStorageIngestionStats versionedStorageIngestionStats;
  protected final BooleanSupplier isCurrentVersion;
  protected final Optional<HybridStoreConfig> hybridStoreConfig;
  protected final ProducerTracker.DIVErrorMetricCallback divErrorMetricCallback;

  protected final long readCycleDelayMs;
  protected final long emptyPollSleepMs;

  protected final DiskUsage diskUsage;

  protected final Optional<RocksDBMemoryStats> rocksDBMemoryStats;

  /** Message bytes consuming interval before persisting offset in offset db for transactional mode database. */
  protected final long databaseSyncBytesIntervalForTransactionalMode;
  /** Message bytes consuming interval before persisting offset in offset db for deferred-write database. */
  protected final long databaseSyncBytesIntervalForDeferredWriteMode;

  /** A quick check point to see if incremental push is supported.
   * It helps fast {@link #isReadyToServe(PartitionConsumptionState)}*/
  protected final boolean isIncrementalPushEnabled;
  protected final IncrementalPushPolicy incrementalPushPolicy;

  protected final boolean readOnlyForBatchOnlyStoreEnabled;
  protected final VeniceServerConfig serverConfig;

  /** Used for reporting error when the {@link #partitionConsumptionStateMap} is empty */
  protected final int errorPartitionId;

  // use this checker to check whether ingestion completion can be reported for a partition
  protected final ReadyToServeCheck defaultReadyToServeChecker;

  protected Map<String, KafkaConsumerWrapper> kafkaUrlToDedicatedConsumerMap;
  protected Map<String, KafkaConsumerWrapper> kafkaUrlToSharedConsumerMap;

  private final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;

  protected final IntSet availableSchemaIds;
  protected final IntSet deserializedSchemaIds;
  protected int idleCounter = 0;

  // This indicates whether it polls nothing from Kafka
  // It's for stats measuring purpose
  protected int recordCount = 0;

  private final StorageUtilizationManager storageUtilizationManager;

  protected final AggKafkaConsumerService aggKafkaConsumerService;
  /**
   * This topic set is used to track all the topics, which have ever been subscribed.
   * It may not reflect the current subscriptions.
   */
  private final Set<String> everSubscribedTopics = new HashSet<>();
  private boolean orderedWritesOnly = true;

  private Optional<RocksDBMemoryEnforcement> rocksDBMemoryEnforcer;

  private final ExecutorService cacheWarmingThreadPool;
  /**
   * This set is used to track the cache warming request for each partition.
   */
  private final IntSet cacheWarmingPartitionIdSet = new IntOpenHashSet();

  /**
   * Please refer to {@link com.linkedin.venice.ConfigKeys#SERVER_DELAY_REPORT_READY_TO_SERVE_MS} to
   * find more details.
   */
  private final long startReportingReadyToServeTimestamp;

  protected int writeComputeFailureCode = 0;

  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  // Do not convert it to a local variable because it is used in test.
  private boolean purgeTransientRecordBuffer = true;

  protected final boolean isWriteComputationEnabled;

  /**
   * Freeze ingestion if ready to serve or local data exists
   */
  private final boolean suppressLiveUpdates;

  /**
   * This flag indicates whether the auto-compaction should be disabled for Samza Reprocessing Job.
   * This feature has the assumption that most of the keys produced by Samza Reprocessing Job are
   * unique, so we won't be too concerned about the storage amplification even though the storage size
   * could be doubled during one-time manual compaction after receiving EOP.
   * Since the compaction won't happen at the same time across all the storage partitions (different storage partition
   * will hit EOP at different time and there are limit number of compaction threads), the storage
   * amplification can be acceptable (need some experiment in prod).
   *
   * If this feature is proved to be useful, we could apply this optimization more widely to the
   * regular hybrid store since we assume Kafka Log Compaction will help keep the most recent
   * entry for each key.
   */
  private final boolean disableAutoCompactionForSamzaReprocessingJob;
  /**
   * This map is used to track the manual compaction progress for each partition.
   * This map won't be cleaned up even the compaction is done since it is being used in {@link #produceToStoreBufferServiceOrKafka}
   * to decide whether any incoming messages should be filtered or errored.
   */
  private final Map<Integer, CompletableFuture<Void>> dbCompactFutureMap = new VeniceConcurrentHashMap<>();
  /**
   * This map is used to track the point where the manual compaction starts.
   * Since the messages including EOP and the following will be skipped when manual compaction is triggered, we will
   * leverage this map to re-subscribe the same topic partition when manual compaction is done.
   */
  private final Map<Integer, ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> eopControlMessageMap = new VeniceConcurrentHashMap<>();
  /**
   * This set is used to track all the partitions whose consumption has been resumed after the completed compaction.
   * So that {@link #checkLongRunningDBCompaction()} won't execute the same consumption resumption more than once.
   */
  private final IntSet consumptionResumedPartitionSet = new IntOpenHashSet();

  private final boolean isActiveActiveReplicationEnabled;

  /**
   * This would be the number of partitions in the StorageEngine and in version topics
   */
  protected final int subPartitionCount;

  protected final int amplificationFactor;

  // Used to construct VenicePartitioner
  protected final VenicePartitioner venicePartitioner;


  //Total number of partition for this store version
  protected final int storeVersionPartitionCount;

  private int subscribedCount = 0;
  private int forceUnSubscribedCount = 0;

  // Push timeout threshold for the store
  protected final long bootstrapTimeoutInMs;

  protected final boolean isIsolatedIngestion;

  protected final ReportStatusAdapter reportStatusAdapter;
  private final Optional<ObjectCacheBackend> cacheBackend;

  protected final AmplificationAdapter amplificationAdapter;

  protected final String localKafkaServer;
  protected final Set<String> localKafkaServerSingletonSet;
  private int valueSchemaId = -1;

  protected final boolean isDaVinciClient;

  private final boolean offsetLagDeltaRelaxEnabled;
  private final boolean ingestionCheckpointDuringGracefulShutdownEnabled;

  protected boolean isDataRecovery;
  protected final MetaStoreWriter metaStoreWriter;

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
      Queue<VeniceNotifier> notifiers) {
    this.readCycleDelayMs = storeConfig.getKafkaReadCycleDelayMs();
    this.emptyPollSleepMs = storeConfig.getKafkaEmptyPollSleepMs();
    this.databaseSyncBytesIntervalForTransactionalMode = storeConfig.getDatabaseSyncBytesIntervalForTransactionalMode();
    this.databaseSyncBytesIntervalForDeferredWriteMode = storeConfig.getDatabaseSyncBytesIntervalForDeferredWriteMode();
    this.kafkaClientFactory = builder.getKafkaClientFactory();
    this.kafkaProps = kafkaConsumerProperties;
    this.storageEngineRepository = builder.getStorageEngineRepository();
    this.storageMetadataService = builder.getStorageMetadataService();
    this.bandwidthThrottler = builder.getBandwidthThrottler();
    this.recordsThrottler = builder.getRecordsThrottler();
    this.unorderedBandwidthThrottler = builder.getUnorderedBandwidthThrottler();
    this.unorderedRecordsThrottler = builder.getUnorderedRecordsThrottler();
    this.storeRepository = builder.getMetadataRepo();
    this.schemaRepository = builder.getSchemaRepo();
    this.kafkaVersionTopic = storeConfig.getStoreVersionName();
    this.storeName = Version.parseStoreFromKafkaTopicName(kafkaVersionTopic);
    this.realTimeTopic = Version.composeRealTimeTopic(storeName);
    this.versionNumber = Version.parseVersionFromKafkaTopicName(kafkaVersionTopic);
    this.availableSchemaIds = new IntOpenHashSet();
    this.deserializedSchemaIds = new IntOpenHashSet();
    this.consumerActionsQueue = new PriorityBlockingQueue<>(CONSUMER_ACTION_QUEUE_INIT_CAPACITY);

    this.kafkaClusterBasedRecordThrottler = builder.getKafkaClusterBasedRecordThrottler();

    // partitionConsumptionStateMap could be accessed by multiple threads: consumption thread and the thread handling kill message
    this.partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    // Could be accessed from multiple threads since there are multiple worker threads.
    this.kafkaDataIntegrityValidator = new KafkaDataIntegrityValidator(this.kafkaVersionTopic);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, kafkaVersionTopic);
    this.topicManagerRepository = builder.getTopicManagerRepository();
    this.topicManagerRepositoryJavaBased = builder.getTopicManagerRepositoryJavaBased();
    this.cachedKafkaMetadataGetter = new CachedKafkaMetadataGetter(storeConfig.getTopicOffsetCheckIntervalMs());

    this.storeIngestionStats = builder.getIngestionStats();
    this.versionedDIVStats = builder.getVersionedDIVStats();
    this.versionedStorageIngestionStats = builder.getVersionedStorageIngestionStats();
    this.isRunning = new AtomicBoolean(true);
    this.emitMetrics = new AtomicBoolean(true);

    this.storeBufferService = builder.getStoreBufferService();
    this.isCurrentVersion = isCurrentVersion;
    this.hybridStoreConfig = Optional.ofNullable(version.isUseVersionLevelHybridConfig() ? version.getHybridStoreConfig() : store.getHybridStoreConfig());
    this.isIncrementalPushEnabled = version.isUseVersionLevelIncrementalPushEnabled() ? version.isIncrementalPushEnabled() : store.isIncrementalPushEnabled();
    this.incrementalPushPolicy = version.getIncrementalPushPolicy();

    this.divErrorMetricCallback = e -> versionedDIVStats.recordException(storeName, versionNumber, e);

    this.diskUsage = builder.getDiskUsage();

    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
    this.rocksDBMemoryStats = Optional.ofNullable(
        storageEngineRepository.hasLocalStorageEngine(kafkaVersionTopic)
            && storageEngine.getType().equals(PersistenceType.ROCKS_DB) ?
            builder.getRocksDBMemoryStats() : null
    );

    this.readOnlyForBatchOnlyStoreEnabled = storeConfig.isReadOnlyForBatchOnlyStoreEnabled();

    this.serverConfig = builder.getServerConfig();

    this.defaultReadyToServeChecker = getDefaultReadyToServeChecker();

    this.aggKafkaConsumerService = builder.getAggKafkaConsumerService();

    if (serverConfig.isSharedConsumerPoolEnabled()) {
      this.kafkaUrlToSharedConsumerMap = new VeniceConcurrentHashMap<>();
    } else {
      this.kafkaUrlToDedicatedConsumerMap = new VeniceConcurrentHashMap<>();
    }

    this.errorPartitionId = errorPartitionId;
    this.cacheWarmingThreadPool = builder.getCacheWarmingThreadPool();
    this.startReportingReadyToServeTimestamp = builder.getStartReportingReadyToServeTimestamp();

    this.isWriteComputationEnabled = store.isWriteComputationEnabled();

    buildRocksDBMemoryEnforcer();

    this.partitionStateSerializer = builder.getPartitionStateSerializer();

    this.suppressLiveUpdates = serverConfig.freezeIngestionIfReadyToServeOrLocalDataExists();

    /*
     * The reason to use a different field name here is that the naming convention will be consistent with RocksDB.
     */
    this.disableAutoCompactionForSamzaReprocessingJob = !serverConfig.isEnableAutoCompactionForSamzaReprocessingJob();

    this.storeVersionPartitionCount = version.getPartitionCount();

    long pushTimeoutInMs;
    try {
      pushTimeoutInMs = HOURS.toMillis(store.getBootstrapToOnlineTimeoutInHours());
    } catch (Exception e) {
      logger.warn("Error when getting bootstrap to online timeout config for store " + storeName
          + ". Will use default timeout threshold which is 24 hours", e);
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
    this.reportStatusAdapter = new ReportStatusAdapter(
        new IngestionNotificationDispatcher(notifiers, kafkaVersionTopic, isCurrentVersion),
        kafkaVersionTopic,
        amplificationFactor,
        incrementalPushPolicy,
        partitionConsumptionStateMap
    );
    this.amplificationAdapter = new AmplificationAdapter(
        amplificationFactor,
        reportStatusAdapter,
        consumerActionsQueue,
        partitionConsumptionStateMap,
        this::nextSeqNum
    );
    this.cacheBackend = cacheBackend;
    this.localKafkaServer = this.kafkaProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    this.localKafkaServerSingletonSet = Collections.unmodifiableSet(Collections.singleton(localKafkaServer));
    this.isDaVinciClient = builder.isDaVinciClient();
    this.isActiveActiveReplicationEnabled = version.isActiveActiveReplicationEnabled();
    this.offsetLagDeltaRelaxEnabled = serverConfig.getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart() > 0;
    this.ingestionCheckpointDuringGracefulShutdownEnabled = serverConfig.isServerIngestionCheckpointDuringGracefulShutdownEnabled();
    this.metaStoreWriter = builder.getMetaStoreWriter();

    this.storageUtilizationManager = new StorageUtilizationManager(
        storageEngine,
        store,
        kafkaVersionTopic,
        subPartitionCount,
        Collections.unmodifiableMap(partitionConsumptionStateMap),
        serverConfig.isHybridQuotaEnabled(),
        serverConfig.isServerCalculateQuotaUsageBasedOnPartitionsAssignmentEnabled(),
        reportStatusAdapter,
        // N.B. quota enforcement has a race condition when the consumer is not in shared mode, and therefore we leave
        //      the enfocement functions disabled. TODO: Clean this up after the non-shared consumer code is deleted
        serverConfig.isSharedConsumerPoolEnabled()
            ? (topic, partition) -> pauseConsumption(topic, partition)
            : (topic, partition) -> {},
        serverConfig.isSharedConsumerPoolEnabled()
            ? (topic, partition) -> resumeConsumption(topic, partition)
            : (topic, partition) -> {}
    );
    this.storeRepository.registerStoreDataChangedListener(this.storageUtilizationManager);

    if (serverConfig.isSharedConsumerPoolEnabled() && aggKafkaConsumerService == null) {
      throw new IllegalArgumentException(AggKafkaConsumerService.class.getSimpleName()
          + " instance does not exist for shared consumer mode.");
    }
  }

  /** Package-private on purpose, only intended for tests. Do not use for production use cases. */
  void setPurgeTransientRecordBuffer(boolean purgeTransientRecordBuffer) {
    this.purgeTransientRecordBuffer = purgeTransientRecordBuffer;
  }

  public boolean isFutureVersion() {
    return versionedStorageIngestionStats.isFutureVersion(storeName, versionNumber);
  }

  protected void throwIfNotRunning() {
    if (!isRunning()) {
      throw new VeniceException(" Topic " + kafkaVersionTopic + " is shutting down, no more messages accepted");
    }
  }

  // TODO: Support memory enforcer with shared consumer as it has been fully rolled out.
  private void buildRocksDBMemoryEnforcer() {
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
    if (rocksDBMemoryStats.isPresent() && storageEngine.getType().equals(PersistenceType.ROCKS_DB)
        && !serverConfig.isSharedConsumerPoolEnabled()) {
      this.rocksDBMemoryEnforcer = Optional.of(new RocksDBMemoryEnforcement(this));
    } else {
      this.rocksDBMemoryEnforcer = Optional.empty();
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

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public synchronized void subscribePartition(String topic, int partition, Optional<LeaderFollowerStateType> leaderState) {
    throwIfNotRunning();
    amplificationAdapter.subscribePartition(topic, partition, leaderState);
  }

  // Visible for testing
  synchronized void subscribePartition(String topic, int partition) {
    subscribePartition(topic, partition, Optional.empty());
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public synchronized void unSubscribePartition(String topic, int partition) {
    throwIfNotRunning();
    amplificationAdapter.unSubscribePartition(topic, partition);
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public synchronized void resetPartitionConsumptionOffset(String topic, int partition) {
    throwIfNotRunning();
    amplificationAdapter.resetPartitionConsumptionOffset(topic, partition);
  }

  /**
   * Get the Store for this ingestion task. Short term solution for funneling versioned stats of zk shared system stores.
   */
  public Store getIngestionStore() {
    return storeRepository.getStoreOrThrow(storeName);
  }

  public String getStoreName() {
    return storeName;
  }

  public abstract void promoteToLeader(String topic, int partitionId, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker);
  public abstract void demoteToStandby(String topic, int partitionId, LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker);

  public void kill() {
    synchronized (this) {
      throwIfNotRunning();
      consumerActionsQueue.add(ConsumerAction.createKillAction(kafkaVersionTopic, nextSeqNum()));
    }

    try (Timer ignored = Timer.run(elapsedTimeInMs -> logger.info(
        "Completed waiting for kill action to take effect. Total elapsed time: " + elapsedTimeInMs + " ms"))) {
      for (int attempt = 0; isRunning() && attempt < MAX_KILL_CHECKING_ATTEMPTS; ++attempt) {
        MILLISECONDS.sleep(KILL_WAIT_TIME_MS / MAX_KILL_CHECKING_ATTEMPTS);
      }
    } catch (InterruptedException e) {
      logger.warn("StoreIngestionTask::kill was interrupted.", e);
    }

    synchronized (this) {
      if (isRunning()) {
        // If task is still running, force close it.
        reportStatusAdapter.reportError(partitionConsumptionStateMap.values(),
            "Received the signal to kill this consumer. Topic " + kafkaVersionTopic,
            new VeniceException("Kill the consumer"));
        /*
         * close can not stop the consumption synchronously, but the status of helix would be set to ERROR after
         * reportError. The only way to stop it synchronously is interrupt the current running thread, but it's an unsafe
         * operation, for example it could break the ongoing db operation, so we should avoid that.
         */
        close();
      }
    }
  }

  private void beginBatchWrite(int partitionId, boolean sorted, PartitionConsumptionState partitionConsumptionState) {
    Map<String, String> checkpointedDatabaseInfo = partitionConsumptionState.getOffsetRecord().getDatabaseInfo();
    StoragePartitionConfig storagePartitionConfig = getStoragePartitionConfig(partitionId, sorted, partitionConsumptionState);
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());

    Optional<Supplier<byte[]>> partitionChecksumSupplier = Optional.empty();
    /**
     * In rocksdb Plain Table mode or in non deferredWrite mode, we can't use rocksdb SSTFileWriter to verify the checksum.
     * So there is no point keep calculating the running checksum here.
     */
    if (serverConfig.isDatabaseChecksumVerificationEnabled() && partitionConsumptionState.isDeferredWrite()
        && !serverConfig.getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled()) {
      partitionConsumptionState.initializeExpectedChecksum();
      partitionChecksumSupplier = Optional.of(() -> {
        byte[] checksum = partitionConsumptionState.getExpectedChecksum();
        partitionConsumptionState.resetExpectedChecksum();
        return checksum;
      });
    }
    if (!sorted && disableAutoCompactionForSamzaReprocessingJob) {
      /**
       * Disable auto-compaction for Samza Reprocessing Job.
       */
      storagePartitionConfig.setDisableAutoCompaction(true);
    }
    storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic)
        .beginBatchWrite(storagePartitionConfig, checkpointedDatabaseInfo, partitionChecksumSupplier);
    logger.info("Started batch write to store: " + kafkaVersionTopic + ", partition: " + partitionId +
        " with checkpointed database info: " + checkpointedDatabaseInfo + " and sorted: " + sorted +
        " and disabledAutoCompaction: " + storagePartitionConfig.isDisableAutoCompaction());
    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get().getStorageEngine(kafkaVersionTopic).beginBatchWrite(storagePartitionConfig, checkpointedDatabaseInfo, partitionChecksumSupplier);
      }
    }
  }

  private StoragePartitionConfig getStoragePartitionConfig(int partitionId, boolean sorted,
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
    orderedWritesOnly &= deferredWrites;
    storagePartitionConfig.setDeferredWrite(deferredWrites);
    storagePartitionConfig.setReadOnly(readOnly);
    return storagePartitionConfig;
  }

  /**
   * This function checks various conditions to verify if a store is ready to serve.
   *
   * Lag = (Source Max Offset - SOBR Source Offset) - (Current Offset - SOBR Destination Offset)
   *
   * @param partitionConsumptionState
   *
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

    if (!hybridStoreConfig.isPresent()) {
      /**
       * In theory, it will be 1 offset ahead of the current offset since getOffset returns the next available offset.
       * Currently, we make it a sloppy in case Kafka topic have duplicate messages.
       * TODO: find a better solution
       */
      final long versionTopicPartitionOffset = cachedKafkaMetadataGetter.getOffset(
          getTopicManager(localKafkaServer),
          kafkaVersionTopic,
          partitionId
      );
      isLagAcceptable =
          versionTopicPartitionOffset <= partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset() + SLOPPY_OFFSET_CATCHUP_THRESHOLD;
    } else {
      try {
        // Looks like none of the short-circuitry fired, so we need to measure lag!
        long offsetThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
        long producerTimeLagThresholdInSeconds = hybridStoreConfig.get().getProducerTimestampLagThresholdToGoOnlineInSeconds();
        String msg = kafkaVersionTopic + "_" + partitionId;

        // Log only once a minute per partition.
        boolean shouldLogLag = !REDUNDANT_LOGGING_FILTER.isRedundantException(msg);
        /**
         * If offset lag threshold is set to -1, time lag threshold will be the only criterion for going online.
         */
        if (offsetThreshold >= 0) {
          long lag = measureHybridOffsetLag(partitionConsumptionState, shouldLogLag);

          boolean lagging = lag > offsetThreshold;

          isLagAcceptable = !lagging;

          if (shouldLogLag) {
            logger.info(String.format("%s [Offset lag] partition %d is %slagging. Lag: [%d] %s Threshold [%d]", consumerTaskId,
                partitionId, (lagging ? "" : "not "), lag, (lagging ? ">" : "<"), offsetThreshold));
          }
        }

        /**
         * If the hybrid producer time lag threshold is positive, check the difference between current time and latest
         * producer timestamp; ready-to-serve will not be reported until the diff is smaller than the defined time lag threshold.
         *
         * If timestamp lag threshold is set to -1, offset lag threshold will be the only criterion for going online.
         */
        if (producerTimeLagThresholdInSeconds > 0) {
          long producerTimeLagThresholdInMS = TimeUnit.SECONDS.toMillis(producerTimeLagThresholdInSeconds);
          long latestConsumedProducerTimestamp = partitionConsumptionState.getOffsetRecord().getLatestProducerProcessingTimeInMs();
          if (amplificationFactor != 1) {
            latestConsumedProducerTimestamp =
                getLatestConsumedProducerTimestampWithSubPartition(latestConsumedProducerTimestamp, partitionConsumptionState);
          }
          long producerTimestampLag = LatencyUtils.getElapsedTimeInMs(latestConsumedProducerTimestamp);
          boolean timestampLagIsAcceptable = (producerTimestampLag < producerTimeLagThresholdInMS);
          if (shouldLogLag) {
            logger.info(String.format("%s [Time lag] partition %d is %slagging. The latest producer timestamp is %d. Timestamp Lag: [%d] %s Threshold [%d]",
                consumerTaskId, partitionId, (!timestampLagIsAcceptable ? "" : "not "), latestConsumedProducerTimestamp, producerTimestampLag,
                (timestampLagIsAcceptable ? "<" : ">"), producerTimeLagThresholdInMS));
          }
          /**
           * If time lag is not acceptable but the producer timestamp of the last message of RT is smaller or equal than
           * the known latest producer timestamp in server, it means ingestion task has reached the end of RT, so it's
           * safe to ignore the time lag.
           *
           * Notice that if EOP is not received, this function will be short circuit before reaching here, so there is
           * no risk of meeting the time lag earlier than expected.
           */
          if (!timestampLagIsAcceptable) {
            String msgIdentifier = this.kafkaVersionTopic + "_" + partitionId + "_ignore_time_lag";
            String realTimeTopicKafkaURL;
            Set<String> realTimeTopicKafkaURLs = getRealTimeDataSourceKafkaAddress(partitionConsumptionState);
            if (realTimeTopicKafkaURLs.isEmpty()) {
              throw new VeniceException("Expect a real-time topic Kafka URL for store " + storeName);
            } else if (realTimeTopicKafkaURLs.size() == 1) {
              realTimeTopicKafkaURL = realTimeTopicKafkaURLs.iterator().next();
            } else if (realTimeTopicKafkaURLs.contains(localKafkaServer)) {
              realTimeTopicKafkaURL = localKafkaServer;
            } else {
              throw new VeniceException(String.format("Expect source RT Kafka URLs contains local Kafka URL. Got local " +
                      "Kafka URL %s and RT source Kafka URLs %s", localKafkaServer, realTimeTopicKafkaURLs));
            }

            final String lagMeasurementTopic, lagMeasurementKafkaUrl;
            // Since DaVinci clients run in embedded mode, they may not have network ACLs to check remote RT to get latest
            // producer timestamp in RT. Only use latest producer time in local RT.
            if (isDaVinciClient) {
              lagMeasurementKafkaUrl = localKafkaServer;
              lagMeasurementTopic = realTimeTopic;
            } else {
              lagMeasurementKafkaUrl = realTimeTopicKafkaURL;
              lagMeasurementTopic = realTimeTopic;
            }

            if (!cachedKafkaMetadataGetter.containsTopic(getTopicManager(lagMeasurementKafkaUrl), lagMeasurementTopic)) {
              timestampLagIsAcceptable = true;
              if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
                logger.info(String.format("%s [Time lag] Topic %s doesn't exist; ignoring time lag.", consumerTaskId, lagMeasurementTopic));
              }
            } else {
              long latestProducerTimestampInTopic = cachedKafkaMetadataGetter.getProducerTimestampOfLastDataMessage(
                  getTopicManager(lagMeasurementKafkaUrl),
                  lagMeasurementTopic,
                  partitionId
              );
              if (latestProducerTimestampInTopic < 0 || latestProducerTimestampInTopic <= latestConsumedProducerTimestamp) {
                timestampLagIsAcceptable = true;
                if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
                  if (latestProducerTimestampInTopic < 0) {
                    logger.info(String.format(
                        "%s [Time lag] Topic %s is empty or all messages have been truncated; ignoring time lag.",
                        consumerTaskId, lagMeasurementTopic));
                  } else {
                    logger.info(String.format("%s [Time lag] Producer timestamp of last message in topic %s " +
                            "partition %d: %d, which is smaller or equal than the known latest producer time: %d. "
                            + "Consumption lag is caught up already.", consumerTaskId, lagMeasurementTopic, partitionId,
                        latestProducerTimestampInTopic, latestConsumedProducerTimestamp));
                  }
                }
              }
            }
          }
          if (offsetThreshold >= 0) {
            /**
             * If both threshold configs are on, both both offset lag and time lag must be within thresholds before online.
             */
            isLagAcceptable &= timestampLagIsAcceptable;
          } else {
            isLagAcceptable = timestampLagIsAcceptable;
          }
        }
      } catch (Exception e) {
        String exceptionMsgIdentifier = new StringBuilder()
            .append(kafkaVersionTopic)
            .append("_isReadyToServe")
            .toString();
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(exceptionMsgIdentifier)) {
          logger.info("Exception when trying to determine if hybrid store is ready to serve: {}", storeName, e);
        }
        isLagAcceptable = false;
      }
    }

    if (isLagAcceptable) {
      amplificationAdapter.lagHasCaughtUp(partitionConsumptionState.getUserPartition());
    }
    return isLagAcceptable;
  }

  public boolean isReadyToServeAnnouncedWithRTLag() {
    return false;
  }

  protected long getLatestConsumedProducerTimestampWithSubPartition(long consumedProducerTimestamp,
      PartitionConsumptionState partitionConsumptionState) {
    long latestConsumedProducerTimestamp = consumedProducerTimestamp;

    IntList subPartitions = PartitionUtils.getSubPartitions(partitionConsumptionState.getUserPartition(), amplificationFactor);
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
  protected abstract long measureHybridOffsetLag(PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag);

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
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumedRecord,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl) throws InterruptedException {
    long queuePutStartTimeInNS = System.nanoTime();
    storeBufferService.putConsumerRecord(consumedRecord, this, leaderProducedRecordContext, subPartition, kafkaUrl); // blocking call
    storeIngestionStats.recordProduceToDrainQueueRecordNum(storeName, 1);
    if (emitMetrics.get()) {
      storeIngestionStats.recordConsumerRecordsQueuePutLatency(storeName, LatencyUtils.getLatencyInMS(queuePutStartTimeInNS));
    }
  }

  /**
   * This function is in charge of producing the consumer records to the writer buffers maintained by {@link StoreBufferService}.
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
   *
   * @param records : received consumer records
   * @param whetherToApplyThrottling : whether to apply throttling in this function or not.
   * @param topicPartition
   * @throws InterruptedException
   */
  protected void produceToStoreBufferServiceOrKafka(Iterable<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> records,
      boolean whetherToApplyThrottling, TopicPartition topicPartition, String kafkaUrl) throws InterruptedException {
    long totalBytesRead = 0;
    double elapsedTimeForPuttingIntoQueue = 0;
    long beforeProcessingTimestamp = System.currentTimeMillis();
    int recordQueuedToDrainer = 0;
    int recordProducedToKafka = 0;
    double elapsedTimeForProducingToKafka = 0;
    /**
     * This set is used to track all partitions, where manual compaction gets triggered in this iteration.
     * Once the manual compaction is triggered, all the following messages belonging to the same partition
     * will be dropped.
     */
    IntSet compactingPartitions = new IntOpenHashSet();
    int subPartition = PartitionUtils.getSubPartition(topicPartition.topic(), topicPartition.partition(), amplificationFactor);
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record: records) {
      if (!shouldProcessRecord(record, subPartition)) {
        continue;
      }

      if (disableAutoCompactionForSamzaReprocessingJob) {
        int consumingPartition = record.partition();
        if (compactingPartitions.contains(consumingPartition)) {
          /**
           * Skip all the leftover messages since some previous message (EOP specifically for current logic) triggers one-time compaction.
           */
          continue;
        }
        CompletableFuture<Void> dbCompactFuture = dbCompactFutureMap.get(consumingPartition);
        if (dbCompactFuture != null) {
          /**
           * One-time db compaction happened before.
           */
          if (!dbCompactFuture.isDone()) {
            throw new VeniceException(
                "Unexpected to receive any new messages while the DB compaction is still " + "ongoing for store: " +
                    kafkaVersionTopic + ", partition: " + consumingPartition);
          } else if (dbCompactFuture.isCompletedExceptionally()) {
            try {
              dbCompactFuture.get();
            } catch (ExecutionException e) {
              throw new VeniceException(
                  "Unexpected to receive any new message since the one-time db compaction failed " + "for store: " +
                      kafkaVersionTopic + ", partition: " + consumingPartition + " with exception: " + e);
            }
          }
        } else {
          /**
           * Try to check whether the current message is EOP or not.
           */
          KafkaKey kafkaKey = record.key();
          if (kafkaKey.isControlMessage() && ControlMessageType.valueOf((ControlMessage) record.value().payloadUnion) == ControlMessageType.END_OF_PUSH) {
            Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
            if (!storeVersionState.isPresent()) {
              /**
               * EOP is received, but {@link StoreVersionState} for current store is not available, which indicates that
               * this is a small push, but to be consistent, we will flush all the pending messages in the drainer queue, and wait
               * for the {@link StoreVersionState} to show up.
               */
              PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(consumingPartition);
              if (null == partitionConsumptionState) {
                /**
                 * Defensive coding.
                 */
                logger.error(
                    "Encountered null 'PartitionConsumptionState' when trying to apply delay compaction to topic: " +
                        kafkaVersionTopic + ", partition: " + consumingPartition);
              }
              waitForAllMessageToBeProcessedFromTopicPartition(record.topic(), record.partition(), partitionConsumptionState);
              storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
              if (!storeVersionState.isPresent()) {
                throw new VeniceException(
                    "Failed to get StoreVersionState after draining all the pending messages for topic: " +
                        kafkaVersionTopic + ", partition: " + consumingPartition);
              }
            }

            if (storeVersionState.get().sorted) {
              /**
               * The batch part is sorted, which indicates the push job is mostly from VPJ, so no delay compaction is required.
               * Nothing needs to be done here
               */
            } else {
              /**
               * The batch part is unordered, and this is a targeted use case to leverage the delay compaction to improve the
               * ingestion performance for the batch part.
               * Since {@link #beginBatchWrite} will disable auto compaction, and we will need to do one-time manual compaction
               * here before processing EOP.
               *
               * There are several steps needed to be done here:
               * 1. Temporarily pause the consumption since during the manual compaction, and the RocksDB update will become very slow,
               *    so, the messages for this specific topic partition after triggering manual compaction could saturate the buffer queue,
               *    which will affect the ingestion speed of all other tenants.
               * 2. Drain all the pending messages belonging to the specific topic partition.
               * 3. Trigger a one-time manual compaction.
               * 4. Abandon all the following messages including EOP, and there are several reasons:
               *    a. Not processing EOP will let us not involve the completion reporting logic, and it will guarantee
               *       the RocksDB will be compacted before reporting completion.
               *    b. Not processing EOP has another benefit to make this behavior idempotent, and it means this logic
               *       will be executed even there is a restart in the middle of the manual RocksDB compaction.
               *    c. All the following messages will be blocked by this manual compaction.
               * 5. {@link #checkLongRunningDBCompaction()}  will periodically check the compaction status and once the compaction
               *    is done, it will resume the consumption from the point where messages gets dropped.
               */
              /**
               * In theory, there should be only one consumer for Samza Reprocessing topic.
               */
              String consumingTopic = record.topic();
              if (serverConfig.isSharedConsumerPoolEnabled()) {
                Set<String> kafkaUrls = aggKafkaConsumerService.getKafkaUrlsFor(kafkaVersionTopic);
                if (kafkaUrls.size() != 1) {
                  throw new VeniceException("Expect to consume for: " + kafkaVersionTopic + ", partition: " + consumingPartition
                      + ", in only one Kafka cluster before processing EOP. But consuming from multiple kafka clusters: " + kafkaUrls);
                }
              } else {
                Collection<KafkaConsumerWrapper> dedicatedConsumers = getDedicatedConsumers();
                if (dedicatedConsumers.size() != 1) {
                  throw new VeniceException("Only one consumer is expected before processing EOP for topic: " +
                      kafkaVersionTopic + ", partition: " + consumingPartition);
                }
              }
              pauseConsumption(consumingTopic, consumingPartition);
              logger.info("Paused consumption to topic: " + consumingTopic + ", partition: " + consumingPartition +
                  " because of one-time db compaction");
              PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(consumingPartition);
              if (null == partitionConsumptionState) {
                /**
                 * Defensive coding.
                 */
                logger.error(
                    "Encountered null 'PartitionConsumptionState' when trying to apply delay compaction to topic: " +
                        kafkaVersionTopic + ", partition: " + consumingPartition);
              }
              waitForAllMessageToBeProcessedFromTopicPartition(record.topic(), consumingPartition,
                  partitionConsumptionState);
              logger.info("All pending messages belonging to topic: " + consumingTopic + ", partition: " +
                  consumingPartition + " are persisted, for one-time db compaction");

              // Trigger one-time compaction
              AbstractStorageEngine engine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
              if (null == engine) {
                throw new VeniceException("Storage Engine for store: " + kafkaVersionTopic + " has been closed");
              }
              AbstractStoragePartition storagePartition = engine.getPartitionOrThrow(consumingPartition);
              dbCompactFutureMap.put(record.partition(), storagePartition.compactDB());
              eopControlMessageMap.put(consumingPartition, record);
              logger.info("Triggered one time db compaction for store: " + kafkaVersionTopic + ", partition: " + consumingPartition);
              compactingPartitions.add(consumingPartition);
              /**
               * Skip the EOP control message after triggering the manual db compaction.
               */
              continue;
            }
          }
        }
      }

      // Check schema id availability before putting consumer record to drainer queue
      waitReadyToProcessRecord(record);
      long kafkaProduceStartTimeInNS = System.nanoTime();
      // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.

      DelegateConsumerRecordResult delegateConsumerRecordResult = delegateConsumerRecord(record, subPartition, kafkaUrl);
      switch (delegateConsumerRecordResult) {
        case QUEUED_TO_DRAINER:
          long queuePutStartTimeInNS = System.nanoTime();
          storeBufferService.putConsumerRecord(record, this, null, subPartition, kafkaUrl); // blocking call
          elapsedTimeForPuttingIntoQueue += LatencyUtils.getLatencyInMS(queuePutStartTimeInNS);
          ++recordQueuedToDrainer;
          break;
        case PRODUCED_TO_KAFKA:
          elapsedTimeForProducingToKafka += LatencyUtils.getLatencyInMS(kafkaProduceStartTimeInNS);
          ++recordProducedToKafka;
          break;
        case SKIPPED_MESSAGE:
        case DUPLICATE_MESSAGE:
          /**
           * DuplicatedDataException can be thrown when leader is consuming from RT and is running DIV check on the message
           * before producing it to VT. Still record the time spent on trying to produce to Kafka, but should not update
           * the counter for records that have been produced to Kafka
           */
          elapsedTimeForProducingToKafka += LatencyUtils.getLatencyInMS(kafkaProduceStartTimeInNS);
          break;
        default:
          throw new VeniceException(consumerTaskId + " received unknown DelegateConsumerRecordResult enum for topic "
              + record.topic() + " partition " + record.partition());
      }
      totalBytesRead += Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize());
      // Update the latest message consumption time
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLatestMessageConsumptionTimestampInMs(System.currentTimeMillis());
      }
    }
    storeIngestionStats.recordProduceToDrainQueueRecordNum(storeName, recordQueuedToDrainer);
    if (recordProducedToKafka > 0) {
      storeIngestionStats.recordProduceToKafkaRecordNum(storeName, recordProducedToKafka);
    }

    long quotaEnforcementStartTimeInNS = System.nanoTime();
    /**
     * Even if the records list is empty, we still need to check quota to potentially resume partition
     */
    storageUtilizationManager.enforcePartitionQuota(topicPartition.partition(), totalBytesRead);

    if (emitMetrics.get()) {
      storeIngestionStats.recordQuotaEnforcementLatency(storeName, LatencyUtils.getLatencyInMS(quotaEnforcementStartTimeInNS));
      if (totalBytesRead > 0) {
        storeIngestionStats.recordTotalBytesReadFromKafkaAsUncompressedSize(totalBytesRead);
      }
      long afterPutTimestamp = System.currentTimeMillis();
      storeIngestionStats.recordConsumerToQueueLatency(storeName, afterPutTimestamp - beforeProcessingTimestamp);
      if (elapsedTimeForPuttingIntoQueue > 0) {
        storeIngestionStats.recordConsumerRecordsQueuePutLatency(storeName, elapsedTimeForPuttingIntoQueue);
      }
      if (elapsedTimeForProducingToKafka > 0) {
        storeIngestionStats.recordProduceToKafkaLatency(storeName, elapsedTimeForProducingToKafka);
      }

      storeIngestionStats.recordStorageQuotaUsed(storeName, storageUtilizationManager.getDiskQuotaUsage());
    }

    if (whetherToApplyThrottling) {
      /**
       * We would like to throttle the ingestion by batch ({@link ConsumerRecords} return by each poll.
       * The batch shouldn't be too big, otherwise, the {@link StoreBufferService#putConsumerRecord(ConsumerRecord, StoreIngestionTask)}
       * could be blocked when the buffer is full, and the throttling could be inaccurate.
       * So every record returned from {@link KafkaConsumerWrapper#poll(long)} should be processed
       * as fast as possible to avoid long-lasting objects in JVM to minimize the 'object copy' time
       * during GC.
       *
       * Here are more details:
       * 1. Previously, the throttling was happening in StoreIngestionTask#processConsumerRecord,
       *    which would be invoked by StoreBufferService;
       * 2. When the ingestion got throttled, the database operations would halt, but the deserialized
       *    records would be kept pushing to the intermediate buffer pool until the pool is full;
       * 3. When Young GC happens, all the objects in the buffer pool could be potentially copied to Survivor
       *    space since they are being actively referenced;
       * 4. The object copy time is the slowest phase in Young GC;
       *
       * By moving the throttling logic after putting deserialized records to buffer, the records in the buffer
       * will be processed as long as there is enough capacity.
       * With this way, the actively referenced object will be reduced greatly during throttling and Young GC
       * won't need to copy many objects from Young regions to Survivor regions, which reduces the overall
       * GC pause time.
       */
      bandwidthThrottler.maybeThrottle(totalBytesRead);
      recordsThrottler.maybeThrottle(recordCount);

      if (!orderedWritesOnly) {
        unorderedBandwidthThrottler.maybeThrottle(totalBytesRead);
        unorderedRecordsThrottler.maybeThrottle(recordCount);
      }
    }
  }

  private void drainExceptionQueue(BlockingQueue<PartitionExceptionInfo> exceptionQueue, String exceptionGroup) {
    while (!exceptionQueue.isEmpty()) {
      try {
        PartitionExceptionInfo exceptionPartitionPair = exceptionQueue.take();
        int exceptionPartition = exceptionPartitionPair.getPartitionId();
        PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(exceptionPartition);
        if (partitionConsumptionState == null) {
          logger.warn("Ignoring exception for partition " + exceptionPartition + " for store version " + kafkaVersionTopic
              + " since this partition has been unsubscribed already.", exceptionPartitionPair.getException());
          continue;
        }

        if (!partitionConsumptionState.isCompletionReported()) {
          reportError(exceptionPartitionPair.getException().getMessage(), exceptionPartition, exceptionPartitionPair.getException());
          if (partitionConsumptionStateMap.containsKey(exceptionPartition)) {
            unSubscribePartition(kafkaVersionTopic, exceptionPartition);
          }
        } else {
          logger.error("Ignoring exception for partition " + exceptionPartition + " for store version " + kafkaVersionTopic
              + " since this partition is already online. Please engage Venice DEV team immediately.", exceptionPartitionPair.getException());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Interrupted while trying to drain exceptions from " + exceptionGroup
            + " exception queue for topic: " + kafkaVersionTopic);
      }
    }
  }

  protected void processMessages(boolean usingSharedConsumer, Store store) throws InterruptedException {
    Exception ingestionTaskException = lastStoreIngestionException.get();
    if (null != ingestionTaskException) {
      throw new VeniceException("Unexpected store ingestion task level exception, will error out the entire" +
          " ingestion task and all its partitions", ingestionTaskException);
    }
    if (null != lastConsumerException) {
      throw new VeniceException("Exception thrown by shared consumer", lastConsumerException);
    }

    /**
     * Drainer and producer exceptions are granular enough for us to achieve partition level exception isolation. We
     * drain the exception queues here and put partitions into error state accordingly without killing the ingestion task.
     */
    drainExceptionQueue(drainerExceptions, EXCEPTION_GROUP_DRAINER);
    drainExceptionQueue(producerExceptions, EXCEPTION_GROUP_PRODUCER);
    drainExceptionQueue(consumerExceptions, EXCEPTION_GROUP_CONSUMER);

    errorPartitions.clear();

    /**
     * Check whether current consumer has any subscription or not since 'poll' function will throw
     * {@link IllegalStateException} with empty subscription.
     */
    if (!consumerHasAnySubscription()) {
      if (++idleCounter <= MAX_IDLE_COUNTER) {
        String message = consumerTaskId + " Not subscribed to any partitions";
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
          logger.info(message);
        }

        Thread.sleep(readCycleDelayMs);
      } else {
        if (!hybridStoreConfig.isPresent() &&
            serverConfig.isUnsubscribeAfterBatchpushEnabled() && subscribedCount != 0 && subscribedCount == forceUnSubscribedCount) {
          String msg = consumerTaskId + " Going back to sleep as consumption has finished and topics are unsubscribed";
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            logger.info(msg);
          }
          Thread.sleep(readCycleDelayMs * 20);
          idleCounter = 0;
        } else {
          logger.warn(consumerTaskId + " Has expired due to not being subscribed to any partitions for too long.");
          complete();
        }
      }
      return;
    }

    idleCounter = 0;
    maybeUnsubscribeCompletedPartitions(store);
    recordDiskQuotaAllowedForStore(store);

    if (usingSharedConsumer) {
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

    } else {
      getRecordsFromDedicatedConsumerToProcess();
    }
  }

  private void maybeUnsubscribeCompletedPartitions(Store store) {
    if (hybridStoreConfig.isPresent() || (!serverConfig.isUnsubscribeAfterBatchpushEnabled())) {
      return;
    }
    // unsubscribe completed backup version and batch-store versions.
    if (versionNumber <= store.getCurrentVersion()) {
      Set<TopicPartition> topicPartitionsToUnsubscribe = new HashSet<>();
      for (PartitionConsumptionState state : partitionConsumptionStateMap.values()) {
        if (state.isCompletionReported() && !state.isIncrementalPushEnabled()
            && consumerHasSubscription(kafkaVersionTopic, state)) {
          logger.info("Unsubscribing completed partitions " + state.getPartition() + " of store : " + store.getName()
              + " version : "  + versionNumber + " current version: " + store.getCurrentVersion());
          topicPartitionsToUnsubscribe.add(new TopicPartition(kafkaVersionTopic, state.getPartition()));
          forceUnSubscribedCount++;
        }
      }
      if (topicPartitionsToUnsubscribe.size() != 0) {
        consumerBatchUnsubscribe(topicPartitionsToUnsubscribe);
      }
    }
  }

  private void recordDiskQuotaAllowedForStore(Store store) {
    if (emitMetrics.get()) {
      long currentQuota = store.getStorageQuotaInByte();
      storeIngestionStats.recordDiskQuotaAllowed(storeName, currentQuota);
    }
  }

  private void getRecordsFromDedicatedConsumerToProcess() throws InterruptedException {
    long beforePollingTimestamp = System.currentTimeMillis();

    boolean wasIdle = (recordCount == 0);
    Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> recordsByKafkaURLs = dedicatedConsumerPoll(readCycleDelayMs);
    recordCount = 0;
    long estimatedSize = 0;
    for (ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords : recordsByKafkaURLs.values()) {
      recordCount += consumerRecords.count();
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : consumerRecords) {
        estimatedSize += Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize());
      }
    }

    long afterPollingTimestamp = System.currentTimeMillis();

    if (rocksDBMemoryEnforcer.isPresent()) {
      rocksDBMemoryEnforcer.get().enforceMemory(estimatedSize);
      if (rocksDBMemoryEnforcer.get().isIngestionPaused()) {
        return;
      }
    }

    if (emitMetrics.get()) {
      storeIngestionStats.recordPollRequestLatency(storeName, afterPollingTimestamp - beforePollingTimestamp);
      storeIngestionStats.recordPollResultNum(storeName, recordCount);
    }

    if (recordCount == 0) {
      if (wasIdle) {
        Thread.sleep(emptyPollSleepMs);
      } else {
        // This is the first time we polled and got an empty response set
        versionedDIVStats.resetCurrentIdleTime(storeName, versionNumber);
      }
      versionedDIVStats.recordCurrentIdleTime(storeName, versionNumber);
      versionedDIVStats.recordOverallIdleTime(storeName, versionNumber);
      return;
    }

    for (Map.Entry<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> entry : recordsByKafkaURLs.entrySet()) {
      for (TopicPartition topicPartition: entry.getValue().partitions()) {
        produceToStoreBufferServiceOrKafka(
            entry.getValue().records(topicPartition),
            true,
            topicPartition,
            entry.getKey());
      }
    }
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
      logger.info("Running " + consumerTaskId);
      versionedStorageIngestionStats.resetIngestionTaskPushTimeoutGauge(storeName, versionNumber);

      while (isRunning()) {
        Store store = storeRepository.getStoreOrThrow(storeName);
        processConsumerActions(store);
        checkLongRunningTaskState();
        checkLongRunningDBCompaction();
        processMessages(serverConfig.isSharedConsumerPoolEnabled(), store);
      }

      // If the ingestion task is stopped gracefully (server stops), persist processed offset to disk
      for (PartitionConsumptionState partitionConsumptionState : partitionConsumptionStateMap.values()) {
        /**
         * Now, there are two threads, which could potentially trigger {@link #syncOffset(String, PartitionConsumptionState)}:
         * 1. {@link StoreBufferService.StoreBufferDrainer}, which will checkpoint
         *    periodically;
         * 2. The main thread of ingestion task here, which will checkpoint when gracefully shutting down;
         *
         * We would like to make sure the syncOffset invocation is sequential with the message processing, so here
         * will try to drain all the messages before checkpointing.
         * Here is the detail::
         * If the checkpointing happens in different threads concurrently, there is no guarantee the atomicity of
         * offset and checksum, since the checksum could change in another thread, but the corresponding offset change
         * hasn't been applied yet, when checkpointing happens in current thread.
         */
        consumerUnSubscribeAllTopics(partitionConsumptionState);

        if (ingestionCheckpointDuringGracefulShutdownEnabled) {
          waitForAllMessageToBeProcessedFromTopicPartition(kafkaVersionTopic, partitionConsumptionState.getPartition(),
              partitionConsumptionState);
          syncOffset(kafkaVersionTopic, partitionConsumptionState);
        }
      }

    } catch (VeniceIngestionTaskKilledException e) {
      logger.info(consumerTaskId + " has been killed.", e);
      reportStatusAdapter.reportKilled(partitionConsumptionStateMap.values(), e);
      doFlush = false;
    } catch (org.apache.kafka.common.errors.InterruptException | InterruptedException e) {
      // Known exceptions during graceful shutdown of storage server. Report error only if the server is still running.
      if (isRunning()) {
        // Report ingestion failure if it is not caused by kill operation or server restarts.
        logger.error(consumerTaskId + " has failed.", e);
        reportStatusAdapter.reportError(partitionConsumptionStateMap.values(), "Caught InterruptException during ingestion.", e);
        storeIngestionStats.recordIngestionFailure(storeName);
      }

    } catch (Throwable t) {
      // After reporting error to controller, controller will ignore the message from this replica if job is aborted.
      // So even this storage node recover eventually, controller will not confused.
      // If job is not aborted, controller is open to get the subsequent message from this replica(if storage node was
      // recovered, it will send STARTED message to controller again)

      /**
       * It's possible to receive checksum verification failure exception here from the above syncOffset() call.
       * If this task is getting closed anyway (most likely due to SN being shut down), we should not report this replica
       * as ERROR. just record the relevant metrics.
       */
      if (t instanceof VeniceChecksumException) {
        recordChecksumVerificationFailure();
        if (!isRunning()) {
          logger.error(consumerTaskId + " received verifyChecksum: exception ", t);
          return;
        }
      }

      logger.error(consumerTaskId + " has failed.", t);
      /**
       * Completed partitions will remain ONLINE without a backing ingestion task. If the partition belongs to a hybrid
       * store then it will remain stale until the host is restarted. This is because both the auto reset task and Helix
       * controller doesn't think there is a problem with the replica since it's COMPLETED and ONLINE. Stale replicas is
       * better than dropping availability and that's why we do not put COMPLETED replicas to ERROR state immediately.
       */
      for (PartitionConsumptionState pcs : partitionConsumptionStateMap.values()) {
        if (pcs.isComplete()) {
          versionedStorageIngestionStats.recordStalePartitionsWithoutIngestionTask(storeName, versionNumber);
        }
      }
      if (t instanceof Exception) {
        reportError(partitionConsumptionStateMap.values(), errorPartitionId,
            "Caught Exception during ingestion.", (Exception) t);
        if (t instanceof VeniceTimeoutException) {
          versionedStorageIngestionStats.setIngestionTaskPushTimeoutGauge(storeName, versionNumber);
        }
      } else {
        reportError(partitionConsumptionStateMap.values(), errorPartitionId,
            "Caught non-exception Throwable during ingestion.", new VeniceException(t));
      }
      storeIngestionStats.recordIngestionFailure(storeName);
    } finally {
      internalClose(doFlush);
    }
  }

  /**
   * This function is used to check whether the long-running db compaction is done or not.
   * Once it is done, it will try to resume the consumption if the current consumer is still subscribing the corresponding
   * topic partition.
   */
  private void checkLongRunningDBCompaction() {
    if (!disableAutoCompactionForSamzaReprocessingJob) {
      return;
    }
    if (dbCompactFutureMap.isEmpty()) {
      // Nothing to check
      return;
    }
    for (Map.Entry<Integer, CompletableFuture<Void>> entry : dbCompactFutureMap.entrySet()) {
      int partition = entry.getKey();
      CompletableFuture<Void> dbCompactFuture = entry.getValue();
      if (dbCompactFuture.isDone() && !consumptionResumedPartitionSet.contains(partition)) {
        /**
         * We couldn't clean up {@link #dbCompactFutureMap} for the completed db compaction since this completed
         * future in the map will be used in {@link #produceToStoreBufferServiceOrKafka}.
         */
        consumptionResumedPartitionSet.add(partition);
        if (dbCompactFuture.isCompletedExceptionally()) {
          try {
            dbCompactFuture.get();
          } catch (Exception e) {
            throw new VeniceException("One-time manual db compaction for store: " + kafkaVersionTopic + ", partition: "
                + partition + " failed with exception", e);
          }
        } else {
          logger.info("One-time compaction is done for store: " + kafkaVersionTopic + ", partition: " + partition +
              ", will resume the consumption");

          ConsumerRecord<KafkaKey, KafkaMessageEnvelope> eopControlMessage = eopControlMessageMap.get(partition);
          if (null == eopControlMessage) {
            throw new VeniceException("EOP control message wasn't persisted before the manual compaction for store: " +
                kafkaVersionTopic + ", partition: " + partition);
          }

          String consumingTopic = eopControlMessage.topic();
          long resumedOffset = eopControlMessage.offset() - 1;
          if (serverConfig.isSharedConsumerPoolEnabled()) {
            Set<String> kafkaUrls = aggKafkaConsumerService.getKafkaUrlsFor(kafkaVersionTopic);
            if (kafkaUrls.size() != 1) {
              throw new VeniceException("Only one kafka cluster is expected for store version to consume: "
                  + kafkaVersionTopic + " when manual compaction is running, but we have: " + kafkaUrls);
            }
            kafkaUrls.forEach(kafkaUrl -> {

              if (aggKafkaConsumerService.hasConsumerAssignedFor(kafkaUrl, kafkaVersionTopic, consumingTopic, partition)) {
                aggKafkaConsumerService.unsubscribeConsumerFor(kafkaUrl, kafkaVersionTopic, consumingTopic, partition);
                aggKafkaConsumerService.subscribeConsumerFor(kafkaUrl, this, consumingTopic,
                    partition, resumedOffset);
                logger.info("Consumer service for " + kafkaUrl + " has resumed consumption to topic: " + consumingTopic
                    + ", partition: " + partition + " from offset: " + resumedOffset);
              } else {
                logger.info("Consumer service for " + kafkaUrl + " does not have consumer subscribing to topic: "
                    + consumingTopic + ", partition: " + partition + ", so we won't resume the consumption");
              }
            });
          } else {
            Collection<KafkaConsumerWrapper> dedicatedConsumers = getDedicatedConsumers();
            if (dedicatedConsumers.size() != 1) {
              throw new VeniceException("Only one dedicated consumer is expected for store version: " + kafkaVersionTopic
                  + " when manual compaction is running, but we have: " + kafkaUrlToDedicatedConsumerMap.keySet());
            }
            dedicatedConsumers.forEach(consumer -> {
              if (consumer.hasSubscription(consumingTopic, partition)) {
                consumer.unSubscribe(consumingTopic, partition);
                consumer.subscribe(consumingTopic, partition, resumedOffset);
                logger.info("Current dedicated consumer has resumed consumption to topic: " + consumingTopic + ", partition: "
                    + partition + " with offset: " + resumedOffset);
              } else {
                logger.info("Current dedicated consumer isn't subscribing to topic: " + consumingTopic + ", partition: "
                    + partition + ", so we won't resume the consumption");
              }
            });
          }

        }
      }
    }
  }

  private void reportError(Collection<PartitionConsumptionState> pcsList, int partitionId, String message,
      Exception consumerEx) {
    if (pcsList.isEmpty()) {
      reportStatusAdapter.reportError(partitionId, message, consumerEx);
    } else {
      reportStatusAdapter.reportError(pcsList, message, consumerEx);
    }
  }

  private void internalClose(boolean doFlush)  {
    // Only reset Offset Messages are important, subscribe/unSubscribe will be handled
    // on the restart by Helix Controller notifications on the new StoreIngestionTask.
    try {
      this.storeRepository.unregisterStoreDataChangedListener(this.storageUtilizationManager);
      for (ConsumerAction message : consumerActionsQueue) {
        ConsumerActionType opType = message.getType();
        if (opType == ConsumerActionType.RESET_OFFSET) {
          String topic = message.getTopic();
          int partition = message.getPartition();
          logger.info(consumerTaskId + " Cleanup Reset OffSet : Topic " + topic + " Partition Id " + partition);
          storageMetadataService.clearOffset(topic, partition);
        } else {
          logger.info(consumerTaskId + " Cleanup ignoring the Message " + message);
        }
      }
    } catch (Exception e) {
      logger.error(consumerTaskId + " Error while resetting offset.", e);
    }
    try {
      partitionConsumptionStateMap.values().parallelStream().forEach(pcs -> {
        consumerUnSubscribeAllTopics(pcs);
        pcs.unsubscribe();
      });
      partitionConsumptionStateMap.clear();
    } catch (Exception e) {
      logger.error(consumerTaskId + " Error while unsubscribing topic.", e);
    }
    try {
      closeVeniceWriters(doFlush);
    } catch (Exception e) {
      logger.error("Error while closing venice writers", e);
    }

    close();
    logger.info("Store ingestion task for store: " + kafkaVersionTopic + " is closed");
  }

  protected void closeVeniceWriters(boolean doFlush) { }

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
      try {
        logger.info("Starting consumer action " + action);
        action.incrementAttempt();
        processConsumerAction(action, store);
        consumerActionsQueue.poll();
        logger.info("Finished consumer action " + action);

      } catch (VeniceIngestionTaskKilledException | InterruptedException e) {
        throw e;

      } catch (Throwable e) {
        if (action.getAttemptsCount() <= MAX_CONSUMER_ACTION_ATTEMPTS) {
          logger.warn("Failed to process consumer action " + action + ", will retry later.", e);
          return;
        }
        logger.error("Failed to execute consumer action " + action + " after " + action.getAttemptsCount() + " attempts.", e);
        // After MAX_CONSUMER_ACTION_ATTEMPTS retries we should give up and error the ingestion task.
        PartitionConsumptionState state = partitionConsumptionStateMap.get(action.getPartition());
        consumerActionsQueue.poll();
        if (state != null && !state.isCompletionReported()) {
          reportError("Error when processing consumer action: " + action.toString(), action.getPartition(),
              (e instanceof Exception) ? (Exception) e : new VeniceException(e));
        }
      }
    }
    if (emitMetrics.get()) {
      storeIngestionStats.recordProcessConsumerActionLatency(storeName, Duration.between(startTime, Instant.now()).toMillis());
    }
  }

  private void checkConsumptionStateWhenStart(OffsetRecord offsetRecord, PartitionConsumptionState newPartitionConsumptionState) {
    int partition = newPartitionConsumptionState.getPartition();
    // Once storage node restart, send the "START" status to controller to rebuild the task status.
    // If this storage node has never consumed data from this topic, instead of sending "START" here, we send it
    // once START_OF_PUSH message has been read.
    if (offsetRecord.getLocalVersionTopicOffset() > 0) {
      Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
      if (storeVersionState.isPresent()) {
        boolean sorted = storeVersionState.get().sorted;
        /**
         * Put TopicSwitch message into in-memory state.
         */
        TopicSwitch topicSwitch = storeVersionState.get().topicSwitch;
        newPartitionConsumptionState.setTopicSwitch(topicSwitch);

        /**
         * Notify the underlying store engine about starting batch push.
         */
        beginBatchWrite(partition, sorted, newPartitionConsumptionState);

        newPartitionConsumptionState.setStartOfPushTimestamp(storeVersionState.get().startOfPushTimestamp);
        newPartitionConsumptionState.setEndOfPushTimestamp(storeVersionState.get().endOfPushTimestamp);

        reportStatusAdapter.reportRestarted(newPartitionConsumptionState);
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
      // Compare the offset lag is acceptable or not, if acceptable, report completed directly, otherwise rely on the normal
      // ready-to-server checker.
      boolean isCompletedReport = false;
      long offsetLagDeltaRelaxFactor = serverConfig.getOffsetLagDeltaRelaxFactorForFastOnlineTransitionInRestart();
      long previousOffsetLag = newPartitionConsumptionState.getOffsetRecord().getOffsetLag();
      if (hybridStoreConfig.isPresent() && newPartitionConsumptionState.isEndOfPushReceived()) {
        long offsetLagThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
        // Only enable this feature with positive offset lag delta relax factor and offset lag threshold.
        if (offsetLagDeltaRelaxEnabled && offsetLagThreshold > 0) {
          long offsetLag = measureHybridOffsetLag(newPartitionConsumptionState, true);
          if (previousOffsetLag != OffsetRecord.DEFAULT_OFFSET_LAG) {
            logger.info("Checking offset Lag behavior: current offset lag: " + offsetLag + ", previous offset lag: "
                + previousOffsetLag + ", offset lag threshold: " + offsetLagThreshold);
            if (offsetLag < previousOffsetLag + offsetLagDeltaRelaxFactor * offsetLagThreshold) {
              amplificationAdapter.lagHasCaughtUp(newPartitionConsumptionState.getUserPartition());
              reportStatusAdapter.reportCompleted(newPartitionConsumptionState, true);
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
      storeIngestionStats.recordInconsistentStoreMetadata(storeName, 1);
      // clear the local store metadata and the replica will be rebuilt from scratch upon retry as part of
      // processConsumerActions.
      storageMetadataService.clearOffset(kafkaVersionTopic, partition);
      storageMetadataService.clearStoreVersionState(kafkaVersionTopic);
      kafkaDataIntegrityValidator.clearPartition(partition);
      throw e;
    }
  }

  Set<String> getEverSubscribedTopics() {
    return everSubscribedTopics;
  }

  protected void processCommonConsumerAction(ConsumerActionType operation, String topic, int partition, LeaderFollowerStateType leaderState) throws InterruptedException {
    switch (operation) {
      case SUBSCRIBE:
        // Drain the buffered message by last subscription.
        storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);
        subscribedCount++;
        OffsetRecord offsetRecord = storageMetadataService.getLastOffset(topic, partition);

        // Initialize partition status;
        reportStatusAdapter.initializePartitionStatus(partition);
        // First let's try to restore the state retrieved from the OffsetManager
        PartitionConsumptionState newPartitionConsumptionState =
            new PartitionConsumptionState(partition, amplificationFactor, offsetRecord, hybridStoreConfig.isPresent(),
                isIncrementalPushEnabled, incrementalPushPolicy);

        newPartitionConsumptionState.setLeaderFollowerState(leaderState);

        partitionConsumptionStateMap.put(partition, newPartitionConsumptionState);
        offsetRecord.getProducerPartitionStateMap().entrySet().forEach(entry -> {
          GUID producerGuid = GuidUtils.getGuidFromCharSequence(entry.getKey());
          ProducerTracker producerTracker = kafkaDataIntegrityValidator.registerProducer(producerGuid);
          producerTracker.setPartitionState(partition, entry.getValue());
        });

        long consumptionStatePrepTimeStart = System.currentTimeMillis();
        checkConsumptionStateWhenStart(offsetRecord, newPartitionConsumptionState);
        reportIfCatchUpVersionTopicOffset(newPartitionConsumptionState);
        versionedStorageIngestionStats.recordSubscribePrepLatency(storeName, versionNumber,
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
          // Subscribe to local version topic.
          consumerSubscribe(topic, newPartitionConsumptionState.getSourceTopicPartition(topic), offsetRecord.getLocalVersionTopicOffset(), localKafkaServer);
          logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition + " Offset " + offsetRecord.getLocalVersionTopicOffset());
        }
        storageUtilizationManager.initPartition(partition);
        break;
      case UNSUBSCRIBE:
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
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
        waitForAllMessageToBeProcessedFromTopicPartition(topic, partition, consumptionState);

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
          reportStatusAdapter.reportStopped(consumptionState);
        }

        /**
         * Since the processing of the buffered messages are using {@link #partitionConsumptionStateMap} and
         * {@link #kafkaDataValidationService}, we would like to drain all the buffered messages before cleaning up those
         * two variables to avoid the race condition.
         */
        partitionConsumptionStateMap.remove(partition);
        storageUtilizationManager.removePartition(partition);
        kafkaDataIntegrityValidator.clearPartition(partition);

        // Clean up the db compaction state.
        if (disableAutoCompactionForSamzaReprocessingJob) {
          dbCompactFutureMap.remove(partition);
        }
        break;
      case RESET_OFFSET:
        /*
         * After auditing all the calls that can result in the RESET_OFFSET action, it turns out we always unsubscribe
         * from the topic/partition before resetting offset, which is unnecessary; but we decided to keep this action
         * for now in case that in future, we do want to reset the consumer without unsubscription.
         */
        PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
        if (partitionConsumptionState != null && consumerHasSubscription(topic, partitionConsumptionState)) {
          logger.error("This shouldn't happen since unsubscription should happen before reset offset for topic: " + topic + ", partition: " + partition);
          /*
           * Only update the consumer and partitionConsumptionStateMap when consumer actually has
           * subscription to this topic/partition; otherwise, we would blindly update the StateMap
           * and mess up other operations on the StateMap.
           */
          try {
            consumerResetOffset(topic, partitionConsumptionState);
            logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition);
          } catch (UnsubscribedTopicPartitionException e) {
            logger.error(consumerTaskId + " Kafka consumer should have subscribed to the partition already but it fails "
                + "on resetting offset for Topic: " + topic + " Partition Id: " + partition);
          }
          partitionConsumptionStateMap.put(partition,
              new PartitionConsumptionState(partition, amplificationFactor, new OffsetRecord(partitionStateSerializer), hybridStoreConfig.isPresent(),
                  isIncrementalPushEnabled, incrementalPushPolicy));
          storageUtilizationManager.initPartition(partition);
        } else {
          logger.info(consumerTaskId + " No need to reset offset by Kafka consumer, since the consumer is not " +
              "subscribing Topic: " + topic + " Partition Id: " + partition);
        }
        kafkaDataIntegrityValidator.clearPartition(partition);
        storageMetadataService.clearOffset(topic, partition);
        break;
      case KILL:
        logger.info("Kill this consumer task for Topic:" + topic);
        // Throw the exception here to break the consumption loop, and then this task is marked as error status.
        throw new VeniceIngestionTaskKilledException("Received the signal to kill this consumer. Topic " + topic);
      default:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + getClass().getName());
    }
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

  public Optional<PartitionConsumptionState> getPartitionConsumptionState(int partitionId) {
    return Optional.ofNullable(partitionConsumptionStateMap.get(partitionId));
  }

  public boolean hasAnyPartitionConsumptionState(Predicate<PartitionConsumptionState> pcsPredicate) {
    for (Map.Entry<Integer, PartitionConsumptionState> partitionToConsumptionState : partitionConsumptionStateMap.entrySet()) {
      if (pcsPredicate.test(partitionToConsumptionState.getValue())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Common record check for different state models:
   * check whether server continues receiving messages after EOP for a batch-only store.
   */
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record, int subPartition) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(subPartition);

    if (null == partitionConsumptionState) {
      logger.info("Topic " + kafkaVersionTopic + " Partition " + subPartition + " has been unsubscribed, skip this record that has offset " + record.offset());
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      logger.info("Topic " + kafkaVersionTopic + " Partition " + subPartition + " is already errored, skip this record that has offset " + record.offset());
      return false;
    }

    if (partitionConsumptionState.isEndOfPushReceived() && partitionConsumptionState.isBatchOnly()) {
      KafkaKey key = record.key();
      KafkaMessageEnvelope value = record.value();
      if (key.isControlMessage() &&
          ControlMessageType.valueOf((ControlMessage)value.payloadUnion) == ControlMessageType.END_OF_SEGMENT) {
        // Still allow END_OF_SEGMENT control message
        return true;
      }
      // emit metric for unexpected messages
      if (emitMetrics.get()) {
        storeIngestionStats.recordUnexpectedMessage(storeName, 1);
      }

      // Report such kind of message once per minute to reduce logging volume
      /*
       * TODO: right now, if we update a store to enable hybrid, {@link StoreIngestionTask} for the existing versions
       * won't know it since {@link #hybridStoreConfig} parameter is passed during construction.
       *
       * Same thing for {@link #isIncrementalPushEnabled}.
       *
       * So far, to make hybrid store/incremental store work, customer needs to do a new push after enabling hybrid/
       * incremental push feature of the store.
       */
      String message = "The record was received after 'EOP', but the store: " + kafkaVersionTopic +
          " is neither hybrid nor incremental push enabled, so will skip it.";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(message)) {
        logger.warn(message);
      }
      return false;
    }

    if (isIncrementalPushEnabled && incrementalPushPolicy.equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {
      KafkaMessageEnvelope value = record.value();
      /*
       * For inc push to RT, filter out the messages if the target version embedded in the message doesn't match the version
       * of this ingestion task.
       */
      if (value.targetVersion > 0 && value.targetVersion != this.versionNumber) {
        return false;
      }
    }
    return true;
  }

  protected boolean shouldPersistRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record,
      PartitionConsumptionState partitionConsumptionState) {
    int partitionId = record.partition();
    String msg;
    if (errorPartitions.contains(partitionId)) {
      msg = "Errors already exist in partition: " + record.partition() + " for resource: " + kafkaVersionTopic
          + ", skipping this record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        logger.info(msg + "that has offset " + record.offset());
      }
      return false;
    }
    if (null == partitionConsumptionState || !partitionConsumptionState.isSubscribed()) {
      msg = "Topic " + kafkaVersionTopic + " Partition " + partitionId + " has been unsubscribed, skip this record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        logger.info(msg + " that has offset " + record.offset());
      }
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      msg = "Topic " + kafkaVersionTopic + " Partition " + partitionId + " is already errored, skip this record";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        logger.info(msg + " that has offset " + record.offset());
      }
      return false;
    }

    if (this.suppressLiveUpdates && partitionConsumptionState.isCompletionReported()) {
      msg = "Skipping message as live update suppression is enabled and store: " + kafkaVersionTopic
          + " partition " + partitionId + " is already ready to serve, these are buffered records in the queue.";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        logger.info(msg);
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
      msg = "Skipping message as it is using ingestion isolation and store: " + kafkaVersionTopic
          + " partition " + partitionId + " is already ready to serve, these are buffered records in the queue.";
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
        logger.info(msg);
      }
      return false;
    }

    return true;
  }

  /**
   * This function will be invoked in {@link StoreBufferService} to process buffered {@link ConsumerRecord}.
   * @param record
   * @param kafkaUrl
   */
  public void processConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record,
      LeaderProducedRecordContext leaderProducedRecordContext,
      String kafkaUrl) throws InterruptedException {
    int subPartition = PartitionUtils.getSubPartition(record.topic(), record.partition(), amplificationFactor);
    // The partitionConsumptionStateMap can be modified by other threads during consumption (for example when unsubscribing)
    // in order to maintain thread safety, we hold onto the reference to the partitionConsumptionState and pass that
    // reference to all downstream methods so that all offset persistence operations use the same partitionConsumptionState
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
          subPartition,
          kafkaUrl);
    } catch (FatalDataValidationException e) {
      int faultyPartition = record.partition();
      String errorMessage;
      if (amplificationFactor != 1 && Version.isRealTimeTopic(record.topic())) {
        errorMessage = "Fatal data validation problem with in RT topic partition " + faultyPartition
            + ", offset " + record.offset() + ", leaderSubPartition: " + subPartition;
      } else {
        errorMessage = "Fatal data validation problem with partition " + faultyPartition + ", offset " + record.offset();
      }
      // TODO need a way to safeguard DIV errors from backup version that have once been current (but not anymore) during re-balancing
      boolean needToUnsub = !(isCurrentVersion.getAsBoolean() || partitionConsumptionState.isEndOfPushReceived());
      if (needToUnsub) {
        errorMessage +=  ". Consumption will be halted.";
        reportStatusAdapter.reportError(Collections.singletonList(partitionConsumptionState), errorMessage, e);
        unSubscribePartition(kafkaVersionTopic, faultyPartition);
      } else {
        logger.warn(errorMessage + ". However, " + kafkaVersionTopic
            + " is the current version or EOP is already received so consumption will continue." + e.getMessage());
      }
    } catch (VeniceMessageException | UnsupportedOperationException e) {
      throw new VeniceException(consumerTaskId + " : Received an exception for message at partition: "
          + record.partition() + ", offset: " + record.offset() + ". Bubbling up.", e);
    }

    partitionConsumptionState.incrementProcessedRecordNum();
    partitionConsumptionState.incrementProcessedRecordSize(recordSize);

    if (diskUsage.isDiskFull(recordSize)) {
      throw new VeniceException(
          "Disk is full: throwing exception to error push: " + storeName + " version " + versionNumber + ". " + diskUsage.getDiskStatus());
    }

    int processedRecordNum = partitionConsumptionState.getProcessedRecordNum();
    if (processedRecordNum >= OFFSET_REPORTING_INTERVAL ||
        partitionConsumptionState.isEndOfPushReceived()) {
      int processedRecordSize = partitionConsumptionState.getProcessedRecordSize();
      /*
       * Report ingestion throughput metric based on the store version
       */
      versionedStorageIngestionStats.recordBytesConsumed(storeName, versionNumber, processedRecordSize);
      versionedStorageIngestionStats.recordRecordsConsumed(storeName, versionNumber, processedRecordNum);

      /*
       * Meanwhile, contribute to the host-level ingestion throughput rate, which aggregates the consumption rate across
       * all store versions.
       */
      storeIngestionStats.recordTotalBytesConsumed(storeName, processedRecordSize);
      storeIngestionStats.recordTotalRecordsConsumed(storeName, processedRecordNum);

      /*
       * Also update this stats separately for Leader and Follower.
       */
      recordProcessedRecordStats(partitionConsumptionState, processedRecordSize, processedRecordNum);
      partitionConsumptionState.resetProcessedRecordNum();
      partitionConsumptionState.resetProcessedRecordSize();
    }

    reportIfCatchUpVersionTopicOffset(partitionConsumptionState);

    partitionConsumptionState.incrementProcessedRecordSizeSinceLastSync(recordSize);
    long syncBytesInterval = partitionConsumptionState.isDeferredWrite() ? databaseSyncBytesIntervalForDeferredWriteMode
        : databaseSyncBytesIntervalForTransactionalMode;
    boolean recordsProcessedAboveSyncIntervalThreshold =
        (syncBytesInterval > 0 && (partitionConsumptionState.getProcessedRecordSizeSinceLastSync() >= syncBytesInterval));
    defaultReadyToServeChecker.apply(partitionConsumptionState, recordsProcessedAboveSyncIntervalThreshold);

    /**
     * Syncing offset checking in syncOffset() should be the very last step for processing a record.
     *
     * Check whether offset metadata checkpoint will happen; if so, update the producer states recorded in OffsetRecord
     * with the updated producer states maintained in {@link #kafkaDataIntegrityValidator}
     */
    boolean syncOffset = shouldSyncOffset(partitionConsumptionState, syncBytesInterval, record, leaderProducedRecordContext);

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
      this.kafkaDataIntegrityValidator.updateOffsetRecord(subPartition, offsetRecord);
      // Potentially update the offset metadata in the OffsetRecord
      updateOffsetMetadataInOffsetRecord(partitionConsumptionState, offsetRecord, record, leaderProducedRecordContext, kafkaUrl);
      syncOffset(kafkaVersionTopic, partitionConsumptionState);
    }
  }

  /**
   * Retrieve current LeaderFollowerState from partition's PCS. This method is used by IsolatedIngestionServer to sync
   * user-partition LeaderFollower status from child process to parent process in ingestion isolation.
   */
  public LeaderFollowerStateType getLeaderState(int partition) {
    return amplificationAdapter.getLeaderState(partition);
  }

  /**
   * Refer to the JavaDoc of {@link #updateOffsetMetadataInOffsetRecord(PartitionConsumptionState, OffsetRecord, ConsumerRecord, LeaderProducedRecordContext, String)}
   * for the criteria of when to sync offset.
   */
  private boolean shouldSyncOffset(PartitionConsumptionState pcs, long syncBytesInterval,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record, LeaderProducedRecordContext leaderProducedRecordContext) {
    boolean syncOffset = false;
    if (record.key().isControlMessage()) {
      ControlMessage controlMessage = (leaderProducedRecordContext == null
          ? (ControlMessage) record.value().payloadUnion
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
      if (controlMessageType != ControlMessageType.START_OF_SEGMENT &&
          controlMessageType != ControlMessageType.END_OF_SEGMENT) {
        syncOffset = true;
      }
    } else {
      syncOffset = (syncBytesInterval > 0 && (pcs.getProcessedRecordSizeSinceLastSync() >= syncBytesInterval));
    }
    return syncOffset;
  }

  private void syncOffset(String topic, PartitionConsumptionState pcs) {
    int partition = pcs.getPartition();
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(topic);
    if (storageEngine == null) {
      logger.warn("Storage engine has been removed. Could not execute sync offset for topic: " + topic + " and partition: " + partition);
      return;
    }
    // Flush data partition
    Map<String, String> dbCheckpointingInfo = storageEngine.sync(partition);

    // Update the partition key in metadata partition
    if (offsetLagDeltaRelaxEnabled) {
      // Try to persist offset lag to make partition online faster when restart.
      updateOffsetLagInMetadata(pcs);
    }
    OffsetRecord offsetRecord = pcs.getOffsetRecord();
    // Check-pointing info required by the underlying storage engine
    offsetRecord.setDatabaseInfo(dbCheckpointingInfo);
    storageMetadataService.put(this.kafkaVersionTopic, partition, offsetRecord);
    pcs.resetProcessedRecordSizeSinceLastSync();
    String msg = "Offset synced for partition " + partition + " of topic " + topic + ": ";
    if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
      logger.info(msg + offsetRecord.getLocalVersionTopicOffset());
    }
  }

  private void updateOffsetLagInMetadata(PartitionConsumptionState ps) {
    // Measure and save real-time offset lag.
    long offsetLag = measureHybridOffsetLag(ps, true);
    ps.getOffsetRecord().setOffsetLag(offsetLag);
  }

  private void offerExceptionToQueue(Exception e, int partitionId,
      BlockingQueue<PartitionExceptionInfo> exceptionQueue) {
    try {
      errorPartitions.add(partitionId);
      exceptionQueue.put(new PartitionExceptionInfo(e, partitionId));
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while trying to offer exception to queue for partition id: "
          + partitionId);
    }
  }

  public void offerDrainerException(Exception e, int partitionId) {
    offerExceptionToQueue(e, partitionId, drainerExceptions);
  }

  public void offerProducerException(Exception e, int partitionId) {
    offerExceptionToQueue(e, partitionId, producerExceptions);
  }

  public void offerConsumerException(Exception e, int partitionId) {
    offerExceptionToQueue(e, partitionId, consumerExceptions);
  }

  public void setLastConsumerException(Exception e) {
    lastConsumerException = e;
  }

  public void setLastStoreIngestionException(Exception e) {
    lastStoreIngestionException.set(e);
  }

  public void recordChecksumVerificationFailure() {
    storeIngestionStats.recordChecksumVerificationFailure(storeName);
  }

  public abstract long getBatchReplicationLag();

  public abstract long getLeaderOffsetLag();

  public abstract long getBatchLeaderOffsetLag();

  public abstract long getHybridLeaderOffsetLag();

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
      logger.debug("Got a negative value for a lag metric. Will report zero.");
      return 0;
    } else {
      return value;
    }
  }

  public boolean isHybridMode() {
    return hybridStoreConfig.isPresent();
  }

  private void syncEndOfPushTimestampToMetadataService(long endOfPushTimestamp) {
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (storeVersionState.isPresent()) {
      storeVersionState.get().endOfPushTimestamp = endOfPushTimestamp;

      // Sync latest store version level metadata to disk
      storageMetadataService.put(kafkaVersionTopic, storeVersionState.get());
    } else {
      throw new VeniceException("Unexpected: received some " + ControlMessageType.END_OF_PUSH.name() +
          " control message in a topic where we have not yet received a " +
          ControlMessageType.START_OF_PUSH.name() + " control message.");
    }
  }

  private void processStartOfPush(KafkaMessageEnvelope startOfPushKME, ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {
    StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
    /*
     * Notify the underlying store engine about starting batch push.
     */
    beginBatchWrite(partition, startOfPush.sorted, partitionConsumptionState);
    partitionConsumptionState.setStartOfPushTimestamp(startOfPushKME.producerMetadata.messageTimestamp);

    reportStatusAdapter.reportStarted(partitionConsumptionState);
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!storeVersionState.isPresent()) {
      // No other partition of the same topic has started yet, let's initialize the StoreVersionState
      StoreVersionState newStoreVersionState = new StoreVersionState();
      newStoreVersionState.sorted = startOfPush.sorted;
      newStoreVersionState.chunked = startOfPush.chunked;
      newStoreVersionState.compressionStrategy = startOfPush.compressionStrategy;
      newStoreVersionState.compressionDictionary = startOfPush.compressionDictionary;
      newStoreVersionState.batchConflictResolutionPolicy = startOfPush.timestampPolicy;
      newStoreVersionState.startOfPushTimestamp = startOfPushKME.producerMetadata.messageTimestamp;

      storageMetadataService.put(kafkaVersionTopic, newStoreVersionState);
    } else if (storeVersionState.get().sorted != startOfPush.sorted) {
      // Something very wrong is going on ): ...
      throw new VeniceException("Unexpected: received multiple " + ControlMessageType.START_OF_PUSH.name() +
          " control messages with inconsistent 'sorted' fields within the same topic!");
    } else if (storeVersionState.get().chunked != startOfPush.chunked) {
      // Something very wrong is going on ): ...
      throw new VeniceException("Unexpected: received multiple " + ControlMessageType.START_OF_PUSH.name() +
          " control messages with inconsistent 'chunked' fields within the same topic!");
    } // else, no need to persist it once more.
  }

  protected void processEndOfPush(KafkaMessageEnvelope endOfPushKME, ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {

    // Do not process duplication EOP messages.
    if (partitionConsumptionState.getOffsetRecord().isEndOfPushReceived()) {
      logger.warn(consumerTaskId + " Received duplicate EOP control message, ignoring it. Partition: " + partition + ", Offset: " + offset);
      return;
    }

    // We need to keep track of when the EOP happened, as that is used within Hybrid Stores' lag measurement
    partitionConsumptionState.getOffsetRecord().endOfPushReceived(offset);
    /*
     * Right now, we assume there are no sorted message after EndOfPush control message.
     * TODO: if this behavior changes in the future, the logic needs to be adjusted as well.
     */
    StoragePartitionConfig storagePartitionConfig = getStoragePartitionConfig(partition, false, partitionConsumptionState);

    /**
     * Update the transactional/deferred mode of the partition.
     */
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());

    /**
     * Indicate the batch push is done, and the internal storage engine needs to do some cleanup.
     */
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
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

    //persist the EOP message producer's timestamp.
    partitionConsumptionState.setEndOfPushTimestamp(endOfPushKME.producerMetadata.messageTimestamp);
    syncEndOfPushTimestampToMetadataService(endOfPushKME.producerMetadata.messageTimestamp);

    /**
     * It's a bit of tricky here. Since the offset is not updated yet, it's actually previous offset reported
     * here.
     * TODO: sync up offset before invoking dispatcher
     */
    reportStatusAdapter.reportEndOfPushReceived(partitionConsumptionState);

    if (isDataRecovery && partitionConsumptionState.isBatchOnly()) {
      partitionConsumptionState.setDataRecoveryCompleted(true);
      reportStatusAdapter.reportDataRecoveryCompleted(partitionConsumptionState);
    }
  }

  protected void processStartOfIncrementalPush(
      ControlMessage startOfIncrementalPush, PartitionConsumptionState partitionConsumptionState) {
    CharSequence startVersion = ((StartOfIncrementalPush) startOfIncrementalPush.controlMessageUnion).version;
    IncrementalPush newIncrementalPush = new IncrementalPush();
    newIncrementalPush.version = startVersion;
    partitionConsumptionState.setIncrementalPush(newIncrementalPush);
    reportStatusAdapter.reportStartOfIncrementalPushReceived(partitionConsumptionState, startVersion.toString());
  }

  protected void processEndOfIncrementalPush(ControlMessage endOfIncrementalPush,
      PartitionConsumptionState partitionConsumptionState) {
    // TODO: it is possible that we could turn incremental store to be read-only when incremental push is done
    CharSequence endVersion = ((EndOfIncrementalPush) endOfIncrementalPush.controlMessageUnion).version;
    // Reset incremental push version
    partitionConsumptionState.setIncrementalPush(null);
    reportStatusAdapter.reportEndOfIncrementalPushReceived(partitionConsumptionState, endVersion.toString());
  }

  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {
    throw new VeniceException(ControlMessageType.TOPIC_SWITCH.name() + " control message should not be received in"
        + "Online/Offline state model. Topic " + kafkaVersionTopic + " partition " + partition);
  }

  /**
   * In this method, we pass both offset and partitionConsumptionState(ps). The reason behind it is that ps's
   * offset is stale and is not updated until the very end
   */
  private void processControlMessage(KafkaMessageEnvelope kafkaMessageEnvelope, ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {
    /**
     * If leader consumes control messages from topics other than version topic, it should produce
     * them to version topic; however, START_OF_SEGMENT and END_OF_SEGMENT should not be forwarded
     * because the leader producer will keep track of its own segment state; besides, leader/follower
     * model will not encounter START_OF_BUFFER_REPLAY because TOPIC_SWITCH will replace it in L/F
     * model; incremental push is also a mutually exclusive feature with hybrid stores.
     */
    final ControlMessageType type = ControlMessageType.valueOf(controlMessage);
    if (!isSegmentControlMsg(type)) {
      logger.info(consumerTaskId + " : Received " + type.name() + " control message. Partition: " + partition + ", Offset: " + offset);
    }
    switch(type) {
      case START_OF_PUSH:
        processStartOfPush(kafkaMessageEnvelope, controlMessage, partition, offset, partitionConsumptionState);
        break;
      case END_OF_PUSH:
        processEndOfPush(kafkaMessageEnvelope, controlMessage, partition, offset, partitionConsumptionState);
        break;
      case START_OF_SEGMENT:
      case END_OF_SEGMENT:
        /**
         * Nothing to do here as all of the processing is being done in {@link StoreIngestionTask#delegateConsumerRecord(ConsumerRecord, int, String)}.
         */
        break;
      case START_OF_BUFFER_REPLAY:
        throw new UnsupportedMessageTypeException(type + " is a legacy mechanism that should never happen anymore.");
      case START_OF_INCREMENTAL_PUSH:
        processStartOfIncrementalPush(controlMessage, partitionConsumptionState);
        break;
      case END_OF_INCREMENTAL_PUSH:
        processEndOfIncrementalPush(controlMessage, partitionConsumptionState);
        break;
      case TOPIC_SWITCH:
        processTopicSwitch(controlMessage, partition, offset, partitionConsumptionState);
        break;
      default:
        throw new UnsupportedMessageTypeException("Unrecognized Control message type " + controlMessage.controlMessageType);
    }
  }

  /**
   * Update the offset metadata in OffsetRecord in two cases:
   * 1. A ControlMessage other than Start_of_Segment and End_of_Segment is processed
   * 2. The size of total processed message has exceeded a threshold: {@link #databaseSyncBytesIntervalForDeferredWriteMode}
   *    for batch data and {@link #databaseSyncBytesIntervalForTransactionalMode} for real-time updates
   */
  protected abstract void updateOffsetMetadataInOffsetRecord(PartitionConsumptionState partitionConsumptionState,
      OffsetRecord offsetRecord, ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
      LeaderProducedRecordContext leaderProducedRecordContext, String kafkaUrl);

  /**
   * Maintain the latest processed offsets by drainers in memory; in most of the time, these offsets are ahead of the
   * checkpoint offsets inside {@link OffsetRecord}
   */
  protected abstract void updateLatestInMemoryProcessedOffset(PartitionConsumptionState partitionConsumptionState,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
      LeaderProducedRecordContext leaderProducedRecordContext, String kafkaUrl);

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param consumerRecord {@link ConsumerRecord} consumed from Kafka.
   * @param kafkaUrl
   * @return the size of the data written to persistent storage.
   */
  private int internalProcessConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      PartitionConsumptionState partitionConsumptionState,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int sizeOfPersistedData = 0;
    try {
      // Assumes the timestamp on the ConsumerRecord is the broker's timestamp when it received the message.
      long consumerTimestampMs = System.currentTimeMillis();
      long producerBrokerLatencyMs = Math.max(consumerRecord.timestamp() - kafkaValue.producerMetadata.messageTimestamp, 0);
      long brokerConsumerLatencyMs = Math.max(consumerTimestampMs - consumerRecord.timestamp(), 0);
      long producerConsumerLatencyMs = Math.max(consumerTimestampMs - kafkaValue.producerMetadata.messageTimestamp, 0);
      recordWriterStats(
          producerBrokerLatencyMs, brokerConsumerLatencyMs, producerConsumerLatencyMs, partitionConsumptionState);
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
        validateMessage(this.kafkaDataIntegrityValidator, consumerRecord, endOfPushReceived, subPartition);
        versionedDIVStats.recordSuccessMsg(storeName, versionNumber);
      } catch (FatalDataValidationException fatalException) {
        if (!endOfPushReceived) {
          throw fatalException;
        } else {
          String errorMessage = String.format("Encountered errors during updating metadata for 2nd round DIV validation "
                  + "after EOP. topic %s partition %s offset %s, ", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
          logger.warn(errorMessage + fatalException.getMessage());
        }
      }
      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (leaderProducedRecordContext == null ? (ControlMessage) kafkaValue.payloadUnion : (ControlMessage) leaderProducedRecordContext.getValueUnion());
        processControlMessage(kafkaValue, controlMessage, consumerRecord.partition(), consumerRecord.offset(), partitionConsumptionState);
      } else {
        sizeOfPersistedData = processKafkaDataMessage(consumerRecord, partitionConsumptionState, leaderProducedRecordContext);
      }
    } catch (DuplicateDataException e) {
      divErrorMetricCallback.execute(e);
      if (logger.isDebugEnabled()) {
        logger.debug(consumerTaskId + " : Skipping a duplicate record at offset: " + consumerRecord.offset());
      }
    } catch (PersistenceFailureException ex) {
      if (partitionConsumptionStateMap.containsKey(consumerRecord.partition())) {
        // If we actually intend to be consuming this partition, then we need to bubble up the failure to persist.
        logger.error("Met PersistenceFailureException while processing record with offset: " + consumerRecord.offset() +
        ", topic: " + kafkaVersionTopic + ", meta data of the record: " + consumerRecord.value().producerMetadata);
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
    if (null == partitionConsumptionState) {
      logger.info("Topic " + kafkaVersionTopic + " Partition " + consumerRecord.partition() + " has been unsubscribed, will skip offset update");
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
      updateLatestInMemoryProcessedOffset(partitionConsumptionState, consumerRecord, leaderProducedRecordContext, kafkaUrl);
    }
    return sizeOfPersistedData;
  }

  protected void recordWriterStats(
      long producerBrokerLatencyMs,
      long brokerConsumerLatencyMs,
      long producerConsumerLatencyMs,
      PartitionConsumptionState partitionConsumptionState) {
    versionedDIVStats.recordLatencies(storeName, versionNumber, producerBrokerLatencyMs, brokerConsumerLatencyMs, producerConsumerLatencyMs);
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
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      boolean endOfPushReceived,
      int subPartition) {

    TopicManager topicManager = topicManagerRepository.getTopicManager();
    // Tolerate missing message if store version is data recovery + hybrid and TS not received yet (due to source topic
    // data may have been log compacted) or log compaction is enabled and record is old enough for log compaction.
    boolean tolerateMissingMsgs =
        (isDataRecovery && isHybridMode() && partitionConsumptionStateMap.get(subPartition).getTopicSwitch() == null) ||
        (topicManager.isTopicCompactionEnabled(consumerRecord.topic()) &&
        LatencyUtils.getElapsedTimeInMs(consumerRecord.timestamp()) >= topicManager.getTopicMinLogCompactionLagMs(consumerRecord.topic()));

    long startTimeForMessageValidationInNS = System.nanoTime();
    try {
      validator.validateMessage(consumerRecord, endOfPushReceived, tolerateMissingMsgs);
      return;
    } catch (FatalDataValidationException fatalException) {
      divErrorMetricCallback.execute(fatalException);
      /**
       * If DIV errors happens after EOP is received, we will not error out the replica.
       */
      if (!endOfPushReceived) {
        throw fatalException;
      }

      FatalDataValidationException warningException = fatalException;
      String warningMessage = String.format("Data integrity validation problem with topic %s partition %s offset %s, "
          + "but consumption will continue since EOP is already received. ",
          consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
      // TODO: remove this condition check after fixing the bug that drainer in leaders is validating RT DIV info
      if (consumerRecord.value().producerMetadata.messageSequenceNumber != 1) {
        logger.warn(warningMessage + warningException.getMessage());
      }

      if (!(warningException instanceof ImproperlyStartedSegmentException)) {
        /**
         * Run a dummy validation to update DIV metadata.
         */
        validator.validateMessage(consumerRecord, endOfPushReceived, true);
      }
    } finally {
      versionedDIVStats.recordDataValidationLatencyMs(storeName, versionNumber, LatencyUtils.getLatencyInMS(startTimeForMessageValidationInNS));
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
  private void prependHeaderAndWriteToStorageEngine(String topic, int partition, byte[] keyBytes, Put put) {
    ByteBuffer putValue = put.putValue;
    /*
     * Since {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer} reuses the original byte
     * array, which is big enough to pre-append schema id, so we just reuse it to avoid unnecessary byte array allocation.
     * This value encoding scheme is used in {@link PartitionConsumptionState#maybeUpdateExpectedChecksum(byte[], Put)} to
     * calculate checksum for all the kafka PUT messages seen so far. Any change here needs to be reflected in that function.
     */
    if (putValue.position() < ValueRecord.SCHEMA_HEADER_LENGTH) {
      throw new VeniceException("Start position of 'putValue' ByteBuffer shouldn't be less than " + ValueRecord.SCHEMA_HEADER_LENGTH);
    } else {
      // Back up the original 4 bytes
      putValue.position(putValue.position() - ValueRecord.SCHEMA_HEADER_LENGTH);
      int backupBytes = putValue.getInt();
      putValue.position(putValue.position() - ValueRecord.SCHEMA_HEADER_LENGTH);
      ByteUtils.writeInt(putValue.array(), put.schemaId, putValue.position());

      writeToStorageEngine(storageEngineRepository.getLocalStorageEngine(topic), partition, keyBytes, put);

      /* We still want to recover the original position to make this function idempotent. */
      putValue.putInt(backupBytes);
    }
  }

  private void writeToStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes, Put put) {
    long putStartTimeNs = System.nanoTime();
    putInStorageEngine(storageEngine, partition, keyBytes, put);
    if (cacheBackend.isPresent()) {
      if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
        cacheBackend.get().getStorageEngine(kafkaVersionTopic).put(partition, keyBytes, put.putValue);
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace(consumerTaskId + " : Completed PUT to Store: " + kafkaVersionTopic + " in " +
          (System.nanoTime() - putStartTimeNs) + " ns at " + System.currentTimeMillis());
    }
    if (emitMetrics.get()) {
      storeIngestionStats.recordStorageEnginePutLatency(storeName, LatencyUtils.getLatencyInMS(putStartTimeNs));
    }
  }

  /**
   * Persist Put record to storage engine.
   */
  protected void putInStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes, Put put) {
    storageEngine.put(partition, keyBytes, put.putValue);
  }

  protected void removeFromStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes,
      Delete delete) {
    storageEngine.delete(partition, keyBytes);
  }

  /**
   * All the dedicated consumer subscriptions belonging to the same {@link StoreIngestionTask} SHOULD use this function
   * instead of {@link KafkaConsumerWrapper#subscribe} directly since we need to let {@link KafkaConsumerService} knows
   * which {@link StoreIngestionTask} to use when receiving result for any specific topic.
   * @param topic
   * @param partition
   */
  protected synchronized void dedicatedConsumerSubscribe(KafkaConsumerWrapper consumer, String topic, int partition, long offset) {
    consumer.subscribe(topic, partition, offset);
  }

  public boolean consumerHasAnySubscription() {
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      return aggKafkaConsumerService.hasAnyConsumerAssignedFor(getVersionTopic(), everSubscribedTopics);
    } else {
      return kafkaUrlToDedicatedConsumerMap.values().stream().anyMatch(consumer -> consumer.hasSubscribedAnyTopic(everSubscribedTopics));
    }
  }

  public boolean consumerHasSubscription(String topic, PartitionConsumptionState partitionConsumptionState) {

    int partitionId = partitionConsumptionState.getSourceTopicPartition(topic);

    if (serverConfig.isSharedConsumerPoolEnabled()) {
      return aggKafkaConsumerService.hasConsumerAssignedFor(getVersionTopic(), topic, partitionId);
    } else {
      for (KafkaConsumerWrapper consumer : kafkaUrlToDedicatedConsumerMap.values()) {
        if (consumer.hasSubscription(topic, partitionId)) {
          return true;
        }
      }
    }

    return false;
  }

  public void consumerUnSubscribe(String topic, PartitionConsumptionState partitionConsumptionState) {

    int partitionId = partitionConsumptionState.getPartition();

    if (serverConfig.isSharedConsumerPoolEnabled()) {
      aggKafkaConsumerService.unsubscribeConsumerFor(getVersionTopic(), topic, partitionId);
    } else {
      Instant startTime = Instant.now();
      kafkaUrlToDedicatedConsumerMap.forEach((kafkaURL, consumer) -> {
        consumer.unSubscribe(topic, partitionConsumptionState.getSourceTopicPartition(topic));
        logger.info(String.format("Consumer unsubscribed topic %s partition %d at %s. Took %d ms",
            topic, partitionId, kafkaURL, Instant.now().toEpochMilli() - startTime.toEpochMilli()));
      });
    }
  }

  public void consumerBatchUnsubscribe(Set<TopicPartition> topicPartitionSet) {
    Instant startTime = Instant.now();
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      aggKafkaConsumerService.batchUnsubscribeConsumerFor(getVersionTopic(), topicPartitionSet);
    } else {
      getDedicatedConsumers().forEach(consumer -> consumer.batchUnsubscribe(topicPartitionSet));
    }
    logger.info(String.format("Consumer unsubscribed %d partitions. Took %d ms", topicPartitionSet.size(),
        Instant.now().toEpochMilli() - startTime.toEpochMilli()));
  }

  public abstract void consumerUnSubscribeAllTopics(PartitionConsumptionState partitionConsumptionState);

  public void consumerSubscribe(String topic, int partition, long startOffset, String kafkaURL) {

    final boolean consumeRemotely = !Objects.equals(kafkaURL, localKafkaServer);
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      // TODO: Move remote KafkaConsumerService creating operations into the aggKafkaConsumerService.
      aggKafkaConsumerService.createKafkaConsumerService(createKafkaConsumerProperties(kafkaProps, kafkaURL, consumeRemotely));
      aggKafkaConsumerService.subscribeConsumerFor(kafkaURL, this, topic, partition, startOffset);
    } else {
      dedicatedConsumerSubscribe(getDedicatedConsumer(kafkaURL, consumeRemotely), topic, partition, startOffset);
    }

    everSubscribedTopics.add(topic);
  }

  public void consumerResetOffset(String topic, PartitionConsumptionState partitionConsumptionState) {
    int partitionId = partitionConsumptionState.getSourceTopicPartition(topic);
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      aggKafkaConsumerService.resetOffsetFor(getVersionTopic(), topic, partitionId);
    } else {
      getDedicatedConsumers().forEach(consumer -> consumer.resetOffset(topic, partitionId));
    }
  }

  public void pauseConsumption(String topic, int  partitionId) {
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      aggKafkaConsumerService.pauseConsumerFor(getVersionTopic(), topic, partitionId);
    } else {
      getDedicatedConsumers().forEach(consumer -> consumer.pause(topic, partitionId));
    }
  }

  public void resumeConsumption(String topic, int  partitionId) {
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      aggKafkaConsumerService.resumeConsumerFor(getVersionTopic(), topic, partitionId);
    } else {
      getDedicatedConsumers().forEach(consumer -> consumer.resume(topic, partitionId));
    }
  }


  /**
   * Poll dedicated consumer(s) to get a map from Kafka URLs to consumed records from that Kafka URL.
   *
   * This method is only called for dedicated Kafka consumer
   * @param pollTimeoutMs
   * @return
   */
  protected Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> dedicatedConsumerPoll(long pollTimeoutMs) {
    if (!storeRepository.getStoreOrThrow(storeName).getVersion(versionNumber).isPresent()) {
      //If the version is not found, it must have been retired/deleted by controller, Let's kill this ingestion task.
      throw new VeniceIngestionTaskKilledException("Version " + versionNumber + " does not exist. Should not poll.");
    }
    /**
     * Since the number of entries in the consumerMap is practically always much smaller than the default map size (16),
     * creating a map with a size of the key set size of consumerMap is more memory efficient.
     */
    final Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> recordsByKafkaURLs = new HashMap<>(
        kafkaUrlToDedicatedConsumerMap.keySet().size());
    kafkaUrlToDedicatedConsumerMap.forEach((kafkaURL, consumer) -> {
      if (consumer.hasAnySubscription()) {
        ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords;

        if (serverConfig.isLiveConfigBasedKafkaThrottlingEnabled()) {
          consumerRecords = kafkaClusterBasedRecordThrottler.poll(consumer, kafkaURL, pollTimeoutMs);
        } else {
          consumerRecords = consumer.poll(pollTimeoutMs);
        }

        if (!consumerRecords.isEmpty()) {
          recordsByKafkaURLs.put(kafkaURL, consumerRecords);
        }
      }
    });
    return recordsByKafkaURLs;
  }

  private KafkaConsumerWrapper getDedicatedConsumer(String kafkaURL, boolean consumeRemotely) {
    KafkaConsumerWrapper consumer = kafkaUrlToDedicatedConsumerMap.computeIfAbsent(kafkaURL, source -> {
      Properties consumerProps = createKafkaConsumerProperties(kafkaProps, kafkaURL, consumeRemotely);
      return kafkaClientFactory.getConsumer(consumerProps);
    });
    return consumer;
  }

  /**
   * @return the size of the data which was written to persistent storage.
   */
  private int processKafkaDataMessage(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      PartitionConsumptionState partitionConsumptionState, LeaderProducedRecordContext leaderProducedRecordContext
  ) {
    int keyLen = 0;
    int valueLen = 0;
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int consumedPartition = consumerRecord.partition(); // partition in consumerRecord
    // partition being produced by VeniceWriter
    // when amplificationFactor != 1, consumedPartition might not equal to producedPartition since
    // RT has different #partitions compared to VTs and StorageEngine
    // when writing to local StorageEngine, use producedPartition.
    // #subPartitionins in StorageEngine should be consistent with #subPartitionins in VTs.
    int producedPartition = partitionConsumptionState.getPartition();
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
    byte[] keyBytes;

    MessageType messageType =
        (leaderProducedRecordContext == null ? MessageType.valueOf(kafkaValue) : leaderProducedRecordContext.getMessageType());

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

        //update checksum for this PUT message if needed.
        partitionConsumptionState.maybeUpdateExpectedChecksum(keyBytes, put);
        prependHeaderAndWriteToStorageEngine(kafkaVersionTopic,
            // Leaders might consume from a RT topic and immediately write into StorageEngine,
            // so we need to re-calculate partition.
            // Followers are not affected since they are always consuming from VTs.
            producedPartition, keyBytes, put);
        // grab the positive schema id (actual value schema id) to be used in schema warm-up value schema id.
        // for hybrid use case in read compute store in future we need revisit this as we can have multiple schemas.
        if (put.schemaId > 0) {
          valueSchemaId = put.schemaId;
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

        long deleteStartTimeNs = System.nanoTime();

        removeFromStorageEngine(storageEngine, producedPartition, keyBytes, delete);
        if (cacheBackend.isPresent()) {
          if (cacheBackend.get().getStorageEngine(kafkaVersionTopic) != null) {
            cacheBackend.get().getStorageEngine(kafkaVersionTopic).delete(producedPartition, keyBytes);
          }
        }

        if (logger.isTraceEnabled()) {
          logger.trace(
              consumerTaskId + " : Completed DELETE to Store: " + kafkaVersionTopic + " in " + (System.nanoTime()
                  - deleteStartTimeNs) + " ns at " + System.currentTimeMillis());
        }
        break;

      case UPDATE:
        throw new VeniceMessageException(
            consumerTaskId + ": Not expecting UPDATE message from  Topic: " + consumerRecord.topic() + " Partition: "
                + consumedPartition + ", Offset: " + consumerRecord.offset());
      default:
        throw new VeniceMessageException(
            consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    /*
     * Potentially clean the mapping from transient record map. consumedOffset may be -1 when individual chunks are getting
     * produced to drainer queue from kafka callback thread {@link LeaderFollowerStoreIngestionTask#LeaderProducerMessageCallback}
     */
    //This flag is introduced to help write integration test to exercise code path during UPDATE message processing in
    //{@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}. This must be always set to true except
    //as needed in integration test.
    if (purgeTransientRecordBuffer && isTransientRecordBufferUsed() && partitionConsumptionState.isEndOfPushReceived()
        && leaderProducedRecordContext != null && leaderProducedRecordContext.getConsumedOffset() != -1) {
      PartitionConsumptionState.TransientRecord transientRecord =
          partitionConsumptionState.mayRemoveTransientRecord(leaderProducedRecordContext.getConsumedKafkaUrl(), leaderProducedRecordContext.getConsumedOffset(), kafkaKey.getKey());
      if (transientRecord != null) {
        //This is perfectly fine, logging to see how often it happens where we get multiple put/update/delete for same key in quick succession.
        String msg =
            consumerTaskId + ": multiple put,update,delete for same key received from Topic: " + consumerRecord.topic()
                + " Partition: " + consumedPartition;
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
          logger.info(msg);
        }
      }
    }

    if (emitMetrics.get()) {
      storeIngestionStats.recordKeySize(storeName, keyLen);
      storeIngestionStats.recordValueSize(storeName, valueLen);
    }

    return keyLen + valueLen;
  }

  /**
   * This method checks whether the given record needs to be checked schema availability. Only PUT and UPDATE message
   * needs to #checkValueSchemaAvail
   * @param record
   */
  private void waitReadyToProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) throws InterruptedException {
    KafkaMessageEnvelope kafkaValue = record.value();
    if (record.key().isControlMessage() || null == kafkaValue) {
      return;
    }

    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(put.schemaId);
        try {
          deserializeValue(put.schemaId, put.putValue);
        } catch (Exception e) {
          byte[] valueBytes = ByteUtils.extractByteArray(put.putValue);
          String errorMsg = String.format("Encounter the error while deserializing PUT message. topic: %s, partition: %d"
              + " offset: %d, schema id: %d, value bytes: %s", record.topic(), record.partition(), record.offset(),
              put.schemaId, Hex.encodeHexString(valueBytes));

          logger.error(errorMsg);
          throw e;
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
            consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
  }

  /**
   * Check whether the given schema id is available for current store.
   * The function will bypass the check if schema id is -1 (H2V job is still using it before we finishes t he integration with schema registry).
   * Right now, this function is maintaining a local cache for schema id of current store considering that the value schema is immutable;
   * If the schema id is not available, this function will polling until the schema appears or timeout: {@link #SCHEMA_POLLING_TIMEOUT_MS};
   *
   * @param schemaId
   */
  private void waitReadyToProcessDataRecord(int schemaId) throws InterruptedException {
    if (-1 == schemaId) {
      // TODO: Once Venice Client (VeniceShellClient) finish the integration with schema registry,
      // we need to remove this check here.
      return;
    }

    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion() ||
        schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      StoreVersionState storeVersionState = waitVersionStateAvailable(kafkaVersionTopic);
      if (!storeVersionState.chunked) {
        throw new VeniceException("Detected chunking in a store-version where chunking is NOT enabled. Will abort ingestion.");
      }
      return;
    }

    waitUntilValueSchemaAvailable(schemaId);
  }

  protected StoreVersionState waitVersionStateAvailable(String kafkaTopic) throws InterruptedException {
    long startTime = System.nanoTime();
    for (;;) {
      Optional<StoreVersionState> state = storageMetadataService.getStoreVersionState(this.kafkaVersionTopic);
      long elapsedTime = System.nanoTime() - startTime;

      if (state.isPresent()) {
        logger.info("Version state is available for " + kafkaTopic + " after " + NANOSECONDS.toSeconds(elapsedTime));
        return state.get();
      }

      if (NANOSECONDS.toMillis(elapsedTime) > SCHEMA_POLLING_TIMEOUT_MS || !isRunning()) {
        logger.warn("Version state is not available for " + kafkaTopic + " after " + NANOSECONDS.toSeconds(elapsedTime));
        throw new VeniceException("Store version state is not available for " + kafkaTopic);
      }

      Thread.sleep(SCHEMA_POLLING_DELAY_MS);
    }
  }

  private void waitUntilValueSchemaAvailable(int schemaId) throws InterruptedException {
    // Considering value schema is immutable for an existing store, we can cache it locally
    if (availableSchemaIds.contains(schemaId)) {
      return;
    }

    long startTime = System.nanoTime();
    for (;;) {
      // Since schema registration topic might be slower than data topic,
      // the consumer will be pending until the new schema arrives.
      // TODO: better polling policy
      // TODO: Need to add metrics to track this scenario
      // In the future, we might consider other polling policies,
      // such as throwing error after certain amount of time;
      // Or we might want to propagate our state to the Controller via the VeniceNotifier,
      // if we're stuck polling more than a certain threshold of time?
      boolean schemaExists = schemaRepository.hasValueSchema(storeName, schemaId);
      long elapsedTime = System.nanoTime() - startTime;
      if (schemaExists) {
        logger.info("Found new value schema [" + schemaId + "] for " + storeName +
            " after " + NANOSECONDS.toSeconds(elapsedTime));
        availableSchemaIds.add(schemaId);
        // TODO: Query metastore for existence of value schema id before doing an update. During bounce of large
        // cluster these metastore writes could be spiky
        if (metaStoreWriter != null && !VeniceSystemStoreType.META_STORE.isSystemStore(storeName)) {
          String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
          String metaStoreRT = Version.composeRealTimeTopic(metaStoreName);
          if (getTopicManager(localKafkaServer).containsTopic(metaStoreRT)) {
            metaStoreWriter.writeInUseValueSchema(storeName, versionNumber, schemaId);
          }
        }
        return;
      }

      if (NANOSECONDS.toMillis(elapsedTime) > SCHEMA_POLLING_TIMEOUT_MS || !isRunning()) {
        logger.warn("Value schema [" + schemaId + "] is not available for " + storeName +
            " after " + NANOSECONDS.toSeconds(elapsedTime));
        throw new VeniceException("Value schema [" + schemaId + "] is not available for " + storeName);
      }

      Thread.sleep(SCHEMA_POLLING_DELAY_MS);
    }
  }

  /**
   * Deserialize a value using the schema that serializes it. Exception will be thrown and ingestion will fail if the
   * value cannot be deserialized. Currently the deserialization dry-run won't happen in the following cases:
   *
   * 1. Value is chunked. A single piece of value cannot be deserialized. In this case, the schema id is not added in
   *    availableSchemaIds by {@link StoreIngestionTask#waitUntilValueSchemaAvailable}.
   * 2. Value is compressed, which cannot be deserialized without decompression.
   * 3. Ingestion isolation is enabled, in which ingestion happens on forked process instead of this main process.
   */
  private void deserializeValue(int schemaId, ByteBuffer value) {
    if (!availableSchemaIds.contains(schemaId) || deserializedSchemaIds.contains(schemaId)) {
      return;
    }
    Optional<StoreVersionState> state = storageMetadataService.getStoreVersionState(this.kafkaVersionTopic);
    if (!state.isPresent() || state.get().compressionStrategy != CompressionStrategy.NO_OP.getValue()) {
      return;
    }
    SchemaEntry valueSchema = schemaRepository.getValueSchema(storeName, schemaId);
    if (valueSchema != null) {
      Schema schema = valueSchema.getSchema();
      new AvroGenericDeserializer<>(schema, schema).deserialize(value);
      logger.info("Value deserialization succeeded with schema [" + schemaId + "] for " + storeName);
      deserializedSchemaIds.add(schemaId);
    }
  }

  private synchronized void complete() {
    if (consumerActionsQueue.isEmpty()) {
      close();
    } else {
      logger.info(consumerTaskId + " consumerActionsQueue is not empty, ignoring complete() call.");
    }
  }

  /**
   * Stops the consumer task.
   */
  public synchronized void close() {
    // Evict any pending repair tasks
    isRunning.set(false);
    // KafkaConsumer is closed at the end of the run method.
    // The operation is executed on a single thread in run method.
    // This method signals the run method to end, which closes the
    // resources before exiting.
  }

  /**
   * A function to allow the service to get the current status of the task.
   * This would allow the service to create a new task if required.
   */
  public boolean isRunning() {
    return isRunning.get();
  }

  public String getVersionTopic() {
    return kafkaVersionTopic;
  }

  protected Collection<KafkaConsumerWrapper> getDedicatedConsumers() {
    if (serverConfig.isSharedConsumerPoolEnabled()) {
      throw new IllegalStateException("Shared consumer code path should not call getDedicatedConsumers().");
    } else {
      return Collections.unmodifiableList(new ArrayList<>(kafkaUrlToDedicatedConsumerMap.values()));
    }
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
  public boolean isPartitionConsuming(int partitionId) {
    return amplificationAdapter.isPartitionConsuming(partitionId);
  }

  /**
   * Override the {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} config with a remote Kafka bootstrap url.
   */
  protected Properties createKafkaConsumerProperties(Properties localConsumerProps, String remoteKafkaSourceAddress, boolean consumeRemotely) {
    Properties newConsumerProps = new Properties();
    newConsumerProps.putAll(localConsumerProps);
    newConsumerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, remoteKafkaSourceAddress);
    VeniceProperties customizedConsumerConfigs = consumeRemotely ? serverConfig.getKafkaConsumerConfigsForRemoteConsumption()
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
      if (extraDisjunctionCondition ||
          (partitionConsumptionState.isEndOfPushReceived() && !partitionConsumptionState.isCompletionReported())) {
        if (isReadyToServe(partitionConsumptionState)) {
          int partition = partitionConsumptionState.getPartition();
          Store store = storeRepository.getStoreOrThrow(storeName);

          AbstractStorageEngine<?> storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
          if (store.isHybrid()) {
            if (storageEngine == null) {
              logger.warn("Storage engine " + kafkaVersionTopic + " was removed before reopening");
            } else {
              logger.info("Reopen partition " + kafkaVersionTopic + "_" + partition + " for reading after ready-to-serve.");
              storageEngine.preparePartitionForReading(partition);
            }
          }
          if (partitionConsumptionState.isCompletionReported()) {
            // Completion has been reported so extraDisjunctionCondition must be true to enter here.
            logger.info(consumerTaskId + " Partition " + partition + " synced offset: "
                + partitionConsumptionState.getLatestProcessedLocalVersionTopicOffset());
          } else {
            // Check whether we need to warm-up cache here.
            if (storageEngine != null
                && serverConfig.isCacheWarmingBeforeReadyToServeEnabled()
                && serverConfig.isCacheWarmingEnabledForStore(storeName) // Only warm up configured stores
                && store.getCurrentVersion() <= versionNumber) { // Only warm up current version or future version
              /*
               * With cache warming, the completion report is asynchronous, so it is possible that multiple queued
               * tasks are trying to warm up the cache and report completion for the same partition.
               * When this scenario happens, this task will end directly.
               */
              synchronized (cacheWarmingPartitionIdSet) {
                if (!cacheWarmingPartitionIdSet.contains(partition)) {
                  cacheWarmingPartitionIdSet.add(partition);
                  cacheWarmingThreadPool.submit(() -> {
                    logger.info("Start warming up store: " + kafkaVersionTopic + ", partition: " + partition);
                    try {
                      storageEngine.warmUpStoragePartition(partition);
                      logger.info("Finished warming up store: " + kafkaVersionTopic + ", partition: " + partition);
                    } catch (Exception e) {
                      logger.error("Received exception while warming up cache for store: " + kafkaVersionTopic + ", partition: " + partition);
                    }
                    // Delay reporting ready-to-serve until the storage node is ready.
                    long extraSleepTime = startReportingReadyToServeTimestamp - System.currentTimeMillis();
                    if (extraSleepTime > 0) {
                      try {
                        Thread.sleep(extraSleepTime);
                      } catch (InterruptedException e) {
                        throw new VeniceException("Sleep before reporting ready to serve got interrupted", e);
                      }
                    }
                    reportStatusAdapter.reportCompleted(partitionConsumptionState);
                    logger.info(consumerTaskId + " Partition " + partition + " is ready to serve");
                  });
                }
              }
            } else {
              reportStatusAdapter.reportCompleted(partitionConsumptionState);
              logger.info(consumerTaskId + " Partition " + partition + " is ready to serve");
            }
            warmupSchemaCache(store);
          }
          if (suppressLiveUpdates) {
            // If live updates suppression is enabled, stop consuming any new messages once the partition is ready to serve.
            String msg = consumerTaskId + " Live update suppression is enabled. Stop consumption for partition " + partition;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
              logger.info(msg);
            }
            unSubscribePartition(kafkaVersionTopic, partition);
          }
        } else {
          reportStatusAdapter.reportProgress(partitionConsumptionState);
        }
      }
    };
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
      logger.error("Got interrupted while trying to fetch value schema");
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
    for (int subPartition : PartitionUtils.getSubPartitions(userPartition, amplificationFactor)) {
      if (partitionConsumptionStateMap.containsKey(subPartition)) {
        pcsList.add(partitionConsumptionStateMap.get(subPartition));
      }
    }
    reportStatusAdapter.reportError(pcsList, message, e);
  }

  public int getAmplificationFactor() {
    return amplificationFactor;
  }

  protected ReportStatusAdapter getReportStatusAdapter() {
    return reportStatusAdapter;
  }

  public boolean isActiveActiveReplicationEnabled() {
    return this.isActiveActiveReplicationEnabled;
  }

  /**
   * A function that would produce messages to topic.
   */
  @FunctionalInterface
  interface ProduceToTopic {
    Future<RecordMetadata> apply(Callback callback, LeaderMetadataWrapper leaderMetadataWrapper);
  }

  /**
   * Invoked by admin request to dump the requested partition consumption states
   */
  public void dumpPartitionConsumptionStates(AdminResponse response, ComplementSet<Integer> partitions) {
    for (Map.Entry<Integer, PartitionConsumptionState> entry : partitionConsumptionStateMap.entrySet()) {
      try {
        if (partitions.contains(entry.getKey())) {
          response.addPartitionConsumptionState(entry.getValue());
        }
      } catch (Throwable e) {
        logger.error("Error when dumping consumption state for store " + this.storeName + " partition " + entry.getKey(), e);
      }
    }
  }

  /**
   * Invoked by admin request to dump store version state metadata.
   */
  public void dumpStoreVersionState(AdminResponse response) {
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    storeVersionState.ifPresent(response::addStoreVersionState);
  }

  public VeniceServerConfig getServerConfig() {
    return serverConfig;
  }

  /**
   * The function returns local or remote topic manager.
   * @param sourceKafkaServer The address of source kafka bootstrap server.
   * @return topic manager
   */
  protected TopicManager getTopicManager(String sourceKafkaServer) {
    if (sourceKafkaServer.equals(localKafkaServer)) {
      // Use default kafka admin client (could be scala or java based) to get local topic manager
      return topicManagerRepository.getTopicManager();
    }
    // Use java-based kafka admin client to get remote topic manager
    return topicManagerRepositoryJavaBased.getTopicManager(sourceKafkaServer);
  }

  /**
   * The purpose of this function is to wait for the complete processing (including persistence to disk) of all the messages
   * those were consumed from this kafka {topic, partition} prior to calling this function.
   * This will make the calling thread to block.
   * @param topic
   * @param partition
   * @throws InterruptedException
   */
  protected void waitForAllMessageToBeProcessedFromTopicPartition(
      String topic,
      int partition,
      PartitionConsumptionState partitionConsumptionState
  ) throws InterruptedException {
    storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);
  }

  protected DelegateConsumerRecordResult delegateConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper, int subPartition, String kafkaUrl) {
    return DelegateConsumerRecordResult.QUEUED_TO_DRAINER;
  }

  /**
   * This enum represents all potential results after calling {@link #delegateConsumerRecord(ConsumerRecord, int, String)}.
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

  protected void recordProcessedRecordStats(PartitionConsumptionState partitionConsumptionState, int processedRecordSize, int processedRecordNum) {

  }

  protected boolean isSegmentControlMsg(ControlMessageType msgType) {
    return ControlMessageType.START_OF_SEGMENT.equals(msgType) || ControlMessageType.END_OF_SEGMENT.equals(msgType);
  }

  /**
   * This is not a per record state. Rather it's used to indicate if the transient record buffer is being used at all
   * for this ingestion task or not.
   * For L/F mode only WC ingestion task needs this buffer.
   */
  protected boolean isTransientRecordBufferUsed() {
    return isWriteComputationEnabled;
  }
}
