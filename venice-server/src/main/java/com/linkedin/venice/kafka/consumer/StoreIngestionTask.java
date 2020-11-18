package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceInconsistentStoreMetadataException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.helix.LeaderFollowerParticipantModel;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfBufferReplay;
import com.linkedin.venice.kafka.protocol.StartOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.KafkaDataIntegrityValidator;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import static com.linkedin.venice.stats.StatsErrorCode.*;
import static com.linkedin.venice.store.record.ValueRecord.*;
import static java.util.concurrent.TimeUnit.*;

/**
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public abstract class StoreIngestionTask implements Runnable, Closeable {
  // TODO: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = Logger.getLogger(StoreIngestionTask.class);

  // Constants
  private static final String CONSUMER_TASK_ID_FORMAT = StoreIngestionTask.class.getSimpleName() + " for [ Topic: %s ]";
  public static long SCHEMA_POLLING_DELAY_MS = SECONDS.toMillis(5);
  private static final long SCHEMA_POLLING_TIMEOUT_MS = MINUTES.toMillis(5);

  /** After processing the following number of messages, Venice SN will report progress metrics. */
  public static int OFFSET_REPORTING_INTERVAL = 1000;
  private static final int MAX_CONTROL_MESSAGE_RETRIES = 3;
  private static final int MAX_IDLE_COUNTER  = 100;
  private static final int CONSUMER_ACTION_QUEUE_INIT_CAPACITY = 11;
  private static final long KILL_WAIT_TIME_MS = 5000l;
  private static final int MAX_KILL_CHECKING_ATTEMPTS = 10;
  private static final int SLOPPY_OFFSET_CATCHUP_THRESHOLD = 100;

  // Final stuff
  /** storage destination for consumption */
  protected final StorageEngineRepository storageEngineRepository;
  protected final String kafkaVersionTopic;
  protected final String storeName;
  protected final int versionNumber;
  protected final ReadOnlySchemaRepository schemaRepository;
  protected final ReadOnlyStoreRepository storeRepository;
  protected final String consumerTaskId;
  protected final Properties kafkaProps;
  protected final KafkaClientFactory factory;
  protected final VeniceWriterFactory veniceWriterFactory;
  protected final AtomicBoolean isRunning;
  protected final AtomicBoolean emitMetrics; // TODO: remove this once we migrate to versioned stats
  protected final AtomicInteger consumerActionSequenceNumber = new AtomicInteger(0);
  protected final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;
  protected final StorageMetadataService storageMetadataService;
  protected final TopicManager topicManager;
  protected final CachedKafkaMetadataGetter cachedKafkaMetadataGetter;
  protected final EventThrottler bandwidthThrottler;
  protected final EventThrottler recordsThrottler;
  protected final EventThrottler unorderedBandwidthThrottler;
  protected final EventThrottler unorderedRecordsThrottler;
  /** Per-partition consumption state map */
  protected final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  protected final StoreBufferService storeBufferService;
  /** Persists the exception thrown by {@link StoreBufferService}. */
  protected Exception lastDrainerException = null;
  /** Persists the exception thrown by {@link KafkaConsumerService}. */
  private Exception lastConsumerException = null;
  /** Persists the exception thrown by kafka producer callback for L/F mode */
  private Exception lastProducerException = null;
  /** Keeps track of every upstream producer this consumer task has seen so far. */
  protected final KafkaDataIntegrityValidator kafkaDataIntegrityValidator;
  protected final AggStoreIngestionStats storeIngestionStats;
  protected final AggVersionedDIVStats versionedDIVStats;
  protected final AggVersionedStorageIngestionStats versionedStorageIngestionStats;
  protected final BooleanSupplier isCurrentVersion;
  protected final Optional<HybridStoreConfig> hybridStoreConfig;
  protected final IngestionNotificationDispatcher notificationDispatcher;
  protected final Optional<ProducerTracker.DIVErrorMetricCallback> divErrorMetricCallback;

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

  protected final boolean readOnlyForBatchOnlyStoreEnabled;

  /**
   * When buffer replay is disabled, {@link #measureHybridOffsetLag(PartitionConsumptionState, boolean)}
   * won't consider the real-time topic offset.
   */
  protected final boolean bufferReplayEnabledForHybrid;

  protected final VeniceServerConfig serverConfig;

  /** Used for reporting error when the {@link #partitionConsumptionStateMap} is empty */
  protected final int errorPartitionId;

  // use this checker to check whether ingestion completion can be reported for a partition
  protected final ReadyToServeCheck defaultReadyToServeChecker;

  // Non-final
  /** Should never be accessed directly. Always use {@link #getVeniceWriter()} instead, so that lazy init can occur. */
  private VeniceWriter<byte[], byte[], byte[]> veniceWriter;
  // Kafka bootstrap url to consumer
  protected Map<String, KafkaConsumerWrapper> consumerMap;

  protected Set<Integer> availableSchemaIds;
  protected int idleCounter = 0;

  // This indicates whether it polls nothing from Kafka
  // It's for stats measuring purpose
  protected int recordCount = 0;

  /** this is used to handle hybrid write quota */
  protected Optional<HybridStoreQuotaEnforcement> hybridQuotaEnforcer;
  protected Optional<Map<Integer, Integer>> subscribedPartitionToSize;

  /**
   * Track all the topics with Kafka log-compaction enabled.
   * The expectation is that the compaction strategy is immutable once Kafka log compaction is enabled.
   */
  private final Set<String> topicWithLogCompaction = new ConcurrentSkipListSet<>();

  protected IngestionTaskWriteComputeAdapter ingestionTaskWriteComputeAdapter;
  protected static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER = RedundantExceptionFilter.getRedundantExceptionFilter();

  private final AggKafkaConsumerService aggKafkaConsumerService;
  /**
   * This topic set is used to track all the topics, which have ever been subscribed.
   * It may not reflect the current subscriptions.
   */
  private final Set<String> everSubscribedTopics = new HashSet<>();
  private boolean orderedWritesOnly = true;

  private Optional<RocksDBMemorryEnforcement> rocksDBMemoryEnforcer;

  private final ExecutorService cacheWarmingThreadPool;
  /**
   * This set is used to track the cache warming request for each partition.
   */
  private final Set<Integer> cacheWarmingPartitionIdSet = new HashSet<>();

  /**
   * Please refer to {@link com.linkedin.venice.ConfigKeys#SERVER_DELAY_REPORT_READY_TO_SERVE_MS} to
   * find more details.
   */
  private final long startReportingReadyToServeTimestamp;

  protected static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER = new ChunkedValueManifestSerializer(false);

  int writeComputeFailureCode = 0;

  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  protected boolean isWriteComputationEnabled = false;

  //This flag is introduced to help write integration test to exercise code path during UPDATE message processing in
  //{@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}. This must be always set to true except
  //as needed in integration test.
  private boolean purgeTransientRecordCache = true;

  /**
   * Freeze ingestion if ready to serve or local data exists
   */
  private final boolean suppressLiveUpdates;

  public StoreIngestionTask(
      VeniceWriterFactory writerFactory,
      KafkaClientFactory consumerFactory,
      Properties kafkaConsumerProperties,
      StorageEngineRepository storageEngineRepository,
      StorageMetadataService storageMetadataService,
      Queue<VeniceNotifier> notifiers,
      EventThrottler bandwidthThrottler,
      EventThrottler recordsThrottler,
      EventThrottler unorderedBandwidthThrottler,
      EventThrottler unorderedRecordsThrottler,
      ReadOnlySchemaRepository schemaRepository,
      ReadOnlyStoreRepository storeRepository,
      TopicManager topicManager,
      AggStoreIngestionStats storeIngestionStats,
      AggVersionedDIVStats versionedDIVStats,
      AggVersionedStorageIngestionStats versionedStorageIngestionStats,
      StoreBufferService storeBufferService,
      BooleanSupplier isCurrentVersion,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean isIncrementalPushEnabled,
      VeniceStoreConfig storeConfig,
      DiskUsage diskUsage,
      RocksDBMemoryStats rocksDBMemoryStats,
      boolean bufferReplayEnabledForHybrid,
      AggKafkaConsumerService kafkaConsumerService,
      VeniceServerConfig serverConfig,
      int errorPartitionId,
      ExecutorService cacheWarmingThreadPool,
      long startReportingReadyToServeTimestamp,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      boolean isWriteComputationEnabled) {
    this.readCycleDelayMs = storeConfig.getKafkaReadCycleDelayMs();
    this.emptyPollSleepMs = storeConfig.getKafkaEmptyPollSleepMs();
    this.databaseSyncBytesIntervalForTransactionalMode = storeConfig.getDatabaseSyncBytesIntervalForTransactionalMode();
    this.databaseSyncBytesIntervalForDeferredWriteMode = storeConfig.getDatabaseSyncBytesIntervalForDeferredWriteMode();
    this.veniceWriterFactory = writerFactory;
    this.factory = consumerFactory;
    this.kafkaProps = kafkaConsumerProperties;
    this.storageEngineRepository = storageEngineRepository;
    this.storageMetadataService = storageMetadataService;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.unorderedBandwidthThrottler = unorderedBandwidthThrottler;
    this.unorderedRecordsThrottler = unorderedRecordsThrottler;
    this.storeRepository = storeRepository;
    this.schemaRepository = schemaRepository;
    this.kafkaVersionTopic = storeConfig.getStoreName();
    this.storeName = Version.parseStoreFromKafkaTopicName(kafkaVersionTopic);
    this.versionNumber = Version.parseVersionFromKafkaTopicName(kafkaVersionTopic);
    this.availableSchemaIds = new HashSet<>();
    this.consumerActionsQueue = new PriorityBlockingQueue<>(CONSUMER_ACTION_QUEUE_INIT_CAPACITY);
    this.consumerMap = new VeniceConcurrentHashMap<>();

    // partitionConsumptionStateMap could be accessed by multiple threads: consumption thread and the thread handling kill message
    this.partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    // Could be accessed from multiple threads since there are multiple worker threads.
    this.kafkaDataIntegrityValidator = new KafkaDataIntegrityValidator(this.kafkaVersionTopic);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, kafkaVersionTopic);
    this.topicManager = topicManager;
    this.cachedKafkaMetadataGetter = new CachedKafkaMetadataGetter(topicManager, storeConfig.getTopicOffsetCheckIntervalMs());

    this.storeIngestionStats = storeIngestionStats;
    this.versionedDIVStats = versionedDIVStats;
    this.versionedStorageIngestionStats = versionedStorageIngestionStats;

    this.isRunning = new AtomicBoolean(true);
    this.emitMetrics = new AtomicBoolean(true);

    this.storeBufferService = storeBufferService;
    this.isCurrentVersion = isCurrentVersion;
    this.hybridStoreConfig = hybridStoreConfig;
    this.isIncrementalPushEnabled = isIncrementalPushEnabled;
    this.notificationDispatcher = new IngestionNotificationDispatcher(notifiers, kafkaVersionTopic, isCurrentVersion);

    this.divErrorMetricCallback = Optional.of(e -> versionedDIVStats.recordException(storeName, versionNumber, e));

    this.diskUsage = diskUsage;

    this.rocksDBMemoryStats = Optional.ofNullable(
        storageEngineRepository.hasLocalStorageEngine(kafkaVersionTopic)
            && storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic).getType().equals(PersistenceType.ROCKS_DB) ?
            rocksDBMemoryStats : null
    );

    this.readOnlyForBatchOnlyStoreEnabled = storeConfig.isReadOnlyForBatchOnlyStoreEnabled();

    this.bufferReplayEnabledForHybrid = bufferReplayEnabledForHybrid;

    this.serverConfig = serverConfig;

    this.defaultReadyToServeChecker = getDefaultReadyToServeChecker();

    this.ingestionTaskWriteComputeAdapter = new IngestionTaskWriteComputeAdapter(storeName, schemaRepository);

    this.hybridQuotaEnforcer = Optional.empty();

    this.subscribedPartitionToSize = Optional.empty();

    if (serverConfig.isHybridQuotaEnabled() || storeRepository.getStoreOrThrow(storeName).isHybridStoreDiskQuotaEnabled()) {
      buildHybridQuotaEnforcer();
    }

    this.aggKafkaConsumerService = kafkaConsumerService;

    this.errorPartitionId = errorPartitionId;
    this.cacheWarmingThreadPool = cacheWarmingThreadPool;
    this.startReportingReadyToServeTimestamp = startReportingReadyToServeTimestamp;

    this.isWriteComputationEnabled = isWriteComputationEnabled;

    buildRocksDBMemoryEnforcer();

    this.partitionStateSerializer = partitionStateSerializer;

    this.suppressLiveUpdates = serverConfig.freezeIngestionIfReadyToServeOrLocalDataExists();
  }

  protected void validateState() {
    if (!isRunning()) {
      throw new VeniceException(" Topic " + kafkaVersionTopic + " is shutting down, no more messages accepted");
    }
  }

  protected void buildHybridQuotaEnforcer() {
    /**
     * We will enforce hybrid quota only if this is hybrid mode && persistence type is rocks DB
     */
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
    if (isHybridMode() && storageEngine.getType().equals(PersistenceType.ROCKS_DB)) {
      this.hybridQuotaEnforcer = Optional.of(new HybridStoreQuotaEnforcement(
          this,
          storageEngine,
          storeRepository.getStoreOrThrow(storeName), kafkaVersionTopic,
          topicManager.getPartitions(kafkaVersionTopic).size(),
          partitionConsumptionStateMap));
      this.storeRepository.registerStoreDataChangedListener(hybridQuotaEnforcer.get());
      this.subscribedPartitionToSize = Optional.of(new HashMap<>());
    }
  }

  private void buildRocksDBMemoryEnforcer() {
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
    if (rocksDBMemoryStats.isPresent() && storageEngine.getType().equals(PersistenceType.ROCKS_DB)) {
      if (serverConfig.isSharedConsumerPoolEnabled()) {
        // TODO: support memory limiter with shared consumer
        throw new VeniceException("RocksDBMemoryEnforcement can not work with shared consumer.");
      }
      this.rocksDBMemoryEnforcer = Optional.of(new RocksDBMemorryEnforcement(
         this
      ));
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
  public synchronized void subscribePartition(String topic, int partition) {
    validateState();
    consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.SUBSCRIBE, topic, partition, nextSeqNum()));
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public synchronized void unSubscribePartition(String topic, int partition) {
    validateState();
    consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.UNSUBSCRIBE, topic, partition, nextSeqNum()));
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public synchronized void resetPartitionConsumptionOffset(String topic, int partition) {
    validateState();
    consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.RESET_OFFSET, topic, partition, nextSeqNum()));
  }

  public abstract void promoteToLeader(String topic, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker);
  public abstract void demoteToStandby(String topic, int partitionId, LeaderFollowerParticipantModel.LeaderSessionIdChecker checker);

  public synchronized void kill() {
    validateState();
    consumerActionsQueue.add(ConsumerAction.createKillAction(kafkaVersionTopic, nextSeqNum()));
    int currentAttempts = 0;
    try {
      // Check whether the task is really killed
      while (isRunning() && currentAttempts < MAX_KILL_CHECKING_ATTEMPTS) {
        MILLISECONDS.sleep(KILL_WAIT_TIME_MS / MAX_KILL_CHECKING_ATTEMPTS);
        currentAttempts ++;
      }
    } catch (InterruptedException e) {
      logger.warn("Wait killing is interrupted.", e);
      Thread.currentThread().interrupt();
    }
    if (isRunning()) {
      // If task is still running, force close it.
      notificationDispatcher.reportError(partitionConsumptionStateMap.values(), "Received the signal to kill this consumer. Topic " + kafkaVersionTopic,
          new VeniceException("Kill the consumer"));
      // close can not stop the consumption synchronizely, but the status of helix would be set to ERROR after
      // reportError. The only way to stop it synchronizely is interrupt the current running thread, but it's an unsafe
      // operation, for example it could break the ongoing db operation, so we should avoid that.
      close();
    }
  }

  private void beginBatchWrite(int partitionId, boolean sorted, PartitionConsumptionState partitionConsumptionState) {
    Map<String, String> checkpointedDatabaseInfo = partitionConsumptionState.getOffsetRecord().getDatabaseInfo();
    StoragePartitionConfig storagePartitionConfig = getStoragePartitionConfig(partitionId, sorted, partitionConsumptionState);
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());

    Optional<Supplier<byte[]>> partitionChecksumSupplier = Optional.empty();
    //For now checksum verification is only enabled in sorted batch push in O/O model.
    if (serverConfig.isDatabaseChecksumVerificationEnabled() && partitionConsumptionState.isDeferredWrite()
        && this instanceof OnlineOfflineStoreIngestionTask) {
      partitionConsumptionState.initializeExpectedChecksum();
      partitionChecksumSupplier = Optional.of(() -> {
        byte[] checksum = partitionConsumptionState.getExpectedChecksum();
        partitionConsumptionState.resetExpectedChecksum();
        return checksum;
      });
    }
    storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic)
        .beginBatchWrite(storagePartitionConfig, checkpointedDatabaseInfo, partitionChecksumSupplier);
    logger.info("Started batch write to store: " + kafkaVersionTopic + ", partition: " + partitionId +
        " with checkpointed database info: " + checkpointedDatabaseInfo + " and sorted: " + sorted);
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

    int partitionId = partitionConsumptionState.getPartition();
    boolean lagIsAcceptable = false;

    if (!hybridStoreConfig.isPresent()) {
    /**
     * In theory, it will be 1 offset ahead of the current offset since getOffset returns the next available offset.
     * Currently, we make it a sloppy in case Kafka topic have duplicate messages.
     * TODO: find a better solution
     */
      lagIsAcceptable = cachedKafkaMetadataGetter.getOffset(kafkaVersionTopic, partitionId) <=
        partitionConsumptionState.getOffsetRecord().getOffset() + SLOPPY_OFFSET_CATCHUP_THRESHOLD;
    } else {
      // Looks like none of the short-circuitry fired, so we need to measure lag!
      long offsetThreshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
      long producerTimeLagThresholdInSeconds = hybridStoreConfig.get().getProducerTimestampLagThresholdToGoOnlineInSeconds();
      String msg = kafkaVersionTopic + "_" + partitionId;

      // Log only once a minute per partition.
      boolean shouldLogLag = !REDUNDANT_LOGGING_FILTER.isRedundantException(msg);

      /**
       * If offset lag threshold is set to -1, time lag threshold will be the only criterion for going online.
       */
      if (offsetThreshold > 0) {
        long lag = measureHybridOffsetLag(partitionConsumptionState, shouldLogLag);
        boolean lagging = lag > offsetThreshold;

        lagIsAcceptable = !lagging;

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
        long latestProducerTimestamp = partitionConsumptionState.getOffsetRecord().getLatestProducerProcessingTimeInMs();
        long producerTimestampLag = LatencyUtils.getElapsedTimeInMs(latestProducerTimestamp);
        boolean timestampLagIsAcceptable = (producerTimestampLag < producerTimeLagThresholdInMS);
        if (shouldLogLag) {
          logger.info(String.format("%s [Time lag] partition %d is %slagging. The latest producer timestamp is %d. Timestamp Lag: [%d] %s Threshold [%d]",
              consumerTaskId, partitionId, (!timestampLagIsAcceptable ? "" : "not "), latestProducerTimestamp, producerTimestampLag,
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
          String realTimeTopic = Version.composeRealTimeTopic(storeName);
          if (!cachedKafkaMetadataGetter.containsTopic(realTimeTopic)) {
            timestampLagIsAcceptable = true;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
              logger.info(String.format("%s [Time lag] Real-time topic %s doesn't exist; ignoring time lag.", consumerTaskId, realTimeTopic));
            }
          } else {
            long latestProducerTimestampInRT = cachedKafkaMetadataGetter.getProducerTimestampOfLastMessage(realTimeTopic, partitionId);
            if (latestProducerTimestampInRT < 0 || latestProducerTimestampInRT <= latestProducerTimestamp) {
              timestampLagIsAcceptable = true;
              if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msgIdentifier)) {
                if (latestProducerTimestampInRT < 0) {
                  logger.info(String.format("%s [Time lag] Real-time topic %s is empty or all messages have been truncated; ignoring time lag.", consumerTaskId, realTimeTopic));
                } else {
                  logger.info(String.format("%s [Time lag] Producer timestamp of last message in Real-time topic %s "
                      + "partition %d: %d, which is smaller or equal than the known latest producer time: %d. Consumption "
                      + "lag is caught up already.", consumerTaskId, realTimeTopic, partitionId, latestProducerTimestampInRT, latestProducerTimestamp));
                }
              }
            }
          }
        }
        if (offsetThreshold > 0) {
          /**
           * If both threshold configs are on, both both offset lag and time lag must be within thresholds before online.
           */
          lagIsAcceptable &= timestampLagIsAcceptable;
        } else {
          lagIsAcceptable = timestampLagIsAcceptable;
        }
      }
    }

    if (lagIsAcceptable) {
      partitionConsumptionState.lagHasCaughtUp();
    }

    return lagIsAcceptable;
  }

  /**
   * Measure the hybrid offset lag for partition being tracked in `partitionConsumptionState`.
   */
  protected abstract long measureHybridOffsetLag(PartitionConsumptionState partitionConsumptionState, boolean shouldLogLag);

  /**
   * Check if the ingestion progress has reached to the end of the version topic. This is currently only
   * used {@link LeaderFollowerStoreIngestionTask}.
   */
  protected abstract void reportIfCatchUpBaseTopicOffset(PartitionConsumptionState partitionConsumptionState);

  /**
   * This function will produce a pair of consumer record and a it's derived produced record to the writer buffers maintained by {@link StoreBufferService}.
   * @param consumedrecord : received consumer record
   * @param producedRecord : derived produced record
   * @throws InterruptedException
   */
  protected void produceToStoreBufferService(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumedrecord, ProducedRecord producedRecord) throws InterruptedException {
    long queuePutStartTimeInNS = System.nanoTime();
    storeBufferService.putConsumerRecord(consumedrecord, this, producedRecord); // blocking call
    storeIngestionStats.recordProduceToDrainQueueRecordNum(storeName, 1);
    if (emitMetrics.get()) {
      storeIngestionStats.recordConsumerRecordsQueuePutLatency(storeName, LatencyUtils.getLatencyInMS(queuePutStartTimeInNS));
    }
  }

  /**
   * This function is in charge of producing the consumer records to the writer buffers maintained by {@link StoreBufferService}.
   * @param records : received consumer records
   * @param whetherToApplyThrottling : whether to apply throttling in this function or not.
   * @throws InterruptedException
   */
  protected void produceToStoreBufferServiceOrKafka(Iterable<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> records,
      boolean whetherToApplyThrottling) throws InterruptedException {
    long totalBytesRead = 0;
    double elapsedTimeForPuttingIntoQueue = 0;
    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> processedRecord = new LinkedList<>();
    long beforeProcessingTimestamp = System.currentTimeMillis();
    int recordQueuedToDrainer = 0;
    int recordProducedToKafka = 0;
    double elapsedTimeForProducingToKafka = 0;
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
      if (!shouldProcessRecord(record)) {
        continue;
      }
      // Check schema id availability before putting consumer record to drainer queue
      waitReadyToProcessRecord(record);
      processedRecord.add(record);
      long kafkaProduceStartTimeInNS = System.nanoTime();
      if (!hasProducedToKafka(record)) {
        long queuePutStartTimeInNS = System.nanoTime();
        storeBufferService.putConsumerRecord(record, this, null); // blocking call
        elapsedTimeForPuttingIntoQueue += LatencyUtils.getLatencyInMS(queuePutStartTimeInNS);
        ++recordQueuedToDrainer;
      } else {
        elapsedTimeForProducingToKafka += LatencyUtils.getLatencyInMS(kafkaProduceStartTimeInNS);
        ++recordProducedToKafka;
      }
      totalBytesRead += Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize());
      // Update the latest message consumption time
      partitionConsumptionStateMap.get(record.partition()).setLatestMessageConsumptionTimestampInMs(System.currentTimeMillis());
    }
    storeIngestionStats.recordProduceToDrainQueueRecordNum(storeName, recordQueuedToDrainer);
    if (recordProducedToKafka > 0) {
      storeIngestionStats.recordProduceToKafkaRecordNum(storeName, recordProducedToKafka);
    }

    long quotaEnforcementStartTimeInNS = System.nanoTime();
    /**
     * Enforces hybrid quota on this batch poll if this is hybrid store and persistence type is rocksDB
     * Even if the records list is empty, we still need to check quota to potentially resume partition
     */
    boolean isHybridQuotaEnabled = storeRepository.getStoreOrThrow(storeName).isHybridStoreDiskQuotaEnabled();
    if (isHybridQuotaEnabled && !hybridQuotaEnforcer.isPresent()) {
      buildHybridQuotaEnforcer();
    }
    if (isHybridQuotaEnabled && hybridQuotaEnforcer.isPresent()) {
      refillPartitionToSizeMap(processedRecord);
      hybridQuotaEnforcer.get().checkPartitionQuota(subscribedPartitionToSize.get());
    }
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

  protected void processMessages(boolean usingSharedConsumer) throws InterruptedException {
    if (null != lastConsumerException) {
      throw new VeniceException("Exception thrown by shared consumer", lastConsumerException);
    }

    if (null != lastDrainerException) {
      // Interrupt the whole ingestion task
      throw new VeniceException("Exception thrown by store buffer drainer", lastDrainerException);
    }

    if (null != lastProducerException) {
      throw new VeniceException("Exception thrown by kafka producer callback", lastProducerException);
    }

    /**
     * Check whether current consumer has any subscription or not since 'poll' function will throw
     * {@link IllegalStateException} with empty subscription.
     */
    if (!consumerHasSubscription()) {
      if (++idleCounter <= MAX_IDLE_COUNTER) {
        logger.warn(consumerTaskId + " No Partitions are subscribed to for store attempt " + idleCounter);
        if (usingSharedConsumer) {
          /**
           * The extended sleep is trying to reduce the contention since the consumer is shared and synchronized.
           * This is not ideal to have those branches, and later we could clean them up once the shared consumer
           * is adopted by default.
           */
          Thread.sleep(readCycleDelayMs * 20);
        } else {
          Thread.sleep(readCycleDelayMs);
        }
        return;
      }

      logger.warn(consumerTaskId + " No Partitions are subscribed to for store attempts expired after " + idleCounter);
      complete();
      return;
    }
    idleCounter = 0;

    if (emitMetrics.get()) {
      long currentQuota = storeRepository.getStoreOrThrow(storeName).getStorageQuotaInByte();
      storeIngestionStats.recordDiskQuotaAllowed(storeName, currentQuota);
    }
    if (usingSharedConsumer) {
      /**
       * While using the shared consumer, we still need to check hybrid quota here since the actual disk usage could change
       * because of compaction or the disk quota could be adjusted even there is no record write.
       * Since {@link #produceToStoreBufferServiceOrKafka} is only being invoked by {@link KafkaConsumerService} when there
       * are available records, this function needs to check whether we need to resume the consumption when there are
       * paused consumption because of hybrid quota violation.
       */
      boolean isHybridQuotaEnabled = storeRepository.getStoreOrThrow(storeName).isHybridStoreDiskQuotaEnabled();
      if (isHybridQuotaEnabled && hybridQuotaEnforcer.isPresent() && hybridQuotaEnforcer.get().hasPausedPartitionIngestion()) {
        hybridQuotaEnforcer.get().checkPartitionQuota(subscribedPartitionToSize.get());
      }
      Thread.sleep(readCycleDelayMs);
      return;
    }

    long beforePollingTimestamp = System.currentTimeMillis();

    boolean wasIdle = (recordCount == 0);
    List<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> records = consumerPoll(readCycleDelayMs);
    recordCount = 0;
    long estimatedSize = 0;
    for (ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords : records) {
      recordCount += ((consumerRecords == null) ? 0 : consumerRecords.count());
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

    for (ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords : records) {
      produceToStoreBufferServiceOrKafka(consumerRecords, true);
    }
  }

  /**
   * Polls the producer for new messages in an infinite loop by a dedicated consumer thread
   * and processes the new messages by current thread.
   */
  @Override
  public void run() {
    // Update thread name to include topic to make it easy debugging
    Thread.currentThread().setName("venice-consumer-" + kafkaVersionTopic);

    logger.info("Running " + consumerTaskId);
    try {
      /**
       * Consumers will be initialized lazily
       */
      versionedStorageIngestionStats.resetIngestionTaskErroredGauge(storeName, versionNumber);
      /**
       * Here we could not use isRunning() since it is a synchronized function, whose lock could be
       * acquired by some other synchronized function, such as {@link #kill()}.
        */
      while (isRunning()) {
        processConsumerActions();
        checkLongRunningTaskState();
        processMessages(serverConfig.isSharedConsumerPoolEnabled());
      }

      // If the ingestion task is stopped gracefully (server stops), persist processed offset to disk
      for (PartitionConsumptionState partitionConsumptionState : partitionConsumptionStateMap.values()) {
        /**
         * Now, there are two threads, which could potentially trigger {@link #syncOffset(String, PartitionConsumptionState)}:
         * 1. {@link com.linkedin.venice.kafka.consumer.StoreBufferService.StoreBufferDrainer}, which will checkpoint
         *    periodically;
         * 2. The main thread of ingestion task here, which will checkpoint when gracefully shutting down;
         *
         * We would like to make sure the syncOffset invocation is sequential with the message processing, so here
         * will try to drain all the messages before checkpointing.
         * Here is the detail::
         * If the checkpointing happens in different threads concurrently, there is no guarantee the atomicity of
         * offset and checksum, since the checksum could change in another thread, but the corresponding offset change
         * hasn't been applied yet, when checkpointing happens in current thread; and you can check the returned
         * {@link OffsetRecordTransformer} of {@link ProducerTracker#validateMessageAndGetOffsetRecordTransformer(ConsumerRecord, boolean, Optional)},
         * where `segment` could be changed by another message independent from current `offsetRecord`;
         */
        consumerUnSubscribe(kafkaVersionTopic, partitionConsumptionState);
        waitForAllMessageToBeProcessedFromTopicPartition(kafkaVersionTopic, partitionConsumptionState.getPartition(), partitionConsumptionState);
        syncOffset(kafkaVersionTopic, partitionConsumptionState);
      }

    } catch (VeniceIngestionTaskKilledException ke) {
      logger.info(consumerTaskId + " is killed, start to report to notifier.", ke);
      notificationDispatcher.reportKilled(partitionConsumptionStateMap.values(), ke);
    } catch (org.apache.kafka.common.errors.InterruptException | InterruptedException interruptException) {
      // Known exceptions during graceful shutdown of storage server. Report error only if the server is still running.
      if (isRunning()) {
        // Report ingestion failure if it is not caused by kill operation or server restarts.
        logger.error(consumerTaskId + " failed with InterruptException.", interruptException);
        notificationDispatcher.reportError(partitionConsumptionStateMap.values(),
            "Caught InterruptException during ingestion.", interruptException);
        storeIngestionStats.recordIngestionFailure(storeName);
        versionedStorageIngestionStats.setIngestionTaskErroredGauge(storeName, versionNumber);
      }
    } catch (Throwable t) {
      // After reporting error to controller, controller will ignore the message from this replica if job is aborted.
      // So even this storage node recover eventually, controller will not confused.
      // If job is not aborted, controller is open to get the subsequent message from this replica(if storage node was
      // recovered, it will send STARTED message to controller again)
      if (t instanceof Exception) {
        logger.error(consumerTaskId + " failed with Exception.", t);
        reportError(partitionConsumptionStateMap.values(), errorPartitionId,
            "Caught Exception during ingestion.", (Exception)t);
      } else {
        logger.error(consumerTaskId + " failed with Error!!!", t);
        reportError(partitionConsumptionStateMap.values(), errorPartitionId,
            "Caught non-exception Throwable during ingestion in " + getClass().getSimpleName() + "'s run() function.", new VeniceException(t));
      }
      storeIngestionStats.recordIngestionFailure(storeName);
      if (partitionConsumptionStateMap.values().stream().anyMatch(PartitionConsumptionState::isEndOfPushReceived)) {
        versionedStorageIngestionStats.setIngestionTaskErroredGauge(storeName, versionNumber);
      }
    } finally {
      internalClose();
    }
  }

  private void reportError(Collection<PartitionConsumptionState> pcsList, int partitionId, String message, Exception consumerEx) {
    if (pcsList.isEmpty()) {
      notificationDispatcher.reportError(partitionId, message, consumerEx);
    } else {
      notificationDispatcher.reportError(pcsList, message, consumerEx);
    }
  }

  private void internalClose() {
    // Only reset Offset Messages are important, subscribe/unSubscribe will be handled
    // on the restart by Helix Controller notifications on the new StoreIngestionTask.
    try {
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

      if (consumerMap == null || consumerMap.size() == 0) {
        // Consumer constructor error-ed out, nothing can be cleaned up.
        logger.warn("Error in consumer creation, skipping close for topic " + kafkaVersionTopic);
      } else {
        consumerMap.values().forEach(consumer -> consumer.close(everSubscribedTopics));
      }
    } catch (Exception e) {
      logger.error("Caught exception while trying to close the current ingestion task", e);
    }
    isRunning.set(false);
    logger.info("Store ingestion task for store: " + kafkaVersionTopic + " is closed");
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processConsumerActions() {
    long consumerActionStartTimeInNS = System.nanoTime();
    while (!consumerActionsQueue.isEmpty()) {
      // Do not want to remove a message from the queue unless it has been processed.
      ConsumerAction message = consumerActionsQueue.peek();
      try {
        message.incrementAttempt();
        processConsumerAction(message);
      } catch (InterruptedException e) {
        throw new VeniceIngestionTaskKilledException(kafkaVersionTopic, e);
      } catch (Exception ex) {
        if (message.getAttemptsCount() < MAX_CONTROL_MESSAGE_RETRIES) {
          logger.info("Error Processing message will retry later " + message , ex);
          return;
        } else {
          logger.error("Ignoring message:  " + message + " after retries " + message.getAttemptsCount(), ex);
        }
      }
      consumerActionsQueue.poll();
    }
    if (emitMetrics.get()) {
      storeIngestionStats.recordProcessConsumerActionLatency(storeName, LatencyUtils.getLatencyInMS(consumerActionStartTimeInNS));
    }
  }

  private void checkConsumptionStateWhenStart(OffsetRecord record, PartitionConsumptionState newPartitionConsumptionState) {
    int partition = newPartitionConsumptionState.getPartition();
    // Once storage node restart, send the "START" status to controller to rebuild the task status.
    // If this storage node has never consumed data from this topic, instead of sending "START" here, we send it
    // once START_OF_PUSH message has been read.
    if (record.getOffset() > 0) {
      Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
      if (storeVersionState.isPresent()) {
        boolean sorted = storeVersionState.get().sorted;
        /**
         * Put TopicSwitch message into in-memory state.
         */
        newPartitionConsumptionState.setTopicSwitch(storeVersionState.get().topicSwitch);
        /**
         * Notify the underlying store engine about starting batch push.
         */
        beginBatchWrite(partition, sorted, newPartitionConsumptionState);

        notificationDispatcher.reportRestarted(newPartitionConsumptionState);
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
      defaultReadyToServeChecker.apply(newPartitionConsumptionState);
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

  protected void processCommonConsumerAction(ConsumerActionType operation, String topic, int partition)
      throws InterruptedException {
    switch (operation) {
      case SUBSCRIBE:
        // Drain the buffered message by last subscription.
        storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);

        OffsetRecord record = storageMetadataService.getLastOffset(topic, partition);

        // First let's try to restore the state retrieved from the OffsetManager
        PartitionConsumptionState newPartitionConsumptionState = new PartitionConsumptionState(partition, record, hybridStoreConfig.isPresent(), isIncrementalPushEnabled);
        partitionConsumptionStateMap.put(partition,  newPartitionConsumptionState);
        record.getProducerPartitionStateMap().entrySet().stream().forEach(entry -> {
              GUID producerGuid = GuidUtils.getGuidFromCharSequence(entry.getKey());
              ProducerTracker producerTracker = kafkaDataIntegrityValidator.registerProducer(producerGuid);
              producerTracker.setPartitionState(partition, entry.getValue());
            }
        );

        checkConsumptionStateWhenStart(record, newPartitionConsumptionState);
        consumerSubscribe(topic, newPartitionConsumptionState, record.getOffset());
        logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition + " Offset "
            + record.getOffset());
        break;
      case UNSUBSCRIBE:
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
        PartitionConsumptionState consumptionState = partitionConsumptionStateMap.get(partition);
        if (consumptionState != null) {
          consumerUnSubscribe(topic, consumptionState);
        }
        // Drain the buffered message by last subscription.
        waitForAllMessageToBeProcessedFromTopicPartition(topic, partition, consumptionState);

        /**
         * Since the processing of the buffered messages are using {@link #partitionConsumptionStateMap} and
         * {@link #kafkaDataValidationService}, we would like to drain all the buffered messages before cleaning up those
         * two variables to avoid the race condition.
         */
        partitionConsumptionStateMap.remove(partition);
        kafkaDataIntegrityValidator.clearPartition(partition);
        break;
      case RESET_OFFSET:
        /**
         * After auditing all the calls that can result in the RESET_OFFSET action, it turns out we always unsubscribe
         * from the topic/partition before resetting offset, which is unnecessary; but we decided to keep this action
         * for now in case that in future, we do want to reset the consumer without unsubscription.
         */
        if (partitionConsumptionStateMap.containsKey(partition) && consumerHasSubscription(topic, partitionConsumptionStateMap.get(partition))) {
          logger.error("This shouldn't happen since unsubscription should happen before reset offset for topic: " + topic + ", partition: " + partition);
          /**
           * Only update the consumer and partitionConsumptionStateMap when consumer actually has
           * subscription to this topic/partition; otherwise, we would blindly update the StateMap
           * and mess up other operations on the StateMap.
           */
          try {
            consumerResetOffset(topic, partitionConsumptionStateMap.get(partition));
            logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition);
          } catch (UnsubscribedTopicPartitionException e) {
            logger.error(consumerTaskId + " Kafka consumer should have subscribed to the partition already but it fails "
                + "on resetting offset for Topic: " + topic + " Partition Id: " + partition);
          }
          partitionConsumptionStateMap.put(partition,
              new PartitionConsumptionState(partition, new OffsetRecord(partitionStateSerializer), hybridStoreConfig.isPresent(), isIncrementalPushEnabled));
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
        throw new InterruptedException("Received the signal to kill this consumer. Topic " + topic);
      default:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + getClass().getName());
    }
  }

  protected abstract void checkLongRunningTaskState() throws InterruptedException;
  protected abstract void processConsumerAction(ConsumerAction message) throws InterruptedException;
  protected abstract String getBatchWriteSourceAddress();

  /**
   * Common record check for different state models:
   * check whether server continues receiving messages after EOP for a batch-only store.
   */
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    int partitionId = record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);

    if (null == partitionConsumptionState) {
      logger.info("Topic " + kafkaVersionTopic + " Partition " + partitionId + " has been unsubscribed, skip this record that has offset " + record.offset());
      return false;
    }

    if (partitionConsumptionState.isErrorReported()) {
      logger.info("Topic " + kafkaVersionTopic + " Partition " + partitionId + " is already errored, skip this record that has offset " + record.offset());
      return false;
    }

    if (partitionConsumptionState.isEndOfPushReceived() &&
        partitionConsumptionState.isBatchOnly()) {

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
      /**
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
    return true;
  }

  protected boolean shouldPersistRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record,
      PartitionConsumptionState partitionConsumptionState) {
    int partitionId = record.partition();
    String msg;
    if (null == partitionConsumptionState) {
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

    return true;
  }

  /**
   * This function will be invoked in {@link StoreBufferService} to process buffered {@link ConsumerRecord}.
   * @param record
   */
  public void processConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record, ProducedRecord producedRecord) throws InterruptedException {
    int partition = record.partition();
    // The partitionConsumptionStateMap can be modified by other threads during consumption (for example when unsubscribing)
    // in order to maintain thread safety, we hold onto the reference to the partitionConsumptionState and pass that
    // reference to all downstream methods so that all offset persistence operations use the same partitionConsumptionState
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    if (!shouldPersistRecord(record, partitionConsumptionState)) {
      return;
    }

    int recordSize = 0;
    try {
      recordSize = internalProcessConsumerRecord(record, partitionConsumptionState, producedRecord);
    } catch (FatalDataValidationException e) {
      int faultyPartition = record.partition();
      String errorMessage = "Fatal data validation problem with partition " + faultyPartition + ", offset " + record.offset();
      // TODO need a way to safeguard DIV errors from backup version that have once been current (but not anymore) during re-balancing
      boolean needToUnsub = !(isCurrentVersion.getAsBoolean() || partitionConsumptionState.isEndOfPushReceived());
      if (needToUnsub) {
        errorMessage +=  ". Consumption will be halted.";
        notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), errorMessage, e);
        unSubscribePartition(kafkaVersionTopic, faultyPartition);
      } else {
        logger.info(errorMessage + ". However, " + kafkaVersionTopic
            + " is the current version or EOP is already received so consumption will continue.");
      }
    } catch (VeniceMessageException | UnsupportedOperationException excp) {
      throw new VeniceException(consumerTaskId + " : Received an exception for message at partition: "
          + record.partition() + ", offset: " + record.offset() + ". Bubbling up.", excp);
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
      // Report metrics
      if (emitMetrics.get()) {
        storeIngestionStats.recordBytesConsumed(storeName, processedRecordSize);
        storeIngestionStats.recordRecordsConsumed(storeName, processedRecordNum);
      }

      partitionConsumptionState.resetProcessedRecordNum();
      partitionConsumptionState.resetProcessedRecordSize();
    }

    partitionConsumptionState.incrementProcessedRecordSizeSinceLastSync(recordSize);
    long syncBytesInterval = partitionConsumptionState.isDeferredWrite() ? databaseSyncBytesIntervalForDeferredWriteMode
        : databaseSyncBytesIntervalForTransactionalMode;

    // If the following condition is true, then we want to sync to disk.
    boolean recordsProcessedAboveSyncIntervalThreshold = false;
    if (syncBytesInterval > 0 && (partitionConsumptionState.getProcessedRecordSizeSinceLastSync() >= syncBytesInterval)) {
      recordsProcessedAboveSyncIntervalThreshold = true;
      syncOffset(kafkaVersionTopic, partitionConsumptionState);
    }

    reportIfCatchUpBaseTopicOffset(partitionConsumptionState);

    // Check whether it's ready to serve
    defaultReadyToServeChecker.apply(partitionConsumptionState, recordsProcessedAboveSyncIntervalThreshold);
  }

  private void syncOffset(String topic, PartitionConsumptionState ps) {
    int partition = ps.getPartition();
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(topic);
    if (storageEngine == null) {
      logger.warn("Storage engine has been removed. Could not execute sync offset for topic: " + topic + " and partition: " + partition);
      return;
    }
    Map<String, String> dbCheckpointingInfo = storageEngine.sync(partition);
    OffsetRecord offsetRecord = ps.getOffsetRecord();
    /**
     * The reason to transform the internal state only during checkpointing is that
     * the intermediate checksum generation is an expensive operation.
     * See {@link ProducerTracker#validateMessageAndGetOffsetRecordTransformer(ConsumerRecord, boolean, Optional)}
     * to find more details.
     */
    offsetRecord.transform();
    // Check-pointing info required by the underlying storage engine
    offsetRecord.setDatabaseInfo(dbCheckpointingInfo);
    storageMetadataService.put(this.kafkaVersionTopic, partition, offsetRecord);
    ps.resetProcessedRecordSizeSinceLastSync();
    logger.info("Synced offset: " + offsetRecord.getOffset() + " for " + topic + " of partition: " + partition);
  }

  public void setLastDrainerException(Exception e) {
    lastDrainerException = e;
  }

  public void setLastConsumerException(Exception e) {
    lastConsumerException = e;
  }

  public void setLastProducerException(Exception e) {
    lastProducerException = e;
  }

  /**
   * @return the total lag for all subscribed partitions between the real-time buffer topic and this consumption task.
   */
  public long getRealTimeBufferOffsetLag() {
    if (!hybridStoreConfig.isPresent()) {
      return METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES.code;
    }

    Optional<StoreVersionState> svs = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!svs.isPresent()) {
      return STORE_VERSION_STATE_UNAVAILABLE.code;
    }

    if (partitionConsumptionStateMap.isEmpty()) {
      return NO_SUBSCRIBED_PARTITION.code;
    }

    long offsetLag = partitionConsumptionStateMap.values().stream()
        .filter(pcs -> pcs.isEndOfPushReceived() &&
            pcs.getOffsetRecord().getStartOfBufferReplayDestinationOffset().isPresent())
        .mapToLong(pcs -> measureHybridOffsetLag(pcs, false))
        .sum();

    return minZeroLag(offsetLag);
  }

  /**
   * Measure the offset lag between follower and leader
   */
  public abstract long getFollowerOffsetLag();

  public abstract int getWriteComputeErrorCode();

  public long getOffsetLagThreshold() {
    if (!hybridStoreConfig.isPresent()) {
      return METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES.code;
    }

    return hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
  }

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

  /**
   * Indicate the number of partitions that haven't received SOBR yet. This method is for Hybrid store
   */
  public long getNumOfPartitionsNotReceiveSOBR() {
    if (!hybridStoreConfig.isPresent()) {
      return METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES.code;
    }

    if (partitionConsumptionStateMap.isEmpty()) {
      return NO_SUBSCRIBED_PARTITION.code;
    }

    return partitionConsumptionStateMap.values().stream()
        .filter(pcs -> !pcs.getOffsetRecord().getStartOfBufferReplayDestinationOffset().isPresent()).count();
  }

  public boolean isHybridMode() {
    if (hybridStoreConfig == null) {
      logger.error("hybrid config shouldn't be null. Something bad happened. Topic: " + getVersionTopic());
    }
    return hybridStoreConfig != null && hybridStoreConfig.isPresent();
  }

  protected void processStartOfPush(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) {
    StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
    /**
     * Notify the underlying store engine about starting batch push.
     */
    beginBatchWrite(partition, startOfPush.sorted, partitionConsumptionState);

    notificationDispatcher.reportStarted(partitionConsumptionState);
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (!storeVersionState.isPresent()) {
      // No other partition of the same topic has started yet, let's initialize the StoreVersionState
      StoreVersionState newStoreVersionState = new StoreVersionState();
      newStoreVersionState.sorted = startOfPush.sorted;
      newStoreVersionState.chunked = startOfPush.chunked;
      newStoreVersionState.compressionStrategy = startOfPush.compressionStrategy;
      newStoreVersionState.compressionDictionary = startOfPush.compressionDictionary;
      storageMetadataService.put(kafkaVersionTopic, newStoreVersionState);
      // Update chunking flag in VeniceWriter
      if (startOfPush.chunked && veniceWriter != null) {
        veniceWriter.updateChunckingEnabled(startOfPush.chunked);
      }
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

  protected void processEndOfPush(ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {

    // We need to keep track of when the EOP happened, as that is used within Hybrid Stores' lag measurement
    partitionConsumptionState.getOffsetRecord().endOfPushReceived(offset);

    /**
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

    /**
     * It's a bit of tricky here. Since the offset is not updated yet, it's actually previous offset reported
     * here.
     * TODO: sync up offset before invoking dispatcher
     */
    notificationDispatcher.reportEndOfPushReceived(partitionConsumptionState);
  }

  private void processStartOfBufferReplay(ControlMessage controlMessage, long offset, PartitionConsumptionState partitionConsumptionState) {
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
    if (storeVersionState.isPresent()) {
      // Update StoreVersionState, if necessary
      StartOfBufferReplay startOfBufferReplay = (StartOfBufferReplay) controlMessage.controlMessageUnion;
      if (null == storeVersionState.get().startOfBufferReplay) {
        // First time we receive a SOBR
        storeVersionState.get().startOfBufferReplay = startOfBufferReplay;
        storageMetadataService.put(kafkaVersionTopic, storeVersionState.get());
      } else if (!Utils.listEquals(storeVersionState.get().startOfBufferReplay.sourceOffsets, startOfBufferReplay.sourceOffsets)) {
        // Something very wrong is going on ): ...
        throw new VeniceException("Unexpected: received multiple " + ControlMessageType.START_OF_BUFFER_REPLAY.name() +
            " control messages with inconsistent 'startOfBufferReplay.offsets' fields within the same topic!" +
            "\nPrevious SOBR: " + storeVersionState.get().startOfBufferReplay.sourceOffsets +
            "\nIncoming SOBR: " + startOfBufferReplay.sourceOffsets);
      }

      partitionConsumptionState.getOffsetRecord().setStartOfBufferReplayDestinationOffset(offset);
      notificationDispatcher.reportStartOfBufferReplayReceived(partitionConsumptionState);
    } else {
      // TODO: If there are cases where this can legitimately happen, then we need a less stringent reaction here...
      throw new VeniceException("Unexpected: received some " + ControlMessageType.START_OF_BUFFER_REPLAY.name() +
          " control message in a topic where we have not yet received a " +
          ControlMessageType.START_OF_PUSH.name() + " control message.");
    }
  }

  protected void processStartOfIncrementalPush(ControlMessage startOfIncrementalPush, int partition, long upstreamOffset,
      PartitionConsumptionState partitionConsumptionState) {
    CharSequence startVersion = ((StartOfIncrementalPush) startOfIncrementalPush.controlMessageUnion).version;
    IncrementalPush newIncrementalPush = new IncrementalPush();
    newIncrementalPush.version = startVersion;

    partitionConsumptionState.setIncrementalPush(newIncrementalPush);
    notificationDispatcher.reportStartOfIncrementalPushReceived(partitionConsumptionState, startVersion.toString());
  }

  protected void processEndOfIncrementalPush(ControlMessage endOfIncrementalPush, int partition, long upstreamOffset,
      PartitionConsumptionState partitionConsumptionState) {
    // TODO: it is possible that we could turn incremental store to be read-only when incremental push is done
    CharSequence endVersion = ((EndOfIncrementalPush) endOfIncrementalPush.controlMessageUnion).version;
    // Reset incremental push version
    partitionConsumptionState.setIncrementalPush(null);
    notificationDispatcher.reportEndOfIncrementalPushRecived(partitionConsumptionState, endVersion.toString());
  }

  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    throw new VeniceException(ControlMessageType.TOPIC_SWITCH.name() + " control message should not be received in"
        + "Online/Offline state model. Topic " + kafkaVersionTopic + " partition " + partition);
  }

  /**
   * In this method, we pass both offset and partitionConsumptionState(ps). The reason behind it is that ps's
   * offset is stale and is not updated until the very end
   */
  private ControlMessageType processControlMessage(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState)
      throws InterruptedException {
    /**
     * If leader consumes control messages from topics other than version topic, it should produce
     * them to version topic; however, START_OF_SEGMENT and END_OF_SEGMENT should not be forwarded
     * because the leader producer will keep track of its own segment state; besides, leader/follower
     * model will not encounter START_OF_BUFFER_REPLAY because TOPIC_SWITCH will replace it in L/F
     * model; incremental push is also a mutually exclusive feature with hybrid stores.
     */
    ControlMessageType type = ControlMessageType.valueOf(controlMessage);
    logger.info(consumerTaskId + " : Received " + type.name() + " control message. Partition: " + partition + ", Offset: " + offset);
    switch(type) {
      case START_OF_PUSH:
        processStartOfPush(controlMessage, partition, offset, partitionConsumptionState);
        break;
      case END_OF_PUSH:
        processEndOfPush(controlMessage, partition, offset, partitionConsumptionState);
        break;
      case START_OF_SEGMENT:
      case END_OF_SEGMENT:
        /**
         * Nothing to do here as all of the processing is being done in {@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}.
         */
        break;
      case START_OF_BUFFER_REPLAY:
        processStartOfBufferReplay(controlMessage, offset, partitionConsumptionState);
       break;
      case START_OF_INCREMENTAL_PUSH:
        processStartOfIncrementalPush(controlMessage, partition, offset, partitionConsumptionState);
        break;
      case END_OF_INCREMENTAL_PUSH:
        processEndOfIncrementalPush(controlMessage, partition, offset, partitionConsumptionState);
        break;
      case TOPIC_SWITCH:
        processTopicSwitch(controlMessage, partition, offset, partitionConsumptionState);
        break;
      default:
        throw new UnsupportedMessageTypeException("Unrecognized Control message type " + controlMessage.controlMessageType);
    }

    return type;
  }

  /**
   * Update the metadata in OffsetRecord, including "offset" which indicates the last successfully consumed message offset
   * of version topic, "leaderOffset" which indicates the last successfully consumed message offset from leader topic.
   */
  protected abstract void updateOffset(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord, ProducedRecord producedRecord);

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param consumerRecord {@link ConsumerRecord} consumed from Kafka.
   * @return the size of the data written to persistent storage.
   */
  private int internalProcessConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
                                            PartitionConsumptionState partitionConsumptionState, ProducedRecord producedRecord) throws InterruptedException {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int sizeOfPersistedData = 0;
    boolean syncOffset = false;

    Optional<OffsetRecordTransformer> offsetRecordTransformer = Optional.empty();
    try {
      // Assumes the timestamp on the ConsumerRecord is the broker's timestamp when it received the message.
      recordWriterStats(kafkaValue.producerMetadata.messageTimestamp, consumerRecord.timestamp(),
          System.currentTimeMillis(), partitionConsumptionState);
      FatalDataValidationException e = null;
      boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();

      /**
       * DIV happens in drainer thread in following case.
       *  1. O/O model: All messages received from any topic.  ProducedRecord will be null.
       *  2. L/F model: Follower: All messages received from any topic. ProducedRecord will be null.
       *  3. L/F model: Leader: All messages received from VT before it switches to consume from RT.
       *
       * DIV happens in consumer thread in following case {@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}
       *  4. L/F model: Leader: All messages received from RT.
       *
       *  Specific notes for Leader:
       *  1.  For DIV pass through mode we are doing DIV in drainer thread with ConsumerRecord only ( ProducedRecord will NOT be null,
       *      and there is 1-1 mapping between consumerRecord and producedRecord). This will implicitely verify all kafka callbacks were
       *      executed properly and in order becuase we queue this pair <consumerRecord, producerRecord> from kafka callback thread only.
       *      If there were any problems with callback thread then that will be caught by this DIV here.
       *
       *  2. For DIV non pass-through mode (RT messages) we are doing DIV in {@link LeaderFollowerStoreIngestionTask#hasProducedToKafka(ConsumerRecord)}
       *      in consumer thread and no further DIV happens for the producedRecord. The main challenege to do DIV for producedRecord
       *      here is that VeniceWriter internally produces SOS/EOS for non-passthrough mode which is not fed into our DIV pipiline currently.
       *      TODO: Explore DIV check for produced record in non pass-through mode which also validates the kafka producer calllback like pass through mode.
       */
      if (producedRecord == null || !Version.isRealTimeTopic(consumerRecord.topic())) {
        try {
          offsetRecordTransformer = validateMessage(consumerRecord, endOfPushReceived);
          versionedDIVStats.recordSuccessMsg(storeName, versionNumber);
        } catch (FatalDataValidationException fatalException) {
          divErrorMetricCallback.get().execute(fatalException);
          if (endOfPushReceived) {
            e = fatalException;
          } else {
            throw fatalException;
          }
        }
      }
      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (producedRecord == null ? (ControlMessage) kafkaValue.payloadUnion : (ControlMessage) producedRecord.getValueUnion());
        ControlMessageType controlMessageType = processControlMessage(controlMessage, consumerRecord.partition(),
            consumerRecord.offset(), partitionConsumptionState);
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
         *
         * We shouldn't encounter {@link ProducerTracker.DataFaultType.UNREGISTERED_PRODUCER} since the logic below is
         * tracking {@link OffsetRecordTransformer} per producer GUID.
         */
        if (controlMessageType != ControlMessageType.START_OF_SEGMENT &&
            controlMessageType != ControlMessageType.END_OF_SEGMENT) {
          syncOffset = true;
        }
      } else if (null == kafkaValue) {
        throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message. Partition " +
              consumerRecord.partition() + " Offset " + consumerRecord.offset());
      } else {
        Pair<Integer, Integer> kvSize = processVeniceMessage(consumerRecord, partitionConsumptionState, producedRecord);
        int keySize = kvSize.getFirst();
        int valueSize = kvSize.getSecond();
        if (emitMetrics.get()) {
          storeIngestionStats.recordKeySize(storeName, keySize);
          storeIngestionStats.recordValueSize(storeName, valueSize);
        }
        sizeOfPersistedData = keySize + valueSize;
      }

      if (e != null) {
        throw e;
      }
    } catch (DuplicateDataException e) {
      versionedDIVStats.recordDuplicateMsg(storeName, versionNumber);
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

      if (offsetRecordTransformer.isPresent()) {
        /**
         * The reason to transform the internal state only during checkpointing is that
         * the intermediate checksum generation is an expensive operation.
         * See {@link ProducerTracker#validateMessageAndGetOffsetRecordTransformer(ConsumerRecord, boolean, Optional)}
         * to find more details.
         */
        offsetRecord.addOffsetRecordTransformer(kafkaValue.producerMetadata.producerGUID, offsetRecordTransformer.get());
      } /** else, {@link #validateMessage(int, KafkaKey, KafkaMessageEnvelope)} threw a {@link DuplicateDataException} */

      // Potentially update the offset metadata in the OffsetRecord
      updateOffset(partitionConsumptionState, offsetRecord, consumerRecord, producedRecord);

      partitionConsumptionState.setOffsetRecord(offsetRecord);
      if (syncOffset) {
        syncOffset(kafkaVersionTopic, partitionConsumptionState);
      }
    }
    return sizeOfPersistedData;
  }

  protected void recordWriterStats(long producerTimestampMs, long brokerTimestampMs, long consumerTimestampMs,
      PartitionConsumptionState partitionConsumptionState) {
    long producerBrokerLatencyMs = Math.max(brokerTimestampMs - producerTimestampMs, 0);
    long brokerConsumerLatencyMs = Math.max(consumerTimestampMs - brokerTimestampMs, 0);
    long producerConsumerLatencyMs = Math.max(consumerTimestampMs - producerTimestampMs, 0);
    versionedDIVStats.recordProducerBrokerLatencyMs(storeName, versionNumber, producerBrokerLatencyMs);
    versionedDIVStats.recordBrokerConsumerLatencyMs(storeName, versionNumber, brokerConsumerLatencyMs);
    versionedDIVStats.recordProducerConsumerLatencyMs(storeName, versionNumber,
        producerConsumerLatencyMs);
  }

  /**
   * The {@param topicName} maybe different from the store version topic, since in {@link LeaderFollowerStoreIngestionTask},
   * the topic could be other topics, such as RT topic or stream reprocessing topic.
   **/
  protected Optional<OffsetRecordTransformer> validateMessage(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      boolean endOfPushReceived) {
    Optional<ProducerTracker.DIVErrorMetricCallback> errorCallback = divErrorMetricCallback;
    try {
      boolean tolerateMissingMessage = endOfPushReceived;
      if (topicWithLogCompaction.contains(consumerRecord.topic())) {
        /**
         * For log-compacted topic, no need to report error metric when message missing issue happens.
         */
        errorCallback = Optional.empty();
        tolerateMissingMessage = true;
      }
      return Optional.of(kafkaDataIntegrityValidator.validateMessageAndGetOffsetRecordTransformer(consumerRecord, tolerateMissingMessage, errorCallback));
    } catch (FatalDataValidationException e) {
      /**
       * Check whether Kafka compaction is enabled in current topic.
       * This function shouldn't be invoked very frequently since {@link ProducerTracker#validateMessageAndGetOffsetRecordTransformer(ConsumerRecord, boolean, Optional)}
       * will update the sequence id if it could tolerate the missing messages.
       *
       * If it is not this case, we need to revisit this logic since this operation is expensive.
       */
      if (! topicManager.isTopicCompactionEnabled(consumerRecord.topic())) {
        /**
         * Not enabled, then bubble up the exception.
         * We couldn't cache this in {@link topicWithLogCompaction} since the log compaction could be enabled in the same topic later.
         */
        logger.error("Encountered DIV error when topic: " + consumerRecord.topic() + " doesn't enable log compaction");
        throw e;
      }
      topicWithLogCompaction.add(consumerRecord.topic());
      /**
       * Since Kafka compaction is enabled, DIV will start tolerating missing messages.
       */
      logger.info("Encountered DIV exception when consuming from topic: " + consumerRecord.topic(), e);
      logger.info("Kafka compaction is enabled in topic: " + consumerRecord.topic() + ", so DIV will tolerate missing message in the future");
      if (e instanceof ImproperlyStartedSegmentException) {
        /**
         * ImproperlyStartedSegmentException is not retriable since internally it has already updated the sequence id of current
         * segment. Retry will throw {@link DuplicateDataException}.
         * So, this function will return empty here, which is being handled properly by the caller {@link #internalProcessConsumerRecord}.
         */
        return Optional.empty();
      } else {
        /**
         * Verify the message again.
         * The assumption here is that {@link ProducerTracker#validateMessageAndGetOffsetRecordTransformer(ConsumerRecord, boolean, Optional)}
         * won't update the sequence id of the current segment if it couldn't tolerate missing messages.
         * In this case, no need to report error metric.
         */
        return Optional.of(kafkaDataIntegrityValidator.validateMessageAndGetOffsetRecordTransformer(consumerRecord, true, Optional.empty()));
      }
    }
  }

  /**
   * Write to the storage engine with the optimization that we leverage the padding in front of the {@param putValue}
   * in order to insert the {@param schemaId} there. This avoids a byte array copy, which can be beneficial in terms
   * of GC.
   */
  protected void prependHeaderAndWriteToStorageEngine(String topic, int partition, byte[] keyBytes, ByteBuffer putValue, int schemaId) {
    /**
     * Since {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer} reuses the original byte
     * array, which is big enough to pre-append schema id, so we just reuse it to avoid unnecessary byte array allocation.
     * This value encoding scheme is used in {@link PartitionConsumptionState#maybeUpdateExpectedChecksum(byte[], Put)} to
     * calculate checksum for all the kafka PUT messages seen so far. Any change here needs to be reflected in that function.
     */
    if (putValue.position() < SCHEMA_HEADER_LENGTH) {
      throw new VeniceException("Start position of 'putValue' ByteBuffer shouldn't be less than " + SCHEMA_HEADER_LENGTH);
    } else {
      // Back up the original 4 bytes
      putValue.position(putValue.position() - SCHEMA_HEADER_LENGTH);
      int backupBytes = putValue.getInt();
      putValue.position(putValue.position() - SCHEMA_HEADER_LENGTH);
      ByteUtils.writeInt(putValue.array(), schemaId, putValue.position());

      writeToStorageEngine(storageEngineRepository.getLocalStorageEngine(topic), partition, keyBytes, putValue);

      /** We still want to recover the original position to make this function idempotent. */
      putValue.putInt(backupBytes);
    }
  }

  private void writeToStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes, ByteBuffer putValue) {
    long putStartTimeNs = System.nanoTime();
    storageEngine.put(partition, keyBytes, putValue);
    if (logger.isTraceEnabled()) {
      logger.trace(consumerTaskId + " : Completed PUT to Store: " + kafkaVersionTopic + " in " +
          (System.nanoTime() - putStartTimeNs) + " ns at " + System.currentTimeMillis());
    }
    if (emitMetrics.get()) {
      storeIngestionStats.recordStorageEnginePutLatency(storeName, LatencyUtils.getLatencyInMS(putStartTimeNs));
    }
  }

  /**
   * N.B.:
   *    With L/F+native replication and many Leader partitions getting assigned to a single SN this function may be called
   *    from multiple thread simultaneously to initialize the veniceWriter during start of batch push. So it needs to be thread safe
   *    and provide initialization guarantee that only one instance of veniceWriter is created for the entire ingestion task.
   *
   * @return the instance of {@link VeniceWriter}, lazily initialized if necessary.
   */
  protected VeniceWriter getVeniceWriter() {
    if (null == veniceWriter) {
      synchronized (this) {
        if (null == veniceWriter) {
          Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(kafkaVersionTopic);
          if (storeVersionState.isPresent()) {
            veniceWriter = veniceWriterFactory.createBasicVeniceWriter(kafkaVersionTopic, storeVersionState.get().chunked);
          } else {
            /**
             * In general, a partition in version topic follows this pattern:
             * {Start_of_Segment, Start_of_Push, End_of_Segment, Start_of_Segment, data..., End_of_Segment, Start_of_Segment, End_of_Push, End_of_Segment}
             * Therefore, in native replication where leader needs to producer all messages it consumes from remote, the first
             * message that leader consumes is not SOP, in this case, leader doesn't know whether chunking is enabled.
             *
             * Notice that the pattern is different in stream reprocessing which contains a lot more segments and is also
             * different in some test cases which reuse the same VeniceWriter.
             */
            veniceWriter = veniceWriterFactory.createBasicVeniceWriter(kafkaVersionTopic);
          }
        }
      }
    }
    return veniceWriter;
  }

  /**
   * All the subscriptions belonging to the same {@link StoreIngestionTask} SHOULD use this function instead of
   * {@link KafkaConsumerWrapper#subscribe} directly since we need to let {@link KafkaConsumerService} knows which
   * {@link StoreIngestionTask} to use when receiving result for any specific topic.
   * @param topic
   * @param partition
   */
  protected synchronized void subscribe(KafkaConsumerWrapper consumer, String topic, int partition, long offset) {
    if (serverConfig.isSharedConsumerPoolEnabled() && consumer instanceof SharedKafkaConsumer) {
      /**
       * We need to let {@link KafkaConsumerService} know that the messages from this topic will be handled by the
       * current {@link StoreIngestionTask}.
       */
      ((SharedKafkaConsumer) consumer).getKafkaConsumerService().attach(consumer, topic, this);
    }
    consumer.subscribe(topic, partition, offset);
    everSubscribedTopics.add(topic);
  }

  public boolean consumerHasSubscription() {
    return consumerMap.values().stream().anyMatch(consumer -> consumer.hasSubscription(everSubscribedTopics));
  }

  public boolean consumerHasSubscription(String topic, PartitionConsumptionState partitionConsumptionState) {
    KafkaConsumerWrapper consumer = getConsumer(partitionConsumptionState);
    return consumer.hasSubscription(topic, partitionConsumptionState.getPartition());
  }

  public void consumerUnSubscribe(String topic, PartitionConsumptionState partitionConsumptionState) {
    KafkaConsumerWrapper consumer = getConsumer(partitionConsumptionState);
    consumer.unSubscribe(topic, partitionConsumptionState.getPartition());
  }

  public void consumerSubscribe(String topic, PartitionConsumptionState partitionConsumptionState, long offset) {
    KafkaConsumerWrapper consumer = getConsumer(partitionConsumptionState);
    subscribe(consumer, topic, partitionConsumptionState.getPartition(), offset);
  }

  public void consumerResetOffset(String topic, PartitionConsumptionState partitionConsumptionState) {
    KafkaConsumerWrapper consumer = getConsumer(partitionConsumptionState);
    consumer.resetOffset(topic, partitionConsumptionState.getPartition());
  }

  public List<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> consumerPoll(long pollTimeout) {
    // TODO: remove this check after verifying the spam log fix
    if (!storeRepository.getStoreOrThrow(storeName).getVersion(versionNumber).isPresent()) {
      throw new VeniceException("Version " + versionNumber + " does not exist. Should not poll.");
    }
    List<ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> records = new ArrayList<>(consumerMap.size());
    consumerMap.values().stream().forEach(consumer -> {
      if (consumer.hasSubscription()) {
        ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = consumer.poll(pollTimeout);
        if (consumerRecords != null) {
          records.add(consumerRecords);
        }
      }
    });
    return records;
  }

  public KafkaConsumerWrapper getConsumer(PartitionConsumptionState partitionConsumptionState) {
    String kafkaSourceAddress = partitionConsumptionState.consumeRemotely() ? getBatchWriteSourceAddress()
        : this.kafkaProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    return consumerMap.computeIfAbsent(kafkaSourceAddress, source -> {
      Properties consumerProps = updateKafkaConsumerProperties(kafkaProps, kafkaSourceAddress, partitionConsumptionState.consumeRemotely());
      if (serverConfig.isSharedConsumerPoolEnabled()) {
        return aggKafkaConsumerService.getConsumer(consumerProps,this);
      } else {
        return factory.getConsumer(consumerProps);
      }
    });
  }

  /**
   * @return the size of the data which was written to persistent storage.
   */
  private Pair<Integer, Integer> processVeniceMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      PartitionConsumptionState partitionConsumptionState, ProducedRecord producedRecord) {
    int keyLen = 0;
    int valueLen = 0;
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int partition = consumerRecord.partition();
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);

    byte[] keyBytes;

    MessageType messageType =
        (producedRecord == null ? MessageType.valueOf(kafkaValue) : producedRecord.getMessageType());
    switch (messageType) {
      case PUT:
        // If single-threaded, we can re-use (and clobber) the same Put instance. // TODO: explore GC tuning later.
        Put put;
        if (producedRecord == null) {
          keyBytes = kafkaKey.getKey();
          put = (Put) kafkaValue.payloadUnion;
        } else {
          keyBytes = producedRecord.getKeyBytes();
          put = (Put) producedRecord.getValueUnion();
        }
        ByteBuffer putValue = put.putValue;
        valueLen = putValue.remaining();
        keyLen = keyBytes.length;

        //update checksum for this PUT message if needed.
        partitionConsumptionState.maybeUpdateExpectedChecksum(keyBytes, put);
        prependHeaderAndWriteToStorageEngine(kafkaVersionTopic, partition, keyBytes, putValue, put.schemaId);
        break;

      case DELETE:
        if (producedRecord == null) {
          keyBytes = kafkaKey.getKey();
        } else {
          keyBytes = producedRecord.getKeyBytes();
        }
        keyLen = keyBytes.length;

        long deleteStartTimeNs = System.nanoTime();
        storageEngine.delete(partition, keyBytes);

        if (logger.isTraceEnabled()) {
          logger.trace(
              consumerTaskId + " : Completed DELETE to Store: " + kafkaVersionTopic + " in " + (System.nanoTime()
                  - deleteStartTimeNs) + " ns at " + System.currentTimeMillis());
        }
        break;

      case UPDATE:
        throw new VeniceMessageException(
            consumerTaskId + ": Not expecting UPDATE message from  Topic: " + consumerRecord.topic() + " Partition: "
                + partition + ", Offset: " + consumerRecord.offset());
      default:
        throw new VeniceMessageException(
            consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }

    /**
     * Potentially clean the mapping from transient record map. consumedOffset may be -1 when individual chunks are getting
     * produced to drainer queue from kafka callback thread {@link LeaderFollowerStoreIngestionTask#LeaderProducerMessageCallback}
     */
    if (purgeTransientRecordCache && isWriteComputationEnabled && partitionConsumptionState.isEndOfPushReceived()
        && producedRecord != null && producedRecord.getConsumedOffset() != -1) {
      PartitionConsumptionState.TransientRecord transientRecord =
          partitionConsumptionState.mayRemoveTransientRecord(producedRecord.getConsumedOffset(), kafkaKey.getKey());
      if (transientRecord != null) {
        //This is perfectly fine, logging to see how often it happens where we get mulitple put/update/delete for same key in quick succession.
        String msg =
            consumerTaskId + ": multiple put,update,delete for same key received from Topic: " + consumerRecord.topic()
                + " Partition: " + partition;
        if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
          logger.info(msg);
        }
      }
    }

    return Pair.create(keyLen, valueLen);
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
        break;
      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(update.schemaId);
        break;
      case DELETE:
        /** we don't need to check schema availability for DELETE */
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

    waitValueSchemaAvailable(schemaId);
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

  private void waitValueSchemaAvailable(int schemaId) throws InterruptedException {
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

  Collection<KafkaConsumerWrapper> getConsumer() {
    return consumerMap.values();
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

  public Optional<Long> getCurrentOffset(int partitionId) {
    PartitionConsumptionState consumptionState = partitionConsumptionStateMap.get(partitionId);
    if (null == consumptionState) {
      return Optional.empty();
    }
    return Optional.of(consumptionState.getOffsetRecord().getOffset());
  }

  /**
   * To check whether the given partition is still consuming message from Kafka
   * @param partitionId
   * @return
   */
  public boolean isPartitionConsuming(int partitionId) {
    return partitionConsumptionStateMap.containsKey(partitionId);
  }

  /**
   * Refill subscribedPartitionToSize map with the batch records size for each subscribed partition
   * Even when @param records are empty, we should still call this method. As stalled partitions
   * won't appear in @param records but the disk might have more space available for them now and
   * these partitions need to be resumed.
   * @param records
   */
  private void refillPartitionToSizeMap(List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> records) {
    subscribedPartitionToSize.get().clear();
    for (int partition : partitionConsumptionStateMap.keySet()) {
      subscribedPartitionToSize.get().put(partition, 0);
    }
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
      int partition = record.partition();
      int recordSize = record.serializedKeySize() + record.serializedValueSize();
      subscribedPartitionToSize.get().put(partition,
          subscribedPartitionToSize.get().getOrDefault(partition, 0) + recordSize);
    }
  }

  /**
   * Override the {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} config with a remote Kafka bootstrap url.
   */
  protected Properties updateKafkaConsumerProperties(Properties localConsumerProps, String remoteKafkaSourceAddress, boolean consumeRemotely) {
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

          AbstractStorageEngine<?> engine = storageEngineRepository.getLocalStorageEngine(kafkaVersionTopic);
          if (store.isHybrid()) {
            if (engine == null) {
              logger.warn("Storage engine " + kafkaVersionTopic + " was removed before reopening");
            } else {
              logger.info("Reopen partition " + kafkaVersionTopic + "_" + partition + " for reading after ready-to-serve.");
              engine.preparePartitionForReading(partition);
            }
          }
          if (partitionConsumptionState.isCompletionReported()) {
            // Completion has been reported so extraDisjunctionCondition must be true to enter here.
            logger.info(consumerTaskId + " Partition " + partition + " synced offset: "
                + partitionConsumptionState.getOffsetRecord().getOffset());
          } else {
            /**
             * Check whether we need to warm-up cache here.
             */
            if (engine != null
                && serverConfig.isCacheWarmingBeforeReadyToServeEnabled()
                && serverConfig.isCacheWarmingEnabledForStore(storeName) // Only warm up configured stores
                && store.getCurrentVersion() <= versionNumber) { // Only warm up current version or future version
              /**
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
                      engine.warmUpStoragePartition(partition);
                      logger.info("Finished warming up store: " + kafkaVersionTopic + ", partition: " + partition);
                    } catch (Exception e) {
                      logger.error("Received exception while warming up cache for store: " + kafkaVersionTopic + ", partition: " + partition);
                    }
                    /**
                     * Delay reporting ready-to-serve until the storage node is ready.
                     */
                    long extraSleepTime = startReportingReadyToServeTimestamp - System.currentTimeMillis();
                    if (extraSleepTime > 0) {
                      try {
                        Thread.sleep(extraSleepTime);
                      } catch (InterruptedException e) {
                        throw new VeniceException("Sleep before reporting ready to serve got interrupted", e);
                      }
                    }
                    notificationDispatcher.reportCompleted(partitionConsumptionState);
                    logger.info(consumerTaskId + " Partition " + partition + " is ready to serve");
                  });
                }
              }
            } else {
              notificationDispatcher.reportCompleted(partitionConsumptionState);
              logger.info(consumerTaskId + " Partition " + partition + " is ready to serve");
            }
          }

          if (suppressLiveUpdates) {
            /**
             * If live updates suppression is enabled, stop consuming any new messages once the partition is ready to serve.
             */
            String msg = consumerTaskId + " Live update suppression is enabled. Stop consumption for partition " + partition;
            if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
              logger.info(msg);
            }
            unSubscribePartition(kafkaVersionTopic, partition);
          }
        } else {
          notificationDispatcher.reportProgress(partitionConsumptionState);
        }
      }
    };
  }

  public void reportError(String message, int partition, Exception e) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);

    notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), message, e);
  }

  public void reportQuotaViolated(int partition) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    notificationDispatcher.reportQuotaViolated(partitionConsumptionState);
  }

  public void reportQuotaNotViolated(int partition) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    notificationDispatcher.reportQuotaNotViolated(partitionConsumptionState);
  }

  /**
   * A function that would produce messages to topic.
   */
  @FunctionalInterface
  interface ProduceToTopic {
    Future<RecordMetadata> apply(Callback callback, long sourceTopicOffset);
  }

  /**
   * Since get real-time topic offset, get producer timestamp, and check topic existence are expensive, so we will only
   * retrieve such information after the predefined ttlMs
   */
  protected class CachedKafkaMetadataGetter {
    private final long ttl;
    private final TopicManager topicManager;
    private final Map<String, Pair<Long, Boolean>> topicExistenceCache = new VeniceConcurrentHashMap<>();
    private final Map<Pair<String, Integer>, Pair<Long, Long>> offsetCache = new VeniceConcurrentHashMap<>();
    private final Map<Pair<String, Integer>, Pair<Long, Long>> lastProducerTimestampCache = new VeniceConcurrentHashMap<>();

    CachedKafkaMetadataGetter(TopicManager topicManager, long timeToLiveMs) {
      this.ttl = MILLISECONDS.toNanos(timeToLiveMs);
      this.topicManager = topicManager;
    }

    long getOffset(String topicName, int partitionId) {
      long now = System.nanoTime();

      Pair key = new Pair<>(topicName, partitionId);
      Pair<Long, Long> entry = offsetCache.get(key);
      if (entry != null && entry.getFirst() > now) {
        return entry.getSecond();
      }

      entry = offsetCache.compute(key, (k, oldValue) ->
          (oldValue != null && oldValue.getFirst() > now) ?
              oldValue : new Pair<>(now + ttl, topicManager.getLatestOffsetAndRetry(topicName, partitionId, 10)));
      return entry.getSecond();
    }

    long getProducerTimestampOfLastMessage(String topicName, int partitionId) {
      long now = System.nanoTime();

      Pair key = new Pair<>(topicName, partitionId);
      Pair<Long, Long> entry = lastProducerTimestampCache.get(key);
      if (entry != null && entry.getFirst() > now) {
        return entry.getSecond();
      }

      entry = lastProducerTimestampCache.compute(key, (k, oldValue) ->
          (oldValue != null && oldValue.getFirst() > now) ?
              oldValue : new Pair<>(now + ttl, topicManager.getLatestProducerTimestampAndRetry(topicName, partitionId, 10)));
      return entry.getSecond();
    }

    boolean containsTopic(String topicName) {
      long now = System.nanoTime();

      Pair<Long, Boolean> entry = topicExistenceCache.get(topicName);
      if (entry != null && entry.getFirst() > now) {
        return entry.getSecond();
      }

      entry = topicExistenceCache.compute(topicName, (k, oldValue) ->
          (oldValue != null && oldValue.getFirst() > now) ?
              oldValue : new Pair<>(now + ttl, topicManager.containsTopic(topicName)));
      return entry.getSecond();
    }
  }

  /**
   * The purpose of this function is to wait for the complete processing (including persistence to disk) of all the messages
   * those were consumed from this kafka {topic, partition} prior to calling this function.
   * This is a common function to be used in O/O and L/F models for all scenarios.
   * This will make the calling thread to block.
   * @param topic
   * @param partition
   * @param partitionConsumptionState
   * @throws InterruptedException
   */
  protected void waitForAllMessageToBeProcessedFromTopicPartition(String topic, int partition,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {

    /**
     * This will wait for all the messages to be processed (persisted to disk) that are already
     * queued up to drainer till now.
     */
    storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);

    /**
     * In case of L/F model in Leader we first produce to local kafka then queue to drainer from kafka callback thread.
     * The above waiting is not sufficient enough since it only waits for whatever was queue prior to calling the
     * above api. This alone would not guarantee that all messages from that topic partition
     * has been processed completely. Additionally we need to wait for the last leader producer future to complete.
     *
     * Practically the above is not needed for Leader at all if we are waiting for the future below. But waiting does not
     * cause any harm and also keep this function simple. Otherwise we might have to check if this is the Leader for the partition.
     *
     * The code should only be effective in L/F model Leader instances as lastFuture should be null in all other scenarios.
     */
    try {
      Future<Void> lastFuture = partitionConsumptionState.getLastLeaderPersistFuture();
      if (lastFuture != null) {
        long synchronizeStartTimeInNS = System.nanoTime();
        lastFuture.get();
        storeIngestionStats.recordLeaderProducerSynchronizeLatency(storeName,
            LatencyUtils.getLatencyInMS(synchronizeStartTimeInNS));
      }
    } catch (Exception e) {
      logger.error(
          "Got exception while waiting for the latest producer future to be completed " + " for topic: " + topic
              + " partition: " + partition, e);
      //No need to fail the push job; just record the failure.
      versionedDIVStats.recordLeaderProducerFailure(storeName, versionNumber);
    }

  }

  protected boolean hasProducedToKafka(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    return false;
  }

}
