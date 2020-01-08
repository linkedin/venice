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
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedDIVStats;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.storage.chunking.ChunkingUtils;
import com.linkedin.venice.storage.chunking.ComputeChunkingAdapter;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static com.linkedin.venice.stats.StatsErrorCode.*;
import static com.linkedin.venice.store.record.ValueRecord.*;
import static java.util.concurrent.TimeUnit.*;


/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public abstract class StoreIngestionTask implements Runnable, Closeable {

  // TOOD: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = Logger.getLogger(StoreIngestionTask.class);

  // Constants

  private static final String CONSUMER_TASK_ID_FORMAT = StoreIngestionTask.class.getSimpleName() + " for [ Topic: %s ]";
  public static long POLLING_SCHEMA_DELAY_MS = TimeUnit.SECONDS.toMillis(5);
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
  protected final StoreRepository storeRepository;
  protected final String topic;
  protected final String storeNameWithoutVersionInfo;
  protected final int storeVersion;
  protected final ReadOnlySchemaRepository schemaRepo;
  protected final ReadOnlyStoreRepository metadataRepo;
  protected final String consumerTaskId;
  protected final Properties kafkaProps;
  protected final VeniceConsumerFactory factory;
  protected final VeniceWriterFactory veniceWriterFactory;
  protected final AtomicBoolean isRunning;
  protected final AtomicBoolean emitMetrics; //TODO: remove this once we migrate to versioned stats
  protected final AtomicInteger consumerActionSequenceNumber = new AtomicInteger(0);
  protected final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;
  protected final StorageMetadataService storageMetadataService;
  protected final TopicManager topicManager;
  protected final CachedLatestOffsetGetter cachedLatestOffsetGetter;
  protected final EventThrottler bandwidthThrottler;
  protected final EventThrottler recordsThrottler;
  /** Per-partition consumption state map */
  protected final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  protected final StoreBufferService storeBufferService;
  /** Persists the exception thrown by {@link StoreBufferService}. */
  protected Exception lastDrainerException = null;
  /** Keeps track of every upstream producer this consumer task has seen so far. */
  protected final Map<GUID, ProducerTracker> producerTrackerMap;
  protected final AggStoreIngestionStats storeIngestionStats;
  protected final AggVersionedDIVStats versionedDIVStats;
  protected final BooleanSupplier isCurrentVersion;
  protected final Optional<HybridStoreConfig> hybridStoreConfig;
  protected final IngestionNotificationDispatcher notificationDispatcher;
  protected final Optional<ProducerTracker.DIVErrorMetricCallback> divErrorMetricCallback;

  protected final Function<GUID, ProducerTracker> producerTrackerCreator;
  protected final long readCycleDelayMs;
  protected final long emptyPollSleepMs;

  protected final DiskUsage diskUsage;

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

  // use this checker to check whether ingestion completion can be reported for a partition
  protected final ReadyToServeCheck defaultReadyToServeChecker;

  // Non-final
  /** Should never be accessed directly. Always use {@link #getVeniceWriter()} instead, so that lazy init can occur. */
  private VeniceWriter<byte[], byte[], byte[]> veniceWriter;
  protected KafkaConsumerWrapper consumer;

  protected Set<Integer> schemaIdSet;
  protected int idleCounter = 0;

  //this indicates whether it polls nothing from Kafka
  //It's for stats measuring purpose
  protected boolean recordsPolled = true;

  /** this is used to handle hybrid write quota */
  protected Optional<HybridStoreQuotaEnforcement> hybridQuotaEnforcer;
  protected Optional<Map<Integer, Integer>> subscribedPartitionToSize;

  /**
   * Track all the topics with Kafka log-compaction enabled.
   * The expectation is that the compaction strategy is immutable once Kafka log compaction is enabled.
   */
  private final Set<String> topicWithLogCompaction = new ConcurrentSkipListSet<>();

  private IngestionTaskWriteComputeAdapter ingestionTaskWriteComputeAdapter;

  public StoreIngestionTask(VeniceWriterFactory writerFactory,
                            VeniceConsumerFactory consumerFactory,
                            Properties kafkaConsumerProperties,
                            StoreRepository storeRepository,
                            StorageMetadataService storageMetadataService,
                            Queue<VeniceNotifier> notifiers,
                            EventThrottler bandwidthThrottler,
                            EventThrottler recordsThrottler,
                            ReadOnlySchemaRepository schemaRepo,
                            ReadOnlyStoreRepository metadataRepo,
                            TopicManager topicManager,
                            AggStoreIngestionStats storeIngestionStats,
                            AggVersionedDIVStats versionedDIVStats,
                            StoreBufferService storeBufferService,
                            BooleanSupplier isCurrentVersion,
                            Optional<HybridStoreConfig> hybridStoreConfig,
                            boolean isIncrementalPushEnabled,
                            VeniceStoreConfig storeConfig,
                            DiskUsage diskUsage,
                            boolean bufferReplayEnabledForHybrid,
                            VeniceServerConfig serverConfig) {
    this.readCycleDelayMs = storeConfig.getKafkaReadCycleDelayMs();
    this.emptyPollSleepMs = storeConfig.getKafkaEmptyPollSleepMs();
    this.databaseSyncBytesIntervalForTransactionalMode = storeConfig.getDatabaseSyncBytesIntervalForTransactionalMode();
    this.databaseSyncBytesIntervalForDeferredWriteMode = storeConfig.getDatabaseSyncBytesIntervalForDeferredWriteMode();
    this.veniceWriterFactory = writerFactory;
    this.factory = consumerFactory;
    this.kafkaProps = kafkaConsumerProperties;
    this.storeRepository = storeRepository;
    this.storageMetadataService = storageMetadataService;
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.topic = storeConfig.getStoreName();
    this.schemaRepo = schemaRepo;
    this.metadataRepo = metadataRepo;
    this.storeNameWithoutVersionInfo = Version.parseStoreFromKafkaTopicName(topic);
    this.storeVersion = Version.parseVersionFromKafkaTopicName(topic);
    this.schemaIdSet = new HashSet<>();
    this.consumerActionsQueue = new PriorityBlockingQueue<>(CONSUMER_ACTION_QUEUE_INIT_CAPACITY);

    // partitionConsumptionStateMap could be accessed by multiple threads: consumption thread and the thread handling kill message
    this.partitionConsumptionStateMap = new VeniceConcurrentHashMap<>();

    // Could be accessed from multiple threads since there are multiple worker threads.
    this.producerTrackerMap = new VeniceConcurrentHashMap<>();
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, topic);
    this.topicManager = topicManager;
    this.cachedLatestOffsetGetter = new CachedLatestOffsetGetter(topicManager, storeConfig.getTopicOffsetCheckIntervalMs());

    this.storeIngestionStats = storeIngestionStats;
    this.versionedDIVStats = versionedDIVStats;

    this.isRunning = new AtomicBoolean(true);
    this.emitMetrics = new AtomicBoolean(true);

    this.storeBufferService = storeBufferService;
    this.isCurrentVersion = isCurrentVersion;
    this.hybridStoreConfig = hybridStoreConfig;
    this.isIncrementalPushEnabled = isIncrementalPushEnabled;
    this.notificationDispatcher = new IngestionNotificationDispatcher(notifiers, topic, isCurrentVersion);

    this.divErrorMetricCallback = Optional.of(e -> versionedDIVStats.recordException(storeNameWithoutVersionInfo, storeVersion, e));
    this.producerTrackerCreator = guid -> new ProducerTracker(guid, topic);

    this.diskUsage = diskUsage;

    this.readOnlyForBatchOnlyStoreEnabled = storeConfig.isReadOnlyForBatchOnlyStoreEnabled();

    this.bufferReplayEnabledForHybrid = bufferReplayEnabledForHybrid;

    this.serverConfig = serverConfig;

    this.defaultReadyToServeChecker = getDefaultReadyToServeChecker();

    this.ingestionTaskWriteComputeAdapter = new IngestionTaskWriteComputeAdapter(storeNameWithoutVersionInfo, schemaRepo);

    this.hybridQuotaEnforcer = Optional.empty();

    this.subscribedPartitionToSize = Optional.empty();

    if (serverConfig.isHybridQuotaEnabled() ||
        metadataRepo.getStore(storeNameWithoutVersionInfo).isHybridStoreDiskQuotaEnabled()) {
      /**
       * We will enforce hybrid quota only if this is hybrid mode && persistence type is rocks DB
       */
      AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);
      if (isHybridMode() && storageEngine.getType().equals(PersistenceType.ROCKS_DB)) {
        this.hybridQuotaEnforcer = Optional.of(new HybridStoreQuotaEnforcement(this,
                                                                    storageEngine,
                                                                    metadataRepo.getStore(storeNameWithoutVersionInfo),
                                                                    topic,
                                                                    topicManager.getPartitions(topic).size()));
        this.metadataRepo.registerStoreDataChangedListener(hybridQuotaEnforcer.get());
        this.subscribedPartitionToSize = Optional.of(new HashMap<>());
      }
    }
  }

  protected void validateState() {
    if(!this.isRunning.get()) {
      throw new VeniceException(" Topic " + topic + " is shutting down, no more messages accepted");
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
    consumerActionsQueue.add(ConsumerAction.createKillAction(topic, nextSeqNum()));
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
      //If task is still running, force close it.
      notificationDispatcher.reportError(partitionConsumptionStateMap.values(), "Received the signal to kill this consumer. Topic " + topic,
          new VeniceException("Kill the consumer"));
      // close can not stop the consumption synchronizely, but the status of helix would be set to ERROR after
      // reportError. The only way to stop it synchronizely is interrupt the current running thread, but it's an unsafe
      // operation, for example it could break the ongoing db operation, so we should avoid that.
      this.close();
    }
  }

  private void beginBatchWrite(int partitionId, boolean sorted, PartitionConsumptionState partitionConsumptionState) {
    Map<String, String> checkpointedDatabaseInfo = partitionConsumptionState.getOffsetRecord().getDatabaseInfo();
    StoragePartitionConfig storagePartitionConfig = getStoragePartitionConfig(partitionId, sorted, partitionConsumptionState);
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());
    storeRepository.getLocalStorageEngine(topic).beginBatchWrite(storagePartitionConfig, checkpointedDatabaseInfo);
    logger.info("Started batch write to store: " + topic + ", partition: " + partitionId +
        " with checkpointed database info: " + checkpointedDatabaseInfo + " and sorted: " + sorted);
  }

  private StoragePartitionConfig getStoragePartitionConfig(int partitionId, boolean sorted,
      PartitionConsumptionState partitionConsumptionState) {
    StoragePartitionConfig storagePartitionConfig = new StoragePartitionConfig(topic, partitionId);
    boolean deferredWrites;
    boolean readOnly = false;
    if (partitionConsumptionState.isEndOfPushReceived()) {
      // After EOP, we never enable deferred writes.
      // No matter what the sorted config was before the EOP message, it doesn't matter anymore.
      deferredWrites = false;
      if (partitionConsumptionState.isBatchOnly() &&
          this.readOnlyForBatchOnlyStoreEnabled) {
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
      //Since we know the EOP has been received, regular batch store is ready to go!
      return true;
    }

    if (!partitionConsumptionState.isWaitingForReplicationLag()) {
      // If we have already crossed the acceptable lag threshold in the past, then we will stick to that,
      // rather than possibly flip flopping
      return true;
    }

    boolean lagIsAcceptable;

    if (!hybridStoreConfig.isPresent()) {
    /**
     * In theory, it will be 1 offset ahead of the current offset since getOffset returns the next available offset.
     * Currently, we make it a sloppy in case Kafka topic have duplicate messages.
     * TODO: find a better solution
     */
      lagIsAcceptable = cachedLatestOffsetGetter.getOffset(topic, partitionConsumptionState.getPartition()) <=
        partitionConsumptionState.getOffsetRecord().getOffset() + SLOPPY_OFFSET_CATCHUP_THRESHOLD;
    } else {
      // Looks like none of the short-circuitry fired, so we need to measure lag!
      long threshold = hybridStoreConfig.get().getOffsetLagThresholdToGoOnline();
      boolean shouldLogLag = partitionConsumptionState.getOffsetRecord().getOffset() % threshold == 0; //Log lag for every <threshold> records.
      long lag = measureHybridOffsetLag(partitionConsumptionState, shouldLogLag);
      boolean lagging = lag > threshold;

      if (shouldLogLag) {
      logger.info(String.format("%s partition %d is %slagging. Lag: [%d] %s Threshold [%d]", consumerTaskId,
          partitionConsumptionState.getPartition(), (lagging ? "" : "not "), lag, (lagging ? ">" : "<"), threshold));
      }

      lagIsAcceptable = !lagging;
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

  protected void processMessages() throws InterruptedException {
    if (null != lastDrainerException) {
      // Interrupt the whole ingestion task
      throw new VeniceException("Exception thrown by store buffer drainer", lastDrainerException);
    }
    /**
     * Check whether current consumer has any subscription or not since 'poll' function will throw
     * {@link IllegalStateException} with empty subscription.
     */
    if (consumer.hasSubscription()) {
      idleCounter = 0;
      long beforePollingTimestamp = System.currentTimeMillis();
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = consumer.poll(readCycleDelayMs);
      long afterPollingTimestamp = System.currentTimeMillis();

      int pollResultNum = 0;
      if (null != records) {
        pollResultNum = records.count();
      }
      if (emitMetrics.get()) {
        storeIngestionStats.recordPollRequestLatency(storeNameWithoutVersionInfo, afterPollingTimestamp - beforePollingTimestamp);
        storeIngestionStats.recordPollResultNum(storeNameWithoutVersionInfo, pollResultNum);
      }

      if (pollResultNum == 0) {
        if (recordsPolled) {     //This is the first time we polled and got an empty response set
          versionedDIVStats.resetCurrentIdleTime(storeNameWithoutVersionInfo, storeVersion);
        } else { //On subsequent empty polls, sleep to reduce thread contention
          Thread.sleep(emptyPollSleepMs);
        }
        recordsPolled = false;
        versionedDIVStats.recordCurrentIdleTime(storeNameWithoutVersionInfo, storeVersion);
        versionedDIVStats.recordOverallIdleTime(storeNameWithoutVersionInfo, storeVersion);
        return;
      } else {
        recordsPolled = true;
      }

      /**
       * Enforces hybrid quota on this batch poll if this is hybrid store and persistence type is rocksDB
       * Even if the records list is empty, we still need to check quota to potentially resume partition
       */
      if(hybridQuotaEnforcer.isPresent()) {
        refillPartitionToSizeMap(records);
        hybridQuotaEnforcer.get().checkPartitionQuota(subscribedPartitionToSize.get());
      }

      long totalRecordSize = 0;
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
        /**
         * check schema id availability before putting consumer record to drainer queue
         */
        checkSchemaIdAvail(record);
        // blocking call
        storeBufferService.putConsumerRecord(record, this);
        if (record.serializedKeySize() > 0) {
          totalRecordSize += record.serializedKeySize();
        }
        if (record.serializedValueSize() > 0) {
          totalRecordSize += record.serializedValueSize();
        }
      }
      if (totalRecordSize > 0) {
        storeIngestionStats.recordTotalBytesReadFromKafkaAsUncompressedSize(totalRecordSize);
      }
      if (recordsPolled) {
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
        bandwidthThrottler.maybeThrottle(totalRecordSize);
        recordsThrottler.maybeThrottle(records.count());
      }
      long afterPutTimestamp = System.currentTimeMillis();
      if (emitMetrics.get()) {
        storeIngestionStats.recordConsumerRecordsQueuePutLatency(storeNameWithoutVersionInfo, afterPutTimestamp - afterPollingTimestamp);
      }
    } else { // the case that consumer doesn't have subscriptions
      idleCounter++;
      if (idleCounter > MAX_IDLE_COUNTER) {
        logger.warn(consumerTaskId + " No Partitions are subscribed to for store attempts expired after " + idleCounter);
        complete();
      } else {
        logger.warn(consumerTaskId + " No Partitions are subscribed to for store attempt " + idleCounter);
        Utils.sleep(readCycleDelayMs);
      }
    }
  }

  /**
   * Polls the producer for new messages in an infinite loop by a dedicated consumer thread
   * and processes the new messages by current thread.
   */
  @Override
  public void run() {
    // Update thread name to include topic to make it easy debugging
    Thread.currentThread().setName("venice-consumer-" + topic);

    logger.info("Running " + consumerTaskId);
    try {
      this.consumer = factory.getConsumer(kafkaProps);
      /**
       * Here we could not use isRunning() since it is a synchronized function, whose lock could be
       * acquired by some other synchronized function, such as {@link #kill()}.
        */
      while (isRunning.get()) {
        processConsumerActions();
        checkLongRunningTaskState();
        processMessages();
      }

      // If the ingestion task is stopped gracefully (server stops or kill push job), persist processed offset to disk
      for (PartitionConsumptionState partitionConsumptionState : partitionConsumptionStateMap.values()) {
        syncOffset(topic, partitionConsumptionState);
      }

    } catch (VeniceIngestionTaskKilledException ke) {
      logger.info(consumerTaskId+"is killed, start to report to notifier.", ke);
      notificationDispatcher.reportKilled(partitionConsumptionStateMap.values(), ke);
    } catch (Throwable t) {
      // After reporting error to controller, controller will ignore the message from this replica if job is aborted.
      // So even this storage node recover eventually, controller will not confused.
      // If job is not aborted, controller is open to get the subsequent message from this replica(if storage node was
      // recovered, it will send STARTED message to controller again)
      if (t instanceof Exception) {
        logger.error(consumerTaskId + " failed with Exception.", t);
        notificationDispatcher.reportError(partitionConsumptionStateMap.values(), "Exception caught during poll.", (Exception)t);
      } else {
        logger.error(consumerTaskId + " failed with Throwable!!!", t);
        notificationDispatcher.reportError(partitionConsumptionStateMap.values(),
            "Non-exception Throwable caught in " + getClass().getSimpleName() + "'s run() function.", new VeniceException(t));
      }
      // Only record ingestion failure if it is not caused by kill operation
      storeIngestionStats.recordIngestionFailure(storeNameWithoutVersionInfo);
    } finally {
      internalClose();
    }
  }

  private void internalClose() {
    // Only reset Offset Messages are important, subscribe/unSubscribe will be handled
    // on the restart by Helix Controller notifications on the new StoreIngestionTask.
    for(ConsumerAction message : consumerActionsQueue) {
      ConsumerActionType opType = message.getType();
      if(opType == ConsumerActionType.RESET_OFFSET) {
        String topic = message.getTopic();
        int partition = message.getPartition();
        logger.info(consumerTaskId + " Cleanup Reset OffSet : Topic " + topic + " Partition Id " + partition );
        storageMetadataService.clearOffset(topic, partition);
      } else {
        logger.info(consumerTaskId + " Cleanup ignoring the Message " + message);
      }
    }

    if(consumer == null) {
      // Consumer constructor error-ed out, nothing can be cleaned up.
      logger.info("Error in consumer creation, skipping close for topic " + topic);
      return;
    }
    consumer.close();
    isRunning.set(false);
    logger.info("Store ingestion task for store: " + topic + " is closed");
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processConsumerActions() {
    while (!consumerActionsQueue.isEmpty()) {
      // Do not want to remove a message from the queue unless it has been processed.
      ConsumerAction message = consumerActionsQueue.peek();
      try {
        message.incrementAttempt();
        processConsumerAction(message);
      } catch (InterruptedException e) {
        throw new VeniceIngestionTaskKilledException(topic, e);
      } catch (Exception ex) {
        if (message.getAttemptsCount() < MAX_CONTROL_MESSAGE_RETRIES) {
          logger.info("Error Processing message will retry later" + message , ex);
          return;
        } else {
          logger.error("Ignoring message:  " + message + " after retries " + message.getAttemptsCount(), ex);
        }
      }
      consumerActionsQueue.poll();
    }
  }

  private void checkConsumptionStateWhenStart(OffsetRecord record, PartitionConsumptionState newPartitionConsumptionState) {
    int partition = newPartitionConsumptionState.getPartition();
    // Once storage node restart, send the "START" status to controller to rebuild the task status.
    // If this storage node has never consumed data from this topic, instead of sending "START" here, we send it
    // once START_OF_PUSH message has been read.
    if (record.getOffset() > 0) {
      Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topic);
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
      storeIngestionStats.recordInconsistentStoreMetadata(storeNameWithoutVersionInfo, 1);
      // clear the local store metadata and the replica will be rebuilt from scratch upon retry as part of
      // processConsumerActions.
      storageMetadataService.clearOffset(topic, partition);
      storageMetadataService.clearStoreVersionState(topic);
      producerTrackerMap.values().forEach(
          producerTracker -> producerTracker.clearPartition(partition)
      );
      throw e;
    }
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
              ProducerTracker producerTracker = producerTrackerMap.computeIfAbsent(producerGuid, producerTrackerCreator);
              producerTracker.setPartitionState(partition, entry.getValue());
              producerTrackerMap.put(producerGuid, producerTracker);
            }
        );

        checkConsumptionStateWhenStart(record, newPartitionConsumptionState);
        consumer.subscribe(topic, partition, record.getOffset());
        logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition + " Offset "
            + record.getOffset());
        break;
      case UNSUBSCRIBE:
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
        consumer.unSubscribe(topic, partition);
        // Drain the buffered message by last subscription.
        storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);
        /**
         * Since the processing of the buffered messages are using {@link #partitionConsumptionStateMap} and
         * {@link #producerTrackerMap}, we would like to drain all the buffered messages before cleaning up those
         * two variables to avoid the race condition.
         */
        partitionConsumptionStateMap.remove(partition);
        producerTrackerMap.values().stream().forEach(
            producerTracker -> producerTracker.clearPartition(partition)
        );
        break;
      case RESET_OFFSET:
        /**
         * After auditing all the calls that can result in the RESET_OFFSET action, it turns out we always unsubscribe
         * from the topic/partition before resetting offset, which is unnecessary; but we decided to keep this action
         * for now in case that in future, we do want to reset the consumer without unsubscription.
         */
        if (consumer.hasSubscription(topic, partition)) {
          logger.error("This shouldn't happen since unsubscription should happen before reset offset for topic: " + topic + ", partition: " + partition);
          /**
           * Only update the consumer and partitionConsumptionStateMap when consumer actually has
           * subscription to this topic/partition; otherwise, we would blindly update the StateMap
           * and mess up other operations on the StateMap.
           */
          try {
            consumer.resetOffset(topic, partition);
            logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition);
          } catch (UnsubscribedTopicPartitionException e) {
            logger.error(consumerTaskId + " Kafka consumer should have subscribed to the partition already but it fails "
                + "on resetting offset for Topic: " + topic + " Partition Id: " + partition);
          }
          partitionConsumptionStateMap.put(
              partition,
              new PartitionConsumptionState(partition, new OffsetRecord(), hybridStoreConfig.isPresent(), isIncrementalPushEnabled));
        } else {
          logger.info(consumerTaskId + " No need to reset offset by Kafka consumer, since the consumer is not " +
              "subscribing Topic: " + topic + " Partition Id: " + partition);
        }
        producerTrackerMap.values().stream().forEach(
            producerTracker -> producerTracker.clearPartition(partition)
        );
        storageMetadataService.clearOffset(topic, partition);
        break;
      case KILL:
        logger.info("Kill this consumer task for Topic:" + topic);
        // Throw the exception here to break the consumption loop, and then this task is marked as error status.
        throw new InterruptedException("Received the signal to kill this consumer. Topic " + topic);
      default:
        throw new UnsupportedOperationException(operation.name() + " is not supported in " + this.getClass().getName());
    }
  }

  protected abstract void checkLongRunningTaskState() throws InterruptedException;
  protected abstract void processConsumerAction(ConsumerAction message) throws InterruptedException;

  /**
   * Common record check for different state models:
   * check whether server continues receiving messages after EOP for a batch-only store.
   */
  protected boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    int partitionId = record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);

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
        storeIngestionStats.recordUnexpectedMessage(storeNameWithoutVersionInfo, 1);
      }

      if (record.offset() % 100 == 0) {
          // Report such kind of message once per 100 messages to reduce logging volume
          /**
           * TODO: right now, if we update a store to enable hybrid, {@link StoreIngestionTask} for the existing versions
           * won't know it since {@link #hybridStoreConfig} parameter is passed during construction.
           *
           * Same thing for {@link #isIncrementalPushEnabled}.
           *
           * So far, to make hybrid store/incremental store work, customer needs to do a new push after enabling hybrid/
           * incremental push feature of the store.
           */
          logger.warn(
              "The record was received after 'EOP', but the store: " + topic +
                  " is neither hybrid nor incremental push enabled, so will skip it.");
      }
      return false;
    }

    return true;
  }

  /**
   * This function will be invoked in {@link StoreBufferService} to process buffered {@link ConsumerRecord}.
   * @param record
   */
  public void processConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) throws InterruptedException {
    int partition = record.partition();
    //The partitionConsumptionStateMap can be modified by other threads during consumption (for example when unsubscribing)
    //in order to maintain thread safety, we hold onto the reference to the partitionConsumptionState and pass that
    //reference to all downstream methods so that all offset persistence operations use the same partitionConsumptionState
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    if (null == partitionConsumptionState) {
      logger.info("Topic " + topic + " Partition " + partition + " has been unsubscribed, skip this record that has offset " + record.offset());
      return;
    }
    if (partitionConsumptionState.isErrorReported()) {
      logger.info("Topic " + topic + " Partition " + partition + " is already errored, skip this record that has offset " + record.offset());
      return;
    }
    if(!shouldProcessRecord(record)) {
      return;
    }
    int recordSize = 0;
    try {
      recordSize = internalProcessConsumerRecord(record, partitionConsumptionState);
    } catch (FatalDataValidationException e) {
      int faultyPartition = record.partition();
      String errorMessage = "Fatal data validation problem with partition " + faultyPartition + ", offset " + record.offset();
      boolean needToUnsub = !(isCurrentVersion.getAsBoolean() || partitionConsumptionState.isEndOfPushReceived());
      if (needToUnsub) {
        errorMessage += ". Consumption will be halted.";
      } else {
        errorMessage += ". Because " + topic + " is the current version, consumption will continue.";
      }
      notificationDispatcher.reportError(Arrays.asList(partitionConsumptionState), errorMessage, e);
      if (needToUnsub) {
        unSubscribePartition(topic, faultyPartition);
      }
    } catch (VeniceMessageException | UnsupportedOperationException excp) {
      throw new VeniceException(consumerTaskId + " : Received an exception for message at partition: "
          + record.partition() + ", offset: " + record.offset() + ". Bubbling up.", excp);
    }

    partitionConsumptionState.incrementProcessedRecordNum();
    partitionConsumptionState.incrementProcessedRecordSize(recordSize);

    if (diskUsage.isDiskFull(recordSize)){
      throw new VeniceException("Disk is full: throwing exception to error push: "
          + storeNameWithoutVersionInfo + " version " + storeVersion + ". "
          + diskUsage.getDiskStatus());
    }

    int processedRecordNum = partitionConsumptionState.getProcessedRecordNum();
    if (processedRecordNum >= OFFSET_REPORTING_INTERVAL ||
        partitionConsumptionState.isEndOfPushReceived()) {
      int processedRecordSize = partitionConsumptionState.getProcessedRecordSize();
      // Report metrics
      if (emitMetrics.get()) {
        storeIngestionStats.recordBytesConsumed(storeNameWithoutVersionInfo, processedRecordSize);
        storeIngestionStats.recordRecordsConsumed(storeNameWithoutVersionInfo, processedRecordNum);
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
      syncOffset(topic, partitionConsumptionState);
    }

    // Check whether it's ready to serve
    defaultReadyToServeChecker.apply(partitionConsumptionState, recordsProcessedAboveSyncIntervalThreshold);
  }

  private void syncOffset(String topic, PartitionConsumptionState ps) {
    int partition = ps.getPartition();
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);
    Map<String, String> dbCheckpointingInfo = storageEngine.sync(partition);
    OffsetRecord offsetRecord = ps.getOffsetRecord();
    /**
     * The reason to transform the internal state only during checkpointing is that
     * the intermediate checksum generation is an expensive operation.
     * See {@link ProducerTracker#addMessage(int, KafkaKey, KafkaMessageEnvelope, boolean, Optional)}
     * to find more details.
     */
    offsetRecord.transform();
    // Checkpointing info required by the underlying storage engine
    offsetRecord.setDatabaseInfo(dbCheckpointingInfo);
    storageMetadataService.put(this.topic, partition, offsetRecord);
    ps.resetProcessedRecordSizeSinceLastSync();
  }

  public void setLastDrainerException(Exception e) {
    lastDrainerException = e;
  }

  /**
   * @return the total lag for all subscribed partitions between the real-time buffer topic and this consumption task.
   */
  public long getRealTimeBufferOffsetLag() {
    if (!hybridStoreConfig.isPresent()) {
      return METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES.code;
    }

    Optional<StoreVersionState> svs = storageMetadataService.getStoreVersionState(topic);
    if (!svs.isPresent()) {
      return STORE_VERSION_STATE_UNAVAILABLE.code;
    }

    if (partitionConsumptionStateMap.isEmpty()) {
      return NO_SUBSCRIBED_PARTITION.code;
    }

    long offsetLag = partitionConsumptionStateMap.values().parallelStream()
        .filter(pcs -> pcs.isEndOfPushReceived() &&
            pcs.getOffsetRecord().getStartOfBufferReplayDestinationOffset().isPresent())
        .mapToLong(pcs -> measureHybridOffsetLag(pcs, false))
        .sum();

    return minZeroLag(offsetLag);
  }

  /**
   * Because of timing considerations, it is possible that some lag metrics could compute negative
   * values. Negative lag does not make sense so the intent is to ease interpretation by applying a
   * lower bound of zero on these metrics...
   */
  private long minZeroLag(long value) {
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
    return partitionConsumptionStateMap.values().parallelStream()
        .filter(pcs -> !pcs.getOffsetRecord().getStartOfBufferReplayDestinationOffset().isPresent()).count();
  }

  public boolean isHybridMode() {
    if (hybridStoreConfig == null) {
      logger.error("hybrid config shouldn't be null. Something bad happened. Topic: " + getTopic());
    }
    return hybridStoreConfig != null && hybridStoreConfig.isPresent();
  }

  protected void processStartOfPush(ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
    StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
    /**
     * Notify the underlying store engine about starting batch push.
     */
    beginBatchWrite(partition, startOfPush.sorted, partitionConsumptionState);

    notificationDispatcher.reportStarted(partitionConsumptionState);
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topic);
    if (!storeVersionState.isPresent()) {
      // No other partition of the same topic has started yet, let's initialize the StoreVersionState
      StoreVersionState newStoreVersionState = new StoreVersionState();
      newStoreVersionState.sorted = startOfPush.sorted;
      newStoreVersionState.chunked = startOfPush.chunked;
      newStoreVersionState.compressionStrategy = startOfPush.compressionStrategy;
      storageMetadataService.put(topic, newStoreVersionState);
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
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);
    storageEngine.endBatchWrite(storagePartitionConfig);

    /**
     * It's a bit of tricky here. Since the offset is not updated yet, it's actually previous offset reported
     * here.
     * TODO: sync up offset before invoking dispatcher
     */
    notificationDispatcher.reportEndOfPushReceived(partitionConsumptionState);
  }

  private void processStartOfBufferReplay(ControlMessage controlMessage, long offset, PartitionConsumptionState partitionConsumptionState) {
    Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topic);
    if (storeVersionState.isPresent()) {
      // Update StoreVersionState, if necessary
      StartOfBufferReplay startOfBufferReplay = (StartOfBufferReplay) controlMessage.controlMessageUnion;
      if (null == storeVersionState.get().startOfBufferReplay) {
        // First time we receive a SOBR
        storeVersionState.get().startOfBufferReplay = startOfBufferReplay;
        storageMetadataService.put(topic, storeVersionState.get());
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

  private void processStartOfIncrementalPush(ControlMessage controlMessage, PartitionConsumptionState partitionConsumptionState) {
    CharSequence startVersion = ((StartOfIncrementalPush) controlMessage.controlMessageUnion).version;
    IncrementalPush newIncrementalPush = new IncrementalPush();
    newIncrementalPush.version = startVersion;

    partitionConsumptionState.setIncrementalPush(newIncrementalPush);
    notificationDispatcher.reportStartOfIncrementalPushReceived(partitionConsumptionState, startVersion.toString());
  }

  private void processEndOfIncrementalPush(ControlMessage controlMessage, PartitionConsumptionState partitionConsumptionState) {
    // TODO: it is possible that we could turn incremental store to be read-only when incremental push is done
    CharSequence endVersion = ((EndOfIncrementalPush) controlMessage.controlMessageUnion).version;
    //reset incremental push version
    partitionConsumptionState.setIncrementalPush(null);
    notificationDispatcher.reportEndOfIncrementalPushRecived(partitionConsumptionState, endVersion.toString());
  }

  protected void processTopicSwitch(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    throw new VeniceException(ControlMessageType.TOPIC_SWITCH.name() + " control message should not be received in"
        + "Online/Offline state model. Topic " + topic + " partition " + partition);
  }

  /**
   * In this method, we pass both offset and partitionConsumptionState(ps). The reason behind it is that ps's
   * offset is stale and is not updated until the very end
   */
  private ControlMessageType processControlMessage(ControlMessage controlMessage, int partition, long offset,
      PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
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
         * No-op for {@link ControlMessageType#START_OF_SEGMENT} and {@link ControlMessageType#END_OF_SEGMENT}.
         * These are handled in the {@link ProducerTracker}.
         */
        break;
      case START_OF_BUFFER_REPLAY:
        processStartOfBufferReplay(controlMessage, offset, partitionConsumptionState);
       break;
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

    return type;
  }

  /**
   * Update the metadata in OffsetRecord, including "offset" which indicates the last successfully consumed message offset
   * of version topic, "leaderOffset" which indicates the last successfully consumed message offset from leader topic.
   */
  protected abstract void updateOffset(PartitionConsumptionState partitionConsumptionState, OffsetRecord offsetRecord,
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord);

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param consumerRecord {@link ConsumerRecord} consumed from Kafka.
   * @return the size of the data written to persistent storage.
   */
  private int internalProcessConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
                                            PartitionConsumptionState partitionConsumptionState) throws InterruptedException {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int sizeOfPersistedData = 0;
    boolean syncOffset = false;

    Optional<OffsetRecordTransformer> offsetRecordTransformer = Optional.empty();
    try {
      // Assumes the timestamp on the ConsumerRecord is the broker's timestamp when it received the message.
      recordWriterStats(kafkaValue.producerMetadata.messageTimestamp, consumerRecord.timestamp(),
          System.currentTimeMillis());
      FatalDataValidationException e = null;
      boolean endOfPushReceived = partitionConsumptionState.isEndOfPushReceived();
      try {
        offsetRecordTransformer = validateMessage(consumerRecord.topic(), consumerRecord.partition(), kafkaKey, kafkaValue, endOfPushReceived);
        versionedDIVStats.recordSuccessMsg(storeNameWithoutVersionInfo, storeVersion);
      } catch (FatalDataValidationException fatalException) {
        divErrorMetricCallback.get().execute(fatalException);
        if (endOfPushReceived) {
          e = fatalException;
        } else {
          throw fatalException;
        }
      }
      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        processControlMessage(controlMessage, consumerRecord.partition(), consumerRecord.offset(), partitionConsumptionState);
        /**
         * Here, we want to sync offset/producer guid info whenever receiving any control message:
         * 1. We want to keep the critical milestones.
         * 2. We want to keep all the historical producer guid info since they could be used after. New producer guid
         * is already coming with Control Message.
         *
         * If we don't sync offset this way, it is very possible to encounter
         * {@link ProducerTracker.DataFaultType.UNREGISTERED_PRODUCER} since
         * {@link syncOffset} is not being called for every message, we might miss some historical producer guids.
         */
        syncOffset = true;
      } else if (null == kafkaValue) {
        throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message. Partition " +
              consumerRecord.partition() + " Offset " + consumerRecord.offset());
      } else {
        int keySize = kafkaKey.getKeyLength();
        int valueSize = processVeniceMessage(consumerRecord);
        if (emitMetrics.get()) {
          storeIngestionStats.recordKeySize(storeNameWithoutVersionInfo, keySize);
          storeIngestionStats.recordValueSize(storeNameWithoutVersionInfo, valueSize);
        }
        sizeOfPersistedData = keySize + valueSize;
      }

      if (e != null) {
        throw e;
      }
    } catch (DuplicateDataException e) {
      versionedDIVStats.recordDuplicateMsg(storeNameWithoutVersionInfo, storeVersion);
      if (logger.isDebugEnabled()) {
        logger.debug(consumerTaskId + " : Skipping a duplicate record at offset: " + consumerRecord.offset());
      }
    } catch (PersistenceFailureException ex) {
      if (partitionConsumptionStateMap.containsKey(consumerRecord.partition())) {
        // If we actually intend to be consuming this partition, then we need to bubble up the failure to persist.
        logger.error("Met PersistenceFailureException while processing record with offset: " + consumerRecord.offset() +
        ", topic: " + topic + ", meta data of the record: " + consumerRecord.value().producerMetadata);
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
      logger.info("Topic " + topic + " Partition " + consumerRecord.partition() + " has been unsubscribed, will skip offset update");
    } else {
      /**
       * Record the time when server consumes the message;
       * the message can come from version topic, real-time topic or grandfathering topic.
       */
      OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
      offsetRecord.setProcessingTimeEpochMs(System.currentTimeMillis());

      if (offsetRecordTransformer.isPresent()) {
        /**
         * The reason to transform the internal state only during checkpointing is that
         * the intermediate checksum generation is an expensive operation.
         * See {@link ProducerTracker#addMessage(int, KafkaKey, KafkaMessageEnvelope, boolean, Optional)}
         * to find more details.
         */
        offsetRecord.addOffsetRecordTransformer(kafkaValue.producerMetadata.producerGUID, offsetRecordTransformer.get());
      } /** else, {@link #validateMessage(int, KafkaKey, KafkaMessageEnvelope)} threw a {@link DuplicateDataException} */

      // Potentially update the offset metadata in the OffsetRecord
      updateOffset(partitionConsumptionState, offsetRecord, consumerRecord);

      partitionConsumptionState.setOffsetRecord(offsetRecord);
      if (syncOffset) {
        syncOffset(topic, partitionConsumptionState);
      }
    }
    return sizeOfPersistedData;
  }

  private void recordWriterStats(long producerTimestampMs, long brokerTimestampMs, long consumerTimestampMs) {
    long producerBrokerLatencyMs = Math.max(brokerTimestampMs - producerTimestampMs, 0);
    long brokerConsumerLatencyMs = Math.max(consumerTimestampMs - brokerTimestampMs, 0);
    long producerConsumerLatencyMs = Math.max(consumerTimestampMs - producerTimestampMs, 0);
    versionedDIVStats.recordProducerBrokerLatencyMs(storeNameWithoutVersionInfo, storeVersion, producerBrokerLatencyMs);
    versionedDIVStats.recordBrokerConsumerLatencyMs(storeNameWithoutVersionInfo, storeVersion, brokerConsumerLatencyMs);
    versionedDIVStats.recordProducerConsumerLatencyMs(storeNameWithoutVersionInfo, storeVersion,
        producerConsumerLatencyMs);
  }

  /**
   * The {@param topicName} maybe different from the store version topic, since in {@link LeaderFollowerStoreIngestionTask},
   * the topic could be other topics, such as RT topic or stream reprocessing topic.
   **/
  private Optional<OffsetRecordTransformer> validateMessage(String topicName, int partition, KafkaKey key, KafkaMessageEnvelope message, boolean endOfPushReceived) {
    final GUID producerGUID = message.producerMetadata.producerGUID;
    ProducerTracker producerTracker = producerTrackerMap.computeIfAbsent(producerGUID, producerTrackerCreator);
    Optional<ProducerTracker.DIVErrorMetricCallback> errorCallback = divErrorMetricCallback;
    try {
      boolean tolerateMissingMessage = endOfPushReceived;
      if (topicWithLogCompaction.contains(topicName)) {
        /**
         * For log-compacted topic, no need to report error metric when message missing issue happens.
         */
        errorCallback = Optional.empty();
        tolerateMissingMessage = true;
      }
      return Optional.of(producerTracker.addMessage(partition, key, message, tolerateMissingMessage, errorCallback));
    } catch (FatalDataValidationException e) {
      /**
       * Check whether Kafka compaction is enabled in current topic.
       * This function shouldn't be invoked very frequently since {@link ProducerTracker#addMessage(int, KafkaKey, KafkaMessageEnvelope, boolean, Optional)}
       * will update the sequence id if it could tolerate the missing messages.
       *
       * If it is not this case, we need to revisit this logic since this operation is expensive.
       */
      if (! topicManager.isTopicCompactionEnabled(topicName)) {
        /**
         * Not enabled, then bubble up the exception.
         * We couldn't cache this in {@link topicWithLogCompaction} since the log compaction could be enabled in the same topic later.
         */
        logger.error("Encountered DIV error when topic: " + topicName + " doesn't enable log compaction");
        throw e;
      }
      topicWithLogCompaction.add(topicName);
      /**
       * Since Kafka compaction is enabled, DIV will start tolerating missing messages.
       */
      logger.info("Encountered DIV exception when consuming from topic: " + topicName, e);
      logger.info("Kafka compaction is enabled in topic: " + topicName + ", so DIV will tolerate missing message in the future");
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
         * The assumption here is that {@link ProducerTracker#addMessage(int, KafkaKey, KafkaMessageEnvelope, boolean, Optional)}
         * won't update the sequence id of the current segment if it couldn't tolerate missing messages.
         * In this case, no need to report error metric.
         */
        return Optional.of(producerTracker.addMessage(partition, key, message, true, Optional.empty()));
      }
    }
  }

  /**
   * Write to database and potentially produce to version topic
   */
  protected abstract void produceAndWriteToDatabase(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      WriteToStorageEngine writeFunction, ProduceToTopic produceFunction);

  /**
   * Write to the storage engine with the optimization that we leverage the padding in front of the {@param putValue}
   * in order to insert the {@param schemaId} there. This avoids a byte array copy, which can be beneficial in terms
   * of GC.
   */
  protected void prependHeaderAndWriteToStorageEngine(String topic, int partition, byte[] keyBytes, ByteBuffer putValue, int schemaId) {
    /**
     * Since {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer} reuses the original byte
     * array, which is big enough to pre-append schema id, so we just reuse it to avoid unnecessary byte array allocation.
     */
    if (putValue.position() < SCHEMA_HEADER_LENGTH) {
      throw new VeniceException("Start position of 'putValue' ByteBuffer shouldn't be less than " + SCHEMA_HEADER_LENGTH);
    }

    // Back up the original 4 bytes
    putValue.position(putValue.position() - SCHEMA_HEADER_LENGTH);
    int backupBytes = putValue.getInt();
    putValue.position(putValue.position() - SCHEMA_HEADER_LENGTH);
    ByteUtils.writeInt(putValue.array(), schemaId, putValue.position());

    writeToStorageEngine(storeRepository.getLocalStorageEngine(topic), partition, keyBytes, putValue);

    /** We still want to recover the original position to make this function idempotent. */
    putValue.putInt(backupBytes);
  }

  private void writeToStorageEngine(AbstractStorageEngine storageEngine, int partition, byte[] keyBytes, ByteBuffer putValue) {
    long putStartTimeNs = System.nanoTime();
    storageEngine.put(partition, keyBytes, putValue);
    if (logger.isTraceEnabled()) {
      logger.trace(consumerTaskId + " : Completed PUT to Store: " + topic + " in " +
          (System.nanoTime() - putStartTimeNs) + " ns at " + System.currentTimeMillis());
    }
  }

  /**
   * N.B.: Although this is a one-time initialization routine, there should be no need to guard it by
   *       synchronization, since the {@link StoreIngestionTask} is supposed to process messages in a
   *       sequential, and therefore inherently thread-safe, fashion. If that assumption changes, then
   *       let's make this synchronized.
   *
   * @return the instance of {@link VeniceWriter}, lazily initialized if necessary.
   */
  protected VeniceWriter getVeniceWriter() {
    if (null == veniceWriter) {
      Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topic);
      if (storeVersionState.isPresent()) {
        veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topic, storeVersionState.get().chunked);
      } else {
        throw new IllegalStateException(
            "Should not attempt to call createVeniceWriter() prior to having received the Start of Push, "
                + "specifying whether the store-version is chunked.");
      }
    }
    return veniceWriter;
  }

  /**
   * @return the size of the data which was written to persistent storage.
   */
  private int processVeniceMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int partition = consumerRecord.partition();
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);

    byte[] keyBytes = kafkaKey.getKey();

    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        // If single-threaded, we can re-use (and clobber) the same Put instance. // TODO: explore GC tuning later.
        Put put = (Put) kafkaValue.payloadUnion;
        ByteBuffer putValue = put.putValue;
        int valueLen = putValue.remaining();

        // Write to storage engine; potentially produce the PUT message to version topic
        produceAndWriteToDatabase(consumerRecord,
            key -> prependHeaderAndWriteToStorageEngine(topic, partition, key, putValue, put.schemaId),
            (callback, sourceTopicOffset) -> {
              /**
               * Unfortunately, Kafka does not support fancy array manipulation via {@link ByteBuffer} or otherwise,
               * so we may be forced to do a copy here, if the backing array of the {@link putValue} has padding,
               * which is the case when using {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer}.
               * Since this is in a closure, it is not guaranteed to be invoked.
               *
               * The {@link OnlineOfflineStoreIngestionTask}, which ignores this closure, will not pay this price.
               *
               * Conversely, the {@link LeaderFollowerStoreIngestionTask}, which does invoke it, will.
               *
               * TODO: Evaluate holistically what is the best way to optimize GC for the L/F case.
               */
              byte[] bytesArrayValue = ByteUtils.extractByteArray(putValue);
              return getVeniceWriter().put(keyBytes, bytesArrayValue, put.schemaId, callback, sourceTopicOffset);
            });

        return valueLen;
      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        int valueSchemaId = update.schemaId;
        int derivedSchemaId = update.updateSchemaId;

        //Since we have an async call to produce the message before writing it to the disk, there is a race
        //condition where, if a record gets updated multiple times shortly, the updates may be still
        //pending in the producer queue and have not been written into disk. In order to prevent such
        //async read case, we wait and do not perform read operation until producer finishes firing
        //the message and persisting it to the disk.
        //TODO: this is not efficient. We should consider optimizing the "waiting" behavior here
        try {
          long synchronizeStartTimeInNS = System.nanoTime();
          Future<RecordMetadata> lastLeaderProduceFuture =
              partitionConsumptionStateMap.get(partition).getLastLeaderProduceFuture();
          if (lastLeaderProduceFuture != null) {
            lastLeaderProduceFuture.get();
          }
          storeIngestionStats.recordLeaderProducerSynchronizeLatency(storeNameWithoutVersionInfo, LatencyUtils.getLatencyInMS(synchronizeStartTimeInNS));
        } catch (Exception e) {
          versionedDIVStats.recordLeaderProducerFailure(storeNameWithoutVersionInfo, storeVersion);
        }

        long lookupStartTimeInNS = System.nanoTime();
        boolean isChunkedTopic = storageMetadataService.isStoreVersionChunked(topic);
        GenericRecord originalValue = ComputeChunkingAdapter.get(storeRepository.getLocalStorageEngine(topic), partition,
            ByteBuffer.wrap(keyBytes), isChunkedTopic, null, null, null,
            storageMetadataService.getStoreVersionCompressionStrategy(topic), serverConfig.isComputeFastAvroEnabled(),
            schemaRepo, storeNameWithoutVersionInfo);
        storeIngestionStats.recordWriteComputeLookUpLatency(storeNameWithoutVersionInfo, LatencyUtils.getLatencyInMS(lookupStartTimeInNS));


        long writeComputeStartTimeInNS = System.nanoTime();
        byte[] updatedValueBytes = ingestionTaskWriteComputeAdapter.getUpdatedValueBytes(originalValue,
            update.updateValue, valueSchemaId, derivedSchemaId);
        storeIngestionStats.recordWriteComputeUpdateLatency(storeNameWithoutVersionInfo, LatencyUtils.getLatencyInMS(writeComputeStartTimeInNS));

        ByteBuffer updateValueWithSchemaId = ByteBuffer.allocate(ValueRecord.SCHEMA_HEADER_LENGTH + updatedValueBytes.length)
            .putInt(valueSchemaId).put(updatedValueBytes);
        updateValueWithSchemaId.flip();

        valueLen = updatedValueBytes.length;

        // Write to storage engine; potentially produce the PUT message to version topic
        //TODO: tweak here. write compute doesn't need to do fancy offset manipulation here.
        produceAndWriteToDatabase(consumerRecord,
            key -> {
              if (isChunkedTopic) {
                //Samza VeniceWriter doesn't handle chunking config properly. It reads chuncking config
                //from user's input instead of getting it from store's metadata repo. This causes SN
                //to der-se of keys a couple of times.
                //TODO: Remove chunking logic form SN side once Samze VeniceWriter gets fixed.
                writeToStorageEngine(storeRepository.getLocalStorageEngine(topic), partition,
                    ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(keyBytes),
                    updateValueWithSchemaId);
              } else {
                writeToStorageEngine(storeRepository.getLocalStorageEngine(topic), partition, keyBytes, updateValueWithSchemaId);
              }
            },
            (callback, sourceTopicOffset) ->
                getVeniceWriter().put(keyBytes, updatedValueBytes, valueSchemaId, callback, sourceTopicOffset));
        return valueLen;
      case DELETE:
        // Write to storage engine; potentially produce the DELETE message to version topic
        produceAndWriteToDatabase(consumerRecord,
            key -> {
              long deleteStartTimeNs = System.nanoTime();
              storageEngine.delete(partition, key);

              if (logger.isTraceEnabled()) {
                logger.trace(consumerTaskId + " : Completed DELETE to Store: " + topic + " in " +
                    (System.nanoTime() - deleteStartTimeNs) + " ns at " + System.currentTimeMillis());
              }
            },
            (callback, sourceTopicOffset) ->
                getVeniceWriter().delete(keyBytes, callback, sourceTopicOffset)
        );

        return 0;
      default:
        throw new VeniceMessageException(
                consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
  }

  /**
   * This method checks whether the given record needs to be checked schema availability. Only PUT and UPDATE message
   * needs to #checkValueSchemaAvail
   * @param record
   */
  private void checkSchemaIdAvail(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    KafkaMessageEnvelope kafkaValue = record.value();
    if (record.key().isControlMessage() || null == kafkaValue) {
      return;
    }
    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        checkValueSchemaAvail(put.schemaId);
        break;
      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        checkValueSchemaAvail(update.schemaId);
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
   * If the schema id is not available, this function will polling until the schema appears;
   *
   * @param schemaId
   */
  private void checkValueSchemaAvail(int schemaId) {
    if (-1 == schemaId) {
      // TODO: Once Venice Client (VeniceShellClient) finish the integration with schema registry,
      // we need to remove this check here.
      return;
    }
    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion() ||
        schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      Optional<StoreVersionState> storeVersionState = storageMetadataService.getStoreVersionState(topic);
      if (storeVersionState.isPresent() && storeVersionState.get().chunked) {
        // We're good!
        // TODO: Record metric for chunk ingestion?
        return;
      } else {
        throw new VeniceException("Detected chunking in a store-version where chunking is NOT enabled. Will abort ingestion.");
      }
    }
    // Considering value schema is immutable for an existing store, we can cache it locally
    if (schemaIdSet.contains(schemaId)) {
      return;
    }
    boolean hasValueSchema = schemaRepo.hasValueSchema(storeNameWithoutVersionInfo, schemaId);
    while (!hasValueSchema && this.isRunning.get()) {
      // Since schema registration topic might be slower than data topic,
      // the consumer will be pending until the new schema arrives.
      // TODO: better polling policy
      // TODO: Need to add metrics to track this scenario
      // In the future, we might consider other polling policies,
      // such as throwing error after certain amount of time;
      // Or we might want to propagate our state to the Controller via the VeniceNotifier,
      // if we're stuck polling more than a certain threshold of time?
      logger.warn("Value schema id: " + schemaId + " is not available for store:" + storeNameWithoutVersionInfo);
      Utils.sleep(POLLING_SCHEMA_DELAY_MS);
      hasValueSchema = schemaRepo.hasValueSchema(storeNameWithoutVersionInfo, schemaId);
    }
    logger.info("Get value schema from zookeeper for schema id: " + schemaId + " in store: " + storeNameWithoutVersionInfo);
    if (!hasValueSchema) {
      throw new VeniceException("Value schema id: " + schemaId + " is still not available for store: "
          + storeNameWithoutVersionInfo + ", and it will stop waiting since the consumption task is being shutdown");
    }
    schemaIdSet.add(schemaId);
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
    isRunning.getAndSet(false);

    // KafkaConsumer is closed at the end of the run method.
    // The operation is executed on a single thread in run method.
    // This method signals the run method to end, which closes the
    // resources before exiting.
  }

  /**
   * A function to allow the service to get the current status of the task.
   * This would allow the service to create a new task if required.
   */
  public synchronized boolean isRunning() {
    return isRunning.get();
  }

  public String getTopic() {
    return topic;
  }

  KafkaConsumerWrapper getConsumer() {
    return this.consumer;
  }

  public boolean isMetricsEmissionEnabled() {
    return this.emitMetrics.get();
  }

  public void enableMetricsEmission() {
    this.emitMetrics.set(true);
  }

  public void disableMetricsEmission() {
    this.emitMetrics.set(false);
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
  private void refillPartitionToSizeMap(ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records) {
    subscribedPartitionToSize.get().clear();
    for (int partition : partitionConsumptionStateMap.keySet()) {
      subscribedPartitionToSize.get().put(partition, 0);
    }
    if (records != null) {
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
        int partition = record.partition();
        int recordSize = record.serializedKeySize() + record.serializedValueSize();
        subscribedPartitionToSize.get().put(partition,
                                            subscribedPartitionToSize.get().getOrDefault(partition, 0) + recordSize);
      }
    }
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
   *         i.  if completion has been reported, no need to report anything again;
   *         ii. if completion hasn't been reported and EndOfPush message has been received,
   *             call {@link StoreIngestionTask#isReadyToServe(PartitionConsumptionState)} to
   *             check whether we can report completion.
   */
  private ReadyToServeCheck getDefaultReadyToServeChecker() {
    return (partitionConsumptionState, extraDisjunctionCondition) -> {
      if (extraDisjunctionCondition ||
          (partitionConsumptionState.isEndOfPushReceived() && !partitionConsumptionState.isCompletionReported())) {
        if (isReadyToServe(partitionConsumptionState)) {
          notificationDispatcher.reportCompleted(partitionConsumptionState);
          logger.info(consumerTaskId + " Partition " + partitionConsumptionState.getPartition() + " is ready to serve");
        } else {
          notificationDispatcher.reportProgress(partitionConsumptionState);
        }
      }
    };
  }

  /**
   * A function that would apply updates on the storage engine.
   */
  @FunctionalInterface
  interface WriteToStorageEngine {
    void apply(byte[] key);
  }

  /**
   * A function that would produce messages to topic.
   */
  @FunctionalInterface
  interface ProduceToTopic {
    Future<RecordMetadata> apply(Callback callback, long sourceTopicOffset);
  }

  /**
   * Since get real-time topic offset is expensive, so we will only retrieve source topic offset after the predefined
   * ttlMs
   */
  protected class CachedLatestOffsetGetter {
    private final long ttl;
    private final TopicManager topicManager;
    private final Map<Pair<String, Integer>, Pair<Long, Long>> offsetCache = new ConcurrentHashMap<>();

    CachedLatestOffsetGetter(TopicManager topicManager, long timeToLiveMs) {
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

      entry = offsetCache.compute(key, (k, e) ->
          (e != null && e.getFirst() > now) ?
              e : new Pair<>(now + ttl, topicManager.getLatestOffsetAndRetry(topicName, partitionId, 10)));
      return entry.getSecond();
    }
  }
}
