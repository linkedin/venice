package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to create a task, which will consume the admin messages from the special admin topics.
 */
public class AdminConsumptionTask implements Runnable, Closeable {
  // Setting this to a value so that admin queue will not go out of memory in case of too many admin messages.
  // If we hit this number , there is most likely something seriously wrong with the system.
  private static final int MAX_WORKER_QUEUE_SIZE = 10000;

  private static class AdminErrorInfo {
    PubSubPosition position;
    Exception exception;
    long executionId;
  }

  public static final int MAX_RETRIES_FOR_NONEXISTENT_STORE = 10;

  // A simplified version of ProducerTracker that only checks against previous message's producer info.
  private static class ProducerInfo {
    private GUID producerGUID;
    private int segmentNumber;
    private int sequenceNumber;

    ProducerInfo(ProducerMetadata producerMetadata) {
      updateProducerMetadata(producerMetadata);
    }

    boolean isIncomingMessageValid(ProducerMetadata incomingProducerMetadata) {
      if (!incomingProducerMetadata.producerGUID.equals(producerGUID)) {
        // We will assume a new producer is always valid regardless of its state (starts with segment 0 and etc.).
        // Since we don't have sufficient information here to make any informative decisions and it's rare for both the
        // execution id and producer id edge case to occur at the same time.
        updateProducerMetadata(incomingProducerMetadata);
        return true;
      }
      if (incomingProducerMetadata.segmentNumber != segmentNumber) {
        return false;
      }
      if (incomingProducerMetadata.messageSequenceNumber == sequenceNumber + 1) {
        // Expected behavior, update the sequenceNumber.
        sequenceNumber = incomingProducerMetadata.messageSequenceNumber;
      }
      // Duplicate message is acceptable.
      return incomingProducerMetadata.messageSequenceNumber <= sequenceNumber;
    }

    void updateProducerMetadata(ProducerMetadata producerMetadata) {
      this.producerGUID = producerMetadata.producerGUID;
      this.segmentNumber = producerMetadata.segmentNumber;
      this.sequenceNumber = producerMetadata.messageSequenceNumber;
    }

    @Override
    public String toString() {
      return String.format(
          "{producerGUID: %s, segmentNumber: %d, sequenceNumber: %d}",
          producerGUID,
          segmentNumber,
          sequenceNumber);
    }
  }

  private static final String CONSUMER_TASK_ID_FORMAT = AdminConsumptionTask.class.getSimpleName() + "-%s";
  private static final long UNASSIGNED_VALUE = -1L;
  private static final int READ_CYCLE_DELAY_MS = 1000;
  private static final int MAX_DUPLICATE_MESSAGE_LOGS = 20;
  private static final long CONSUMPTION_LAG_UPDATE_INTERVAL_IN_MS = TimeUnit.MINUTES.toMillis(5);
  public static final int IGNORED_CURRENT_VERSION = -1;

  /** Used by the storage persona in the admin operations map because the persona operations are not associated with
   * any particular store */
  private static final String STORAGE_PERSONA_MAP_KEY = "STORAGE_PERSONA";

  private final Logger LOGGER;

  private final String clusterName;
  private final PubSubTopic pubSubAdminTopic;
  private final PubSubTopicPartition adminTopicPartition;
  private final String consumerTaskId;
  private final AdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private final VeniceHelixAdmin admin;
  private final boolean isParentController;
  private final AtomicBoolean isRunning;
  private final AdminOperationSerializer deserializer;
  private final AdminConsumptionStats stats;
  private final int adminTopicReplicationFactor;
  private final Optional<Integer> minInSyncReplicas;
  private final boolean remoteConsumptionEnabled;

  private boolean isSubscribed;
  private final PubSubConsumerAdapter consumer;
  private volatile PubSubPosition positionToSkip = PubSubSymbolicPosition.EARLIEST;
  private volatile long executionIdToSkip = UNASSIGNED_VALUE;
  private volatile PubSubPosition positionToSkipDIV = PubSubSymbolicPosition.EARLIEST;
  private volatile long executionIdToSkipDIV = UNASSIGNED_VALUE;
  /**
  * The smallest or first failing position.
  */
  private volatile PubSubPosition failingPosition = PubSubSymbolicPosition.EARLIEST;
  private volatile long failingExecutionId = UNASSIGNED_VALUE;
  private boolean topicExists;
  /**
   * A {@link Map} of stores to admin operations belonging to each store. The corresponding pubsub position and other
   * metadata of each admin operation are included in the {@link AdminOperationWrapper}.
   */
  private final Map<String, Queue<AdminOperationWrapper>> adminOperationsByStore;

  /**
   * Map of store names that have encountered some sort of exception during consumption to {@link AdminErrorInfo}
   * that has the details about the exception and the position of the problematic admin message.
   */
  private final ConcurrentHashMap<String, AdminErrorInfo> problematicStores;
  private final Queue<DefaultPubSubMessage> undelegatedRecords;

  private final Map<String, Map<PubSubPosition, Integer>> storeRetryCountMap;

  private final ExecutionIdAccessor executionIdAccessor;
  private ExecutorService executorService;

  private TopicManager topicManager;

  public ExecutorService getExecutorService() {
    return executorService;
  }

  private final long processingCycleTimeoutInMs;
  /**
   * Once all admin messages in a cycle is processed successfully, the id would be updated together with the position.
   * It represents a kind of comparable progress of admin topic consumption among all controllers.
   */
  private long lastPersistedExecutionId = UNASSIGNED_VALUE;
  /**
   * The corresponding position to {@code lastPersistedExecutionId}
   */
  private PubSubPosition lastPersistedPosition = PubSubSymbolicPosition.EARLIEST;
  /**
   * The execution id of the last message that was delegated to a store's queue. Used for DIV check when fetching
   * messages from the admin topic.
   */
  private long lastDelegatedExecutionId = UNASSIGNED_VALUE;
  /**
   * The corresponding position to {@code lastDelegatedExecutionId}
   */
  private PubSubPosition lastDelegatedPosition = PubSubSymbolicPosition.EARLIEST;
  /**
   * Track the latest consumed position; this variable is updated as long as the consumer consumes new messages,
   * no matter whether the message has any issue or not.
   */
  private PubSubPosition lastConsumedPosition = PubSubSymbolicPosition.EARLIEST;
  /**
   * The local position value in ZK during initialization phase; the value will not be updated during admin topic consumption.
   *
   * Currently, there are two potential position: local position and upstream position, and we only update and
   * maintain one of them. While persisting the position to ZK, we would like to keep the original value
   * for the other one, so that rollback/roll-forward of the remote consumption feature can be faster.
   */
  private PubSubPosition localPositionCheckpointAtStartTime = PubSubSymbolicPosition.EARLIEST;
  private PubSubPosition upstreamPositionCheckpointAtStartTime = PubSubSymbolicPosition.EARLIEST;

  /**
   * Map of store names to their last succeeded execution id
   */
  private volatile ConcurrentHashMap<String, Long> lastSucceededExecutionIdMap;
  /**
   * An in-memory DIV tracker used as a backup to execution id to verify the integrity of admin messages.
   */
  private ProducerInfo producerInfo = null;

  /**
   * During roll-out/roll-back phase of the remote consumption feature for admin topic, child controllers will consume
   * messages that have already been processed before. In order to not flood the log with duplicate message logging,
   * keep track of consecutive duplicate messages and stop logging after {@link AdminConsumptionTask#MAX_DUPLICATE_MESSAGE_LOGS}
   * consecutive duplicates.
   */
  private int consecutiveDuplicateMessageCount = 0;

  /**
   * Timestamp in millisecond: the last time when updating the consumption position lag metric
   */
  private long lastUpdateTimeForConsumptionPositionLag = 0;

  /**
   * The local region name of the controller.
   */
  private final String regionName;

  public AdminConsumptionTask(
      String clusterName,
      PubSubConsumerAdapter consumer,
      boolean remoteConsumptionEnabled,
      Optional<String> remoteKafkaServerUrl,
      VeniceHelixAdmin admin,
      AdminTopicMetadataAccessor adminTopicMetadataAccessor,
      ExecutionIdAccessor executionIdAccessor,
      boolean isParentController,
      AdminConsumptionStats stats,
      int adminTopicReplicationFactor,
      Optional<Integer> minInSyncReplicas,
      long processingCycleTimeoutInMs,
      int maxWorkerThreadPoolSize,
      PubSubTopicRepository pubSubTopicRepository,
      String regionName) {
    this.clusterName = clusterName;
    this.pubSubAdminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
    this.adminTopicPartition = new PubSubTopicPartitionImpl(pubSubAdminTopic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, clusterName);
    this.LOGGER = LogManager.getLogger(consumerTaskId);
    this.admin = admin;
    this.isParentController = isParentController;
    this.deserializer = new AdminOperationSerializer();
    this.isRunning = new AtomicBoolean(true);
    this.isSubscribed = false;
    this.topicExists = false;
    this.stats = stats;
    this.adminTopicReplicationFactor = adminTopicReplicationFactor;
    this.minInSyncReplicas = minInSyncReplicas;
    this.consumer = consumer;
    this.remoteConsumptionEnabled = remoteConsumptionEnabled;
    this.adminTopicMetadataAccessor = adminTopicMetadataAccessor;
    this.executionIdAccessor = executionIdAccessor;
    this.processingCycleTimeoutInMs = processingCycleTimeoutInMs;

    this.adminOperationsByStore = new ConcurrentHashMap<>();
    this.problematicStores = new ConcurrentHashMap<>();
    // since we use an unbounded queue the core pool size is really the max pool size
    this.executorService = new ThreadPoolExecutor(
        maxWorkerThreadPoolSize,
        maxWorkerThreadPoolSize,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(MAX_WORKER_QUEUE_SIZE),
        new DaemonThreadFactory(String.format("Venice-Admin-Execution-Task-%s", clusterName), admin.getLogContext()));
    this.undelegatedRecords = new LinkedList<>();
    this.stats.setAdminConsumptionFailedPosition(failingPosition);
    this.regionName = regionName;
    this.storeRetryCountMap = new ConcurrentHashMap<>();

    if (remoteConsumptionEnabled) {
      if (!remoteKafkaServerUrl.isPresent()) {
        throw new VeniceException(
            "Admin topic remote consumption is enabled but no config found for the source Kafka bootstrap server url");
      }
      this.topicManager = admin.getTopicManager(remoteKafkaServerUrl.get());
    } else {
      this.topicManager = admin.getTopicManager();
    }
  }

  // For testing purpose only
  void setAdminExecutionTaskExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public synchronized void close() throws IOException {
    isRunning.getAndSet(false);
  }

  @Override
  public void run() {
    LOGGER.info("Running {}", this.getClass().getSimpleName());
    long lastLogTime = 0;
    while (isRunning.get()) {
      try {
        Utils.sleep(READ_CYCLE_DELAY_MS);
        if (!admin.isLeaderControllerFor(clusterName) || !admin.isAdminTopicConsumptionEnabled(clusterName)) {
          unSubscribe();
          continue;
        }
        if (!isSubscribed) {
          if (whetherTopicExists(pubSubAdminTopic)) {
            // Topic was not created by this process, so we make sure it has the right retention.
            makeSureAdminTopicUsingInfiniteRetentionPolicy(pubSubAdminTopic);
          } else {
            String logMessageFormat = "Admin topic: {} hasn't been created yet. {}";
            if (!isParentController) {
              // To reduce log bloat, only log once per minute
              if (System.currentTimeMillis() - lastLogTime > 60 * Time.MS_PER_SECOND) {
                LOGGER.info(
                    logMessageFormat,
                    pubSubAdminTopic,
                    "Since this is a child controller, it will not be created by this process.");
                lastLogTime = System.currentTimeMillis();
              }
              continue;
            }
            LOGGER.info(
                logMessageFormat,
                pubSubAdminTopic,
                "Since this is the parent controller, it will be created now.");
            admin.getTopicManager()
                .createTopic(pubSubAdminTopic, 1, adminTopicReplicationFactor, true, false, minInSyncReplicas);
            LOGGER.info("Admin topic {} is created.", pubSubAdminTopic);
          }
          subscribe();
        }
        Iterator<DefaultPubSubMessage> recordsIterator;
        // Only poll the kafka channel if there are no more undelegated records due to exceptions.
        if (undelegatedRecords.isEmpty()) {
          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = consumer.poll(READ_CYCLE_DELAY_MS);
          if (messages == null || messages.isEmpty()) {
            LOGGER.debug("Received null or no messages");
          } else {
            int polledMessageCount = messages.values().stream().mapToInt(List::size).sum();
            LOGGER.info("Consumed {} admin messages from kafka. Will queue them up for processing", polledMessageCount);
            recordsIterator = Utils.iterateOnMapOfLists(messages);
            while (recordsIterator.hasNext()) {
              DefaultPubSubMessage newRecord = recordsIterator.next();
              lastConsumedPosition = newRecord.getPosition();
              undelegatedRecords.add(newRecord);
            }
          }
        } else {
          LOGGER.info(
              "There are {} admin messages in the undelegated message queue. "
                  + "Will consume from the undelegated queue first before polling the admin topic.",
              undelegatedRecords.size());
        }

        while (!undelegatedRecords.isEmpty()) {
          DefaultPubSubMessage record = undelegatedRecords.peek();
          if (record == null) {
            break;
          }
          long executionId = UNASSIGNED_VALUE;
          try {
            executionId = delegateMessage(record);
            if (executionId == lastDelegatedExecutionId) {
              updateLastPosition(record.getPosition());
            }
            undelegatedRecords.remove();
          } catch (DataValidationException dve) {
            // Very unlikely but DataValidationException could be thrown here.
            LOGGER.error(
                "Admin consumption task is blocked due to DataValidationException with position {}",
                record.getPosition(),
                dve);
            failingPosition = record.getPosition();
            stats.recordFailedAdminConsumption();
            stats.recordAdminTopicDIVErrorReportCount();
            break;
          } catch (Exception e) {
            LOGGER
                .error("Admin consumption task is blocked due to Exception with position {}", record.getPosition(), e);
            failingPosition = record.getPosition();
            failingExecutionId = executionId;
            stats.recordFailedAdminConsumption();
            break;
          }
        }

        if (remoteConsumptionEnabled && LatencyUtils.getElapsedTimeFromMsToMs(
            lastUpdateTimeForConsumptionPositionLag) > getConsumptionLagUpdateIntervalInMs()) {
          recordConsumptionLag();
          lastUpdateTimeForConsumptionPositionLag = System.currentTimeMillis();
        }
        executeMessagesAndCollectResults();
        stats.setAdminConsumptionFailedPosition(failingPosition);
      } catch (Exception e) {
        LOGGER.error("Exception thrown while running admin consumption task", e);
        // Unsubscribe and resubscribe in the next cycle to start over and avoid missing messages from poll.
        unSubscribe();
      }
    }
    // Release resources
    internalClose();
  }

  private void subscribe() {
    AdminMetadata metaData = adminTopicMetadataAccessor.getMetadata(clusterName);
    boolean hasCheckpoint = remoteConsumptionEnabled
        ? !PubSubSymbolicPosition.EARLIEST.equals(metaData.getUpstreamPosition())
        : !PubSubSymbolicPosition.EARLIEST.equals(metaData.getPosition());
    if (hasCheckpoint) {
      localPositionCheckpointAtStartTime = metaData.getPosition();
      upstreamPositionCheckpointAtStartTime = metaData.getUpstreamPosition();
      lastPersistedPosition =
          remoteConsumptionEnabled ? upstreamPositionCheckpointAtStartTime : localPositionCheckpointAtStartTime;
      lastPersistedExecutionId = metaData.getExecutionId();
      /**
       * For the first poll after subscription, Controller will try to consume one message older than {@link #lastPersistedPosition}
       * to initialize the {@link #producerInfo}, which will be used to decide whether an execution id gap is a false alarm or not
       * in {@link #checkAndValidateMessage}.
       *
       */
      lastDelegatedPosition = lastPersistedPosition;
      lastDelegatedExecutionId = lastPersistedExecutionId;
    } else {
      LOGGER.info("Admin topic metadata is empty, will resume consumption from the starting position");
      lastDelegatedPosition = PubSubSymbolicPosition.EARLIEST;
      lastDelegatedExecutionId = UNASSIGNED_VALUE;
    }
    // Subscribe the admin topic
    consumer.subscribe(adminTopicPartition, lastDelegatedPosition, true);
    isSubscribed = true;
    LOGGER.info(
        "Subscribed to topic name: {}, with position: {} and execution id: {}. Remote consumption flag: {}",
        adminTopicPartition,
        lastDelegatedPosition,
        lastPersistedExecutionId,
        remoteConsumptionEnabled);
  }

  private void unSubscribe() {
    if (isSubscribed) {
      consumer.unSubscribe(adminTopicPartition);
      adminOperationsByStore.clear();
      problematicStores.clear();
      undelegatedRecords.clear();
      failingPosition = PubSubSymbolicPosition.EARLIEST;
      failingExecutionId = UNASSIGNED_VALUE;
      positionToSkip = PubSubSymbolicPosition.EARLIEST;
      executionIdToSkip = UNASSIGNED_VALUE;
      positionToSkipDIV = PubSubSymbolicPosition.EARLIEST;
      executionIdToSkipDIV = UNASSIGNED_VALUE;
      lastDelegatedExecutionId = UNASSIGNED_VALUE;
      lastPersistedExecutionId = UNASSIGNED_VALUE;
      lastDelegatedPosition = PubSubSymbolicPosition.EARLIEST;
      lastPersistedPosition = PubSubSymbolicPosition.EARLIEST;
      producerInfo = null;
      stats.recordPendingAdminMessagesCount(UNASSIGNED_VALUE);
      stats.recordStoresWithPendingAdminMessagesCount(UNASSIGNED_VALUE);
      resetConsumptionLag();
      isSubscribed = false;
      LOGGER.info(
          "Unsubscribed from topic name: {}. Remote consumption flag before unsubscription: {}",
          adminTopicPartition,
          remoteConsumptionEnabled);
    }
  }

  /**
   * Package private for testing purpose
   *
   * Delegate work from the {@link #adminOperationsByStore} to the worker threads. Wait for the worker threads
   * to complete or when timeout {@code processingCycleTimeoutInMs} is reached. Collect the result of each thread.
   * The result can either be success: all given {@link AdminOperation}s were processed successfully or made progress
   * but couldn't finish processing all of it within the time limit for each cycle. Failure is when either an exception
   * was thrown or the thread got stuck while processing the problematic {@link AdminOperation}.
   * @throws InterruptedException
   */
  private void executeMessagesAndCollectResults() throws InterruptedException {
    lastSucceededExecutionIdMap =
        new ConcurrentHashMap<>(executionIdAccessor.getLastSucceededExecutionIdMap(clusterName));
    /** This set is used to track which store has a task scheduled, so that we schedule at most one per store. */
    Set<String> storesWithScheduledTask = new HashSet<>();
    /** List of tasks to be executed by the worker threads. */
    List<Callable<Void>> tasks = new ArrayList<>();
    /**
     * Note that tasks and stores are parallel lists (the elements at each index correspond to one another), and both
     * lists are also parallel to the results list, declared later in this function.
     */
    List<String> stores = new ArrayList<>();
    // Create a task for each store that has admin messages pending to be processed.
    boolean skipOffsetCommandHasBeenProcessed = false;
    boolean skipExecutionIdCommandHasBeenProcessed = false;
    for (Map.Entry<String, Queue<AdminOperationWrapper>> entry: adminOperationsByStore.entrySet()) {
      String storeName = entry.getKey();
      Queue<AdminOperationWrapper> storeQueue = entry.getValue();
      if (!storeQueue.isEmpty()) {
        AdminOperationWrapper nextOp = storeQueue.peek();
        if (nextOp == null) {
          continue;
        }
        PubSubPosition adminMessagePosition = nextOp.getPosition();
        if (checkPositionToSkip(adminMessagePosition, false)) {
          storeQueue.remove();
          skipOffsetCommandHasBeenProcessed = true;
        }
        // We are replacing `skipping admin messages by offset` with `skipping admin messages by execution id`.
        // Temporarily, skipping will be supported by both offset and execution id (but not both in the same admin tool
        // command)
        // and hence some duplicate code, e.g. `checkOffsetToSkip()` and `checkExecutionIdToSkip()`,
        // `skipMessageWithOffset()` and `skipMessageWithExecutionId()`.
        // This is to allow users to transition from using offset to using execution id.
        // Very soon in the future, we will remove the support for skipping by offset.

        if (checkExecutionIdToSkip(nextOp.getExecutionId(), false)) {
          storeQueue.remove();
          skipExecutionIdCommandHasBeenProcessed = true;
        }
        AdminExecutionTask newTask = new AdminExecutionTask(
            LOGGER,
            clusterName,
            storeName,
            lastSucceededExecutionIdMap,
            lastPersistedExecutionId,
            storeQueue,
            admin,
            executionIdAccessor,
            isParentController,
            stats,
            regionName);
        // Check if there is previously created scheduled task still occupying one thread from the pool.
        if (storesWithScheduledTask.add(storeName)) {
          // Log the store name and the position of the task being added into the task list
          LOGGER.info(
              "Adding admin message from store {} with position {} to the task list",
              storeName,
              adminMessagePosition);
          tasks.add(newTask);
          stores.add(storeName);
        }
      }
    }
    if (skipOffsetCommandHasBeenProcessed) {
      resetPositionToSkip();
    }
    if (skipExecutionIdCommandHasBeenProcessed) {
      resetExecutionIdToSkip();
    }

    if (isRunning.get()) {
      if (!tasks.isEmpty()) {
        int pendingAdminMessagesCount = 0;
        int storesWithPendingAdminMessagesCount = 0;
        long adminExecutionTasksInvokeTime = System.currentTimeMillis();
        // Wait for the worker threads to finish processing the internal admin topics.
        List<Future<Void>> results =
            executorService.invokeAll(tasks, processingCycleTimeoutInMs, TimeUnit.MILLISECONDS);
        stats.recordAdminConsumptionCycleDurationMs(System.currentTimeMillis() - adminExecutionTasksInvokeTime);
        Map<String, Long> newLastSucceededExecutionIdMap =
            executionIdAccessor.getLastSucceededExecutionIdMap(clusterName);
        boolean internalQueuesEmptied = true;
        for (int i = 0; i < results.size(); i++) {
          String storeName = stores.get(i);
          Future<Void> result = results.get(i);
          try {
            result.get();
            problematicStores.remove(storeName);
            if (internalQueuesEmptied) {
              Queue<AdminOperationWrapper> storeQueue = adminOperationsByStore.get(storeName);
              if (storeQueue != null && !storeQueue.isEmpty()) {
                internalQueuesEmptied = false;
              }
            }
          } catch (ExecutionException | CancellationException e) {
            internalQueuesEmptied = false;
            AdminErrorInfo errorInfo = new AdminErrorInfo();
            Queue<AdminOperationWrapper> storeQueue = adminOperationsByStore.get(storeName);
            int perStorePendingMessagesCount = storeQueue == null ? 0 : storeQueue.size();
            pendingAdminMessagesCount += perStorePendingMessagesCount;
            storesWithPendingAdminMessagesCount += perStorePendingMessagesCount > 0 ? 1 : 0;

            // Check if the cause is VeniceNoStoreException
            Throwable cause = e.getCause();
            if (cause instanceof VeniceNoStoreException) {
              // Get the retry count for this store and version combination
              Map<PubSubPosition, Integer> retryCountMap =
                  storeRetryCountMap.computeIfAbsent(storeName, s -> new ConcurrentHashMap<>());
              AdminOperationWrapper nextOp = storeQueue != null ? storeQueue.peek() : null;
              boolean allowAutoSkip = false;
              if (nextOp != null) {
                AdminMessageType messageType = AdminMessageType.valueOf(nextOp.getAdminOperation());
                // Only allow auto skipping when store not exist for update-store and delete-store admin messages.
                allowAutoSkip =
                    messageType == AdminMessageType.UPDATE_STORE || messageType == AdminMessageType.DELETE_STORE;
              }

              PubSubPosition position = nextOp != null ? nextOp.getPosition() : PubSubSymbolicPosition.EARLIEST;
              long executionId = nextOp != null ? nextOp.getExecutionId() : UNASSIGNED_VALUE;
              int currentRetryCount = retryCountMap.getOrDefault(position, 0);

              if (currentRetryCount >= MAX_RETRIES_FOR_NONEXISTENT_STORE && allowAutoSkip) {
                // We've reached the maximum retry limit for this store/message, so remove it from the queue
                if (storeQueue != null && !storeQueue.isEmpty()) {
                  AdminOperationWrapper removedOp = storeQueue.remove();
                  LOGGER.info(
                      "Exceeded maximum retry attempts ({}) for store {} that does not exist. Skipping admin message with offset {}.",
                      MAX_RETRIES_FOR_NONEXISTENT_STORE,
                      storeName,
                      removedOp.getPosition().getNumericOffset());
                  retryCountMap.remove(position);
                  problematicStores.remove(storeName);
                  continue;
                }
              } else {
                // Increment the retry count
                retryCountMap.put(position, currentRetryCount + 1);
                LOGGER.warn(
                    "Store {} does not exist. Retry attempt {}/{}. Will retry admin message with position {}.",
                    storeName,
                    currentRetryCount + 1,
                    MAX_RETRIES_FOR_NONEXISTENT_STORE,
                    position);

                // Add the error info as normal for retry
                errorInfo.exception = (VeniceNoStoreException) cause;
                errorInfo.position = position;
                errorInfo.executionId = executionId;
                problematicStores.put(storeName, errorInfo);
              }
            } else if (e instanceof CancellationException) {
              long lastSucceededId = lastSucceededExecutionIdMap.getOrDefault(storeName, UNASSIGNED_VALUE);
              long newLastSucceededId = newLastSucceededExecutionIdMap.getOrDefault(storeName, UNASSIGNED_VALUE);

              if (lastSucceededId == UNASSIGNED_VALUE) {
                LOGGER.error("Could not find last successful execution ID for store {}", storeName);
              }

              if (lastSucceededId == newLastSucceededId && perStorePendingMessagesCount > 0) {
                // only mark the store problematic if no progress is made and there are still message(s) in the queue.
                errorInfo.exception = new VeniceException(
                    "Could not finish processing admin message for store " + storeName + " in time");
                errorInfo.position = getNextOperationPositionIfAvailable(storeName);
                errorInfo.executionId = getNextOperationExecutionIdIfAvailable(storeName);
                problematicStores.put(storeName, errorInfo);
                LOGGER.warn(errorInfo.exception.getMessage());
              }
            } else {
              errorInfo.exception = e;
              errorInfo.position = getNextOperationPositionIfAvailable(storeName);
              errorInfo.executionId = getNextOperationExecutionIdIfAvailable(storeName);
              problematicStores.put(storeName, errorInfo);
            }
          } catch (Throwable e) {
            PubSubPosition errorMsgPosition = getNextOperationPositionIfAvailable(storeName);
            if (PubSubSymbolicPosition.EARLIEST.equals(errorMsgPosition)) {
              LOGGER.error("Could not get the position of the problematic admin message for store {}", storeName);
            }

            LOGGER.error(
                "Unexpected exception thrown while processing admin message for store {} at position {}",
                storeName,
                errorMsgPosition,
                e);
            // Throw it above to have the consistent behavior as before
            throw e;
          }
        }
        if (problematicStores.isEmpty() && internalQueuesEmptied) {
          // All admin operations were successfully executed or skipped.
          // 1. Clear the failing position.
          // 3. Persist the latest execution id and position (cluster wide) to ZK.

          // Ensure failingPosition from the delegateMessage is not overwritten.

          if (topicManager.diffPosition(adminTopicPartition, failingPosition, lastDelegatedPosition) <= 0) {
            failingPosition = PubSubSymbolicPosition.EARLIEST;
          }
          if (failingExecutionId <= lastDelegatedExecutionId) {
            failingExecutionId = UNASSIGNED_VALUE;
          }
          persistAdminTopicMetadata();
        } else {
          // One or more stores encountered problems while executing their admin operations.
          // 1. Do not persist the latest position (cluster wide) to ZK.
          // 2. Find and set the smallest failing position amongst the problematic stores.
          PubSubPosition smallestPosition = PubSubSymbolicPosition.EARLIEST;
          long smallestExecutionId = UNASSIGNED_VALUE;

          for (Map.Entry<String, AdminErrorInfo> problematicStore: problematicStores.entrySet()) {
            if (PubSubSymbolicPosition.EARLIEST.equals(smallestPosition) || topicManager
                .diffPosition(adminTopicPartition, problematicStore.getValue().position, smallestPosition) < 0) {
              smallestPosition = problematicStore.getValue().position;
              smallestExecutionId = problematicStore.getValue().executionId;
            }
          }
          // Ensure failingPosition from the delegateMessage is not overwritten.
          if (topicManager.diffPosition(adminTopicPartition, failingPosition, lastDelegatedPosition) <= 0) {
            failingPosition = smallestPosition;
            failingExecutionId = smallestExecutionId;
          }
        }
        stats.recordPendingAdminMessagesCount(pendingAdminMessagesCount);
        stats.recordStoresWithPendingAdminMessagesCount(storesWithPendingAdminMessagesCount);
      } else {
        // in situations when we skipped a blocking message (while delegating) and no other messages are queued up.
        persistAdminTopicMetadata();
      }
    }
  }

  /**
   * @return the position of the next enqueued operation for the given store name, or {@link #UNASSIGNED_VALUE} if unavailable.
   */
  private PubSubPosition getNextOperationPositionIfAvailable(String storeName) {
    Queue<AdminOperationWrapper> storeQueue = adminOperationsByStore.get(storeName);
    AdminOperationWrapper nextOperation = storeQueue == null ? null : storeQueue.peek();
    return nextOperation == null ? PubSubSymbolicPosition.EARLIEST : nextOperation.getPosition();
  }

  private long getNextOperationExecutionIdIfAvailable(String storeName) {
    Queue<AdminOperationWrapper> storeQueue = adminOperationsByStore.get(storeName);
    AdminOperationWrapper nextOperation = storeQueue == null ? null : storeQueue.peek();
    return nextOperation == null ? UNASSIGNED_VALUE : nextOperation.getExecutionId();
  }

  private void internalClose() {
    unSubscribe();
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(processingCycleTimeoutInMs, TimeUnit.MILLISECONDS)) {
        LOGGER.warn(
            "consumer Task Id {}: Unable to shutdown worker executor thread pool in admin consumption task in time",
            consumerTaskId);
      }
    } catch (InterruptedException e) {
      LOGGER.warn(
          "consumer Task Id {}: Interrupted while waiting for worker executor thread pool to shutdown",
          consumerTaskId);
    }
    LOGGER.info("Closed consumer for admin topic: {}", adminTopicPartition);
    consumer.close();
  }

  private boolean whetherTopicExists(PubSubTopic topicName) {
    if (topicExists) {
      return true;
    }
    // Check it again if it is false
    return topicManager.containsTopicAndAllPartitionsAreOnline(topicName);
  }

  private void makeSureAdminTopicUsingInfiniteRetentionPolicy(PubSubTopic topicName) {
    topicManager.updateTopicRetention(topicName, Long.MAX_VALUE);
    LOGGER.info("Admin topic: {} has been updated to use infinite retention policy", adminTopicPartition);
  }

  /**
   * This method groups {@link AdminOperation}s by their corresponding store.
   * @param record The {@link PubSubMessage} containing the {@link AdminOperation}.
   * @return corresponding executionId if applicable.
   */
  private long delegateMessage(DefaultPubSubMessage record) {
    if (checkPositionToSkip(record.getPosition(), true) || !shouldProcessRecord(record)) {
      // Return lastDelegatedExecutionId to update the offset without changing the execution id. Skip DIV should/can be
      // used if the skip requires executionId to be reset because this skip here is skipping the message without doing
      // any processing. This may be the case when a message cannot be deserialized properly therefore we don't know
      // what's the right execution id and producer info to set moving forward.
      return lastDelegatedExecutionId;
    }
    KafkaKey kafkaKey = record.getKey();
    KafkaMessageEnvelope kafkaValue = record.getValue();
    MessageType messageType = MessageType.valueOf(kafkaValue);
    Pair<Long, AdminOperation> executionIdAndAdminOperation = extractExecutionIdAndAndAdminOperation(record);
    long executionId = executionIdAndAdminOperation.getFirst();
    AdminOperation adminOperation = executionIdAndAdminOperation.getSecond();

    if (kafkaKey.isControlMessage()) {
      LOGGER.debug("Received control message: {}", kafkaValue);
      return UNASSIGNED_VALUE;
    }
    // check message type
    if (MessageType.PUT != messageType) {
      throw new VeniceException("Received unexpected message type: " + messageType);
    }
    if (checkExecutionIdToSkip(executionId, true)) {
      // we have delegated this record and now skipping it as instructed, so update the last delegated execution id
      // this will also update the last position
      lastDelegatedExecutionId = executionId;
      return executionId;
    }
    try {
      checkAndValidateMessage(executionId, record);
      LOGGER.info(
          "Received admin operation: {}, message {} position: {}",
          AdminMessageType.valueOf(adminOperation).name(),
          adminOperation,
          record.getPosition());
      consecutiveDuplicateMessageCount = 0;
    } catch (DuplicateDataException e) {
      // Previously processed message, safe to skip
      if (consecutiveDuplicateMessageCount < MAX_DUPLICATE_MESSAGE_LOGS) {
        consecutiveDuplicateMessageCount++;
        LOGGER.info(e.getMessage());
      } else if (consecutiveDuplicateMessageCount == MAX_DUPLICATE_MESSAGE_LOGS) {
        LOGGER.info(
            "It appears that controller is consuming from a low position and encounters many admin messages that "
                + "have already been processed. Will stop logging duplicate messages until a fresh admin message.");
      }
      return executionId;
    }

    AdminMessageType adminMessageType = AdminMessageType.valueOf(adminOperation);
    if (adminMessageType.isBatchUpdate()) {
      long producerTimestamp = kafkaValue.producerMetadata.messageTimestamp;
      long brokerTimestamp = record.getPubSubMessageTime();
      List<Store> stores = admin.getAllStores(clusterName);
      for (Store store: stores) {
        String storeName = store.getName();
        Queue<AdminOperationWrapper> operationQueue =
            adminOperationsByStore.computeIfAbsent(storeName, n -> new LinkedList<>());
        AdminOperationWrapper adminOperationWrapper = new AdminOperationWrapper(
            adminOperation,
            record.getPosition(),
            executionId,
            producerTimestamp,
            brokerTimestamp,
            System.currentTimeMillis());
        operationQueue.add(adminOperationWrapper);
        stats.recordAdminMessageMMLatency(
            Math.max(
                0,
                adminOperationWrapper.getLocalBrokerTimestamp() - adminOperationWrapper.getProducerTimestamp()));
        stats.recordAdminMessageDelegateLatency(
            Math.max(
                0,
                adminOperationWrapper.getDelegateTimestamp() - adminOperationWrapper.getLocalBrokerTimestamp()));
      }
    } else {
      long producerTimestamp = kafkaValue.producerMetadata.messageTimestamp;
      long brokerTimestamp = record.getPubSubMessageTime();
      AdminOperationWrapper adminOperationWrapper = new AdminOperationWrapper(
          adminOperation,
          record.getPosition(),
          executionId,
          producerTimestamp,
          brokerTimestamp,
          System.currentTimeMillis());
      stats.recordAdminMessageMMLatency(
          Math.max(0, adminOperationWrapper.getLocalBrokerTimestamp() - adminOperationWrapper.getProducerTimestamp()));
      stats.recordAdminMessageDelegateLatency(
          Math.max(0, adminOperationWrapper.getDelegateTimestamp() - adminOperationWrapper.getLocalBrokerTimestamp()));
      String storeName = extractStoreName(adminOperation);
      adminOperationsByStore.putIfAbsent(storeName, new LinkedList<>());
      adminOperationsByStore.get(storeName).add(adminOperationWrapper);
    }
    return executionId;
  }

  /**
   * Returns a pair of execution id and {@link AdminOperation} of the provided {@link DefaultPubSubMessage}
   * If the message type is not PUT, {@link AdminConsumptionTask#UNASSIGNED_VALUE} is returned in execution id.
   */
  private Pair<Long, AdminOperation> extractExecutionIdAndAndAdminOperation(DefaultPubSubMessage record) {
    KafkaMessageEnvelope kafkaValue = record.getValue();
    MessageType messageType = MessageType.valueOf(kafkaValue);
    AdminOperation adminOperation = null;
    long executionId = UNASSIGNED_VALUE;
    long executionIdFromPayload = UNASSIGNED_VALUE;
    long executionIdFromHeader = UNASSIGNED_VALUE;
    if (MessageType.PUT == messageType) {
      Put put = (Put) kafkaValue.payloadUnion;
      try {
        adminOperation = deserializer.deserialize(put.putValue, put.schemaId);
        executionIdFromPayload = adminOperation.executionId;
      } catch (Exception e) {
        LOGGER.error("Failed to deserialize admin operation", e);
      }
      PubSubMessageHeader header = record.getPubSubMessageHeaders().get(PubSubMessageHeaders.EXECUTION_ID_KEY);
      if (header != null) {
        try {
          executionIdFromHeader = ByteBuffer.wrap(header.value()).getLong();
        } catch (Exception e) {
          LOGGER
              .error("Failed to read execution id from the message header, fallback to reading it from the payload", e);
        }
      }
      executionId = resolveExecutionId(executionIdFromHeader, executionIdFromPayload, adminOperation != null);
    }
    return new Pair<>(executionId, adminOperation);
  }

  private long resolveExecutionId(long fromHeader, long fromPayload, boolean payloadDeserialized) {
    if (fromHeader != UNASSIGNED_VALUE && fromPayload != UNASSIGNED_VALUE && fromHeader != fromPayload) {
      if (payloadDeserialized) {
        LOGGER.error("Execution id mismatch (header={}, payload={}). Using payload.", fromHeader, fromPayload);
        return fromPayload;
      } else {
        LOGGER.error("Using execution id from the header {} because payload could not be deserialized.", fromHeader);
        return fromHeader;
      }
    }
    return payloadDeserialized ? fromPayload : fromHeader;
  }

  private void checkAndValidateMessage(long incomingExecutionId, DefaultPubSubMessage record) {
    if (checkOffsetToSkipDIV(record.getPosition()) || checkExecutionIdToSkipDIV(incomingExecutionId)
        || lastDelegatedExecutionId == UNASSIGNED_VALUE) {
      lastDelegatedExecutionId = incomingExecutionId;
      LOGGER.info(
          "Updated lastDelegatedExecutionId to {} because lastDelegatedExecutionId is currently UNASSIGNED",
          lastDelegatedExecutionId);
      updateProducerInfo(record.getValue().producerMetadata);
      return;
    }
    if (incomingExecutionId == lastDelegatedExecutionId + 1) {
      // Expected behavior
      lastDelegatedExecutionId++;
      LOGGER.info("Updated lastDelegatedExecutionId to {}", lastDelegatedExecutionId);
      updateProducerInfo(record.getValue().producerMetadata);
    } else if (incomingExecutionId <= lastDelegatedExecutionId) {
      updateProducerInfo(record.getValue().producerMetadata);
      throw new DuplicateDataException(
          "Skipping message with execution id: " + incomingExecutionId + " because last delegated execution id was: "
              + lastDelegatedExecutionId);
    } else {
      // Cross-reference with producerInfo to see if the missing data is false positive.
      boolean throwException = true;
      String exceptionString = "Last delegated execution id was: " + lastDelegatedExecutionId
          + " ,but incoming execution id is: " + incomingExecutionId;
      String producerInfoString =
          " Previous producer info: " + (producerInfo == null ? "null" : producerInfo.toString())
              + " Incoming message producer info: " + record.getValue().producerMetadata;
      if (producerInfo != null) {
        throwException = !producerInfo.isIncomingMessageValid(record.getValue().producerMetadata);
      }
      if (throwException) {
        if (producerInfo != null) {
          exceptionString += producerInfoString;
        } else {
          exceptionString += " Cannot cross-reference with previous producer info because it's not available yet";
        }
        failingExecutionId = incomingExecutionId;
        throw new MissingDataException(exceptionString);
      } else {
        LOGGER.info("Ignoring {} Cross-reference with producerInfo passed. {}", exceptionString, producerInfoString);
        updateProducerInfo(record.getValue().producerMetadata);
        lastDelegatedExecutionId = incomingExecutionId;
      }
    }
  }

  private void updateProducerInfo(ProducerMetadata producerMetadata) {
    if (producerInfo != null) {
      producerInfo.updateProducerMetadata(producerMetadata);
    } else {
      producerInfo = new ProducerInfo(producerMetadata);
    }
  }

  /**
   * This method extracts the corresponding store name from the {@link AdminOperation} based on the
   * {@link AdminMessageType}.
   * @param adminOperation the {@link AdminOperation} to have its store name extracted.
   * @return the corresponding store name.
   */
  private String extractStoreName(AdminOperation adminOperation) {
    String storeName;
    switch (AdminMessageType.valueOf(adminOperation)) {
      case CREATE_STORAGE_PERSONA:
      case DELETE_STORAGE_PERSONA:
      case UPDATE_STORAGE_PERSONA:
        return STORAGE_PERSONA_MAP_KEY;
      case KILL_OFFLINE_PUSH_JOB:
        KillOfflinePushJob message = (KillOfflinePushJob) adminOperation.payloadUnion;
        storeName = Version.parseStoreFromKafkaTopicName(message.kafkaTopic.toString());
        break;
      case CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER:
        throw new VeniceException(
            "Operation " + AdminMessageType.CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER + " is a batch "
                + "update that affects all existing store in cluster " + clusterName
                + ". Cannot extract a specific store name.");
      case CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER:
        throw new VeniceException(
            "Operation " + AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER + " is a batch "
                + "update that affects all existing store in cluster " + clusterName
                + ". Cannot extract a specific store name.");
      case CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER:
        throw new VeniceException(
            "Operation " + AdminMessageType.CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER + " is a batch "
                + "update that affects all existing store in cluster " + clusterName
                + ". Cannot extract a specific store name.");
      default:
        try {
          GenericRecord payload = (GenericRecord) adminOperation.payloadUnion;
          storeName = payload.get("storeName").toString();
        } catch (Exception e) {
          throw new VeniceException(
              "Failed to handle operation type: " + adminOperation.operationType
                  + " because it does not contain a storeName field");
        }
    }
    storeName = VeniceSystemStoreType.extractUserStoreName(storeName);
    return storeName;
  }

  private void updateLastPosition(PubSubPosition position) {
    if (topicManager.diffPosition(adminTopicPartition, position, lastDelegatedPosition) > 0) {
      lastDelegatedPosition = position;
    }
  }

  private void persistAdminTopicMetadata() {
    if (lastDelegatedExecutionId == lastPersistedExecutionId && lastDelegatedPosition.equals(lastPersistedPosition)) {
      // Skip since there are no new admin messages processed.
      return;
    }
    try (AutoCloseableLock ignore =
        admin.getHelixVeniceClusterResources(clusterName).getClusterLockManager().createClusterWriteLock()) {
      AdminMetadata adminMetadata = new AdminMetadata();
      adminMetadata.setExecutionId(lastDelegatedExecutionId);
      if (remoteConsumptionEnabled) {
        adminMetadata.setPubSubPosition(localPositionCheckpointAtStartTime);
        adminMetadata.setUpstreamPubSubPosition(lastDelegatedPosition);
      } else {
        adminMetadata.setPubSubPosition(lastDelegatedPosition);
        adminMetadata.setUpstreamPubSubPosition(upstreamPositionCheckpointAtStartTime);
      }
      adminTopicMetadataAccessor.updateMetadata(clusterName, adminMetadata);
      lastPersistedPosition = lastDelegatedPosition;
      lastPersistedExecutionId = lastDelegatedExecutionId;
      LOGGER.info("Updated lastPersistedPosition to {}", lastPersistedPosition);
    }
  }

  void skipMessageWithPosition(PubSubPosition position) {
    if (failingPosition.equals(position)) {
      positionToSkip = position;
    } else {
      throw new VeniceException(
          "Cannot skip a position that isn't the first one failing. Last failed position is: " + failingPosition);
    }
  }

  void skipMessageWithExecutionId(long executionId) {
    if (executionId == failingExecutionId) {
      executionIdToSkip = executionId;
    } else {
      throw new VeniceException(
          "Cannot skip an execution id that isn't the first one failing.  Last failed execution id is: "
              + failingExecutionId);
    }
  }

  void skipMessageDIVWithPosition(PubSubPosition position) {
    if (failingPosition.equals(position)) {
      positionToSkipDIV = position;
    } else {
      throw new VeniceException(
          "Cannot skip a position that isn't the first one failing. Last failed position is: " + failingPosition);
    }
  }

  void skipMessageDIVWithExecutionId(long executionId) {
    if (executionId == failingExecutionId) {
      executionIdToSkipDIV = executionId;
    } else {
      throw new VeniceException(
          "Cannot skip an execution id that isn't the first one failing.  Last failed execution id is: "
              + failingExecutionId);
    }
  }

  private void resetPositionToSkip() {
    positionToSkip = PubSubSymbolicPosition.EARLIEST;
  }

  private void resetExecutionIdToSkip() {
    executionIdToSkip = UNASSIGNED_VALUE;
  }

  private boolean checkPositionToSkip(PubSubPosition position, boolean reset) {
    boolean skip = false;
    if (position.equals(positionToSkip)) {
      LOGGER.warn("Skipping admin message with position {} as instructed", positionToSkip);
      if (reset) {
        resetPositionToSkip();
      }
      skip = true;
    }
    return skip;
  }

  private boolean checkExecutionIdToSkip(long executionId, boolean reset) {
    boolean skip = false;
    if (executionId == executionIdToSkip) {
      LOGGER.warn("Skipping admin message with executionId {} as instructed", executionId);
      if (reset) {
        resetExecutionIdToSkip();
      }
      skip = true;
    }
    return skip;
  }

  private boolean checkOffsetToSkipDIV(PubSubPosition position) {
    boolean skip = false;
    if (position.equals(positionToSkipDIV)) {
      LOGGER.warn("Skipping DIV for admin message with position {} as instructed", position);
      positionToSkipDIV = PubSubSymbolicPosition.EARLIEST;
      skip = true;
    }
    return skip;
  }

  private boolean checkExecutionIdToSkipDIV(long executionId) {
    boolean skip = false;
    if (executionId == executionIdToSkipDIV) {
      LOGGER.warn("Skipping DIV for admin message with executionId {} as instructed", executionId);
      executionIdToSkipDIV = UNASSIGNED_VALUE;
      skip = true;
    }
    return skip;
  }

  Long getLastSucceededExecutionId() {
    return lastPersistedExecutionId;
  }

  public Long getLastSucceededExecutionId(String storeName) {
    if (lastSucceededExecutionIdMap != null) {
      return lastSucceededExecutionIdMap.get(storeName);
    } else {
      return null;
    }
  }

  Exception getLastExceptionForStore(String storeName) {
    if (problematicStores.containsKey(storeName)) {
      return problematicStores.get(storeName).exception;
    } else {
      return null;
    }
  }

  PubSubPosition getFailingPosition() {
    return failingPosition;
  }

  long getFailingExecutionId() {
    return failingExecutionId;
  }

  private boolean shouldProcessRecord(DefaultPubSubMessage record) {
    // check topic
    PubSubTopic recordTopic = record.getTopicPartition().getPubSubTopic();
    if (!pubSubAdminTopic.equals(recordTopic)) {
      throw new VeniceException(
          consumerTaskId + " received message from different topic: " + recordTopic + ", expected: "
              + pubSubAdminTopic);
    }
    // check partition
    int recordPartition = record.getTopicPartition().getPartitionNumber();
    if (AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID != recordPartition) {
      throw new VeniceException(
          consumerTaskId + " received message from different partition: " + recordPartition + ", expected: "
              + AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    }
    PubSubPosition recordPosition = record.getPosition();
    // check position
    // if it is the first record, we want to consume it even it is a duplicate
    // we use producerInfo to check if it is the first record
    if (producerInfo != null
        && topicManager.diffPosition(adminTopicPartition, recordPosition, lastDelegatedPosition) <= 0) {
      LOGGER.error(
          "Current record has been processed, last known position: {}, current position: {}",
          lastDelegatedPosition,
          recordPosition);
      return false;
    }

    return true;
  }

  /**
   * Record metrics for consumption lag.
   */
  void recordConsumptionLag() {
    try {
      /**
       *  In the default read_uncommitted isolation level, the end position is the high watermark (that is, the position
       *  of the last successfully replicated message plus one), so subtract 1 from the max position result.
       */

      PubSubPosition latestPosition = topicManager.getLatestPositionWithRetries(adminTopicPartition, 10);
      if (PubSubSymbolicPosition.LATEST.equals(latestPosition)) {
        LOGGER.debug(
            "Cannot get latest position for admin topic: {}, skip this round of lag metrics emission",
            adminTopicPartition);
        stats.setAdminConsumptionOffsetLag(Long.MAX_VALUE);
        return;
      }

      /**
       * If the first consumer poll returns nothing, "lastConsumedOffset" will remain as {@link #UNASSIGNED_VALUE}, so a
       * huge lag will be reported, but actually that's not case since consumer is subscribed to the last checkpoint offset.
       */
      if (!PubSubSymbolicPosition.EARLIEST.equals(lastConsumedPosition)) {
        stats.setAdminConsumptionOffsetLag(
            topicManager.diffPosition(adminTopicPartition, latestPosition, lastConsumedPosition) - 1);
      }
      stats.setMaxAdminConsumptionOffsetLag(
          topicManager.diffPosition(adminTopicPartition, latestPosition, lastPersistedPosition) - 1);
    } catch (Exception e) {
      LOGGER.error(
          "Error when emitting admin consumption lag metrics; only log for warning; admin channel will continue to work.");
    }
  }

  /**
   * When leadership handover or the host shutdown, we don't want to leave the lag metric to stay on a high value, since
   * this specific host is not responsible for the admin topic anymore.
   */
  private void resetConsumptionLag() {
    stats.setAdminConsumptionOffsetLag(0L);
    stats.setMaxAdminConsumptionOffsetLag(0L);
  }

  // Visible for testing
  TopicManager getTopicManager() {
    return topicManager;
  }

  // Visible for testing
  long getConsumptionLagUpdateIntervalInMs() {
    return CONSUMPTION_LAG_UPDATE_INTERVAL_IN_MS;
  }
}
