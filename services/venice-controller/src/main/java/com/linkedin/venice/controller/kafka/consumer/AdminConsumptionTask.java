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
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
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
    long offset;
    Exception exception;
  }

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

  private static final String CONSUMER_TASK_ID_FORMAT = AdminConsumptionTask.class.getSimpleName() + " [Topic: %s] ";
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
  private final String topic;
  private final PubSubTopic pubSubTopic;
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
  private volatile long offsetToSkip = UNASSIGNED_VALUE;
  private volatile long offsetToSkipDIV = UNASSIGNED_VALUE;
  /**
   * The smallest or first failing offset.
   */
  private volatile long failingOffset = UNASSIGNED_VALUE;
  private boolean topicExists;
  /**
   * A {@link Map} of stores to admin operations belonging to each store. The corresponding kafka offset and other
   * metadata of each admin operation are included in the {@link AdminOperationWrapper}.
   */
  private final Map<String, Queue<AdminOperationWrapper>> storeAdminOperationsMapWithOffset;

  /**
   * Map of store names that have encountered some sort of exception during consumption to {@link AdminErrorInfo}
   * that has the details about the exception and the offset of the problematic admin message.
   */
  private final ConcurrentHashMap<String, AdminErrorInfo> problematicStores;
  private final Queue<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> undelegatedRecords;

  private final ExecutionIdAccessor executionIdAccessor;
  private final ExecutorService executorService;

  private TopicManager sourceKafkaClusterTopicManager;

  public ExecutorService getExecutorService() {
    return executorService;
  }

  private final long processingCycleTimeoutInMs;
  /**
   * Once all admin messages in a cycle is processed successfully, the id would be updated together with the offset.
   * It represents a kind of comparable progress of admin topic consumption among all controllers.
   */
  private long lastPersistedExecutionId = UNASSIGNED_VALUE;
  /**
   * The corresponding offset to {@code lastPersistedExecutionId}
   */
  private long lastPersistedOffset = UNASSIGNED_VALUE;
  /**
   * The execution id of the last message that was delegated to a store's queue. Used for DIV check when fetching
   * messages from the admin topic.
   */
  private long lastDelegatedExecutionId = UNASSIGNED_VALUE;
  /**
   * The corresponding offset to {@code lastDelegatedExecutionId}
   */
  private long lastOffset = UNASSIGNED_VALUE;
  /**
   * Track the latest consumed offset; this variable is updated as long as the consumer consumes new messages,
   * no matter whether the message has any issue or not.
   */
  private long lastConsumedOffset = UNASSIGNED_VALUE;
  /**
   * The local offset value in ZK during initialization phase; the value will not be updated during admin topic consumption.
   *
   * Currently, there are two potential offset: local offset and upstream offset, and we only update and
   * maintain one of them. While persisting the offset to ZK, we would like to keep the original value
   * for the other one, so that rollback/roll-forward of the remote consumption feature can be faster.
   */
  private long localOffsetCheckpointAtStartTime = UNASSIGNED_VALUE;
  /**
   * The upstream offset value in ZK during initialization phase; the value will not be updated during admin topic consumption.
   *
   * Currently, there are two potential offset: local offset and upstream offset, and we only update and
   * maintain one of them. While persisting the offset to ZK, we would like to keep the original value
   * for the other one, so that rollback/roll-forward of the remote consumption feature can be faster.
   */
  private long upstreamOffsetCheckpointAtStartTime = UNASSIGNED_VALUE;
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
   * Timestamp in millisecond: the last time when updating the consumption offset lag metric
   */
  private long lastUpdateTimeForConsumptionOffsetLag = 0;

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
    this.topic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, this.topic);
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

    this.storeAdminOperationsMapWithOffset = new ConcurrentHashMap<>();
    this.problematicStores = new ConcurrentHashMap<>();
    // since we use an unbounded queue the core pool size is really the max pool size
    this.executorService = new ThreadPoolExecutor(
        maxWorkerThreadPoolSize,
        maxWorkerThreadPoolSize,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(MAX_WORKER_QUEUE_SIZE),
        new DaemonThreadFactory(String.format("Venice-Admin-Execution-Task-%s", clusterName)));
    this.undelegatedRecords = new LinkedList<>();
    this.stats.setAdminConsumptionFailedOffset(failingOffset);
    this.pubSubTopic = pubSubTopicRepository.getTopic(topic);
    this.regionName = regionName;

    if (remoteConsumptionEnabled) {
      if (!remoteKafkaServerUrl.isPresent()) {
        throw new VeniceException(
            "Admin topic remote consumption is enabled but no config found for the source Kafka bootstrap server url");
      }
      this.sourceKafkaClusterTopicManager = admin.getTopicManager(remoteKafkaServerUrl.get());
    }
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
          if (whetherTopicExists(pubSubTopic)) {
            // Topic was not created by this process, so we make sure it has the right retention.
            makeSureAdminTopicUsingInfiniteRetentionPolicy(pubSubTopic);
          } else {
            String logMessageFormat = "Admin topic: {} hasn't been created yet. {}";
            if (!isParentController) {
              // To reduce log bloat, only log once per minute
              if (System.currentTimeMillis() - lastLogTime > 60 * Time.MS_PER_SECOND) {
                LOGGER.info(
                    logMessageFormat,
                    topic,
                    "Since this is a child controller, it will not be created by this process.");
                lastLogTime = System.currentTimeMillis();
              }
              continue;
            }
            LOGGER.info(logMessageFormat, topic, "Since this is the parent controller, it will be created now.");
            admin.getTopicManager()
                .createTopic(pubSubTopic, 1, adminTopicReplicationFactor, true, false, minInSyncReplicas);
            LOGGER.info("Admin topic {} is created.", topic);
          }
          subscribe();
        }
        Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> recordsIterator;
        // Only poll the kafka channel if there are no more undelegated records due to exceptions.
        if (undelegatedRecords.isEmpty()) {
          Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages =
              consumer.poll(READ_CYCLE_DELAY_MS);
          if (messages == null || messages.isEmpty()) {
            LOGGER.debug("Received null or no messages");
          } else {
            int polledMessageCount = messages.values().stream().mapToInt(List::size).sum();
            LOGGER.info("Consumed {} admin messages from kafka. Will queue them up for processing", polledMessageCount);
            recordsIterator = Utils.iterateOnMapOfLists(messages);
            while (recordsIterator.hasNext()) {
              PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> newRecord = recordsIterator.next();
              lastConsumedOffset = newRecord.getOffset();
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
          PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = undelegatedRecords.peek();
          if (record == null) {
            break;
          }
          try {
            long executionId = delegateMessage(record);
            if (executionId == lastDelegatedExecutionId) {
              updateLastOffset(record.getOffset());
            }
            undelegatedRecords.remove();
          } catch (DataValidationException dve) {
            // Very unlikely but DataValidationException could be thrown here.
            LOGGER.error(
                "Admin consumption task is blocked due to DataValidationException with offset {}",
                record.getOffset(),
                dve);
            failingOffset = record.getOffset();
            stats.recordFailedAdminConsumption();
            stats.recordAdminTopicDIVErrorReportCount();
            break;
          } catch (Exception e) {
            LOGGER.error("Admin consumption task is blocked due to Exception with offset {}", record.getOffset(), e);
            failingOffset = record.getOffset();
            stats.recordFailedAdminConsumption();
            break;
          }
        }

        if (remoteConsumptionEnabled && LatencyUtils
            .getElapsedTimeFromMsToMs(lastUpdateTimeForConsumptionOffsetLag) > getConsumptionLagUpdateIntervalInMs()) {
          recordConsumptionLag();
          lastUpdateTimeForConsumptionOffsetLag = System.currentTimeMillis();
        }
        executeMessagesAndCollectResults();
        stats.setAdminConsumptionFailedOffset(failingOffset);
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
    Map<String, Long> metaData = adminTopicMetadataAccessor.getMetadata(clusterName);
    if (!metaData.isEmpty()) {
      Pair<Long, Long> localAndUpstreamOffsets = AdminTopicMetadataAccessor.getOffsets(metaData);
      localOffsetCheckpointAtStartTime = localAndUpstreamOffsets.getFirst();
      upstreamOffsetCheckpointAtStartTime = localAndUpstreamOffsets.getSecond();
      lastPersistedOffset =
          remoteConsumptionEnabled ? upstreamOffsetCheckpointAtStartTime : localOffsetCheckpointAtStartTime;
      lastPersistedExecutionId = AdminTopicMetadataAccessor.getExecutionId(metaData);
      /**
       * For the first poll after subscription, Controller will try to consume one message older than {@link #lastPersistedOffset}
       * to initialize the {@link #producerInfo}, which will be used to decide whether an execution id gap is a false alarm or not
       * in {@link #checkAndValidateMessage}.
       *
       */
      lastOffset = lastPersistedOffset - 1;
      lastDelegatedExecutionId = lastPersistedExecutionId;
    } else {
      LOGGER.info("Admin topic metadata is empty, will resume consumption from the starting offset");
      lastOffset = UNASSIGNED_VALUE;
      lastDelegatedExecutionId = UNASSIGNED_VALUE;
    }
    stats.setAdminConsumptionCheckpointOffset(lastPersistedOffset);
    stats.registerAdminConsumptionCheckpointOffset();
    // Subscribe the admin topic
    consumer.subscribe(new PubSubTopicPartitionImpl(pubSubTopic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID), lastOffset);
    isSubscribed = true;
    LOGGER.info(
        "Subscribed to topic name: {}, with offset: {} and execution id: {}. Remote consumption flag: {}",
        topic,
        lastOffset,
        lastPersistedExecutionId,
        remoteConsumptionEnabled);
  }

  private void unSubscribe() {
    if (isSubscribed) {
      consumer.unSubscribe(new PubSubTopicPartitionImpl(pubSubTopic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID));
      storeAdminOperationsMapWithOffset.clear();
      problematicStores.clear();
      undelegatedRecords.clear();
      failingOffset = UNASSIGNED_VALUE;
      offsetToSkip = UNASSIGNED_VALUE;
      offsetToSkipDIV = UNASSIGNED_VALUE;
      lastDelegatedExecutionId = UNASSIGNED_VALUE;
      lastPersistedExecutionId = UNASSIGNED_VALUE;
      lastOffset = UNASSIGNED_VALUE;
      lastPersistedOffset = UNASSIGNED_VALUE;
      producerInfo = null;
      stats.recordPendingAdminMessagesCount(UNASSIGNED_VALUE);
      stats.recordStoresWithPendingAdminMessagesCount(UNASSIGNED_VALUE);
      resetConsumptionLag();
      isSubscribed = false;
      LOGGER.info(
          "Unsubscribed from topic name: {}. Remote consumption flag before unsubscription: {}",
          topic,
          remoteConsumptionEnabled);
    }
  }

  /**
   * Delegate work from the {@code storeAdminOperationsMapWithOffset} to the worker threads. Wait for the worker threads
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
    for (Map.Entry<String, Queue<AdminOperationWrapper>> entry: storeAdminOperationsMapWithOffset.entrySet()) {
      String storeName = entry.getKey();
      Queue<AdminOperationWrapper> storeQueue = entry.getValue();
      if (!storeQueue.isEmpty()) {
        AdminOperationWrapper nextOp = storeQueue.peek();
        if (nextOp == null) {
          continue;
        }
        long adminMessageOffset = nextOp.getOffset();
        if (checkOffsetToSkip(nextOp.getOffset(), false)) {
          storeQueue.remove();
          skipOffsetCommandHasBeenProcessed = true;
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
          // Log the store name and the offset of the task being added into the task list
          LOGGER.info(
              "Adding admin message from store {} with offset {} to the task list",
              storeName,
              adminMessageOffset);
          tasks.add(newTask);
          stores.add(storeName);
        }
      }
    }
    if (skipOffsetCommandHasBeenProcessed) {
      resetOffsetToSkip();
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
              Queue<AdminOperationWrapper> storeQueue = storeAdminOperationsMapWithOffset.get(storeName);
              if (storeQueue != null && !storeQueue.isEmpty()) {
                internalQueuesEmptied = false;
              }
            }
          } catch (ExecutionException | CancellationException e) {
            internalQueuesEmptied = false;
            AdminErrorInfo errorInfo = new AdminErrorInfo();
            Queue<AdminOperationWrapper> storeQueue = storeAdminOperationsMapWithOffset.get(storeName);
            int perStorePendingMessagesCount = storeQueue == null ? 0 : storeQueue.size();
            pendingAdminMessagesCount += perStorePendingMessagesCount;
            storesWithPendingAdminMessagesCount += perStorePendingMessagesCount > 0 ? 1 : 0;
            if (e instanceof CancellationException) {
              long lastSucceededId = lastSucceededExecutionIdMap.getOrDefault(storeName, UNASSIGNED_VALUE);
              long newLastSucceededId = newLastSucceededExecutionIdMap.getOrDefault(storeName, UNASSIGNED_VALUE);

              if (lastSucceededId == UNASSIGNED_VALUE) {
                LOGGER.error("Could not find last successful execution ID for store {}", storeName);
              }

              if (lastSucceededId == newLastSucceededId && perStorePendingMessagesCount > 0) {
                // only mark the store problematic if no progress is made and there are still message(s) in the queue.
                errorInfo.exception = new VeniceException(
                    "Could not finish processing admin message for store " + storeName + " in time");
                errorInfo.offset = getNextOperationOffsetIfAvailable(storeName);
                problematicStores.put(storeName, errorInfo);
                LOGGER.warn(errorInfo.exception.getMessage());
              }
            } else {
              errorInfo.exception = e;
              errorInfo.offset = getNextOperationOffsetIfAvailable(storeName);
              problematicStores.put(storeName, errorInfo);
            }
          } catch (Throwable e) {
            long errorMsgOffset = getNextOperationOffsetIfAvailable(storeName);
            if (errorMsgOffset == UNASSIGNED_VALUE) {
              LOGGER.error("Could not get the offset of the problematic admin message for store {}", storeName);
            }

            LOGGER.error(
                "Unexpected exception thrown while processing admin message for store {} at offset {}",
                storeName,
                errorMsgOffset,
                e);
            // Throw it above to have the consistent behavior as before
            throw e;
          }
        }
        if (problematicStores.isEmpty() && internalQueuesEmptied) {
          // All admin operations were successfully executed or skipped.
          // 1. Clear the failing offset.
          // 3. Persist the latest execution id and offset (cluster wide) to ZK.

          // Ensure failingOffset from the delegateMessage is not overwritten.
          if (failingOffset <= lastOffset) {
            failingOffset = UNASSIGNED_VALUE;
          }
          persistAdminTopicMetadata();
        } else {
          // One or more stores encountered problems while executing their admin operations.
          // 1. Do not persist the latest offset (cluster wide) to ZK.
          // 2. Find and set the smallest failing offset amongst the problematic stores.
          long smallestOffset = UNASSIGNED_VALUE;

          for (Map.Entry<String, AdminErrorInfo> problematicStore: problematicStores.entrySet()) {
            if (smallestOffset == UNASSIGNED_VALUE || problematicStore.getValue().offset < smallestOffset) {
              smallestOffset = problematicStore.getValue().offset;
            }
          }
          // Ensure failingOffset from the delegateMessage is not overwritten.
          if (failingOffset <= lastOffset) {
            failingOffset = smallestOffset;
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
   * @return the offset of the next enqueued operation for the given store name, or {@link #UNASSIGNED_VALUE} if unavailable.
   */
  private long getNextOperationOffsetIfAvailable(String storeName) {
    Queue<AdminOperationWrapper> storeQueue = storeAdminOperationsMapWithOffset.get(storeName);
    AdminOperationWrapper nextOperation = storeQueue == null ? null : storeQueue.peek();
    return nextOperation == null ? UNASSIGNED_VALUE : nextOperation.getOffset();
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
    LOGGER.info("Closed consumer for admin topic: {}", topic);
    consumer.close();
  }

  private boolean whetherTopicExists(PubSubTopic topicName) {
    if (topicExists) {
      return true;
    }
    // Check it again if it is false
    if (remoteConsumptionEnabled) {
      topicExists = sourceKafkaClusterTopicManager.containsTopicAndAllPartitionsAreOnline(topicName);
    } else {
      topicExists = admin.getTopicManager().containsTopicAndAllPartitionsAreOnline(topicName);
    }
    return topicExists;
  }

  private void makeSureAdminTopicUsingInfiniteRetentionPolicy(PubSubTopic topicName) {
    if (remoteConsumptionEnabled) {
      sourceKafkaClusterTopicManager.updateTopicRetention(topicName, Long.MAX_VALUE);
    } else {
      admin.getTopicManager().updateTopicRetention(topicName, Long.MAX_VALUE);
    }
    LOGGER.info("Admin topic: {} has been updated to use infinite retention policy", topic);
  }

  /**
   * This method groups {@link AdminOperation}s by their corresponding store.
   * @param record The {@link PubSubMessage} containing the {@link AdminOperation}.
   * @return corresponding executionId if applicable.
   */
  private long delegateMessage(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    if (checkOffsetToSkip(record.getOffset(), true) || !shouldProcessRecord(record)) {
      // Return lastDelegatedExecutionId to update the offset without changing the execution id. Skip DIV should/can be
      // used if the skip requires executionId to be reset because this skip here is skipping the message without doing
      // any processing. This may be the case when a message cannot be deserialized properly therefore we don't know
      // what's the right execution id and producer info to set moving forward.
      return lastDelegatedExecutionId;
    }
    KafkaKey kafkaKey = record.getKey();
    KafkaMessageEnvelope kafkaValue = record.getValue();
    if (kafkaKey.isControlMessage()) {
      LOGGER.debug("Received control message: {}", kafkaValue);
      return UNASSIGNED_VALUE;
    }
    // check message type
    MessageType messageType = MessageType.valueOf(kafkaValue);
    if (MessageType.PUT != messageType) {
      throw new VeniceException("Received unexpected message type: " + messageType);
    }
    Put put = (Put) kafkaValue.payloadUnion;
    AdminOperation adminOperation = deserializer.deserialize(put.putValue, put.schemaId);
    long executionId = adminOperation.executionId;
    try {
      checkAndValidateMessage(adminOperation, record);
      LOGGER.info("Received admin message: {} offset: {}", adminOperation, record.getOffset());
      consecutiveDuplicateMessageCount = 0;
    } catch (DuplicateDataException e) {
      // Previously processed message, safe to skip
      if (consecutiveDuplicateMessageCount < MAX_DUPLICATE_MESSAGE_LOGS) {
        consecutiveDuplicateMessageCount++;
        LOGGER.info(e.getMessage());
      } else if (consecutiveDuplicateMessageCount == MAX_DUPLICATE_MESSAGE_LOGS) {
        LOGGER.info(
            "It appears that controller is consuming from a low offset and encounters many admin messages that "
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
            storeAdminOperationsMapWithOffset.computeIfAbsent(storeName, n -> new LinkedList<>());
        AdminOperationWrapper adminOperationWrapper = new AdminOperationWrapper(
            adminOperation,
            record.getOffset(),
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
          record.getOffset(),
          producerTimestamp,
          brokerTimestamp,
          System.currentTimeMillis());
      stats.recordAdminMessageMMLatency(
          Math.max(0, adminOperationWrapper.getLocalBrokerTimestamp() - adminOperationWrapper.getProducerTimestamp()));
      stats.recordAdminMessageDelegateLatency(
          Math.max(0, adminOperationWrapper.getDelegateTimestamp() - adminOperationWrapper.getLocalBrokerTimestamp()));
      String storeName = extractStoreName(adminOperation);
      storeAdminOperationsMapWithOffset.putIfAbsent(storeName, new LinkedList<>());
      storeAdminOperationsMapWithOffset.get(storeName).add(adminOperationWrapper);

    }
    return executionId;
  }

  private void checkAndValidateMessage(
      AdminOperation message,
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    long incomingExecutionId = message.executionId;
    if (checkOffsetToSkipDIV(record.getOffset()) || lastDelegatedExecutionId == UNASSIGNED_VALUE) {
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

  private void updateLastOffset(long offset) {
    if (offset > lastOffset) {
      lastOffset = offset;
    }
  }

  private void persistAdminTopicMetadata() {
    if (lastDelegatedExecutionId == lastPersistedExecutionId && lastOffset == lastPersistedOffset) {
      // Skip since there are no new admin messages processed.
      return;
    }
    Map<String, Long> metadata = remoteConsumptionEnabled
        ? AdminTopicMetadataAccessor
            .generateMetadataMap(localOffsetCheckpointAtStartTime, lastOffset, lastDelegatedExecutionId)
        : AdminTopicMetadataAccessor
            .generateMetadataMap(lastOffset, upstreamOffsetCheckpointAtStartTime, lastDelegatedExecutionId);
    adminTopicMetadataAccessor.updateMetadata(clusterName, metadata);
    lastPersistedOffset = lastOffset;
    lastPersistedExecutionId = lastDelegatedExecutionId;
    LOGGER.info("Updated lastPersistedOffset to {}", lastPersistedOffset);
    stats.setAdminConsumptionCheckpointOffset(lastPersistedOffset);
  }

  void skipMessageWithOffset(long offset) {
    if (offset == failingOffset) {
      offsetToSkip = offset;
    } else {
      throw new VeniceException(
          "Cannot skip an offset that isn't the first one failing.  Last failed offset is: " + failingOffset);
    }
  }

  void skipMessageDIVWithOffset(long offset) {
    if (offset == failingOffset) {
      offsetToSkipDIV = offset;
    } else {
      throw new VeniceException(
          "Cannot skip an offset that isn't the first one failing.  Last failed offset is: " + failingOffset);
    }
  }

  private void resetOffsetToSkip() {
    offsetToSkip = UNASSIGNED_VALUE;
  }

  private boolean checkOffsetToSkip(long offset, boolean reset) {
    boolean skip = false;
    if (offset == offsetToSkip) {
      LOGGER.warn("Skipping admin message with offset {} as instructed", offset);
      if (reset) {
        resetOffsetToSkip();
      }
      skip = true;
    }
    return skip;
  }

  private boolean checkOffsetToSkipDIV(long offset) {
    boolean skip = false;
    if (offset == offsetToSkipDIV) {
      LOGGER.warn("Skipping DIV for admin message with offset {} as instructed", offset);
      offsetToSkipDIV = UNASSIGNED_VALUE;
      skip = true;
    }
    return skip;
  }

  Long getLastSucceededExecutionId() {
    return lastPersistedExecutionId;
  }

  Long getLastSucceededExecutionId(String storeName) {
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

  long getFailingOffset() {
    return failingOffset;
  }

  private boolean shouldProcessRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    // check topic
    PubSubTopic recordTopic = record.getTopicPartition().getPubSubTopic();
    if (!pubSubTopic.equals(recordTopic)) {
      throw new VeniceException(
          consumerTaskId + " received message from different topic: " + recordTopic + ", expected: " + topic);
    }
    // check partition
    int recordPartition = record.getTopicPartition().getPartitionNumber();
    if (AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID != recordPartition) {
      throw new VeniceException(
          consumerTaskId + " received message from different partition: " + recordPartition + ", expected: "
              + AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    }
    long recordOffset = record.getOffset();
    // check offset
    if (lastOffset >= recordOffset) {
      LOGGER.error(
          "Current record has been processed, last known offset: {}, current offset: {}",
          lastOffset,
          recordOffset);
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
       *  In the default read_uncommitted isolation level, the end offset is the high watermark (that is, the offset of
       *  the last successfully replicated message plus one), so subtract 1 from the max offset result.
       */
      PubSubTopicPartition adminTopicPartition =
          new PubSubTopicPartitionImpl(pubSubTopic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
      long sourceAdminTopicEndOffset =
          sourceKafkaClusterTopicManager.getLatestOffsetWithRetries(adminTopicPartition, 10) - 1;
      /**
       * If the first consumer poll returns nothing, "lastConsumedOffset" will remain as {@link #UNASSIGNED_VALUE}, so a
       * huge lag will be reported, but actually that's not case since consumer is subscribed to the last checkpoint offset.
       */
      if (lastConsumedOffset != UNASSIGNED_VALUE) {
        stats.setAdminConsumptionOffsetLag(sourceAdminTopicEndOffset - lastConsumedOffset);
      }
      stats.setMaxAdminConsumptionOffsetLag(sourceAdminTopicEndOffset - lastPersistedOffset);
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
  TopicManager getSourceKafkaClusterTopicManager() {
    return sourceKafkaClusterTopicManager;
  }

  // Visible for testing
  long getConsumptionLagUpdateIntervalInMs() {
    return CONSUMPTION_LAG_UPDATE_INTERVAL_IN_MS;
  }
}
