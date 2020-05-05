package com.linkedin.venice.controller.kafka.consumer;

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
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;


/**
 * This class is used to create a task, which will consume the admin messages from the special admin topics.
 */
public class AdminConsumptionTask implements Runnable, Closeable {

  private static class AdminErrorInfo {
    long offset;
    Exception exception;
  }

  private static final String CONSUMER_TASK_ID_FORMAT = AdminConsumptionTask.class.getSimpleName() + " [Topic: %s] ";
  private static final long UNASSIGNED_VALUE = -1L;
  private static final int READ_CYCLE_DELAY_MS = 1000;
  public static int IGNORED_CURRENT_VERSION = -1;

  private final Logger logger;

  private final String clusterName;
  private final String topic;
  private final String consumerTaskId;
  private final OffsetManager offsetManager;
  private final AdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private final VeniceHelixAdmin admin;
  private final boolean isParentController;
  private final AtomicBoolean isRunning;
  private final AdminOperationSerializer deserializer;
  private final AdminConsumptionStats stats;
  private final int adminTopicReplicationFactor;

  private boolean isSubscribed;
  private KafkaConsumerWrapper consumer;
  private volatile long offsetToSkip = UNASSIGNED_VALUE;
  private volatile long offsetToSkipDIV = UNASSIGNED_VALUE;
  /**
   * The smallest or first failing offset.
   */
  private volatile long failingOffset = UNASSIGNED_VALUE;
  private boolean topicExists;
  /**
   * A {@link Map} of stores to admin operations belonging to each store. The corresponding kafka offset of each admin
   * operation is also attached as the first element of the {@link Pair}.
   */
  private final Map<String, Queue<AdminOperationWrapper>> storeAdminOperationsMapWithOffset;
  /**
   * Map of store names that have encountered some sort of exception during consumption to {@link AdminErrorInfo}
   * that has the details about the exception and the offset of the problematic admin message.
   */
  private final ConcurrentHashMap<String, AdminErrorInfo> problematicStores;
  private Queue<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> undelegatedRecords;


  private final ExecutionIdAccessor executionIdAccessor;
  private final ExecutorService executorService;
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
   * Map of store names to their last succeeded execution id
   */
  private volatile ConcurrentHashMap<String, Long> lastSucceededExecutionIdMap;

  public AdminConsumptionTask(String clusterName,
      KafkaConsumerWrapper consumer,
      VeniceHelixAdmin admin,
      OffsetManager offsetManager,
      AdminTopicMetadataAccessor adminTopicMetadataAccessor,
      ExecutionIdAccessor executionIdAccessor,
      boolean isParentController,
      AdminConsumptionStats stats,
      int adminTopicReplicationFactor,
      long processingCycleTimeoutInMs,
      int maxWorkerThreadPoolSize) {
    this.clusterName = clusterName;
    this.topic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, this.topic);
    this.logger = Logger.getLogger(consumerTaskId);
    this.admin = admin;
    this.isParentController = isParentController;
    this.deserializer = new AdminOperationSerializer();
    this.isRunning = new AtomicBoolean(true);
    this.isSubscribed = false;
    this.topicExists = false;
    this.stats = stats;
    this.adminTopicReplicationFactor = adminTopicReplicationFactor;
    this.consumer = consumer;
    this.offsetManager = offsetManager;
    this.adminTopicMetadataAccessor = adminTopicMetadataAccessor;
    this.executionIdAccessor = executionIdAccessor;
    this.processingCycleTimeoutInMs = processingCycleTimeoutInMs;

    this.storeAdminOperationsMapWithOffset = new ConcurrentHashMap<>();
    this.problematicStores = new ConcurrentHashMap<>();
    this.executorService = new ThreadPoolExecutor(1, maxWorkerThreadPoolSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), new DaemonThreadFactory("Venice-Admin-Execution-Task"));
    this.undelegatedRecords = new LinkedList<>();
    this.stats.setAdminConsumptionFailedOffset(failingOffset);
  }

  @Override
  public synchronized void close() throws IOException {
    isRunning.getAndSet(false);
  }

  @Override
  public void run() {
    logger.info("Running " + this.getClass().getSimpleName());
    long lastLogTime = 0;
    while (isRunning.get()) {
      try {
        Utils.sleep(READ_CYCLE_DELAY_MS);
        if (!admin.isMasterController(clusterName)) {
          unSubscribe();
          continue;
        }
        if (!isSubscribed) {
          if (whetherTopicExists(topic)) {
            // Topic was not created by this process, so we make sure it has the right retention.
            makeSureAdminTopicUsingInfiniteRetentionPolicy(topic);
          } else {
            String logMessage = "Admin topic: " + topic + " hasn't been created yet. ";
            if (!isParentController) {
              // To reduce log bloat, only log once per minute
              if (System.currentTimeMillis() - lastLogTime > 60 * Time.MS_PER_SECOND) {
                logger.info(logMessage + "Since this is a child controller, it will not be created by this process.");
                lastLogTime = System.currentTimeMillis();
              }
              continue;
            }
            logger.info(logMessage + "Since this is the parent controller, it will be created it now.");
            admin.getTopicManager().createTopic(topic, 1, adminTopicReplicationFactor, true,
                false, Optional.of(adminTopicReplicationFactor - 1));
          }
          subscribe();
        }
        Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordsIterator;
        // Only poll the kafka channel if there are no more undelegated records due to exceptions.
        if (undelegatedRecords.isEmpty()) {
          ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
          if (null == records || records.isEmpty()) {
            logger.debug("Received null or no records");
          } else {
            logger.info("Consumed " + records.count() + " admin messages from kafka. Will queue them up for processing");
            recordsIterator = records.iterator();
            while (recordsIterator.hasNext()) {
              undelegatedRecords.add(recordsIterator.next());
            }
          }
        } else {
          logger.info("There are " + undelegatedRecords.size() + " admin messages in the undelegated message queue. "
              + "Will consume from the undelegated queue first before polling the admin topic.");
        }

        while (!undelegatedRecords.isEmpty()) {
          long executionId;
          try {
            executionId = delegateMessage(undelegatedRecords.peek());
          } catch (DataValidationException e) {
            // Very unlikely but DataValidationException could be thrown here.
            logger.error("Admin consumption task is blocked due to DataValidationException with offset "
                + undelegatedRecords.peek().offset(), e);
            failingOffset = undelegatedRecords.peek().offset();
            stats.recordFailedAdminConsumption();
            stats.recordAdminTopicDIVErrorReportCount();
            break;
          }
          if (executionId == lastDelegatedExecutionId) {
            updateLastOffset(undelegatedRecords.poll().offset());
          } else {
            undelegatedRecords.poll();
          }
        }
        executeMessagesAndCollectResults();
        stats.setAdminConsumptionFailedOffset(failingOffset);
      } catch (Exception e) {
        logger.error("Exception thrown while running admin consumption task", e);
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
      lastPersistedOffset = AdminTopicMetadataAccessor.getOffset(metaData);
      lastPersistedExecutionId = AdminTopicMetadataAccessor.getExecutionId(metaData);
      lastOffset = lastPersistedOffset;
      lastDelegatedExecutionId = lastPersistedExecutionId;
    } else {
      // should only happen when we first try to move away from PartitionState to admin topic metadata
      lastOffset = offsetManager.getLastOffset(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID).getOffset();
      lastDelegatedExecutionId = executionIdAccessor.getLastSucceededExecutionId(clusterName);
      persistAdminTopicMetadata();
    }
    // Subscribe the admin topic
    consumer.subscribe(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, lastOffset);
    isSubscribed = true;
    logger.info("Subscribe to topic name: " + topic + ", with offset: " + lastOffset + " and execution id: "
        + lastPersistedExecutionId);
  }

  private void unSubscribe() {
    if (isSubscribed) {
      consumer.unSubscribe(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
      storeAdminOperationsMapWithOffset.clear();
      problematicStores.clear();
      undelegatedRecords.clear();
      failingOffset = UNASSIGNED_VALUE;
      offsetToSkip = UNASSIGNED_VALUE;
      offsetToSkipDIV = UNASSIGNED_VALUE;
      lastDelegatedExecutionId = UNASSIGNED_VALUE;
      lastPersistedExecutionId = UNASSIGNED_VALUE;
      stats.recordPendingAdminMessagesCount(UNASSIGNED_VALUE);
      stats.recordStoresWithPendingAdminMessagesCount(UNASSIGNED_VALUE);
      isSubscribed = false;
      logger.info("Unsubscribe from topic name: " + topic);
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
    lastSucceededExecutionIdMap = new ConcurrentHashMap<>(executionIdAccessor.getLastSucceededExecutionIdMap(clusterName));
    List<Callable<Void>> tasks = new ArrayList<>();
    List<String> stores = new ArrayList<>();
    // Create a task for each store that has admin messages pending to be processed.
    for (Map.Entry<String, Queue<AdminOperationWrapper>> entry : storeAdminOperationsMapWithOffset.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        if (checkOffsetToSkip(entry.getValue().peek().getOffset())) {
          entry.getValue().poll();
        }
        tasks.add(new AdminExecutionTask(logger, clusterName, entry.getKey(), lastSucceededExecutionIdMap,
            lastPersistedExecutionId, entry.getValue(), admin, executionIdAccessor, isParentController, stats));
        stores.add(entry.getKey());
      }
    }
    if (isRunning.get()) {
      if (!tasks.isEmpty()) {
        int pendingAdminMessagesCount = 0;
        int storesWithPendingAdminMessagesCount = 0;
        long adminExecutionTasksInvokeTime = System.currentTimeMillis();
        // Wait for the worker threads to finish processing the internal admin topics.
        List<Future<Void>> results = executorService.invokeAll(tasks, processingCycleTimeoutInMs, TimeUnit.MILLISECONDS);
        stats.recordAdminConsumptionCycleDurationMs(System.currentTimeMillis() - adminExecutionTasksInvokeTime);
        Map<String, Long> newLastSucceededExecutionIdMap = executionIdAccessor.getLastSucceededExecutionIdMap(clusterName);
        for (int i = 0; i < results.size(); i++) {
          String storeName = stores.get(i);
          Future<Void> result = results.get(i);
          try {
            result.get();
            problematicStores.remove(storeName);
          } catch (ExecutionException | CancellationException e) {
            AdminErrorInfo errorInfo = new AdminErrorInfo();
            int perStorePendingMessagesCount = storeAdminOperationsMapWithOffset.get(storeName).size();
            pendingAdminMessagesCount += perStorePendingMessagesCount;
            storesWithPendingAdminMessagesCount += perStorePendingMessagesCount > 0 ? 1 : 0;
            if (e instanceof CancellationException) {
              if (lastSucceededExecutionIdMap.get(storeName).equals(newLastSucceededExecutionIdMap.get(storeName)) &&
                  perStorePendingMessagesCount > 0) {
                // only mark the store problematic if no progress is made and there are still message(s) in the queue.
                errorInfo.exception =
                    new VeniceException("Could not finish processing admin message for store " + storeName + " in time");
                errorInfo.offset = storeAdminOperationsMapWithOffset.get(storeName).peek().getOffset();
                problematicStores.put(storeName, errorInfo);
                logger.warn(errorInfo.exception.getMessage());
              }
            } else {
              errorInfo.exception = e;
              errorInfo.offset = storeAdminOperationsMapWithOffset.get(storeName).peek().getOffset();
              problematicStores.put(storeName, errorInfo);
            }
          }
        }
        if (problematicStores.isEmpty()) {
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

          for (Map.Entry<String, AdminErrorInfo> problematicStore : problematicStores.entrySet()) {
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

  private void internalClose() {
    unSubscribe();
    executorService.shutdownNow();
    try {
      if (!executorService.awaitTermination(processingCycleTimeoutInMs, TimeUnit.MILLISECONDS)) {
        logger.warn(
            "Unable to shutdown worker executor thread pool in admin consumption task in time " + consumerTaskId);
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for worker executor thread pool to shutdown " + consumerTaskId);
    }
    logger.info("Closed consumer for admin topic: " + topic);
    consumer.close();
  }

  private boolean whetherTopicExists(String topicName) {
    if (topicExists) {
      return true;
    }
    // Check it again if it is false
    topicExists = admin.getTopicManager().containsTopicAndAllPartitionsAreOnline(topicName);
    return topicExists;
  }

  private void makeSureAdminTopicUsingInfiniteRetentionPolicy(String topicName) {
    admin.getTopicManager().updateTopicRetention(topicName, Long.MAX_VALUE);
    logger.info("Admin topic: " + topic + " has been updated to use infinite retention policy");
  }

  /**
   * This method groups {@link AdminOperation}s by their corresponding store.
   * @param record The {@link ConsumerRecord} containing the {@link AdminOperation}.
   * @return corresponding executionId if applicable.
   */
  private long delegateMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    if (checkOffsetToSkip(record.offset()) || !shouldProcessRecord(record)) {
      // Return lastDelegatedExecutionId to update the offset without changing the execution id. Skip DIV should/can be
      // used if the skip requires executionId to be reset.
      return lastDelegatedExecutionId;
    }
    KafkaKey kafkaKey = record.key();
    KafkaMessageEnvelope kafkaValue = record.value();
    if (kafkaKey.isControlMessage()) {
      logger.debug("Received control message: " + kafkaValue);
      return UNASSIGNED_VALUE;
    }
    // check message type
    MessageType messageType = MessageType.valueOf(kafkaValue);
    if (MessageType.PUT != messageType) {
      throw new VeniceException("Received unexpected message type: " + messageType);
    }
    Put put = (Put) kafkaValue.payloadUnion;
    AdminOperation adminOperation = deserializer.deserialize(put.putValue.array(), put.schemaId);
    long executionId = adminOperation.executionId;
    logger.info("Received admin message: " + adminOperation + " offset: " + record.offset());
    try {
      checkAndValidateMessage(adminOperation, record.offset());
    } catch (DuplicateDataException e) {
      // Previous processed message, safe to skip
      logger.info(e.getMessage());
      return executionId;
    }
    String storeName = extractStoreName(adminOperation);
    storeAdminOperationsMapWithOffset.putIfAbsent(storeName, new LinkedList<>());
    long producerTimestamp = kafkaValue.producerMetadata.messageTimestamp;
    long brokerTimestamp = record.timestamp();
    AdminOperationWrapper adminOperationWrapper = new AdminOperationWrapper(adminOperation, record.offset(),
        producerTimestamp, brokerTimestamp, System.currentTimeMillis());
    storeAdminOperationsMapWithOffset.get(storeName).add(adminOperationWrapper);
    stats.recordAdminMessageMMLatency(Math.max(0,
        adminOperationWrapper.getLocalBrokerTimestamp() - adminOperationWrapper.getProducerTimestamp()));
    stats.recordAdminMessageDelegateLatency(Math.max(0,
        adminOperationWrapper.getDelegateTimestamp() - adminOperationWrapper.getLocalBrokerTimestamp()));
    return executionId;
  }

  private void checkAndValidateMessage(AdminOperation message, long offset) {
    long incomingExecutionId = message.executionId;
    if (checkOffsetToSkipDIV(offset) || lastDelegatedExecutionId == UNASSIGNED_VALUE) {
      lastDelegatedExecutionId = incomingExecutionId;
      return;
    }
    if (incomingExecutionId == lastDelegatedExecutionId + 1) {
      // Expected behavior
      lastDelegatedExecutionId++;
    } else if (incomingExecutionId <= lastDelegatedExecutionId) {
      throw new DuplicateDataException("Skipping message with execution id: " + incomingExecutionId
          + " because last delegated execution id was: " + lastDelegatedExecutionId);
    } else {
      throw new MissingDataException("Last delegated execution id was: " + lastDelegatedExecutionId
          + " ,but incoming execution id is: " + incomingExecutionId);
    }
  }

  /**
   * This method extracts the corresponding store name from the {@link AdminOperation} based on the
   * {@link AdminMessageType}.
   * @param adminOperation the {@link AdminOperation} to have its store name extracted.
   * @return the corresponding store name.
   */
  private String extractStoreName(AdminOperation adminOperation) {
    switch (AdminMessageType.valueOf(adminOperation)) {
      case KILL_OFFLINE_PUSH_JOB:
        KillOfflinePushJob message = (KillOfflinePushJob) adminOperation.payloadUnion;
        return Version.parseStoreFromKafkaTopicName(message.kafkaTopic.toString());
      default:
        try {
          GenericRecord payload = (GenericRecord) adminOperation.payloadUnion;
          return payload.get("storeName").toString();
        } catch (Exception e) {
          throw new VeniceException("Failed to handle operation type: " + adminOperation.operationType
              + " because it does not contain a storeName field");
        }
    }
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
    Map<String, Long> metadata = AdminTopicMetadataAccessor.generateMetadataMap(lastOffset, lastDelegatedExecutionId);
    adminTopicMetadataAccessor.updateMetadata(clusterName, metadata);
    lastPersistedOffset = lastOffset;
    lastPersistedExecutionId = lastDelegatedExecutionId;
  }

  public void skipMessageWithOffset(long offset) {
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

  private boolean checkOffsetToSkip(long offset) {
    boolean skip = false;
    if (offset == offsetToSkip) {
      logger.warn("Skipping admin message with offset " + offset + " as instructed");
      offsetToSkip = UNASSIGNED_VALUE;
      skip = true;
    }
    return skip;
  }

  private boolean checkOffsetToSkipDIV(long offset) {
    boolean skip = false;
    if (offset == offsetToSkipDIV) {
      logger.warn("Skipping DIV for admin message with offset " + offset + " as instructed");
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

  private boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    // check topic
    String recordTopic = record.topic();
    if (!topic.equals(recordTopic)) {
      throw new VeniceException(
          consumerTaskId + " received message from different topic: " + recordTopic + ", expected: " + topic);
    }
    // check partition
    int recordPartition = record.partition();
    if (AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID != recordPartition) {
      throw new VeniceException(
          consumerTaskId + " received message from different partition: " + recordPartition + ", expected: "
              + AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    }
    long recordOffset = record.offset();
    // check offset
    if (lastOffset >= recordOffset) {
      logger.error(
          "Current record has been processed, last known offset: " + lastOffset + ", current offset: " + recordOffset);
      return false;
    }

    return true;
  }
}
