package com.linkedin.venice.kafka.consumer;

import com.google.common.collect.Lists;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StoragePartitionConfig;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import javax.validation.constraints.NotNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;


/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public class StoreIngestionTask implements Runnable, Closeable {

  /**
   * This class is used to maintain internal state for consumption of each partition.
   */
  private static class PartitionConsumptionState {
    private final int partition;
    private OffsetRecord offsetRecord;
    // whether the ingestion of current partition is deferred-write.
    private boolean deferredWrite;
    private boolean completed;
    private boolean errored;
    private boolean started;
    /**
     * The following priorities are used to store the progress of processed records since it is not efficient to
     * update offset db for every record.
      */
    private int processedRecordNum;
    private int processedRecordSize;

    private int processedRecordNumSinceLastSync;

    public PartitionConsumptionState(int partition, OffsetRecord offsetRecord) {
      this.partition = partition;
      this.offsetRecord = offsetRecord;
      this.deferredWrite = false;
      this.completed = false;
      this.errored = false;
      this.started = false;
      this.processedRecordNum = 0;
      this.processedRecordSize = 0;
      this.processedRecordNumSinceLastSync = 0;
    }

    public int getPartition() {
      return this.partition;
    }
    public void setOffsetRecord(OffsetRecord offsetRecord) {
      this.offsetRecord = offsetRecord;
    }
    public OffsetRecord getOffsetRecord() {
      return this.offsetRecord;
    }
    public void setDeferredWrite(boolean deferredWrite) {
      this.deferredWrite = deferredWrite;
    }
    public boolean isDeferredWrite() {
      return this.deferredWrite;
    }
    public void start() {
      this.started = true;
    }
    public boolean isStarted() {
      return this.started;
    }
    public void complete() {
      this.completed = true;
    }
    public boolean isCompleted() {
      return this.completed;
    }
    public void error() {
      this.errored = true;
    }
    public boolean isErrored() {
      return this.errored;
    }
    public void incrProcessedRecordNum() {
      ++this.processedRecordNum;
    }
    public int getProcessedRecordNum() {
      return this.processedRecordNum;
    }
    public void resetProcessedRecordNum() {
      this.processedRecordNum = 0;
    }
    public void incrProcessedRecordSize(int recordSize) {
      this.processedRecordSize += recordSize;
    }
    public int getProcessedRecordSize() {
      return this.processedRecordSize;
    }
    public void resetProcessedRecordSize() {
      this.processedRecordSize = 0;
    }

    @Override
    public String toString() {
      return "PartitionConsumptionState{" +
          "partition=" + partition +
          ", offsetRecord=" + offsetRecord +
          ", completed=" + completed +
          ", errored=" + errored +
          ", started=" + started +
          ", processedRecordNum=" + processedRecordNum +
          ", processedRecordSize=" + processedRecordSize +
          '}';
    }
    public int getProcessedRecordNumSinceLastSync() {
      return this.processedRecordNumSinceLastSync;
    }
    public void incrProcessedRecordNumSinceLastSync() {
      ++this.processedRecordNumSinceLastSync;
    }
    public void resetProcessedRecordNumSinceLastSync() {
      this.processedRecordNumSinceLastSync = 0;
    }
  }

  // TOOD: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = Logger.getLogger(StoreIngestionTask.class);

  // Constants

  private static final String CONSUMER_TASK_ID_FORMAT = StoreIngestionTask.class.getSimpleName() + " for [ Topic: %s ]";
  // TODO: consider to make those delay time configurable for operability purpose. Some are non-final so tests can alter the values.
  public static int READ_CYCLE_DELAY_MS = 1000;
  public static int POLLING_SCHEMA_DELAY_MS = 5 * READ_CYCLE_DELAY_MS;
  public static long PROGRESS_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  /** After processing the following number of messages, Venice SN will invoke throttling. */
  public static int OFFSET_THROTTLE_INTERVAL = 1000;
  /** Record offset interval before persisting it in offset db for transaction mode database. */
  public static int OFFSET_UPDATE_INTERVAL_PER_PARTITION_FOR_TRANSACTION_MODE = 1000;
  /** Record offset interval before persisting it in offset db for deferred write database. */
  public static int OFFSET_UPDATE_INTERNAL_PER_PARTITION_FOR_DEFERRED_WRITE = 100000;
  private static final int MAX_CONTROL_MESSAGE_RETRIES = 3;
  private static final int MAX_IDLE_COUNTER  = 100;
  private static final int CONSUMER_ACTION_QUEUE_INIT_CAPACITY = 11;
  private static final long KILL_WAIT_TIME_MS = 5000l;
  private static final int MAX_KILL_CHECKING_ATTEMPTS = 10;

  // Final stuffz

  /** Ack producer */
  private final Queue<VeniceNotifier> notifiers;
  /** storage destination for consumption */
  private final StoreRepository storeRepository;
  private final String topic;
  private final String storeNameWithoutVersionInfo;
  private final ReadOnlySchemaRepository schemaRepo;
  private final String consumerTaskId;
  private final Properties kafkaProps;
  private final VeniceConsumerFactory factory;
  private final AtomicBoolean isRunning;
  private final AtomicBoolean emitMetrics;
  private final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;
  private final OffsetManager offsetManager;
  private final TopicManager topicManager;
  private final EventThrottler throttler;
  /** Per-partition consumption state map */
  private final ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap;
  private final StoreBufferService storeBufferService;
  /** Persists the exception thrown by {@link StoreBufferService}. */
  private Exception lastDrainerException = null;
  /** Keeps track of every upstream producer this consumer task has seen so far. */
  private final ConcurrentMap<GUID, ProducerTracker> producerTrackerMap;
  private final AggStoreIngestionStats stats;
  private final BooleanSupplier isCurrentVersion;
  private final Optional<HybridStoreConfig> hybridStoreConfig;

  // Non-final
  private KafkaConsumerWrapper consumer;
  private Set<Integer> schemaIdSet;
  private long lastProgressReportTime = 0;
  private int idleCounter = 0;


  public StoreIngestionTask(@NotNull VeniceConsumerFactory factory,
                            @NotNull Properties kafkaConsumerProperties,
                            @NotNull StoreRepository storeRepository,
                            @NotNull OffsetManager offsetManager,
                            @NotNull Queue<VeniceNotifier> notifiers,
                            @NotNull EventThrottler throttler,
                            @NotNull String topic,
                            @NotNull ReadOnlySchemaRepository schemaRepo,
                            @NotNull TopicManager topicManager,
                            @NotNull AggStoreIngestionStats stats,
                            @NotNull StoreBufferService storeBufferService,
                            @NotNull BooleanSupplier isCurrentVersion,
                            @NotNull Optional<HybridStoreConfig> hybridStoreConfig) {
    this.factory = factory;
    this.kafkaProps = kafkaConsumerProperties;
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;
    this.notifiers = notifiers;
    this.throttler = throttler;
    this.topic = topic;
    this.schemaRepo = schemaRepo;
    this.storeNameWithoutVersionInfo = Version.parseStoreFromKafkaTopicName(topic);
    this.schemaIdSet = new HashSet<>();
    this.consumerActionsQueue = new PriorityBlockingQueue<>(CONSUMER_ACTION_QUEUE_INIT_CAPACITY,
        new ConsumerAction.ConsumerActionPriorityComparator());

    // partitionConsumptionStateMap could be accessed by multiple threads: consumption thread and the thread handling kill message
    this.partitionConsumptionStateMap = new ConcurrentHashMap<>();

    // Could be accessed from multiple threads since there are multiple worker threads.
    this.producerTrackerMap = new ConcurrentHashMap<>();
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, topic);
    this.topicManager = topicManager;

    this.stats = stats;
    stats.updateStoreConsumptionTask(storeNameWithoutVersionInfo, this);

    this.isRunning = new AtomicBoolean(true);
    this.emitMetrics = new AtomicBoolean(true);

    this.storeBufferService = storeBufferService;
    this.isCurrentVersion = isCurrentVersion;
    this.hybridStoreConfig = hybridStoreConfig;
  }

  private void validateState() {
    if(!this.isRunning.get()) {
      throw new VeniceException(" Topic " + topic + " is shutting down, no more messages accepted");
    }
  }

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public synchronized void subscribePartition(String topic, int partition) {
    validateState();
    consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.SUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public synchronized void unSubscribePartition(String topic, int partition) {
    validateState();
    consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.UNSUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public synchronized void resetPartitionConsumptionOffset(String topic, int partition) {
    validateState();
    consumerActionsQueue.add(new ConsumerAction(ConsumerActionType.RESET_OFFSET, topic, partition));
  }

  public synchronized void kill() {
    validateState();
    consumerActionsQueue.add(ConsumerAction.createKillAction(topic));
    int currentAttempts = 0;
    try {
      // Check whether the task is really killed
      while (isRunning() && currentAttempts < MAX_KILL_CHECKING_ATTEMPTS) {
        TimeUnit.MILLISECONDS.sleep(KILL_WAIT_TIME_MS / MAX_KILL_CHECKING_ATTEMPTS);
        currentAttempts ++;
      }
    } catch (InterruptedException e) {
      logger.warn("Wait killing is interrupted.", e);
    }
    if (isRunning()) {
      //If task is still running, force close it.
      reportError(partitionConsumptionStateMap.keySet(), "Received the signal to kill this consumer. Topic " + topic,
          new VeniceException("Kill the consumer"));
      // close can not stop the consumption synchronizely, but the status of helix would be set to ERROR after
      // reportError. The only way to stop it synchronizely is interrupt the current running thread, but it's an unsafe
      // operation, for example it could break the ongoing db operation, so we should avoid that.
      this.close();
    }
  }

  private void reportProgress(int partition, long partitionOffset ) {
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    if (null == partitionConsumptionState) {
      logger.info("Topic " + topic + " Partition " + partition + " has been unsubscribed, no need to report progress");
      return;
    }
    if (!isCurrentVersion.getAsBoolean() && // The currently-active version should always report progress.
        (!partitionConsumptionState.isStarted() ||
            partitionConsumptionState.isCompleted() ||
            partitionConsumptionState.isErrored())) {
      logger.warn(
          "Can not report progress for Topic:" + topic + " Partition:" + partition + " offset:" + partitionOffset
              + ", because it has not been started or already been terminated. "
              + "partitionConsumptionState: " + partitionConsumptionState.toString());
      return;
    }
    // Progress reporting happens too frequently for each Kafka Pull,
    // Report progress only if configured intervals have elapsed.
    // This has a drawback if there are messages but the interval has not elapsed
    // they will not be reported. But if there are no messages after that
    // for a long time, no progress will be reported. That is OK for now.
    long timeElapsed = System.currentTimeMillis() - lastProgressReportTime;
    if(timeElapsed < PROGRESS_REPORT_INTERVAL) {
      return;
    }

    lastProgressReportTime = System.currentTimeMillis();
    for(VeniceNotifier notifier : notifiers) {
      try {
        notifier.progress(topic, partition, partitionOffset);
      } catch(Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }
  }

  private void adjustStorageEngine(int partitionId, boolean sorted, PartitionConsumptionState partitionConsumptionState) {
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);
    StoragePartitionConfig storagePartitionConfig = new StoragePartitionConfig(topic, partitionId);
    storagePartitionConfig.setDeferredWrite(sorted);
    storageEngine.adjustStoragePartition(storagePartitionConfig);
    partitionConsumptionState.setDeferredWrite(storagePartitionConfig.isDeferredWrite());

    if (sorted) {
      logger.info("The messages in current topic: " + topic + ", partition: " + partitionId
          + " is sorted, switched to deferred-write database");
    } else {
      logger.info("The messages in current topic: " + topic + ", partition: " + partitionId
          + " is not sorted, switched to regular transactional database");
    }
  }

  private void reportStarted(int partition, PartitionConsumptionState partitionConsumptionState) {
    if (null == partitionConsumptionState) {
      logger.info("Topic " + topic + " Partition " + partition + " has been unsubscribed, no need to report started");
      return;
    }
    partitionConsumptionState.start();
    for(VeniceNotifier notifier : notifiers) {
      try {
        notifier.started(topic, partition);
      } catch(Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }
  }

  private void reportRestarted(int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
    if (null == partitionConsumptionState) {
      logger.info("Topic " + topic + " Partition " + partition + " has been unsubscribed, no need to report restarted");
      return;
    }
    partitionConsumptionState.start();
    for (VeniceNotifier notifier : notifiers) {
      try {
        notifier.restarted(topic, partition, offset);
      } catch (Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass(), ex);
      }
    }
  }

  private void reportCompleted(int partition, PartitionConsumptionState partitionConsumptionState) {
    if (null == partitionConsumptionState) {
      logger.info("Topic " + topic + " Partition " + partition + " has been unsubscribed, no need to report completed");
      return;
    }
    OffsetRecord lastOffsetRecord = partitionConsumptionState.getOffsetRecord();

    if (partitionConsumptionState.isErrored()) {
      // Notifiers will not be sent a completion notification, they should only receive the previously-sent
      // error notification.
      logger.error("Processing completed WITH ERRORS for topic " + topic + " Partition " + partition +
          " Last Offset " + lastOffsetRecord.getOffset());
    } else {
      logger.info("Processing completed for topic " + topic + " Partition " + partition + " Last Offset "
          + lastOffsetRecord.getOffset());
      boolean isHybrid = hybridStoreConfig.isPresent();
      for(VeniceNotifier notifier : notifiers) {
        try {
          logger.debug("reportCompleted called by VeniceNotifier class: " + notifier.getClass().getName() +
              ", isHybrid: " + isHybrid +
              ", notifier.replicationLagShouldBlockCompletion(): " + notifier.replicationLagShouldBlockCompletion() +
              ", isReplicationLagging(): " + isReplicationLagging());
          if (isHybrid
              && notifier.replicationLagShouldBlockCompletion()
              && isReplicationLagging()) {
            // skip
          } else {
            notifier.completed(topic, partition, lastOffsetRecord.getOffset());
          }
        } catch(Exception ex) {
          logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
        }
      }
    }
    partitionConsumptionState.complete();
  }

  /**
   * Lag = (RT Max - SOBR) - (Current - EOP)
   *
   * @return true if Lag > threshold
   */
  private boolean isReplicationLagging() {
    // TODO: Implement this!
    return true;
  }

  private void reportError(Collection<Integer> partitions, String message, Exception consumerEx) {
    for(Integer partitionId: partitions) {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
      if (null == partitionConsumptionState) {
        logger.info("Topic " + topic + " Partition " + partitionId + " has been unsubscribed, no need to report error");
        continue;
      }
      // TODO: Decide if the if below can be fully eliminated?
      if (!isCurrentVersion.getAsBoolean() && partitionConsumptionState.isCompleted()) {
        logger.warn("Topic:" + topic + " Partition:" + partitionId
            + " has been marked as completed, so an error will not be reported..");
        continue;
      }
      if (partitionConsumptionState.isErrored()) {
        logger.warn("Topic:" + topic + " Partition:" + partitionId + " has been reported as error before.");
        continue;
      }
      partitionConsumptionState.error();

      for(VeniceNotifier notifier : notifiers) {
        try {
          notifier.error(topic, partitionId, message, consumerEx);
        } catch(Exception notifierEx) {
          logger.error(consumerTaskId + " Error reporting status to notifier " + notifier.getClass() , notifierEx);
        }
      }
    }
  }


  private void processMessages() throws InterruptedException {
    if (null != lastDrainerException) {
      // Interrupt the whole ingestion task
      throw new VeniceException("Exception thrown by store buffer drainer", lastDrainerException);
    }
    if (partitionConsumptionStateMap.size() > 0) {
      idleCounter = 0;
      long beforePollingTimestamp = System.currentTimeMillis();
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = consumer.poll(READ_CYCLE_DELAY_MS);
      long afterPollingTimestamp = System.currentTimeMillis();

      int pollResultNum = 0;
      if (null != records) {
        pollResultNum = records.count();
      }
      if (emitMetrics.get()) {
        stats.recordPollRequestLatency(storeNameWithoutVersionInfo, afterPollingTimestamp - beforePollingTimestamp);
        stats.recordPollResultNum(storeNameWithoutVersionInfo, pollResultNum);
      }

      if (null == records) {
        return;
      }
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
        // blocking call
        storeBufferService.putConsumerRecord(record, this);
      }
      long afterPutTimestamp = System.currentTimeMillis();
      if (emitMetrics.get()) {
        stats.recordConsumerRecordsQueuePutLatency(storeNameWithoutVersionInfo, afterPutTimestamp - afterPollingTimestamp);
      }
    } else {
      idleCounter ++;
      if(idleCounter > MAX_IDLE_COUNTER) {
        logger.warn(consumerTaskId + " No Partitions are subscribed to for store attempts expired after " + idleCounter);
        complete();
      } else {
        logger.warn(consumerTaskId + " No Partitions are subscribed to for store attempt " + idleCounter);
        Utils.sleep(READ_CYCLE_DELAY_MS);
      }
    }
  }


  /**
   * Polls the producer for new messages in an infinite loop by a dedicated consumer thread
   * and processes the new messages by current thread.
   */
  @Override
  public void run() {
    logger.info("Running " + consumerTaskId);
    try {
      this.consumer = factory.getConsumer(kafkaProps);
      /**
       * Here we could not use isRunning() since it is a synchronized function, whose lock could be
       * acquired by some other synchronized function, such as {@link #kill()}.
        */
      while (isRunning.get()) {
        processConsumerActions();
        processMessages();
      }
    } catch (Exception e) {
      // After reporting error to controller, controller will ignore the message from this replica if job is aborted.
      // So even this storage node recover eventually, controller will not confused.
      // If job is not aborted, controller is open to get the subsequent message from this replica(if storage node was
      // recovered, it will send STARTED message to controller again)
      logger.error(consumerTaskId + " failed with Exception.", e);
      reportError(partitionConsumptionStateMap.keySet(), "Exception caught during poll.", e);
    } catch (Throwable t) {
      logger.error(consumerTaskId + " failed with Throwable!!!", t);
      reportError(partitionConsumptionStateMap.keySet(), "Non-exception Throwable caught in " + getClass().getSimpleName() +
          "'s run() function.", new VeniceException(t));
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
        offsetManager.clearOffset(topic , partition);
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
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processConsumerActions() {
    Iterator<ConsumerAction> iter = consumerActionsQueue.iterator();
    while (iter.hasNext()) {
      // Do not want to remove a message from the queue unless it has been processed.
      ConsumerAction message = iter.next();
      try {
        message.incrementAttempt();
        processConsumerAction(message);
      } catch (InterruptedException e){
        // task is killed
        throw new VeniceException("Consumption task is killed", e);
      } catch (Exception ex) {
        if (message.getAttemptsCount() < MAX_CONTROL_MESSAGE_RETRIES) {
          logger.info("Error Processing message will retry later" + message , ex);
          return;
        } else {
          logger.info("Ignoring message:  " + message + " after retries " + message.getAttemptsCount(), ex);
        }
      }
      iter.remove();
    }
  }

  private void processConsumerAction(ConsumerAction message)
      throws InterruptedException {
    ConsumerActionType operation = message.getType();
    String topic = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case SUBSCRIBE:
        // Drain the buffered message by last subscription.
        storeBufferService.drainBufferedRecordsFromTopicPartition(topic, partition);

        OffsetRecord record = offsetManager.getLastOffset(topic, partition);

        // First let's try to restore the state retrieved from the OffsetManager
        PartitionConsumptionState newPartitionConsumptionState = new PartitionConsumptionState(partition, record);
        partitionConsumptionStateMap.put(partition,  newPartitionConsumptionState);
        record.getProducerPartitionStateMap().entrySet().stream().forEach(entry -> {
              GUID producerGuid = GuidUtils.getGuidFromCharSequence(entry.getKey());
              ProducerTracker producerTracker = producerTrackerMap.get(producerGuid);
              if (null == producerTracker) {
                producerTracker = new ProducerTracker(producerGuid);
              }
              producerTracker.setPartitionState(partition, entry.getValue());
              producerTrackerMap.put(producerGuid, producerTracker);
            }
        );

        // Once storage node restart, send the "START" status to controller to rebuild the task status.
        // If this storage node has ever consumed data from this topic, instead of sending "START" here, we send it
        // once START_OF_PUSH message has been read.
        if (record.getOffset() > 0) {
          adjustStorageEngine(partition, record.sorted(), newPartitionConsumptionState);
          reportRestarted(partition, record.getOffset(), newPartitionConsumptionState);
        }
        // Second, take care of informing the controller about our status, and starting consumption
        if (record.isCompleted()) {
          /**
           * There could be two cases in this scenario:
           * 1. The job is completed, so Controller will ignore any status message related to the completed/archived job.
           * 2. The job is still running: some partitions are in 'ONLINE' state, but other partitions are still in
           * 'BOOTSTRAP' state.
           * In either case, StoreIngestionTask should report 'started' => ['progress'] => 'completed' to accomplish
           * task state transition in Controller.
           */
          reportCompleted(partition, newPartitionConsumptionState);
          logger.info("Topic: " + topic + ", Partition Id: " + partition + " is already done.");
        } else {
          // TODO: When we support hybrid batch/streaming mode, the subscription logic should be moved out after the if
          consumer.subscribe(topic, partition, record);
          logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition + " Offset " + record.getOffset());
        }
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
        try {
          consumer.resetOffset(topic, partition);
          logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition);
        } catch (UnsubscribedTopicPartitionException e) {
          logger.info(consumerTaskId + " No need to reset offset by Kafka consumer, since the consumer is not " +
              "subscribing Topic: " + topic + " Partition Id: " + partition);
        }
        partitionConsumptionStateMap.put(partition, new PartitionConsumptionState(partition, new OffsetRecord()));
        producerTrackerMap.values().stream().forEach(
            producerTracker -> producerTracker.clearPartition(partition)
        );
        offsetManager.clearOffset(topic, partition);
        break;
      case KILL:
        logger.info("Kill this consumer task for Topic:" + topic);
        // Throw the exception here to break the consumption loop, and then this task is marked as error status.
        throw new InterruptedException("Received the signal to kill this consumer. Topic " + topic);
      default:
        throw new UnsupportedOperationException(operation.name() + "not implemented.");
    }
  }

  private boolean shouldProcessRecord(ConsumerRecord record) {
    String recordTopic = record.topic();
    if(!topic.equals(recordTopic)) {
      throw new VeniceMessageException(consumerTaskId + "Message retrieved from different topic. Expected " + this.topic + " Actual " + recordTopic);
    }

    int partitionId = record.partition();
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partitionId);
    if(null == partitionConsumptionState) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + topic + " Partition Id: " + partitionId );
      return false;
    }
    long lastOffset = partitionConsumptionState.getOffsetRecord()
        .getOffset();
    if(lastOffset >= record.offset()) {
      logger.info(consumerTaskId + "The record was already processed Partition" + partitionId + " LastKnown " + lastOffset + " Current " + record.offset());
      return false;
    }

    return true;
  }

  /**
   * This function will be invoked in {@link StoreBufferService} to process buffered {@link ConsumerRecord}.
   * @param record
   */
  public void processConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    int partition = record.partition();
    //The partitionConsumptionStateMap can be modified by other threads during consumption (for example when unsubscribing)
    //in order to maintain thread safety, we hold onto the reference to the partitionConsumptionState and pass that
    //reference to all downstream methods so that all offset persistence operations use the same partitionConsumptionState
    PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateMap.get(partition);
    if (null == partitionConsumptionState) {
      logger.info("Topic " + topic + " Partition " + partition + " has been unsubscribed, skip this record that has offset " + record.offset());
      return;
    }
    if (partitionConsumptionState.isErrored()) {
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
      String errorMessage = "Fatal data validation problem with partition " + faultyPartition;
      boolean needToUnsub = !(isCurrentVersion.getAsBoolean() || partitionConsumptionState.completed);
      if (needToUnsub) {
        errorMessage += ". Consumption will be halted.";
      } else {
        errorMessage += ". Because " + topic + " is the current version, consumption will continue.";
      }
      reportError(Lists.newArrayList(faultyPartition), errorMessage, e);
      if (needToUnsub) {
        unSubscribePartition(topic, faultyPartition);
      }
    } catch (VeniceMessageException | UnsupportedOperationException ex) {
      throw new VeniceException(consumerTaskId + " : Received an exception for message at partition: "
          + record.partition() + ", offset: " + record.offset() + ". Bubbling up.", ex);
    }

    partitionConsumptionState.incrProcessedRecordNum();
    partitionConsumptionState.incrProcessedRecordSize(recordSize);

    int processedRecordNum = partitionConsumptionState.getProcessedRecordNum();
    if (processedRecordNum >= OFFSET_THROTTLE_INTERVAL ||
        partitionConsumptionState.isCompleted()) {
      int processedRecordSize = partitionConsumptionState.getProcessedRecordSize();
      throttler.maybeThrottle(processedRecordSize);
      // Report metrics
      if (emitMetrics.get()) {
        stats.recordBytesConsumed(storeNameWithoutVersionInfo, processedRecordSize);
        stats.recordRecordsConsumed(storeNameWithoutVersionInfo, processedRecordNum);
      }

      partitionConsumptionState.resetProcessedRecordNum();
      partitionConsumptionState.resetProcessedRecordSize();
    }

    partitionConsumptionState.incrProcessedRecordNumSinceLastSync();
    int offsetPersistInterval = partitionConsumptionState.isDeferredWrite() ?
        OFFSET_UPDATE_INTERNAL_PER_PARTITION_FOR_DEFERRED_WRITE : OFFSET_UPDATE_INTERVAL_PER_PARTITION_FOR_TRANSACTION_MODE;
    if (partitionConsumptionState.getProcessedRecordNumSinceLastSync() >= offsetPersistInterval ||
        partitionConsumptionState.isCompleted()) {
      AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);
      storageEngine.sync(partition);
      OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
      if (partitionConsumptionState.isCompleted()) {
        offsetRecord.complete();
      }
      // Always persist whether the messages are sorted together with offset.
      offsetRecord.setSorted(partitionConsumptionState.isDeferredWrite());
      offsetManager.recordOffset(this.topic, partition, offsetRecord);
      reportProgress(partition, offsetRecord.getOffset());
      partitionConsumptionState.resetProcessedRecordNumSinceLastSync();
    }
  }

  public void setLastDrainerException(Exception e) {
    lastDrainerException = e;
  }

  public long getOffsetLag() {
    if (!emitMetrics.get()) {
      return 0;
    }

    Map<Integer, Long> latestOffsets = topicManager.getLatestOffsets(topic);
    long offsetLag = partitionConsumptionStateMap.entrySet().stream()
        .map(entry -> latestOffsets.get(entry.getKey()) - entry.getValue().getOffsetRecord().getOffset())
        .reduce(0l, (lag1, lag2) -> lag1 + lag2);

    return offsetLag > 0 ? offsetLag : 0;
  }

  private void processControlMessage(ControlMessage controlMessage, int partition, long offset, PartitionConsumptionState partitionConsumptionState) {
     switch(ControlMessageType.valueOf(controlMessage)) {
       case START_OF_PUSH:
         logger.info(consumerTaskId + " : Received Begin of push message Setting resetting count. Partition: "
             + partition + ", Offset: " + offset);
         StartOfPush startOfPush = (StartOfPush)controlMessage.controlMessageUnion;
         adjustStorageEngine(partition, startOfPush.sorted, partitionConsumptionState);
         reportStarted(partition, partitionConsumptionState);
         break;
       case END_OF_PUSH:
         logger.info(consumerTaskId + " : Receive End of Push message. Partition: " + partition + ", Offset: " + offset);
         /**
          * Right now, we assume there are no sorted message after EndOfPush control message.
          * TODO: if this behavior changes in the future, the logic needs to be adjusted as well.
          */
         adjustStorageEngine(partition, false, partitionConsumptionState);
         reportCompleted(partition, partitionConsumptionState);
         break;
       case START_OF_SEGMENT:
       case END_OF_SEGMENT:
         /**
          * No-op for {@link ControlMessageType#START_OF_SEGMENT} and {@link ControlMessageType#END_OF_SEGMENT}.
          * These are handled in the {@link ProducerTracker}.
          */
         break;
       default:
         throw new UnsupportedMessageTypeException("Unrecognized Control message type " + controlMessage.controlMessageType);
    }
  }

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param consumerRecord {@link ConsumerRecord} consumed from Kafka.
   * @return the size of the data written to persistent storage.
   */
  private int internalProcessConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord, PartitionConsumptionState partitionConsumptionState) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int sizeOfPersistedData = 0;

    Optional<OffsetRecordTransformer> offsetRecordTransformer = Optional.empty();
    try {
      offsetRecordTransformer = Optional.of(validateMessage(consumerRecord.partition(), kafkaKey, kafkaValue));

      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        processControlMessage(controlMessage, consumerRecord.partition(), consumerRecord.offset(), partitionConsumptionState);
      } else if (null == kafkaValue) {
        throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message. Partition " +
              consumerRecord.partition() + " Offset " + consumerRecord.offset());
      } else {
        int keySize = kafkaKey.getKeyLength();
        int valueSize = processVeniceMessage(kafkaKey, kafkaValue, consumerRecord.partition());
        if (emitMetrics.get()) {
          stats.recordKeySize(storeNameWithoutVersionInfo, keySize);
          stats.recordValueSize(storeNameWithoutVersionInfo, valueSize);
        }
        sizeOfPersistedData = keySize + valueSize;
      }
    } catch (DuplicateDataException e) {
      logger.info("Skipping a duplicate record in topic: '" + topic + "', offset: " + consumerRecord.offset());
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
      OffsetRecord offsetRecord = partitionConsumptionState.getOffsetRecord();
      if (offsetRecordTransformer.isPresent()) {
        // This closure will have the side effect of altering the OffsetRecord
        offsetRecord = offsetRecordTransformer.get().transform(offsetRecord);
      } /** else, {@link #validateMessage(int, KafkaKey, KafkaMessageEnvelope)} threw a {@link DuplicateDataException} */

      offsetRecord.setOffset(consumerRecord.offset());
      partitionConsumptionState.setOffsetRecord(offsetRecord);
    }
    return sizeOfPersistedData;
  }

  private OffsetRecordTransformer validateMessage(int partition, KafkaKey key, KafkaMessageEnvelope message) {
    final GUID producerGUID = message.producerMetadata.producerGUID;
    producerTrackerMap.putIfAbsent(producerGUID, new ProducerTracker(producerGUID));
    ProducerTracker producerTracker = producerTrackerMap.get(producerGUID);

    return producerTracker.addMessage(partition, key, message);
  }

  /**
   * @return the size of the data which was written to persistent storage.
   */
  private int processVeniceMessage(KafkaKey kafkaKey, KafkaMessageEnvelope kafkaValue, int partition) {
    long startTimeNs = -1;
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);

    byte[] keyBytes = kafkaKey.getKey();

    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }
        // If single-threaded, we can re-use (and clobber) the same Put instance. // TODO: explore GC tuning later.
        Put put = (Put) kafkaValue.payloadUnion;
        // Validate schema id first
        checkValueSchemaAvail(put.schemaId);
        byte[] valueBytes = put.putValue.array();
        /** TODO: Right now, the concatenation part will allocate a new byte array and copy over schema id and data,
          * which might cause some GC issue since this operation will be triggered for every 'PUT'.
          * If this issue happens, we need to consider other ways to improve it:
          * 1. Maybe we can do the concatenation in VeniceWriter, which is being used by KafkaPushJob;
          * 2. Investigate whether DB can accept multiple binary arrays for 'PUT' operation;
          * 3. ...
          */
        ValueRecord valueRecord = ValueRecord.create(put.schemaId, valueBytes);
        storageEngine.put(partition, keyBytes, valueRecord.serialize());
        if (logger.isTraceEnabled()) {
          logger.trace(consumerTaskId + " : Completed PUT to Store: " + topic + " for key: " +
                  ByteUtils.toHexString(keyBytes) + ", value: " + ByteUtils.toHexString(put.putValue.array()) + " in " +
                  (System.nanoTime() - startTimeNs) + " ns at " + System.currentTimeMillis());
        }
        return valueBytes == null ? 0 : valueBytes.length;

      case DELETE:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }

        storageEngine.delete(partition, keyBytes);

        if (logger.isTraceEnabled()) {
          logger.trace(consumerTaskId + " : Completed DELETE to Store: " + topic + " for key: " +
                  ByteUtils.toHexString(keyBytes) + " in " + (System.nanoTime() - startTimeNs) + " ns at " +
                  System.currentTimeMillis());
        }
        return 0;
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

  public void enableMetricsEmission() {
    this.emitMetrics.set(true);
  }

  public void disableMetricsEmission() {
    this.emitMetrics.set(false);
  }
}
