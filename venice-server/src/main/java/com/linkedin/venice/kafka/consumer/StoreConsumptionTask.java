package com.linkedin.venice.kafka.consumer;

import com.google.common.collect.Lists;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggStoreConsumptionStats;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import javax.validation.constraints.NotNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;


/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public class StoreConsumptionTask implements Runnable, Closeable {

  // TOOD: Make this logger prefix everything with the CONSUMER_TASK_ID_FORMAT
  private static final Logger logger = Logger.getLogger(StoreConsumptionTask.class);

  private static final String CONSUMER_TASK_ID_FORMAT = StoreConsumptionTask.class.getSimpleName() + " for [ Topic: %s ]";

  // Making it non final to shorten the time in testing.
  // TODO: consider to make those delay time configurable for operability purpose
  public static int READ_CYCLE_DELAY_MS = 1000;
  public static int POLLING_SCHEMA_DELAY_MS = 5 * READ_CYCLE_DELAY_MS;

  private static final int MAX_CONTROL_MESSAGE_RETRIES = 3;

  //Ack producer
  private final Queue<VeniceNotifier> notifiers;

  // storage destination for consumption
  private final StoreRepository storeRepository;

  private final String topic;

  private final String storeNameWithoutVersionInfo;

  private final ReadOnlySchemaRepository schemaRepo;
  private Set<Integer> schemaIdSet;

  private final String consumerTaskId;
  private final Properties kafkaProps;
  private final VeniceConsumerFactory factory;

  private KafkaConsumerWrapper consumer;

  private final AtomicBoolean isRunning;

  private final PriorityBlockingQueue<ConsumerAction> consumerActionsQueue;

  private final OffsetManager offsetManager;

  private final TopicManager topicManager;

  private final EventThrottler throttler;

  private long lastProgressReportTime = 0;
  private static final long PROGRESS_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  /**
   * The source of truth for the currently subscribed partitions. The list maintained by the kafka consumer
   * is not always up to date because of the asynchronous nature of subscriptions of partitions in Kafka Consumer.
   */
  private final ConcurrentMap<Integer, OffsetRecord> partitionToOffsetMap;
  private final Set<Integer> completedPartitions;

  private final Set<Integer> errorPartitions;

  private final Set<Integer> startedPartitions;

  /**
   * Keeps track of every upstream producer this consumer task has seen so far.
   */
  private final Map<GUID, ProducerTracker> producerTrackerMap;

  private static int MAX_IDLE_COUNTER  = 100;
  private int idleCounter = 0;

  private static int CONSUMER_ACTION_QUEUE_INIT_CAPACITY = 11;

  private static final long KILL_WAIT_TIME_MS = 5000l;
  public static final int MAX_KILL_CHECKING_ATTEMPTS = 10;

  private final AggStoreConsumptionStats stats;

  public StoreConsumptionTask(@NotNull VeniceConsumerFactory factory,
                              @NotNull Properties kafkaConsumerProperties,
                              @NotNull StoreRepository storeRepository,
                              @NotNull OffsetManager offsetManager,
                              @NotNull Queue<VeniceNotifier> notifiers,
                              @NotNull EventThrottler throttler,
                              @NotNull String topic,
                              @NotNull ReadOnlySchemaRepository schemaRepo,
                              @NotNull TopicManager topicManager,
                              @NotNull AggStoreConsumptionStats stats) {
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

    // partitionToOffsetMap is accessed by multiple threads: consumption thread and the thread handle kill message.
    this.partitionToOffsetMap = new ConcurrentHashMap<>();
    // We need thread safe set here because it could be visited by two threads, kill thread and consumption thread.
    this.completedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    this.errorPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    this.startedPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // Should be accessed only from a single thread.
    this.producerTrackerMap = new HashMap<>();
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, topic);
    this.topicManager = topicManager;

    this.stats = stats;
    stats.updateStoreConsumptionTask(storeNameWithoutVersionInfo, this);

    this.isRunning = new AtomicBoolean(true);
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
    int currentAttemp = 0;
    try {
      // Check whether the task is really killed
      while (isRunning() && currentAttemp < MAX_KILL_CHECKING_ATTEMPTS) {
        TimeUnit.MILLISECONDS.sleep(KILL_WAIT_TIME_MS / MAX_KILL_CHECKING_ATTEMPTS);
        currentAttemp ++;
      }
    } catch (InterruptedException e) {
      logger.warn("Wait killing is interrupted.");
    }
    if (isRunning()) {
      //If task is still running, force close it.
      reportError(partitionToOffsetMap.keySet(), "Received the signal to kill this consumer. Topic " + topic,
          new VeniceException("Kill the consumer"));
      // close can not stop the consumption synchronizely, but the status of helix would be set to ERROR after
      // reportError. The only way to stop it synchronizely is interrupt the current running thread, but it's an unsafe
      // operation, for example it could break the ongoing db operation, so we should avoid that.
      this.close();
    }

  }

  private void reportProgress(int partition, long partitionOffset ) {
    if (!startedPartitions.contains(partition)) {
      logger.warn(
          "Can not report progress for Topic:" + topic + " Partition:" + partition + " offset:" + partitionOffset
              + ", because it has not been started or already been terminated.");
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

  private void reportStarted(int partition) {
    startedPartitions.add(partition);
    for(VeniceNotifier notifier : notifiers) {
      try {
        notifier.started(topic, partition);
      } catch(Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }
  }

  private void reportRestarted(int partition, long offset) {
    startedPartitions.add(partition);
    for (VeniceNotifier notifier : notifiers) {
      try {
        notifier.restarted(topic, partition, offset);
      } catch (Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass(), ex);
      }
    }
  }

  private void reportCompleted(int partition) {
    OffsetRecord lastOffsetRecord = partitionToOffsetMap.getOrDefault(partition , new OffsetRecord());


    if (errorPartitions.contains(partition)) {
      // Notifiers will not be sent a completion notification, they should only receive the previously-sent
      // error notification.
      logger.error("Processing completed WITH ERRORS for topic " + topic + " Partition " + partition + " Last Offset " + lastOffsetRecord.getOffset());
    } else {
      logger.info("Processing completed for topic " + topic + " Partition " + partition + " Last Offset " + lastOffsetRecord.getOffset());
      for(VeniceNotifier notifier : notifiers) {
        try {
          notifier.completed(topic, partition, lastOffsetRecord.getOffset());
        } catch(Exception ex) {
          logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
        }
      }
    }
    startedPartitions.remove(partition);
    completedPartitions.add(partition);
  }

  private void reportError(Collection<Integer> partitions, String message, Exception consumerEx) {
    for(Integer partitionId: partitions) {
      if (completedPartitions.contains(partitionId)) {
        logger.warn("Topic:" + topic + " Partition:" + partitionId
            + " has been reported as completed, do not be allowed to become error..");
        continue;
      }
      if (errorPartitions.contains(partitionId)) {
        logger.warn("Topic:" + topic + " Partition:" + partitionId + " has been reported as error before.");
        continue;
      }
      startedPartitions.remove(partitionId);
      errorPartitions.add(partitionId);

      for(VeniceNotifier notifier : notifiers) {
        try {
          notifier.error(topic, partitionId, message, consumerEx);
        } catch(Exception notifierEx) {
          logger.error(consumerTaskId + " Error reporting status to notifier " + notifier.getClass() , notifierEx);
        }
      }
    }
  }


  private void processMessages() {
    if (partitionToOffsetMap.size() > 0) {
      idleCounter = 0;
      long beforePollTimestamp = System.currentTimeMillis();
      ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
      long afterPollTimestamp = System.currentTimeMillis();
      stats.recordPollRequest(storeNameWithoutVersionInfo);
      stats.recordPollRequestLatency(storeNameWithoutVersionInfo, afterPollTimestamp - beforePollTimestamp);
      stats.recordPollResultNum(storeNameWithoutVersionInfo, records.count());
      processTopicConsumerRecords(records);
      long afterProcessingTimestamp = System.currentTimeMillis();
      stats.recordProcessPollResultLatency(storeNameWithoutVersionInfo, afterProcessingTimestamp - afterPollTimestamp);

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


  @Override
  /**
   * Polls the producer for new messages in an infinite loop and processes the new messages.
   */
  public void run() {
    logger.info("Running " + consumerTaskId);
    try {
      this.consumer = factory.getConsumer(kafkaProps);
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
      reportError(partitionToOffsetMap.keySet(), "Exception caught during poll.", e);
    } catch (Throwable t) {
      logger.error(consumerTaskId + " failed with Throwable!!!", t);
      reportError(partitionToOffsetMap.keySet(), "Non-exception Throwable caught in " + getClass().getSimpleName() +
          "'s run() function.", new VeniceException(t));
    } finally {
      internalClose();
    }
  }

  private void internalClose() {

    // Only reset Offset Messages are important, subscribe/unSubscribe will be handled
    // on the restart by Helix Controller notifications on the new StoreConsumptionTask.
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
        OffsetRecord record = offsetManager.getLastOffset(topic, partition);

        // First let's try to restore the state retrieved from the OffsetManager
        partitionToOffsetMap.put(partition, record);
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
          reportRestarted(partition, record.getOffset());
        }
        // Second, take care of informing the controller about our status, and starting consumption
        if (record.isCompleted()) {
          /**
           * There could be two cases in this scenario:
           * 1. The job is completed, so Controller will ignore any status message related to the completed/archived job.
           * 2. The job is still running: some partitions are in 'ONLINE' state, but other partitions are still in
           * 'BOOTSTRAP' state.
           * In either case, StoreConsumptionTask should report 'started' => ['progress'] => 'completed' to accomplish
           * task state transition in Controller.
           */
          reportCompleted(partition);
          logger.info("Topic: " + topic + ", Partition Id: " + partition + " is already done.");
        } else {
          // TODO: When we support hybrid batch/streaming mode, the subscription logic should be moved out after the if
          consumer.subscribe(topic, partition, record);
          logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition + " Offset " + record.getOffset());
        }
        break;
      case UNSUBSCRIBE:
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
        partitionToOffsetMap.remove(partition);
        completedPartitions.remove(partition);
        producerTrackerMap.values().stream().forEach(
            producerTracker -> producerTracker.clearPartition(partition)
        );
        consumer.unSubscribe(topic, partition);
        break;
      case RESET_OFFSET:
        partitionToOffsetMap.put(partition, new OffsetRecord());
        completedPartitions.remove(partition);
        producerTrackerMap.values().stream().forEach(
            producerTracker -> producerTracker.clearPartition(partition)
        );
        offsetManager.clearOffset(topic, partition);
        consumer.resetOffset(topic, partition);
        logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition );
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
    Long lastOffset = partitionToOffsetMap.get(partitionId).getOffset();
    if(lastOffset == null) {
      logger.info("Skipping message as partition is no longer actively subscribed. Topic: " + topic + " Partition Id: " + partitionId );
      return false;
    }

    if(lastOffset >= record.offset()) {
      logger.info(consumerTaskId + "The record was already processed Partition" + partitionId + " LastKnown " + lastOffset + " Current " + record.offset());
      return false;
    }

    return true;
  }

  private void processTopicConsumerRecords(ConsumerRecords records) {
    if (records == null ) {
      return;
    }

    Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordsIterator = records.iterator();
    if(!recordsIterator.hasNext()) {
      return;
    }

    int totalSize = 0;
    int totalRecords = 0;
    Set<Integer> processedPartitions = new HashSet<>();
    while (recordsIterator.hasNext()) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = recordsIterator.next();
      totalRecords++;
      if(shouldProcessRecord(record)) {
        try {
          totalSize += processConsumerRecord(record);
          processedPartitions.add(record.partition());
        } catch (FatalDataValidationException e) {
          int faultyPartition = record.partition();
          reportError(Lists.newArrayList(faultyPartition), "Fatal data validation problem with partition " + faultyPartition, e);
          unSubscribePartition(topic, faultyPartition);
        } catch (VeniceMessageException | UnsupportedOperationException ex) {
          throw new VeniceException(consumerTaskId + " : Received an exception for message at partition: "
              + record.partition() + ", offset: " + record.offset() + ". Bubbling up.", ex);
        }
      }
    }

    throttler.maybeThrottle(totalSize);
    stats.recordBytesConsumed(storeNameWithoutVersionInfo, totalSize);
    stats.recordRecordsConsumed(storeNameWithoutVersionInfo, totalRecords);

    for(Integer partition: processedPartitions) {
      if(!partitionToOffsetMap.containsKey(partition)) {
        // Partition is unSubscribed.
        continue;
      }
      OffsetRecord record = partitionToOffsetMap.get(partition);
      if (completedPartitions.contains(partition)) {
        record.complete();
      }
      offsetManager.recordOffset(this.topic, partition, record);
      reportProgress(partition, record.getOffset());
    }
  }

  public long getOffsetLag() {
    Map<Integer, Long> latestOffsets = topicManager.getLatestOffsets(topic);
    long offsetLag = partitionToOffsetMap.entrySet().stream()
        .map(entry -> latestOffsets.get(entry.getKey()) - entry.getValue().getOffset())
        .reduce(0l, (lag1, lag2) -> lag1 + lag2);

    return offsetLag > 0 ? offsetLag : 0;
  }

  private void processControlMessage(ControlMessage controlMessage, int partition, long offset) {
     switch(ControlMessageType.valueOf(controlMessage)) {
       case START_OF_PUSH:
         reportStarted(partition);
         logger.info(consumerTaskId + " : Received Begin of push message Setting resetting count. Partition: " +
             partition + ", Offset: " + offset);
         break;
       case END_OF_PUSH:
         logger.info(consumerTaskId + " : Receive End of Pushes message. Partition: " + partition + ", Offset: " + offset);
         reportCompleted(partition);
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
  private int processConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = consumerRecord.key();
    KafkaMessageEnvelope kafkaValue = consumerRecord.value();
    int sizeOfPersistedData = 0;

    Optional<OffsetRecordTransformer> offsetRecordTransformer = Optional.empty();
    try {
      offsetRecordTransformer = Optional.of(validateMessage(consumerRecord.partition(), kafkaKey, kafkaValue));

      if (kafkaKey.isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
        processControlMessage(controlMessage, consumerRecord.partition(), consumerRecord.offset());
      } else if (null == kafkaValue) {
        throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message. Partition " +
              consumerRecord.partition() + " Offset " + consumerRecord.offset());
      } else {
        sizeOfPersistedData = kafkaKey.getKeyLength() + processVeniceMessage(kafkaKey, kafkaValue, consumerRecord.partition());
      }
    } catch (DuplicateDataException e) {
      logger.info("Skipping a duplicate record in topic: '" + topic + "', offset: " + consumerRecord.offset());
    } catch (PersistenceFailureException ex) {
      if (partitionToOffsetMap.containsKey(consumerRecord.partition())) {
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
    OffsetRecord offsetRecord = partitionToOffsetMap.get(consumerRecord.partition());
    if (offsetRecordTransformer.isPresent()) {
      // This closure will have the side effect of altering the OffsetRecord
      offsetRecord = offsetRecordTransformer.get().transform(offsetRecord);
    } /** else, {@link #validateMessage(int, KafkaKey, KafkaMessageEnvelope)} threw a {@link DuplicateDataException} */

    offsetRecord.setOffset(consumerRecord.offset());
    partitionToOffsetMap.put(consumerRecord.partition(), offsetRecord); // Is this put necessary? We are mutating the record anyway...
    return sizeOfPersistedData;
  }

  private OffsetRecordTransformer validateMessage(int partition, KafkaKey key, KafkaMessageEnvelope message) {
    final GUID producerGUID = message.producerMetadata.producerGUID;
    ProducerTracker producerTracker = producerTrackerMap.get(producerGUID);
    if (producerTracker == null) {
      producerTracker = new ProducerTracker(producerGUID);
      producerTrackerMap.put(producerGUID, producerTracker);
    }
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
}
