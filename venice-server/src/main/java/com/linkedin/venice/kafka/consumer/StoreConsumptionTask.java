package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
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
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
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

  private static final Logger logger = Logger.getLogger(StoreConsumptionTask.class);

  private static final String CONSUMER_TASK_ID_FORMAT = StoreConsumptionTask.class.getSimpleName() + " for [ Node: %d, Topic: %s ]";

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

  private VeniceConsumer consumer;

  private final AtomicBoolean isRunning;

  private final Queue<ConsumerAction> consumerActionsQueue;

  private final OffsetManager offsetManager;

  private final EventThrottler throttler;

  private long lastProgressReportTime = 0;
  private static final long PROGRESS_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  // The source of truth for the currently subscribed partitions. The list maintained by the kafka consumer is not
  // always up to date because of the asynchronous nature of subscriptions of partitions in Kafka Consumer.
  private final Map<Integer, Long> partitionToOffsetMap;

  private static int MAX_IDLE_COUNTER  = 100;
  private int idleCounter = 0;

  public StoreConsumptionTask(@NotNull VeniceConsumerFactory factory,
                              @NotNull Properties kafkaConsumerProperties,
                              @NotNull StoreRepository storeRepository,
                              @NotNull OffsetManager offsetManager,
                              @NotNull Queue<VeniceNotifier> notifiers,
                              @NotNull EventThrottler throttler,
                              int nodeId,
                              @NotNull String topic,
                              @NotNull ReadOnlySchemaRepository schemaRepo) {
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
    this.consumerActionsQueue = new ConcurrentLinkedQueue<>();

    // Should be accessed only from a single thread.
    this.partitionToOffsetMap = new HashMap<>();
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, nodeId, topic);

    this.isRunning = new AtomicBoolean(true);
  }

  private void validateState() {
    if(!isRunning()) {
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

  private void reportProgress(int partition, long partitionOffset ) {
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
    for(VeniceNotifier notifier : notifiers) {
      try {
        notifier.started(topic, partition);
      } catch(Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }
  }

  private void reportCompleted(int partition) {

    Long lastOffset = partitionToOffsetMap.getOrDefault(partition , -1L);

    logger.info(" Processing completed for topic " + topic + " Partition " + partition +" Last Offset " + lastOffset);
    for(VeniceNotifier notifier : notifiers) {
      try {
        notifier.completed(topic, partition, lastOffset);
      } catch(Exception ex) {
        logger.error("Error reporting status to notifier " + notifier.getClass() , ex);
      }
    }
  }

  private void reportError(Collection<Integer> partitions, String message, Exception consumerEx) {
    for(Integer partitionId: partitions) {
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
      ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
      processTopicConsumerRecords(records);
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
        processControlMessages();
        processMessages();
      }
    } catch (Exception e) {
      // TODO : The Exception is handled inconsistently here.
      // An Error is reported to the controller, so the controller will abort the job.
      // But the Storage Node might eventually recover and the job may complete in success
      // This will confused the controller.
      // FGV: Actually, the Controller doesn't seem to be aware at all that the consumer failed.
      //      It just keeps reporting "Push status: STARTED..." indefinitely to the H2V job. TODO: fix this
      logger.error(consumerTaskId + " failed with Exception.", e);
      reportError(partitionToOffsetMap.keySet() , "Exception caught during poll." , e);
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
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processControlMessages() {
    Iterator<ConsumerAction> iter = consumerActionsQueue.iterator();
    while (iter.hasNext()) {
      // Do not want to remove a message from the queue unless it has been processed.
      ConsumerAction message = iter.next();
      try {
        message.incrementAttempt();
        processControlMessage(message);
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

  private void processControlMessage(ConsumerAction message) {
    ConsumerActionType operation = message.getType();
    String topic = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case SUBSCRIBE:
        OffsetRecord record = offsetManager.getLastOffset(topic, partition);
        partitionToOffsetMap.put(partition, record.getOffset());
        consumer.subscribe(topic, partition, record);
        logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition + " Offset " + record.getOffset());
        break;
      case UNSUBSCRIBE:
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
        partitionToOffsetMap.remove(partition);
        consumer.unSubscribe(topic, partition);
        break;
      case RESET_OFFSET:
        partitionToOffsetMap.put(partition , OffsetRecord.LOWEST_OFFSET);
        offsetManager.clearOffset(topic, partition);
        consumer.resetOffset(topic, partition);
        logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition );
        break;
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
    Long lastOffset = partitionToOffsetMap.get(partitionId);
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

    Iterator recordsIterator = records.iterator();
    if(!recordsIterator.hasNext()) {
      return;
    }

    int totalSize = 0;
    int totalRecords = 0;
    Set<Integer> processedPartitions = new HashSet<>();
    while (recordsIterator.hasNext()) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = (ConsumerRecord<KafkaKey, KafkaMessageEnvelope>) recordsIterator.next();
      totalRecords++;
      if(shouldProcessRecord(record)) {
        try {
          totalSize += processConsumerRecord(record);
          processedPartitions.add(record.partition());
        } catch (VeniceMessageException | UnsupportedOperationException ex) {
          logger.error(consumerTaskId + " : Received an exception ! Skipping the message at partition " + record.partition() + " offset " + record.offset(), ex);
        }
      }
    }

    throttler.maybeThrottle(totalSize);

    for(Integer partition: processedPartitions) {
      if(!partitionToOffsetMap.containsKey(partition)) {
        // Partition is completed or unSubscribed.
        continue;
      }
      long partitionOffset = partitionToOffsetMap.get(partition);
      OffsetRecord record = new OffsetRecord(partitionOffset);
      offsetManager.recordOffset(this.topic, partition, record);
      reportProgress(partition, partitionOffset);
    }
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
         partitionToOffsetMap.remove(partition);
         break;
       default:
         throw new VeniceMessageException("Unrecognized Control message type " + controlMessage.controlMessageType);
    }
  }

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param record       ConsumerRecord consumed from Kafka
   */
  private int processConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = record.key();
    KafkaMessageEnvelope kafkaValue = record.value();

    int keySize = kafkaKey.getLength();
    if(kafkaKey.isControlMessage()) {
      ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
      processControlMessage(controlMessage, record.partition(), record.offset());
      return keySize;
    }

    if (null == kafkaValue) {
      throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message. Partition " +
              record.partition() + " Offset " + record.offset());
    }

    int valueSize = 0;
    try {
      valueSize = processVeniceMessage(kafkaKey, kafkaValue, record.partition());
    } catch (PersistenceFailureException ex) {
      /*
       * We can ignore this exception for unsubscribed partitions. The unsubscription of partitions in Kafka Consumer
       * is an asynchronous event. Thus, it is possible that the partition has been dropped from the local storage
       * engine but want unsubscribed by the Kafka Consumer. Therefore, leading to consumption of useless messages.
       */
      if (partitionToOffsetMap.containsKey(record.partition())) {
        throw ex;
      }
    }
    partitionToOffsetMap.put(record.partition() , record.offset());
    return keySize + valueSize;
  }

  /* TODO : There should be a better place for calculating the header size
    Kafka returns de-serialized value, Avro record does not expose the size
    Classes are auto-created by Avro, so it is not possible to enhance these
    classes as well.
   */
  private static int VALUE_HEADER_SIZE = ByteUtils.SIZE_OF_INT + //  messageType
                                          16 + // Size of GUID
                                          ByteUtils.SIZE_OF_INT + // segment number
                                          ByteUtils.SIZE_OF_INT + // message sequence number
                                          ByteUtils.SIZE_OF_LONG; // message time stamp;


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
        return VALUE_HEADER_SIZE + (valueBytes == null ? 0 : valueBytes.length);

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
        return VALUE_HEADER_SIZE;
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
    while (!hasValueSchema && isRunning()) {
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


  private synchronized  void complete() {
    if (consumerActionsQueue.isEmpty()) {
      close();
    } else {
      logger.info(consumerTaskId + "Control messages not empty, ignoring complete ");
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
}
