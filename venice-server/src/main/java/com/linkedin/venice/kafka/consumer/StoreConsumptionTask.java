package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.consumer.message.ControlMessage;
import com.linkedin.venice.kafka.consumer.message.ControlOperationType;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.notifier.VeniceNotifier;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
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

  private static final Logger logger = Logger.getLogger(StoreConsumptionTask.class.getName());

  private static final String CONSUMER_TASK_ID_FORMAT = "KafkaPerStoreConsumptionTask for " + "[ Node: %d, Topic: %s ]";

  // Making it non final to shorten the time in testing.
  public static int READ_CYCLE_DELAY_MS = 1000;

  private static final int MAX_RETRIES = 10;

  //Ack producer
  private final Queue<VeniceNotifier> notifiers;

  // storage destination for consumption
  private final StoreRepository storeRepository;

  private final String topic;

  private final String consumerTaskId;
  private final Properties kafkaProps;
  private final VeniceConsumerFactory factory;

  private VeniceConsumer consumer;

  private final AtomicBoolean isRunning;

  private final Queue<ControlMessage> kafkaActionMessages;

  private final OffsetManager offsetManager;

  private long lastProgressReportTime = 0;
  private static final long PROGRESS_REPORT_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

  // The source of truth for the currently subscribed partitions. The list maintained by the kafka consumer is not
  // always up to date because of the asynchronous nature of subscriptions of partitions in Kafka Consumer.
  private final Map<Integer, Long> partitionToOffsetMap;

  public StoreConsumptionTask(@NotNull VeniceConsumerFactory factory,
                              @NotNull Properties kafkaConsumerProperties,
                              @NotNull StoreRepository storeRepository,
                              @NotNull OffsetManager offsetManager,
                              @NotNull Queue<VeniceNotifier> notifiers,
                              int nodeId,
                              String topic) {
    this.factory = factory;
    this.kafkaProps = kafkaConsumerProperties;
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;

    this.topic = topic;

    kafkaActionMessages = new ConcurrentLinkedQueue<>();

    // Should be accessed only from a single thread.
    partitionToOffsetMap = new HashMap<>();

    this.notifiers = notifiers;
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, nodeId, topic);

    isRunning = new AtomicBoolean(true);
  }

  private void validateState() {
    if(!isRunning()) {
      throw new VeniceException(" Topic " + topic + " is shutting down, no more messages accepted");
    }
  }

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public void subscribePartition(String topic, int partition) {
    validateState();
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.SUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public void unSubscribePartition(String topic, int partition) {
    validateState();
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.UNSUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public void resetPartitionConsumptionOffset(String topic, int partition) {
    validateState();
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.RESET_OFFSET, topic, partition));
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
          logger.error("Error reporting status to notifier " + notifier.getClass() , notifierEx);
        }
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
        processKafkaActionMessages();

        if (partitionToOffsetMap.size() > 0) {
          ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
          processTopicConsumerRecords(records);
        } else {
          // TODO : None of the partitions are consuming, probably the thread should die,
          // but not handled yet, thread should die and inform the ConsumerService to clean up handling the synchronization.
          logger.warn("No Partitions are subscribed to for store " + topic + " Case is not handled yet.. Please fix");
          Utils.sleep(READ_CYCLE_DELAY_MS);
        }
      }
    } catch (Exception e) {
      // TODO : The Exception is handled inconsistently here.
      // An Error is reported to the controller, so the controller will abort the job.
      // But the Storage Node might eventually recover and the job may complete in success
      // This will confused the controller.
      // FGV: Actually, the Controller doesn't seem to be aware at all that the consumer failed.
      //      It just keeps reporting "Push status: STARTED..." indefinitely to the H2V job. TODO: fix this
      logger.error(consumerTaskId + " failed with Exception: ", e);
      reportError(partitionToOffsetMap.keySet() , " Errors occurred during poll " , e );
    } finally {
      internalClose();
    }
  }

  private void internalClose() {
    // Process left over kafka action messages if any.
    try {
      processKafkaActionMessages();
    } catch(Exception e) {
      logger.info("Error clearing Kafka Action Messages for topic " + topic, e);
    }
    if(!kafkaActionMessages.isEmpty()) {
      logger.warn("Some Kafka Action messages are ignored during close for topic " + topic);
    }
    if(consumer != null) {
      consumer.close();
    }
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processKafkaActionMessages() {
    Iterator<ControlMessage> iter = kafkaActionMessages.iterator();
    while (iter.hasNext()) {
      // Do not want to remove a message from the queue unless it has been processed.
      ControlMessage message = iter.next();
      boolean removeMessage = true;
      /**
       * TODO: Get rid of this workaround. Once, the Kafka Clients have been fixed.
       * Retries processing messages. Because of the following Kafka limitations:
       *  1. a partition is only subscribed after a poll call is made. (Ticket: KAFKA-2387)
       *  2. You cannot call Subscribe/Unsubscribe multiple times without polling in between. (Ticket: KAFKA-2413)
       */
      try {
        message.incrementAttempt();
        processKafkaActionMessage(message);
      } catch (Exception ex) {
        if (message.getAttemptsCount() < MAX_RETRIES) {
          logger.info("Error Processing message " + message , ex);
          // The message processing should be reattempted after polling
          // Fine to return here as this message should not be removed from the queue.
          removeMessage = false;
        } else {
          logger.info("Ignoring message:  " + message + " after retries " + message.getAttemptsCount(), ex);
        }
      }
      if (removeMessage) {
        iter.remove();
      }
    }
  }

  private void processKafkaActionMessage(ControlMessage message) {
    ControlOperationType operation = message.getOperation();
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
    if (records == null) {
      logger.info(consumerTaskId + " received null ConsumerRecords");
      return;
    }

    Iterator recordsIterator = records.iterator();
    Set<Integer> processedPartitions = new HashSet<>();
    while (recordsIterator.hasNext()) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = (ConsumerRecord<KafkaKey, KafkaMessageEnvelope>) recordsIterator.next();
      if(shouldProcessRecord(record)) {
        try {
          processConsumerRecord(record);
          processedPartitions.add(record.partition());
          partitionToOffsetMap.put(record.partition(), record.offset());
        } catch (VeniceMessageException | UnsupportedOperationException ex) {
          logger.error(consumerTaskId + " : Received an exception ! Skipping the message at partition " + record.partition() + " offset " + record.offset(), ex);
        }
      }
    }

    for(Integer partition: processedPartitions) {
      long partitionOffset = partitionToOffsetMap.get(partition);
      OffsetRecord record = new OffsetRecord(partitionOffset);
      offsetManager.recordOffset(this.topic, partition, record);
      reportProgress(partition, partitionOffset);
    }
  }

  private void processControlMessage(com.linkedin.venice.kafka.protocol.ControlMessage controlMessage, int partition, long offset) {
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
       default:
         throw new VeniceMessageException("Unrecognized Control message type " + controlMessage.controlMessageType);
    }
  }

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param record       ConsumerRecord consumed from Kafka
   */
  private void processConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = record.key();
    KafkaMessageEnvelope kafkaValue = record.value();

    if(kafkaKey.isControlMessage()) {
      // TODO: Clean up our many kinds of "ControlMessage". There should be at most one thing called as such.
      com.linkedin.venice.kafka.protocol.ControlMessage controlMessage = (com.linkedin.venice.kafka.protocol.ControlMessage) kafkaValue.payloadUnion;
      processControlMessage(controlMessage, record.partition(), record.offset());
      return;
    }

    if (null == kafkaValue) {
      throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message. Partition " +
              record.partition() + " Offset " + record.offset());
    }

    try {
      processVeniceMessage(kafkaKey, kafkaValue, record.partition());
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
  }

  private void processVeniceMessage(KafkaKey kafkaKey, KafkaMessageEnvelope kafkaValue, int partition) {

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
        storageEngine.put(partition, keyBytes, put.putValue.array());
        if (logger.isTraceEnabled()) {
          logger.trace(consumerTaskId + " : Completed PUT to Store: " + topic + " for key: " +
                  ByteUtils.toHexString(keyBytes) + ", value: " + ByteUtils.toHexString(put.putValue.array()) + " in " +
                  (System.nanoTime() - startTimeNs) + " ns at " + System.currentTimeMillis());
        }
        break;

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
        break;

      case CONTROL_MESSAGE:
        logger.info("Received a control message! Marvelous.");
        break;
      default:
        throw new VeniceMessageException(
                consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.messageType);
    }
  }

  /**
   * Stops the consumer task.
   */
  public void close() {
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
  public boolean isRunning() {
    return isRunning.get();
  }
}
