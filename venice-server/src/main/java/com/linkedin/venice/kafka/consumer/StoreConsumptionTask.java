package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.consumer.message.ControlMessage;
import com.linkedin.venice.kafka.consumer.message.ControlOperationType;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;


/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public class StoreConsumptionTask implements Runnable, Closeable {

  private static final Logger logger = Logger.getLogger(StoreConsumptionTask.class.getName());
  private static final int UNSET_JOB_ID  = -1;

  private static final String CONSUMER_TASK_ID_FORMAT = "KafkaPerStoreConsumptionTask for " + "[ Node: %d, Topic: %s ]";

  public static final int READ_CYCLE_DELAY_MS = 1000;

  private static final int MAX_RETRIES = 10;

  //Ack producer
  private final VeniceNotifier notifier;

  // storage destination for consumption
  private final StoreRepository storeRepository;

  private final String topic;

  private long jobId = UNSET_JOB_ID;
  private long totalMessagesProcessed;

  private final String consumerTaskId;

  private final VeniceConsumer consumer;

  private final AtomicBoolean isRunning;

  private final Queue<ControlMessage> kafkaActionMessages;

  private final OffsetManager offsetManager;

  // The source of truth for the currently subscribed partitions. The list maintained by the kafka consumer is not
  // always up to date because of the asynchronous nature of subscriptions of partitions in Kafka Consumer.
  private final Map<Integer, Long> partitionOffsets;

  public StoreConsumptionTask(Properties kafkaConsumerProperties, StoreRepository storeRepository,
          OffsetManager offsetManager, VeniceNotifier notifier, int nodeId, String topic) {

    this.consumer = new ApacheKafkaConsumer(kafkaConsumerProperties);
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;

    this.topic = topic;

    kafkaActionMessages = new ConcurrentLinkedQueue<>();

    // Should be accessed only from a single thread.
    partitionOffsets = new HashMap<>();

    this.notifier = notifier;
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, nodeId, topic);

    isRunning = new AtomicBoolean(true);
  }

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public void subscribePartition(String topic, int partition) {
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.SUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public void unSubscribePartition(String topic, int partition) {
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.UNSUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous resetting partition consumption offset request for the task.
   */
  public void resetPartitionConsumptionOffset(String topic, int partition) {
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.RESET_OFFSET, topic, partition));
  }


  @Override
  /**
   * Polls the producer for new messages in an infinite loop and processes the new messages.
   */
  public void run() {
    logger.info("Running " + consumerTaskId);

    try {
      while (isRunning.get()) {
        processKafkaActionMessages();

        if (partitionOffsets.size() > 0) {
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
      // TODO: Figure out how to notify Helix of replica's failure.
      logger.error(consumerTaskId + " failed with Exception: ", e);
    } finally {
      close();
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
        logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition);
        OffsetRecord record = offsetManager.getLastOffset(topic, partition);
        partitionOffsets.put(partition, record.getOffset());
        consumer.subscribe(topic, partition, record);
        break;
      case UNSUBSCRIBE:
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
        partitionOffsets.remove(partition);
        consumer.unSubscribe(topic, partition);
        break;
      case RESET_OFFSET:

        consumer.resetOffset(topic, partition);
        logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition );
        break;
      default:
        throw new UnsupportedOperationException(operation.name() + "not implemented.");
    }
  }

  private boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaValue> record) {
    String recordTopic = record.topic();
    if(!topic.equals(recordTopic)) {
      throw new VeniceMessageException(consumerTaskId + "Message retrieved from different topic. Expected " + this.topic + " Actual " + recordTopic);
    }

    int partitionId = record.partition();
    Long lastOffset = partitionOffsets.get(partitionId);
    if(lastOffset == null) {
      logger.info("Skipping message as partition is not actively subscribed to anymore " + " topic " + topic + " Partition Id " + partitionId );
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
      ConsumerRecord<KafkaKey, KafkaValue> record = (ConsumerRecord<KafkaKey, KafkaValue>) recordsIterator.next();
      if(shouldProcessRecord(record)) {
        try {
          processConsumerRecord(record);
          processedPartitions.add(record.partition());
          partitionOffsets.put(record.partition(), record.offset());
        } catch (VeniceMessageException | UnsupportedOperationException ex) {
          logger.error(consumerTaskId + " : Received an exception ! Skipping the message at partition " + record.partition() + " offset " + record.offset(), ex);
        }
      }
    }

    for(Integer partition: processedPartitions) {
      long partitionOffset = partitionOffsets.get(partition);
      OffsetRecord record = new OffsetRecord(partitionOffset);
      offsetManager.recordOffset(this.topic, partition , record);
      notifier.progress(jobId, topic, partition, partitionOffset);
    }
  }

  private void processControlMessage(KafkaKey kafkaKey, int partition) {
    ControlFlagKafkaKey controlKafkaKey = (ControlFlagKafkaKey) kafkaKey;
    // TODO : jobId should not be handled directly here. It is a function of Data Ingestion Validation
    // The jobId will be moved there when Data Ingestion Validation starts.
    long currentJobId = controlKafkaKey.getJobId();
     switch(kafkaKey.getOperationType()) {
       case BEGIN_OF_PUSH :
         if(jobId != UNSET_JOB_ID) {
           logger.warn(
                   consumerTaskId + " : Received new push job message while other job is still in progress. Partition "
                           + partition);
         }
         jobId = currentJobId;
         notifier.started(jobId, topic, partition);
         totalMessagesProcessed = 0L; //Need to figure out what happens when multiple jobs are run parallely.
         logger.info(consumerTaskId + " : Received Begin of push message from job id: " + jobId + "Setting count to "
                 + totalMessagesProcessed);

         break;
       case END_OF_PUSH:
         logger.info(consumerTaskId + " : Receive End of Pushes message. Consumed #records: " + totalMessagesProcessed
                 + ", from job id: " + currentJobId + " Remembered Job Id " + jobId);

         if (jobId == currentJobId) {  // check if the BOP job id matched EOP job id.
           if (notifier != null) {
             notifier.completed(jobId, topic, partition, totalMessagesProcessed);
           } else {
             logger.warn(" Received end of Push for a different Job Id. Expected " + jobId + " Actual " + currentJobId);
           }
         }
         jobId = UNSET_JOB_ID;
         break;
       default:
         throw new VeniceMessageException("Unrecognized Control message type " + kafkaKey.getOperationType());
    }
  }

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param record       ConsumerRecord consumed from Kafka
   */
  private void processConsumerRecord(ConsumerRecord<KafkaKey, KafkaValue> record) {
    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = record.key();
    KafkaValue kafkaValue = record.value();

    if( OperationType.isControlOperation(kafkaKey.getOperationType())) {
      processControlMessage(kafkaKey , record.partition());
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
      if (partitionOffsets.containsKey(record.partition())) {
        throw ex;
      }
    }
  }

  private void processVeniceMessage(KafkaKey kafkaKey, KafkaValue kafkaValue, int partition) {

    long startTimeNs = -1;
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(topic);

    byte[] keyBytes = kafkaKey.getKey();

    switch (kafkaValue.getOperationType()) {
      case PUT:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }
        storageEngine.put(partition, keyBytes, kafkaValue.getValue());
        if (logger.isTraceEnabled()) {
          logger.trace(consumerTaskId + " : Completed PUT to Store: " + topic + " for key: " + ByteUtils
                  .toHexString(keyBytes) + ", value: " + ByteUtils.toHexString(kafkaValue.getValue()) + " in " + (
                  System.nanoTime() - startTimeNs) + " ns at " + System.currentTimeMillis());
        }
        totalMessagesProcessed++;
        break;

      // deleting values
      case DELETE:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }

        storageEngine.delete(partition, keyBytes);

        if (logger.isTraceEnabled()) {
          logger.trace(consumerTaskId + " : Completed DELETE to Store: " + topic + " for key: " + ByteUtils
                  .toHexString(keyBytes) + " in " + (System.nanoTime() - startTimeNs) + " ns at " + System
                  .currentTimeMillis());
        }
        totalMessagesProcessed++;
        break;

      // partial update
      case PARTIAL_WRITE:
        throw new UnsupportedOperationException(consumerTaskId + " : Partial puts not yet implemented");

        // error
      default:
        throw new VeniceMessageException(
                consumerTaskId + " : Invalid/Unrecognized operation type submitted: " + kafkaValue.getOperationType());
    }
  }

  /**
   * Stops the consumer task.
   */
  public void close() {
    boolean isStillRunning = isRunning.getAndSet(false);
    if (isStillRunning && consumer != null) {
      // TODO : Consumer is not thread safe and close can be invoked on the other thread.
      // Need to use wakeup, which is not available in this version yet to make this
      // thread safe.
      consumer.close();
    }
  }

  /**
   * A function to allow the service to get the current status of the task.
   * This would allow the service to create a new task if required.
   */
  public boolean isRunning() {
    return isRunning.get();
  }
}
