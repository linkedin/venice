package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.consumer.message.ControlMessage;
import com.linkedin.venice.kafka.consumer.message.ControlOperationType;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ByteUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.log4j.Logger;

/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public class StoreConsumptionTask implements Runnable {

  private static final Logger logger = Logger.getLogger(StoreConsumptionTask.class.getName());

  private static final String CONSUMER_TASK_ID_FORMAT = "KafkaPerStoreConsumptionTask for "
      + "[ Node: %d, Topic: %s ]";

  private static final int READ_CYCLE_DELAY_MS = 1000;

  private static final long RETRY_SEEK_TO_BEGINNING_WAIT_TIME_MS = 1000;
  private static final int RETRY_SEEK_TO_BEGINNING_COUNT = 10;

  //Ack producer
  private final VeniceNotifier notifier;

  // storage destination for consumption
  private final StoreRepository storeRepository;

  private final String topic;
  private final int nodeId;

  private long jobId;
  private long totalMessagesProcessed;

  private final String consumerTaskId;

  private final VeniceConsumer consumer;

  private final AtomicBoolean isRunning;

  private final Queue <ControlMessage> kafkaActionMessages;

  // The source of truth for the currently subscribed partitions. The list maintained by the kafka consumer is not
  // always up to date because of the asynchronous nature of subscriptions of partitions in Kafka Consumer.
  private final Set<Integer> subscribedPartitions;

  public StoreConsumptionTask(Properties kafkaConsumerProperties, StoreRepository storeRepository,
          VeniceNotifier notifier, int nodeId,
          String topic) {

    this.consumer = new ApacheKafkaConsumer(kafkaConsumerProperties);
    this.storeRepository = storeRepository;

    this.topic = topic;
    this.nodeId = nodeId;

    kafkaActionMessages = new ConcurrentLinkedQueue<>();

    subscribedPartitions = Collections.synchronizedSet(new HashSet<>());

    this.notifier = notifier;
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, nodeId, topic);

    isRunning = new AtomicBoolean(true);
  }

  /**
   * Adds an asynchronous partition subscription request for the task.
   */
  public void subscribePartition(String topic, int partition) {
    subscribedPartitions.add(partition);
    kafkaActionMessages.add(new ControlMessage(ControlOperationType.SUBSCRIBE, topic, partition));
  }

  /**
   * Adds an asynchronous partition unsubscription request for the task.
   */
  public void unsubscribePartition(String topic, int partition) {
    if(subscribedPartitions.contains(partition)) {
      subscribedPartitions.remove(partition);
    }
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
        if(subscribedPartitions.size() > 0) {
          ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
          processTopicConsumerRecords(records);
        }
        processKafkaActionMessages();
      }
    } catch (Exception e) {
      // TODO: Figure out how to notify Helix of replica's failure.
      logger.error(consumerTaskId + " failed with Exception: ", e);
    } finally {
      stop();
    }
  }

  /**
   * Consumes the kafka actions messages in the queue.
   */
  private void processKafkaActionMessages() {
    while(!kafkaActionMessages.isEmpty()) {
      // Do not want to remove a message from the queue unless it has been processed.
      ControlMessage message = kafkaActionMessages.peek();
      boolean removeMessage = false;
      if(message != null) {
        /**
         * TODO: Get rid of this workaround. Once, the Kafka Clients have been fixed.
         * Retries processing messages. Because of the following Kafka limitations:
         *  1. a partition is only subscribed after a poll call is made. (Ticket: KAFKA-2387)
         *  2. You cannot call Subscribe/Unsubscribe multiple times without polling in between. (Ticket: KAFKA-2413)
         */
        try {
          message.incrementAttempt();
          processKafkaActionMessage(message);
          removeMessage = true;
        } catch (Exception ex) {
          if(message.getAttemptsCount() >= RETRY_SEEK_TO_BEGINNING_COUNT) {
            logger.info("Ignoring message:  " + message.toString(), ex);
            removeMessage = true;
          } else {
            logger.info(ex);
            // The message processing should be reattempted after polling
            // Fine to return here as this message should not be removed from the queue.
            removeMessage = false;
          }
        }
        if(removeMessage) {
          kafkaActionMessages.remove(message);
        }
      }
    }
  }

  private void processKafkaActionMessage(ControlMessage message) {
    ControlOperationType operation = message.getOperation();
    String topic = message.getTopic();
    int partition = message.getPartition();
    switch (operation) {
      case SUBSCRIBE:
        consumer.subscribe(topic, partition);
        logger.info(consumerTaskId + " subscribed to: Topic " + topic + " Partition Id " + partition);
        break;
      case UNSUBSCRIBE:
        consumer.unSubscribe(topic, partition);
        logger.info(consumerTaskId + " UnSubscribed to: Topic " + topic + " Partition Id " + partition);
        break;
      case RESET_OFFSET:
        long newOffSet = consumer.resetOffset(topic, partition);
        logger.info(consumerTaskId + " Reset OffSet : Topic " + topic + " Partition Id " + partition + " New Offset "
                + newOffSet);
        break;
      default:
        throw new UnsupportedOperationException(operation.name() + "not implemented.");
    }
  }

  private void processTopicConsumerRecords(ConsumerRecords records) {
    if(records == null) {
      logger.info(consumerTaskId + " received null ConsumerRecords");
    }
    if(records != null) {
      Iterator recordsIterator = records.iterator();
      while(recordsIterator.hasNext()) {
        ConsumerRecord <KafkaKey, KafkaValue> record = (ConsumerRecord<KafkaKey, KafkaValue>) recordsIterator.next();
        long readOffset = getLastOffset(record.topic(), record.partition());
        if(record.offset() < readOffset) {
          logger.error(consumerTaskId + " : Found an old offset: " + record.offset() + " Expecting: " + readOffset);
          continue;
        }
        try {
          processConsumerRecord(record);
        } catch (VeniceMessageException|UnsupportedOperationException ex) {
          logger.error(consumerTaskId + " : Received an exception ! Skipping the message.", ex);
          if (logger.isDebugEnabled()) {
            logger.debug(consumerTaskId + " : Skipping message at Offset " + readOffset);
          }
          consumer.seek(record.topic(), record.partition(), record.offset() + 1);
        }
      }
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

    if (null == kafkaValue) {
      throw new VeniceMessageException(consumerTaskId + " : Given null Venice Message.");
    }

    if (kafkaKey.getOperationType() == OperationType.BEGIN_OF_PUSH) {
      ControlFlagKafkaKey controlKafkaKey = (ControlFlagKafkaKey) kafkaKey;
      jobId = controlKafkaKey.getJobId();
      totalMessagesProcessed = 0L; //Need to figure out what happens when multiple jobs are run parallely.
      logger.info(consumerTaskId + " : Received Begin of push message from job id: " + jobId + "Setting count to "
          + totalMessagesProcessed);
      return; // Its fine to return here, since this is just a control message.
    }
    if (kafkaKey.getOperationType() == OperationType.END_OF_PUSH) {
      ControlFlagKafkaKey controlKafkaKey = (ControlFlagKafkaKey) kafkaKey;
      long currentJobId = controlKafkaKey.getJobId();
      logger.info(consumerTaskId + " : Receive End of Pushes message. Consumed #records: " + totalMessagesProcessed
              + ", from job id: " + currentJobId + " Remembered Job Id "  + jobId);

      if (jobId == currentJobId) {  // check if the BOP job id matched EOP job id.
        // TODO need to handle the case when multiple jobs are run in parallel.
        if(notifier != null) {
          notifier.completed(jobId, topic, record.partition(), totalMessagesProcessed);
        }
      }
      return; // Its fine to return here, since this is just a control message.
    }

    try {
      processVeniceMessage(kafkaKey, kafkaValue, record.partition());
    } catch (PersistenceFailureException ex) {
      /*
       * We can ignore this exception for unsubscribed partitions. The unsubscription of partitions in Kafka Consumer
       * is an asynchronous event. Thus, it is possible that the partition has been dropped from the local storage
       * engine but want unsubscribed by the Kafka Consumer. Therefore, leading to consumption of useless messages.
       */
      if(subscribedPartitions.contains(record.partition())) {
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
        try {
          storageEngine.put(partition, keyBytes, kafkaValue.getValue());
          logger.debug(new String(keyBytes) + "-" + new String(kafkaValue.getValue()) + " @ " + partition);
          if (logger.isTraceEnabled()) {
            logger.trace(
                consumerTaskId + " : Completed PUT to Store: " + topic + " for key: " + ByteUtils.toHexString(keyBytes)
                    + ", value: " + ByteUtils.toHexString(kafkaValue.getValue()) + " in " + (System.nanoTime()
                    - startTimeNs) + " ns at " + System.currentTimeMillis());
          }
          totalMessagesProcessed++;
        } catch (VeniceException e) {
          throw e;
        }
        break;

      // deleting values
      case DELETE:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }
        try {
          storageEngine.delete(partition, keyBytes);

          if (logger.isTraceEnabled()) {
            logger.trace(consumerTaskId + " : Completed DELETE to Store: " + topic + " for key: " + ByteUtils
                .toHexString(keyBytes) + " in " + (System.nanoTime() - startTimeNs) + " ns at " + System
                .currentTimeMillis());
          }
          totalMessagesProcessed++;
        } catch (VeniceException e) {
          throw e;
        }
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
   * @param topic Kafka Topic to which the partition belongs.
   * @param partition Kafka Partition for which the offset is required.
   *
   * @return 1. Valid offset for the given topic-partition for the group id to which the consumer is registered. (OR)
   * -1 if: 1) the offset management is not enabled or 2) has issues or if the consumer is new.
   */
  private long getLastOffset(String topic, int partition) {
    long offset = -1;
    try {
      consumer.getLastOffset(topic, partition);
      logger.info(consumerTaskId + " : Last known read offset for " + topic + "-" + partition + ": " + offset);
    } catch (NoOffsetForPartitionException ex) {
      logger.info(consumerTaskId + " : No offset found for " + topic + "-" + partition);
    }
    return offset;
  }

  /**
   * Stops the consumer task.
   */
  public void stop() {
    isRunning.set(false);
    if(consumer != null){
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
