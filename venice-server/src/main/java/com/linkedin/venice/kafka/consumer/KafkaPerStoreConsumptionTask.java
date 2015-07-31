package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.Avro.AzkabanJobAvroAckRecordGenerator;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ByteUtils;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public class KafkaPerStoreConsumptionTask implements Runnable {

  private static final Logger logger = Logger.getLogger(KafkaPerStoreConsumptionTask.class.getName());

  private static final String CONSUMER_TASK_ID_FORMAT = "KafkaPerStoreConsumptionTask for "
      + "[ Node: %d, Topic: %s ]";

  private static final int READ_CYCLE_DELAY_MS = 1000;

  //Ack producer
  private final Producer<byte[], byte[]> ackProducer;
  private final AzkabanJobAvroAckRecordGenerator ackRecordGenerator;

  // storage destination for consumption
  private final StoreRepository storeRepository;

  private final String topic;
  private final int nodeId;

  private long jobId;
  private long totalMessagesProcessed;

  private final String consumerTaskId;

  private Consumer kafkaConsumer;

  private final AtomicBoolean canConsume;

  public KafkaPerStoreConsumptionTask(Consumer kafkaConsumer, StoreRepository storeRepository,
      Producer ackPartitionConsumptionProducer, AzkabanJobAvroAckRecordGenerator ackRecordGenerator, int nodeId,
      String topic) {

    this.kafkaConsumer = kafkaConsumer;
    this.storeRepository = storeRepository;

    this.topic = topic;
    this.nodeId = nodeId;

    this.ackProducer = ackPartitionConsumptionProducer;
    this.ackRecordGenerator = ackRecordGenerator;
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, nodeId, topic);

    canConsume = new AtomicBoolean(true);
  }

  @Override
  /**
   * Polls the producer for new messages in an infinite loop and processes the new messages.
   */
  public void run() {
    logger.info("Running " + consumerTaskId);
    try {
      while (canConsume.get() == true) {
        ConsumerRecords records = kafkaConsumer.poll(READ_CYCLE_DELAY_MS);
        processTopicConsumerRecords(records);
      }
    } catch (VeniceException e) {
      logger.error(consumerTaskId + " : Killing consumer task ", e);
    } catch (IllegalStateException e) {
      /*
       * We can ignore the IllegalStateException when consumption has been stopped. There is a race condition where
       * the KafkaConsumerService shuts off the consumer but the KafkaConsumerTask was blocked on the poll call.
       */
      if(canConsume.get()) {
        throw e;
      }
    } finally {
      if (kafkaConsumer != null) {
        logger.error(consumerTaskId + " : Closing consumer..");
        kafkaConsumer.close();
      }
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
        } catch (VeniceMessageException ex) {
          logger.error(consumerTaskId + " : Received an illegal Venice message! Skipping the message.", ex);
          if (logger.isDebugEnabled()) {
            logger.debug(consumerTaskId + " : Skipping message at Offset " + readOffset);
          }
          kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        } catch (UnsupportedOperationException ex) {
          logger.error(consumerTaskId + " : Received an invalid operation type! Skipping the message.", ex);
          if (logger.isDebugEnabled()) {
            logger.debug(consumerTaskId + " : Skipping message at Offset: " + readOffset);
          }
          // forcefully skip over this bad offset
          kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset()+1);
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
      if (jobId == controlKafkaKey.getJobId()) {  // check if the BOP job id matched EOP job id.
        // TODO need to handle the case when multiple jobs are run in parallel.
        logger.info(consumerTaskId + " : Receive End of Pushes message. Consumed #records: " + totalMessagesProcessed
            + ", from job id: " + jobId);
        ProducerRecord<byte[], byte[]> kafkaMessage = ackRecordGenerator
            .getKafkaProducerRecord(jobId, topic, record.partition(), nodeId, totalMessagesProcessed);
        ackProducer.send(kafkaMessage);
      }
      return; // Its fine to return here, since this is just a control message.
    }

    processVeniceMessage(kafkaKey, kafkaValue, record.partition());
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
          logger.info(new String(keyBytes) + "-" + new String(kafkaValue.getValue()));
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
      offset = kafkaConsumer.committed(new TopicPartition(topic, partition));
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
    canConsume.set(false);
  }

}
