package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.Avro.AzkabanJobAvroAckRecordGenerator;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Assumes: One to One mapping between a Venice Store and Kafka Topic.
 * A runnable Kafka Consumer consuming messages from all the partition assigned to current node for a Kafka Topic.
 */
public class KafkaPerStorePerNodeConsumerTask implements Runnable {

  private static final Logger logger = Logger.getLogger(KafkaPerStorePerNodeConsumerTask.class.getName());

  private static final String GROUP_ID_FORMAT = "%s_%s_%d";
  private static final String CONSUMER_TASK_ID_FORMAT = "KafkaPerStorePerNodeConsumerTask for "
      + "[ Node: %d, Topic: %s, Partitions: %s ]";

  private static final int READ_CYCLE_DELAY_MS = 1000;

  // Venice Serialization
  private final KafkaKeySerializer kafkaKeySerializer;
  private final KafkaValueSerializer kafkaValueSerializer;

  //Ack producer
  private final KafkaProducer<byte[], byte[]> ackProducer;
  private final AzkabanJobAvroAckRecordGenerator ackRecordGenerator;

  // Store specific configs
  private final VeniceStoreConfig storeConfig;

  // storage destination for consumption
  private final AbstractStorageEngine storageEngine;

  private final String topic;
  private final Set<Integer> partitionIds;

  private long jobId;
  private long totalMessagesProcessed;

  private final String consumerTaskId;

  private KafkaConsumer kafkaConsumer;

  public KafkaPerStorePerNodeConsumerTask(VeniceStoreConfig storeConfig, AbstractStorageEngine storageEngine,
      Set<Integer> partitionIds, KafkaProducer ackPartitionConsumptionProducer,
      AzkabanJobAvroAckRecordGenerator ackRecordGenerator, boolean enableKafkaAutoOffsetManagement) {

    this.storeConfig = storeConfig;
    this.storageEngine = storageEngine;

    this.kafkaKeySerializer = new KafkaKeySerializer();
    this.kafkaValueSerializer = new KafkaValueSerializer();

    this.topic = storeConfig.getStoreName();
    this.partitionIds = Collections.synchronizedSet(partitionIds);

    this.ackProducer = ackPartitionConsumptionProducer;
    this.ackRecordGenerator = ackRecordGenerator;
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, storeConfig.getNodeId(), topic, partitionIds.toString());

    Properties kafkaConsumerProperties = getKafkaConsumerProperties(storeConfig, enableKafkaAutoOffsetManagement);
    this.kafkaConsumer = new KafkaConsumer(kafkaConsumerProperties, null, kafkaKeySerializer, kafkaValueSerializer);
    subscribeToPartitions();
  }

  @Override
  /**
   * Parallelized method which performs Kafka consumption and relays messages to the Storage engine
   */
  public void run() {
    logger.info("Running " + consumerTaskId);
    try {
      while (true) {
        ConsumerRecords records = kafkaConsumer.poll(READ_CYCLE_DELAY_MS);
        processTopicConsumerRecords(records);
      }
    } catch (VeniceException e) {
      logger.error(consumerTaskId + " : Killing consumer task ", e);
    } catch (Exception e) {
      logger.error(consumerTaskId + " Unknown Exception caught: ", e);
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
            .getKafkaProducerRecord(jobId, topic, record.partition(), this.storeConfig.getNodeId(), totalMessagesProcessed);
        ackProducer.send(kafkaMessage);
      }
      return; // Its fine to return here, since this is just a control message.
    }

    processVeniceMessage(kafkaKey, kafkaValue, record.partition());
  }

  private void processVeniceMessage(KafkaKey kafkaKey, KafkaValue kafkaValue, int partition) {

    long startTimeNs = -1;

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

  private void subscribeToPartitions() {
    TopicPartition [] partitionsArray = new TopicPartition[partitionIds.size()];
    convertPartitionIdsToTopicPartitions(partitionIds).toArray(partitionsArray);
    kafkaConsumer.subscribe(partitionsArray);
    logger.info(consumerTaskId + " subscribed to KafkaPartitions: " + Arrays.toString(partitionsArray));
  }

  private Set<TopicPartition> convertPartitionIdsToTopicPartitions (Set<Integer> partitionIds) {
    Set<TopicPartition> partitions =
        partitionIds.stream().map(partitionId -> new TopicPartition(topic, partitionId)).collect(Collectors.toSet());
    return partitions;
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
      logger.info(consumerTaskId + " : Last known read offset: " + offset);
    } catch (NoOffsetForPartitionException ex) {
      logger.info(consumerTaskId + " : No offset found for " + topic + "-" + partition);
    }
    return offset;
  }

  /**
   * @return Group Id for kafka consumer.
   */
  private String getGroupId() {
    return String.format(GROUP_ID_FORMAT, topic, Utils.getHostName(), storeConfig.getNodeId());
  }

  /**
   * Unsubscribe Kafka Consumption for the specified topic-partition.
   * @param topic Topic for which the partition that needs to be unsubscribed.
   * @param partitionId Partition Id that needs to be unsubscribed.
   * @throws VeniceException If the topic or the topic-partition was already not subscribed.
   */
  public void unsubscribePartition(String topic, int partitionId) throws VeniceException {
    if(!topic.equals(this.topic)) {
      throw new VeniceException(consumerTaskId + " is not responsible for KafkaTopic: " + topic);
    }
    if(!partitionIds.contains(partitionId)) {
      throw new VeniceException(consumerTaskId + " is not subscribed for KafkaPartition: " + partitionId);
    }
    this.partitionIds.remove(partitionId);
    kafkaConsumer.unsubscribe(new TopicPartition(topic, partitionId));
    logger.info(consumerTaskId + ": Kafka Partition " + topic + "-" + partitionId + " unsubscribed.");
  }

  /**
   * Subscribe Kafka Consumption for the specified topic-partition.
   * @param topic Topic for which the partition that needs to be subscribed.
   * @param partitionId Partition Id that needs to be subscribed.
   * @throws VeniceException If the topic or the topic-partition was already subscribed.
   */
  public void subscribePartition(String topic, int partitionId) throws VeniceException {
    if(!topic.equals(this.topic)) {
      throw new VeniceException(consumerTaskId + " is not responsible for KafkaTopic: " + topic);
    }
    if(partitionIds.contains(partitionId)) {
      throw new VeniceException(consumerTaskId + " is already subscribed for KafkaPartition: " + partitionId);
    }
    this.partitionIds.add(partitionId);
    TopicPartition topicPartition = new TopicPartition(topic, partitionId);
    kafkaConsumer.subscribe(topicPartition);
    // Make sure that the consumption starts from beginning.
    kafkaConsumer.seekToBeginning(topicPartition);
    // Commit the beginning offset to prevent the use of old committed offset.
    kafkaConsumer.commit(CommitType.SYNC);
    logger.info(consumerTaskId + ": Kafka Partition " + topic + "-" + partitionId + " subscribed.");
  }

  /**
   * @return set of partition Ids subscribed by the consumer.
   */
  public Set<Integer> getSubscribedPartitions() {
    Set<Integer> subscribedPartitionIds = new HashSet<>();
    Set<TopicPartition> subscribedTopicPartitions = kafkaConsumer.subscriptions();
    subscribedPartitionIds
        .addAll(subscribedTopicPartitions.stream().map(TopicPartition::partition).collect(Collectors.toList()));
    return subscribedPartitionIds;
  }

  private Properties getKafkaConsumerProperties(VeniceStoreConfig storeConfig, boolean enableKafkaAutoOffsetManagement) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, storeConfig.getKafkaBootstrapServers());
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        String.valueOf(enableKafkaAutoOffsetManagement));
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        String.valueOf(storeConfig.getKafkaAutoCommitIntervalMs()));
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
    return kafkaConsumerProperties;
  }
}
