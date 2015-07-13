package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.KafkaConsumerException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.consumer.offsets.OffsetManager;
import com.linkedin.venice.kafka.consumer.offsets.OffsetRecord;
import com.linkedin.venice.message.ControlFlagKafkaKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.KafkaValue;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.Avro.AzkabanJobAvroAckRecordGenerator;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KafkaValueSerializer;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.ByteUtils;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.common.LeaderNotAvailableException;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.nio.ByteBuffer;
import java.util.*;


/**
 * Runnable class which performs Kafka consumption from the Simple Consumer API.
 */
public class SimpleKafkaConsumerTask implements Runnable {

  private static final Logger logger = Logger.getLogger(SimpleKafkaConsumerTask.class.getName());
  private final int READ_CYCLE_DELAY = 1000;
  //This is a user-supplied integer. It will be passed back in the response by the server,
  // unmodified. It is useful for matching request and response between the client and server.
  private static final int CORELATION_ID = 17;

  // Venice Serialization
  private final KafkaKeySerializer kafkaKeySerializer;
  private final KafkaValueSerializer kafkaValueSerializer;

  //offsetManager
  private final OffsetManager offsetManager;

  //Ack producer
  private final KafkaProducer<byte[], byte[]> ackProducer;
  private final AzkabanJobAvroAckRecordGenerator ackRecordGenerator;

  // Store specific configs
  private final VeniceStoreConfig storeConfig;

  // storage destination for consumption
  private final AbstractStorageEngine storageEngine;

  // Replica kafka brokers
  private List<String> replicaBrokers;

  private final String topic;
  private final int partition;
  private final String clientName; // a unique client name for Kafka debugging

  private long jobId;
  private long totalMessagesProcessed;

  private final String consumerTaskId;

  private final AtomicBoolean canConsume;

  public SimpleKafkaConsumerTask(VeniceStoreConfig storeConfig, AbstractStorageEngine storageEngine, int partition,
      OffsetManager offsetManager, KafkaProducer ackPartitionConsumptionProducer,
      AzkabanJobAvroAckRecordGenerator ackRecordGenerator) {
    this.storeConfig = storeConfig;
    this.storageEngine = storageEngine;

    this.kafkaKeySerializer = new KafkaKeySerializer();
    this.kafkaValueSerializer = new KafkaValueSerializer();
    this.replicaBrokers = new ArrayList<String>();
    this.topic = storeConfig.getStoreName();
    this.partition = partition;
    this.offsetManager = offsetManager;
    this.clientName = "Client_" + topic + "_" + partition;
    this.ackProducer = ackPartitionConsumptionProducer;
    this.ackRecordGenerator = ackRecordGenerator;
    this.canConsume = new AtomicBoolean(true);
    consumerTaskId =
        "SimpleConsumerTask for [ Topic: " + topic + ", Partition: " + partition + " in node: " + storeConfig.getNodeId() + " ]";
  }

  /**
   * Parallelized method which performs Kafka consumption and relays messages to the Storage engine
   */
  public void run() {
    logger.info("Running " + consumerTaskId);

    SimpleConsumer consumer = null;

    try {
      // find the meta data
      PartitionMetadata metadata =
          findLeader(storeConfig.getKafkaBrokers(), storeConfig.getKafkaBrokerPort(), topic, partition);
      validateConsumerMetadata(metadata);
      String leadBroker = metadata.leader().get().host();

      consumer = new SimpleConsumer(leadBroker, storeConfig.getKafkaBrokerPort(), storeConfig.getSocketTimeoutMs(),
          storeConfig.getFetchBufferSize(), clientName);

      long readOffset = getLastOffset(consumer);
      while (canConsume.get() == true) {
        long numReads = 0;
        Iterator<MessageAndOffset> messageAndOffsetIterator = null;
        try {
          messageAndOffsetIterator = getMessageAndOffsetIterator(consumer, readOffset);
          while (messageAndOffsetIterator.hasNext()) {
            MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
            long currentOffset = messageAndOffset.offset();

            if (currentOffset < readOffset) {
              logger.error(consumerTaskId + " : Found an old offset: " + currentOffset + " Expecting: " + readOffset);
              continue;
            }
            processMessage(messageAndOffset.message(), currentOffset);
            readOffset = messageAndOffset.nextOffset();
            numReads++;
          }
        } catch (LeaderNotAvailableException e) {
          logger.error(consumerTaskId + " : Kafka error found! Skipping....", e);

          consumer.close();
          consumer = null;

          try {
            leadBroker = findNewLeader(leadBroker, topic, partition, storeConfig.getKafkaBrokerPort());
          } catch (Exception ex) {
            logger.error(consumerTaskId + " : Error while finding new leader: " + ex);
            throw new VeniceException(ex);
          }
        } catch (VeniceMessageException ex) {
          logger.error(consumerTaskId + " : Received an illegal Venice message! Skipping the message.", ex);
          if (logger.isDebugEnabled()) {
            logger.debug(consumerTaskId + " : Skipping message at Offset " + readOffset);
          }
          // forcefully skip over this bad offset
          readOffset++;
        } catch (UnsupportedOperationException ex) {
          logger.error(consumerTaskId + " : Received an invalid operation type! Skipping the message.", ex);
          if (logger.isDebugEnabled()) {
            logger.debug(consumerTaskId + " : Skipping message at Offset: " + readOffset);
          }
          // forcefully skip over this bad offset
          readOffset++;
        }

        if (0 == numReads) {

          try {
            Thread.sleep(READ_CYCLE_DELAY);
          } catch (InterruptedException ie) {
          }
        }
      }
    } catch (VeniceException e) {
      logger.error(consumerTaskId + " : Killing consumer task ", e);
    } catch (Exception e) {
      logger.error(consumerTaskId + " Unknown Exception caught: ", e);
    } finally {
      if (consumer != null) {
        logger.error(consumerTaskId + " : Closing consumer..");
        consumer.close();
      }
    }
  }

  /**
   * Validates that a given PartitionMetadata is valid: it is non-null and a leader is defined.
   *
   * @param metadata the metadata to validate
   */
  private void validateConsumerMetadata(PartitionMetadata metadata) {
    if (null == metadata) {
      throw new VeniceException(consumerTaskId + " : Cannot find metadata for topic and partition");
    }

    if (null == metadata.leader()) {
      throw new VeniceException(consumerTaskId + " : Cannot find leader for the topic and partition");
    }
  }

  /**
   * @param consumer - A SimpleConsumer object for Kafka consumption
   * @return 1. valid offset if the offset manager is enabled and available. (OR)
   * 2. earliest offset from kafka log if: 1) the offset manager is not enabled or 2) has issues or 3) if the
   * consumer is new.
   */
  private long getLastOffset(SimpleConsumer consumer) {
  /*
    * The LatestTime() gives the last offset from Kafka log, instead of the last consumed offset. So in case where a
    * consumer goes down and is instantiated, it starts to consume new messages and there is a possibility for missing
    * data that were produced in between.
    *
    * So we need to manage the offsets explicitly.On a best effort basis we try to get the last consumed offset. In the
    * worst case we should start consume from earliest data in Kafka log.
    *
    * */
    long readOffset = -1;
    if (storeConfig.isEnableKafkaConsumersOffsetManagement()) {
      try {
        /**
         * Access the the offset manager and fetch the last consumed offset that was persisted by this consumer thread
         * before shutdown or crash
         */
        OffsetRecord offsetRecord = offsetManager.getLastOffset(topic, partition);
        if(offsetRecord == null) {
          logger.info("Offset record null for " + topic + "_" + partition);
        }
        readOffset = (offsetRecord == null ? -1 : offsetRecord.getOffset());
        logger.info(consumerTaskId + " : Last known read offset: " + readOffset);
      } catch (VeniceException e) {
        logger.error(consumerTaskId + " : Some error fetching the last offset from offset manager.");
      }
    }
    if (readOffset == -1) {
      /**
       * Control reaches here in these cases:
       * 1. if offsetManagement is disabled
       * 2. some exception in trying to get the last offset. Reprocess all data from the beginning in the log.
       */
      logger.info(consumerTaskId
          + " : Either offset Manager is not enabled or is not available!  Starting to consume from start of the log.");
      readOffset = getStartingOffsetFromKafkaLog(consumer, kafka.api.OffsetRequest.EarliestTime());
    }
    return readOffset;
  }

  /**
   * Finds the latest offset after a given time
   *
   * @param consumer  - A SimpleConsumer object for Kafka consumption
   * @param whichTime - Time at which to being reading offsets
   * @return long - last offset after the given time
   */
  private long getStartingOffsetFromKafkaLog(SimpleConsumer consumer, long whichTime) {

    TopicAndPartition tp = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    int numValidOffsetsToReturn = 1; // this will return as many starting offsets for the segments before the whichTime.
    // Say for example if the size is 3 then, the starting offset of last 3 segments are returned
    requestInfoMap.put(tp, new PartitionOffsetRequestInfo(whichTime, numValidOffsetsToReturn));

    // TODO: Investigate if the conversion can be done in a cleaner way
    kafka.javaapi.OffsetRequest req =
        new kafka.javaapi.OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientName);

    kafka.api.OffsetResponse scalaResponse = consumer.getOffsetsBefore(req.underlying());
    kafka.javaapi.OffsetResponse javaResponse = new kafka.javaapi.OffsetResponse(scalaResponse);

    if (javaResponse.hasError()) {
      throw new KafkaException(consumerTaskId + " : Error fetching data offset ");
    }

    long[] offsets = javaResponse.offsets(topic, partition);

    logger.info(consumerTaskId + ", last known offset: " + offsets[0]);

    return offsets[0];
  }

  /**
   * Returns an iterator object for the current position in the Kafka log.Handles Kafka request/response semantics
   *
   * @param consumer   A SimpleConsumer object tied to the Kafka instance
   * @param readOffset The offset in the Kafka log to begin reading from
   * @return
   */
  private Iterator<MessageAndOffset> getMessageAndOffsetIterator(SimpleConsumer consumer, long readOffset) {

    Iterator<MessageAndOffset> messageAndOffsetIterator;

    FetchRequest req = new FetchRequestBuilder().clientId(clientName)
        .addFetch(topic, partition, readOffset, storeConfig.getFetchBufferSize()).build();

    try {
      FetchResponse fetchResponse = consumer.fetch(req);
      if (fetchResponse.hasError()) {
        throw new Exception(
            consumerTaskId + " : FetchResponse error code: " + fetchResponse.errorCode(topic, partition));
      }
      messageAndOffsetIterator = fetchResponse.messageSet(topic, partition).iterator();
    } catch (Exception e) {
      logger.error(
          consumerTaskId + " : Consumer could not fetch message and offset iterator for the topic and partition");
      throw new LeaderNotAvailableException(e.getMessage());
    }
    return messageAndOffsetIterator;
  }

  /**
   * Process the message consumed from Kafka by de-serializing it and persisting it with the storage engine.
   *
   * @param message       Message consumed from Kafka
   * @param currentOffset Current offset being processed
   */
  private void processMessage(Message message, long currentOffset) {

    // Get the Venice Key
    ByteBuffer key = message.key();
    byte[] keyBytes = new byte[key.limit()];
    key.get(keyBytes);

    // Read Payload
    ByteBuffer payload = message.payload();
    byte[] payloadBytes = new byte[payload.limit()];
    payload.get(payloadBytes);

    // De-serialize payload into Venice Message format
    KafkaKey kafkaKey = kafkaKeySerializer.deserialize(topic, keyBytes);
    KafkaValue kafkaValue = kafkaValueSerializer.deserialize(topic, payloadBytes);

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
            .getKafkaProducerRecord(jobId, topic, partition, this.storeConfig.getNodeId(), totalMessagesProcessed);
        ackProducer.send(kafkaMessage);
      }
      return; // Its fine to return here, since this is just a control message.
    }

    processVeniceMessage(kafkaKey, kafkaValue, currentOffset);
  }

  private void processVeniceMessage(KafkaKey kafkaKey, KafkaValue kafkaValue, long currentOffset) {

    long startTimeNs = -1;

    byte[] keyBytes = kafkaKey.getKey();

    switch (kafkaValue.getOperationType()) {
      case PUT:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }
        try {
          storageEngine.put(partition, keyBytes, kafkaValue.getValue());
          if (logger.isTraceEnabled()) {
            logger.trace(
                consumerTaskId + " : Completed PUT to Store: " + topic + " for key: " + ByteUtils.toHexString(keyBytes)
                    + ", value: " + ByteUtils.toHexString(kafkaValue.getValue()) + " in " + (System.nanoTime()
                    - startTimeNs) + " ns at " + System.currentTimeMillis());
          }
          if (offsetManager != null) {
            this.offsetManager
                .recordOffset(storageEngine.getName(), partition, currentOffset, System.currentTimeMillis());
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
          if (offsetManager != null) {
            offsetManager.recordOffset(storageEngine.getName(), partition, currentOffset, System.currentTimeMillis());
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
   * This method taken from Kafka 0.8 SimpleConsumer Example
   * Used when the lead Kafka partition dies, and the new leader needs to be elected
   */
  private String findNewLeader(String oldLeader, String topic, int partition, int port)
      throws KafkaConsumerException {

    for (int i = 0; i < storeConfig.getNumMetadataRefreshRetries(); i++) {
      logger.info(consumerTaskId + " : Retry: " + i + " to get the new leader ...");
      boolean goToSleep;
      PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);

      if (metadata == null || metadata.leader() == null || (oldLeader.equalsIgnoreCase(metadata.leader().get().host())
          && i == 0)) {
        /**
         * Introduce thread delay - for reasons above
         *
         * For third condition - first time through if the leader hasn't changed give ZooKeeper a second to recover
         * second time, assume the broker did recover before failover, or it was a non-Broker issue
         */
        try {
          int sleepTime = storeConfig.getMetadataRefreshBackoffMs();
          logger.info(consumerTaskId + " : Will retry after " + sleepTime + " ms");
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          // ignore and continue with the loop
        }
      } else {
        return metadata.leader().get().host();
      }
    }
    String errorMsg = consumerTaskId + " : Unable to find new leader after Broker failure. Exiting";
    logger.error(errorMsg);
    throw new KafkaConsumerException(errorMsg);
  }

  /**
   * Finds the leader for a given Kafka topic and partition
   *
   * @param seedBrokers - List of all Kafka Brokers
   * @param port        - Port to connect to
   * @param topic       - String name of the topic to search for
   * @param partition   - Partition Number to search for
   * @return A PartitionMetadata Object for the partition found
   */
  private PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {

    PartitionMetadata returnMetaData = null;

    loop:
    /* Iterate through all the Brokers, Topics and their Partitions */
    for (String host : seedBrokers) {

      SimpleConsumer consumer = null;

      try {

        consumer = new SimpleConsumer(host, port, storeConfig.getSocketTimeoutMs(), storeConfig.getFetchBufferSize(),
            "leaderLookup");

        Seq<String> topics = JavaConversions.asScalaBuffer(Collections.singletonList(topic));

        TopicMetadataRequest request = new TopicMetadataRequest(topics, CORELATION_ID);
        TopicMetadataResponse resp = consumer.send(request);

        Seq<TopicMetadata> metaData = resp.topicsMetadata();
        Iterator<TopicMetadata> metadataIterator = metaData.iterator();

        while (metadataIterator.hasNext()) {
          TopicMetadata item = metadataIterator.next();

          Seq<PartitionMetadata> partitionsMetaData = item.partitionsMetadata();
          Iterator<PartitionMetadata> innerIterator = partitionsMetaData.iterator();

          while (innerIterator.hasNext()) {
            PartitionMetadata partitionMetadata = innerIterator.next();
            if (partitionMetadata.partitionId() == partition) {
              returnMetaData = partitionMetadata;
              break loop;
            }
          } /* End of Partition Loop */
        } /* End of Topic Loop */
      } catch (Exception e) {
        logger.error(consumerTaskId + " : Error communicating with " + host + " to find  given topic and  partition: ",
            e);
      } finally {

        // safely close consumer
        if (consumer != null) {
          consumer.close();
        }
      }
    } /* End of Broker Loop */

    if (returnMetaData != null) {
      replicaBrokers.clear();

      Seq<Broker> replicasSequence = returnMetaData.replicas();
      Iterator<Broker> replicaIterator = replicasSequence.iterator();

      while (replicaIterator.hasNext()) {
        Broker replica = replicaIterator.next();
        replicaBrokers.add(replica.host());
      }
    }

    return returnMetaData;
  }

  /**
   * Stops the Consumer task.
   */
  public void stop() {
    this.canConsume.set(false);
  }
}