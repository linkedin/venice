package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.KafkaConsumerException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.offsets.OffsetManager;
import com.linkedin.venice.kafka.consumer.offsets.OffsetRecord;
import com.linkedin.venice.serialization.VeniceMessageSerializer;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.message.VeniceMessage;

import com.linkedin.venice.store.AbstractStorageEngine;
import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.api.PartitionMetadata;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataResponse;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.VerifiableProperties;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;


/**
 * Runnable class which performs Kafka consumption from the Simple Consumer API.
 */
public class SimpleKafkaConsumerTask implements Runnable {

  private static final Logger logger = Logger.getLogger(SimpleKafkaConsumerTask.class.getName());

  //This is a user-supplied integer. It will be passed back in the response by the server,
  // unmodified. It is useful for matching request and response between the client and server.
  private static final int CORELATION_ID = 17;

  private final String ENCODING = "UTF-8";

  // Venice Serialization
  private VeniceMessage vm;
  private static VeniceMessageSerializer messageSerializer;

  //offsetManager
  private final OffsetManager offsetManager;

  // Store specific configs
  private final VeniceStoreConfig storeConfig;

  // storage destination for consumption
  private final AbstractStorageEngine storageEngine;

  // Replica kafka brokers
  private List<String> replicaBrokers;

  private final String topic;
  private final int partition;

  public SimpleKafkaConsumerTask(VeniceStoreConfig storeConfig, AbstractStorageEngine storageEngine, int partition,
      OffsetManager offsetManager) {
    this.storeConfig = storeConfig;
    this.storageEngine = storageEngine;

    messageSerializer = new VeniceMessageSerializer(new VerifiableProperties());
    this.replicaBrokers = new ArrayList<String>();
    this.topic = storeConfig.getStoreName();
    this.partition = partition;
    this.offsetManager = offsetManager;
  }

  /**
   *  Parallelized method which performs Kafka consumption and relays messages to the Storage engine
   * */
  public void run() {

    // find the meta data
    PartitionMetadata metadata =
        findLeader(storeConfig.getKafkaBrokers(), storeConfig.getKafkaBrokerPort(), topic, partition);

    if (null == metadata) {
      logger.error("Cannot find metadata for Topic: " + topic + " Partition: " + partition);
      return;
    }

    if (null == metadata.leader()) {
      logger.error("Cannot find leader for Topic: " + topic + " Partition: " + partition);
      return;
    }

    String leadBroker = metadata.leader().get().host();
    String clientName = "Client_" + topic + "_" + partition;

    SimpleConsumer consumer =
        new SimpleConsumer(leadBroker, storeConfig.getKafkaBrokerPort(), storeConfig.getSocketTimeoutMs(),
            storeConfig.getFetchBufferSize(), clientName);

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
        readOffset = (offsetRecord == null ? -1: offsetRecord.getOffset());
      } catch (VeniceException e) {
        readOffset = -1;
      }
    }
    if (readOffset == -1) {
      /**
       * Control reaches here in these cases:
       * 1. if offsetManagement is disabled
       * 2. some exception in trying to get the last offset. Reprocess all data from the beginning in the log.
       */
      readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
    }

    boolean execute = true;

    while (execute) {

      FetchRequest req = new FetchRequestBuilder().clientId(clientName)
          .addFetch(topic, partition, readOffset, storeConfig.getFetchBufferSize()).build();

      FetchResponse fetchResponse = consumer.fetch(req);

      if (fetchResponse.hasError()) {

        logger.error("Kafka error found! Skipping....");
        logger.error("Message: " + fetchResponse.errorCode(topic, partition));

        consumer.close();
        consumer = null;

        try {
          leadBroker = findNewLeader(leadBroker, topic, partition, storeConfig.getKafkaBrokerPort());
        } catch (KafkaConsumerException e) {
          logger.error("Error while finding new leader: " + e);
          // TODO keep trying ?
        }

        continue;
      }

      long numReads = 0;

      Iterator<MessageAndOffset> messageAndOffsetIterator = fetchResponse.messageSet(topic, partition).iterator();

      while (messageAndOffsetIterator.hasNext() && execute) {

        MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
        long currentOffset = messageAndOffset.offset();

        if (currentOffset < readOffset) {
          logger.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
          continue;
        }

        readOffset = messageAndOffset.nextOffset();

        try {

          Message msg = messageAndOffset.message();
          String keyString;

          // Get the Venice Key
          ByteBuffer key = msg.key();
          byte[] keyBytes = new byte[key.limit()];
          key.get(keyBytes);
          keyString = new String(keyBytes, ENCODING);

          // Read Payload
          ByteBuffer payload = msg.payload();
          byte[] payloadBytes = new byte[payload.limit()];
          payload.get(payloadBytes);

          // De-serialize payload into Venice Message format
          vm = messageSerializer.fromBytes(payloadBytes);

          processMessage(keyString, vm, currentOffset);

          numReads++;
        } catch (UnsupportedEncodingException e) {
          logger.error("Unsupported encoding: " + ENCODING, e);
          // end the thread
          execute = false;
        } catch (VeniceMessageException e) {
          logger.error("Received an illegal Venice message!", e);
        } catch (UnsupportedOperationException e) {
          logger.error("Received an invalid operation type!", e);
        } catch (Exception e) {
          logger
              .error("Venice Storage Engine for store: " + storageEngine.getName() + " has been corrupted! Exiting...",
                  e);
          // end the thread
          execute = false;
        }
      }

      if (0 == numReads) {

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }

    if (consumer != null) {
      logger.error("Closing consumer..");
      consumer.close();
    }
  }

  /**
   * Given the attached storage engine, interpret the VeniceMessage and perform the required action
   * */
  private void processMessage(String key, VeniceMessage msg, long msgOffset) {

    long startTimeNs = -1;

    // check for invalid inputs
    if (null == msg) {
      throw new VeniceMessageException("Given null Venice Message.");
    }

    if (null == msg.getOperationType()) {
      throw new VeniceMessageException("Venice Message does not have operation type!");
    }

    switch (msg.getOperationType()) {

      // adding new values
      case PUT:
        if (logger.isTraceEnabled()) {
          startTimeNs = System.nanoTime();
        }
        try {
          storageEngine.put(partition, key.getBytes(), msg.getPayload().getBytes());

          if (logger.isTraceEnabled()) {
            logger.trace(
                "Completed PUT to Store: " + topic + " for key: " + key + ", value: " + msg.getPayload() + " in " + (
                    System.nanoTime() - startTimeNs) + " ns at " + System.currentTimeMillis());
          }
          if (offsetManager != null) {
            this.offsetManager.recordOffset(storageEngine.getName(), partition, msgOffset, System.currentTimeMillis());
          }
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
          storageEngine.delete(partition, key.getBytes());

          if (logger.isTraceEnabled()) {
            logger.trace(
                "Completed DELETE to Store: " + topic + " for key: " + key + " in " + (System.nanoTime() - startTimeNs)
                    + " ns at " + System.currentTimeMillis());
          }
          if (offsetManager != null) {
            offsetManager.recordOffset(storageEngine.getName(), partition, msgOffset, System.currentTimeMillis());
          }
        } catch (VeniceException e) {
          throw e;
        }
        break;

      // partial update
      case PARTIAL_PUT:
        throw new UnsupportedOperationException("Partial puts not yet implemented");

        // error
      default:
        throw new VeniceMessageException("Invalid operation type submitted: " + msg.getOperationType());
    }
  }

  /**
   * Finds the latest offset after a given time
   *
   * @param consumer - A SimpleConsumer object for Kafka consumption
   * @param topic - Kafka topic
   * @param partition - Partition number within the topic
   * @param whichTime - Time at which to being reading offsets
   * @param clientName - Name of the client (combination of topic + partition)
   * @return long - last offset after the given time
   *
   * */
  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
      String clientName) {

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
      throw new KafkaException("Error fetching data offset for topic: " + topic + " partition: " + partition);
    }

    long[] offsets = javaResponse.offsets(topic, partition);

    logger.info("Partition " + partition + " last offset at: " + offsets[0]);

    return offsets[0];
  }

  /**
   * This method taken from Kafka 0.8 SimpleConsumer Example
   * Used when the lead Kafka partition dies, and the new leader needs to be elected
   * */
  private String findNewLeader(String oldLeader, String topic, int partition, int port)
      throws KafkaConsumerException {

    for (int i = 0; i < storeConfig.getNumMetadataRefreshRetries(); i++) {
      logger.info("Retry: " + i + " to get the new leader ...");
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
          logger.info("Will retry after " + sleepTime + " ms");
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          // ignore and continue with the loop
        }
      } else {
        return metadata.leader().get().host();
      }
    }
    String errorMsg = "Unable to find new leader after Broker failure. Exiting";
    logger.error(errorMsg);
    throw new KafkaConsumerException(errorMsg);
  }

  /**
   * Finds the leader for a given Kafka topic and partition
   * @param seedBrokers - List of all Kafka Brokers
   * @param port - Port to connect to
   * @param topic - String name of the topic to search for
   * @param partition - Partition Number to search for
   * @return A PartitionMetadata Object for the partition found
   * */
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
        logger
            .error("Error communicating with " + host + " to find topic: " + topic + " and partition:" + partition, e);
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
}