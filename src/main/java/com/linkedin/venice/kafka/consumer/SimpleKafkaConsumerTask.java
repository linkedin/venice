package com.linkedin.venice.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.api.OffsetResponse;
import kafka.api.PartitionMetadata;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.PartitionOffsetsResponse;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataResponse;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.VerifiableProperties;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.message.VeniceMessageSerializer;
import org.apache.log4j.Logger;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import com.linkedin.venice.storage.VeniceStoreManager;
import com.linkedin.venice.client.VeniceClient;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

/**
 * Created by clfung on 9/22/14.
 */
public class SimpleKafkaConsumerTask implements Runnable {

  static final Logger logger = Logger.getLogger(SimpleKafkaConsumerTask.class.getName());

  private final int NUM_RETRIES = 3;
  private final int TIMEOUT = 100000;
  private final int FETCH_SIZE = 100000;
  private final int BUFFER_SIZE = 64 * 1024;
  private final String ENCODING = "UTF-8";

  private List<String> replicaBrokers = null;
  private VeniceMessage vm = null;
  private static VeniceMessageSerializer messageSerializer = null;

  private long maxReads;
  private String topic;
  private int partition;
  private List<String> seedBrokers;
  private int port;
  private int threadNumber;

  public SimpleKafkaConsumerTask(long maxReads, String topic, int partition, List<String> seedBrokers, int port,
                                 int threadNumber) {
    replicaBrokers = new ArrayList<String>();
    messageSerializer = new VeniceMessageSerializer(new VerifiableProperties());

    this.maxReads = maxReads;
    this.topic = topic;
    this.partition = partition;
    this.seedBrokers = seedBrokers;
    this.port = port;

    this.threadNumber = threadNumber;

  }

  public void run() {

    // find the meta data
    PartitionMetadata metadata = findLeader(seedBrokers, port, topic, partition);

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

    SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, TIMEOUT, BUFFER_SIZE, clientName);
    long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

    int numErrors = 0;
    while (maxReads > 0) {

      FetchRequest req = new FetchRequestBuilder()
          .clientId(clientName)
          .addFetch(topic, partition, readOffset, FETCH_SIZE)
          .build();

      FetchResponse fetchResponse = consumer.fetch(req);

      if (fetchResponse.hasError()) {

        logger.error("Kafka error found! Skipping....");
        logger.error("Message: " + fetchResponse.errorCode(topic, partition));

        consumer.close();
        consumer = null;

        try {
          leadBroker = findNewLeader(leadBroker, topic, partition, port);
        } catch (Exception e) {
          logger.error("Error while finding new leader: " + e);
        }

        continue;
      }

      long numReads = 0;

      Iterator<MessageAndOffset> messageAndOffsetIterator = fetchResponse.messageSet(topic, partition).iterator();

      while (messageAndOffsetIterator.hasNext()) {

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

          // Read Key: Note that messages from the console producer do not have a key
          if (msg.hasKey()) {

            ByteBuffer key = msg.key();
            byte[] keyBytes = new byte[key.limit()];
            key.get(keyBytes);
            keyString = new String(keyBytes, ENCODING);

          } else {

            keyString = VeniceClient.TEST_KEY;

          }

          // Read Payload
          ByteBuffer payload = messageAndOffset.message().payload();
          byte[] payloadBytes = new byte[payload.limit()];
          payload.get(payloadBytes);

          // Serialize header from payload into Venice Message format
          vm = messageSerializer.fromBytes(payloadBytes);

          VeniceStoreManager manager = VeniceStoreManager.getInstance();
          manager.storeValue(keyString, vm);

          numReads++;
          maxReads--;

        } catch (UnsupportedEncodingException e) {

          logger.error("Encoding is not supported: " + ENCODING);
          logger.error(e);

        } catch (Exception e) {

          logger.error(e);
          e.printStackTrace();

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

  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
                                   String clientName) {

    TopicAndPartition tp = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap
        = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfoMap.put(tp, new PartitionOffsetRequestInfo(whichTime, 1));
    kafka.javaapi.OffsetRequest req = new kafka.javaapi.OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(req.underlying());

    if (response.hasError()) {
      logger.error("Error fetching data offset");
      return 0;
    }

    // TODO: implement response.offsets() method. Need to ask Kafka office hour
    PartitionOffsetsResponse por = JavaConversions.asJavaMap(response.offsetsGroupedByTopic()).get(topic).get(tp).get();

    return 0;

  }

  /**
   * This method taken from Kafka 0.8 SimpleConsumer Example
   * */
  private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {

    for (int i = 0; i < NUM_RETRIES; i++) {

      boolean goToSleep;
      PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);

      if (metadata == null) {
        goToSleep = true;
      } else if (metadata.leader() == null) {
        goToSleep = true;
      } else if (oldLeader.equalsIgnoreCase(metadata.leader().get().host()) && i == 0) {

        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        goToSleep = true;

      } else {
        return metadata.leader().get().host();
      }

      // introduce thread delay - for reasons above
      if (goToSleep) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }

    logger.error("Unable to find new leader after Broker failure. Exiting");
    throw new Exception("Unable to find new leader after Broker failure. Exiting");
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
    for (String host : seedBrokers) {

      SimpleConsumer consumer = null;

      try {

        consumer = new SimpleConsumer(host, port, TIMEOUT, BUFFER_SIZE, "leaderLookup");

        Seq<String> topics = JavaConversions.asScalaBuffer(Collections.singletonList(topic));
        TopicMetadataRequest request = new TopicMetadataRequest(topics, 17);
        TopicMetadataResponse resp = consumer.send(request);

        /* Iterate through all the Topics and their Partitions  */
        Seq<TopicMetadata> metaData = resp.topicsMetadata();
        Iterator<TopicMetadata> it = metaData.iterator();

        while (it.hasNext()) {
          TopicMetadata item = it.next();

          Seq<PartitionMetadata> partitionMetaData = item.partitionsMetadata();
          Iterator<PartitionMetadata> innerIt = partitionMetaData.iterator();

          while (innerIt.hasNext()) {
            PartitionMetadata pm = innerIt.next();
            if (pm.partitionId() == partition) {
              returnMetaData = pm;
              break loop;
            }

          } /* End of Partition Loop */

        } /* End of Topic Loop */

      } catch (Exception e) {

        logger.error("Error communicating with " + host + " to find " + topic + ", " + partition);
        logger.error(e);

      } finally {

        // safely close consumer
        if (consumer != null) {
          consumer.close();
        }
      }

    } /* End of Broker Loop */

    return returnMetaData;

  }

}
