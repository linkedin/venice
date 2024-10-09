package com.linkedin.venice.spark.input.pubsub.table;

import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.input.pubsub.PartitionSplitters;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;


public class VenicePubsubInputScan implements Scan, Batch {
  private final Properties jobConfig;

  public VenicePubsubInputScan(Properties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    try {
      String topicName = "test_topic_v_1";
      String regionName = "ei-ltx1";
      int splitCount = 1000;
      VeniceProperties veniceProperties = new VeniceProperties(jobConfig);
      PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
      PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
      PubSubClientsFactory clientsFactory = new PubSubClientsFactory(veniceProperties);
      // PubSubAdminAdapter pubsubAdminClient =
      // clientsFactory.getAdminAdapterFactory().create(veniceProperties, pubSubTopicRepository);
      PubSubMessageDeserializer pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
      PubSubConsumerAdapter pubSubConsumer = clientsFactory.getConsumerAdapterFactory()
          .create(veniceProperties, false, pubSubMessageDeserializer, "Spark_KIF_planner");

      // surround by retries from retryUtils or something
      List<PubSubTopicPartitionInfo> listOfPartitions = pubSubConsumer.partitionsFor(pubSubTopic);

      int numPartitions = listOfPartitions.size(); // number of partitions in the topic

      // need a map of int to long,long to store the start and end offsets for each partition
      Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();

      for (PubSubTopicPartitionInfo partition: listOfPartitions) {
        PubSubTopicPartition pubSubTopicPartition = partition.getTopicPartition();
        int partitionNumber = partition.getTopicPartition().getPartitionNumber();

        // do these with retries from retryUtils or something
        long startOffset = pubSubConsumer.beginningOffset(pubSubTopicPartition, Duration.ofSeconds(60));
        long endOffset = pubSubConsumer.endOffset(pubSubTopicPartition);
        //

        partitionOffsetsMap.put(partitionNumber, Arrays.asList(startOffset, endOffset));
      }

      Map<Integer, List<List<Long>>> splits = PartitionSplitters.segmentCountSplitter(partitionOffsetsMap, splitCount);
      InputPartition[] inputPartitions =
          PartitionSplitters.convertToInputPartitions(regionName, topicName, splits).toArray(new InputPartition[0]);
      return (inputPartitions);
    } catch (Exception e) {
      // handle exception
      // something broke in the process of getting the splits
      return null; // ? how do I tell spart that this is a failure?
    }

  }

  @Override
  public PartitionReaderFactory createReaderFactory() {

    return new VenicePubsubInputPartitionReaderFactory(jobConfig);
  }

  @Override
  public StructType readSchema() {
    return null;
  }
}
