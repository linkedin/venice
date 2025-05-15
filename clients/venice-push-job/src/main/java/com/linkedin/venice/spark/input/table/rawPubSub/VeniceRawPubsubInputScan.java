package com.linkedin.venice.spark.input.table.rawPubSub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;


public class VeniceRawPubsubInputScan implements Scan, Batch {
  private final VeniceProperties jobConfig;

  public VeniceRawPubsubInputScan(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    try {
      String topicName = jobConfig.getString(KAFKA_INPUT_TOPIC);
      String regionName = jobConfig.getString(KAFKA_INPUT_FABRIC);
      PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
      PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
      PubSubClientsFactory clientsFactory = new PubSubClientsFactory(jobConfig);
      PubSubConsumerAdapterFactory consumerAdapterFactory = clientsFactory.getConsumerAdapterFactory();

      PubSubConsumerAdapterContext context = new PubSubConsumerAdapterContext.Builder().setVeniceProperties(jobConfig)
          .setPubSubTopicRepository(pubSubTopicRepository)
          .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
          .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(jobConfig))
          .setConsumerName("Spark_raw_pubsub_input_planner")
          .build();
      PubSubConsumerAdapter pubSubConsumer = consumerAdapterFactory.create(context);

      // surround by retries from retryUtils or something
      List<PubSubTopicPartitionInfo> listOfPartitions = pubSubConsumer.partitionsFor(pubSubTopic);

      // int numPartitions = listOfPartitions.size(); // number of partitions in the topic in case.

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
      return PartitionSplitters.convertToInputPartitions(regionName, topicName, splits).toArray(new InputPartition[0]);
    } catch (Exception e) {
      throw new VeniceException("Could not get FileSystem", e);// handle exception
      // something broke in the process of getting the splits
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
