package com.linkedin.venice.spark.input.table.rawPubSub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;

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
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.List;
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

      // surround by retries from retryUtils or something ?
      List<PubSubTopicPartitionInfo> listOfPartitions = pubSubConsumer.partitionsFor(pubSubTopic);
      List<VeniceBasicPubsubInputPartition> veniceInputPartitions = new ArrayList<>();
      for (PubSubTopicPartitionInfo partition: listOfPartitions) {
        int partitionNumber = partition.getTopicPartition().getPartitionNumber();
        veniceInputPartitions.add(new VeniceBasicPubsubInputPartition(regionName, topicName, partitionNumber));
      }

      return veniceInputPartitions.toArray(new InputPartition[0]);
    } catch (Exception e) {
      throw new VeniceException("Could not breakdown Pubsub topic into input partitions", e);// handle exception
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {

    return new VeniceRawPubsubInputPartitionReaderFactory(jobConfig);
  }

  @Override
  public StructType readSchema() {
    return null;
  }
}
