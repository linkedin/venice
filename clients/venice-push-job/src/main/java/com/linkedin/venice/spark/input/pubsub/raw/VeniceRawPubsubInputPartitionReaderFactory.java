package com.linkedin.venice.spark.input.pubsub.raw;

import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;


public class VeniceRawPubsubInputPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 1L;

  private final VeniceProperties jobConfig;

  public VeniceRawPubsubInputPartitionReaderFactory(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition genericInputPartition) {

    if (!(genericInputPartition instanceof VeniceBasicPubsubInputPartition)) {
      throw new IllegalArgumentException(
          "VeniceRawPubsubInputPartitionReaderFactory can only create readers for VeniceBasicPubsubInputPartition");
    }

    VeniceBasicPubsubInputPartition inputPartition = (VeniceBasicPubsubInputPartition) genericInputPartition;

    String topicName = inputPartition.getTopicName();
    PubSubTopic pubSubTopic;

    PubSubConsumerAdapter pubSubConsumer = PubSubClientsFactory.createConsumerFactory(this.jobConfig)
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(this.jobConfig)
                .setPubSubTopicRepository(new PubSubTopicRepository())
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
                .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(jobConfig))
                .setConsumerName("raw_kif_" + inputPartition.getTopicName() + "_" + inputPartition.getPartitionNumber())
                .build());

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

    // Get topic reference
    try {
      pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get topic: " + topicName, e);
    }

    // Find the target partition
    PubSubTopicPartition targetPubSubTopicPartition = pubSubConsumer.partitionsFor(pubSubTopic)
        .stream()
        .map(PubSubTopicPartitionInfo::getTopicPartition)
        .filter(partition -> partition.getPartitionNumber() == inputPartition.getPartitionNumber())
        .findFirst()
        .orElseThrow(
            () -> new RuntimeException(
                "Partition not found for topic: " + topicName + " partition number: "
                    + inputPartition.getPartitionNumber()));

    return new VeniceRawPubsubInputPartitionReader(
        inputPartition,
        pubSubConsumer,
        pubSubTopic,
        targetPubSubTopicPartition);
  }

  // Make it explicit that this reader does not support columnar reads.
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
