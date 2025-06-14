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

  public VeniceRawPubsubInputPartitionReaderFactory(final VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public PartitionReader<InternalRow> createReader(final InputPartition genericInputPartition) {
    if (!(genericInputPartition instanceof VeniceBasicPubsubInputPartition)) {
      throw new IllegalArgumentException(
          "VeniceRawPubsubInputPartitionReaderFactory can only create readers for VeniceBasicPubsubInputPartition");
    }

    final VeniceBasicPubsubInputPartition inputPartition = (VeniceBasicPubsubInputPartition) genericInputPartition;
    final String topicName = inputPartition.getTopicName();
    final int partitionNumber = inputPartition.getPartitionNumber();
    final String consumerName = String.format("raw_kif_%s_%d", topicName, partitionNumber);

    // Create a single topic repository instance to be used throughout
    final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

    // Get topic reference
    final PubSubTopic pubSubTopic;
    try {
      pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get topic: " + topicName, e);
    }

    // Create consumer adapter with proper context
    final PubSubConsumerAdapterContext consumerContext =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(this.jobConfig)
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(jobConfig))
            .setConsumerName(consumerName)
            .build();

    final PubSubConsumerAdapter pubSubConsumer =
        PubSubClientsFactory.createConsumerFactory(this.jobConfig).create(consumerContext);

    // Find the target partition
    final PubSubTopicPartition targetPubSubTopicPartition = pubSubConsumer.partitionsFor(pubSubTopic)
        .stream()
        .map(PubSubTopicPartitionInfo::getTopicPartition)
        .filter(partition -> partition.getPartitionNumber() == partitionNumber)
        .findFirst()
        .orElseThrow(
            () -> new RuntimeException(
                String.format("Partition not found for topic: %s partition number: %d", topicName, partitionNumber)));

    return new VeniceRawPubsubInputPartitionReader(inputPartition, pubSubConsumer, targetPubSubTopicPartition);
  }

  // Make it explicit that this reader does not support columnar reads.
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
