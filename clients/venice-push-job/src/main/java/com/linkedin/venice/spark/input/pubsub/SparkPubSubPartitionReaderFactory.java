package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET;

import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;


public class SparkPubSubPartitionReaderFactory implements PartitionReaderFactory {
  private static final Logger LOGGER = LogManager.getLogger(SparkPubSubPartitionReaderFactory.class);

  private static final long serialVersionUID = 1L;

  private final VeniceProperties jobConfig;

  public SparkPubSubPartitionReaderFactory(final VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public PartitionReader<InternalRow> createReader(final InputPartition genericInputPartition) {
    if (!(genericInputPartition instanceof SparkPubSubInputPartition)) {
      throw new IllegalArgumentException(
          "SparkPubSubPartitionReaderFactory can only create readers for SparkPubSubInputPartitionReader");
    }

    final SparkPubSubInputPartition inputPartition = (SparkPubSubInputPartition) genericInputPartition;
    final PubSubPartitionSplit partitionSplit = inputPartition.getPubSubPartitionSplit();
    final PubSubTopicPartition topicPartition = partitionSplit.getPubSubTopicPartition();
    final PubSubTopicRepository topicRepository = partitionSplit.getTopicRepository();
    final String inputRegionBroker = jobConfig.getString(KAFKA_INPUT_BROKER_URL);
    final String regionName = jobConfig.getString(KAFKA_INPUT_FABRIC, inputRegionBroker);
    final String consumerName = String.format("raw_kif_%s_%s", inputRegionBroker, topicPartition);

    // Create consumer adapter with proper context
    final PubSubConsumerAdapterContext consumerContext =
        new PubSubConsumerAdapterContext.Builder().setPubSubBrokerAddress(inputRegionBroker)
            .setVeniceProperties(jobConfig)
            .setPubSubTopicRepository(topicRepository)
            .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(jobConfig))
            .setConsumerName(consumerName)
            .build();
    final PubSubConsumerAdapter pubSubConsumer =
        PubSubClientsFactory.createConsumerFactory(jobConfig).create(consumerContext);

    boolean shouldUseLocallyBuiltIndexAsOffset = !jobConfig.getBoolean(
        PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET,
        DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET);
    SparkPubSubInputPartitionReader reader = new SparkPubSubInputPartitionReader(
        inputPartition,
        pubSubConsumer,
        regionName,
        shouldUseLocallyBuiltIndexAsOffset);
    LOGGER.info(
        "Created SparkPubSubInputPartitionReader for topic-partition: {} with consumer: {} to read from region: {}",
        topicPartition,
        consumerName,
        regionName);
    return reader;
  }

  // Make it explicit that this reader does not support columnar reads.
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
