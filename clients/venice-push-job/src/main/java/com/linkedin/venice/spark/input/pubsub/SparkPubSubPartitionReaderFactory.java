package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;

import com.linkedin.venice.chunking.ChunkKeyValueTransformer;
import com.linkedin.venice.chunking.ChunkKeyValueTransformerImpl;
import com.linkedin.venice.hadoop.utils.VPJSSLUtils;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import org.apache.avro.Schema;
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
  private final boolean isChunkingEnabled;

  public SparkPubSubPartitionReaderFactory(final VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;

    this.isChunkingEnabled = jobConfig.getBoolean(VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED);
    LOGGER.info("Chunking is {} for Spark PubSub input", isChunkingEnabled ? "enabled" : "disabled");
  }

  @Override
  public PartitionReader<InternalRow> createReader(final InputPartition genericInputPartition) {
    if (!(genericInputPartition instanceof SparkPubSubInputPartition)) {
      throw new IllegalArgumentException(
          "SparkPubSubPartitionReaderFactory can only create readers for SparkPubSubInputPartitionReader");
    }

    // Setup SSL on the executor side
    VeniceProperties configWithSsl = VPJSSLUtils.setupSSLForExecutor(jobConfig);

    final SparkPubSubInputPartition inputPartition = (SparkPubSubInputPartition) genericInputPartition;
    final PubSubPartitionSplit partitionSplit = inputPartition.getPubSubPartitionSplit();
    final PubSubTopicPartition topicPartition = partitionSplit.getPubSubTopicPartition();
    final PubSubTopicRepository topicRepository = partitionSplit.getTopicRepository();
    final String inputRegionBroker = configWithSsl.getString(VENICE_REPUSH_SOURCE_PUBSUB_BROKER);
    final String regionName = configWithSsl.getString(KAFKA_INPUT_FABRIC, inputRegionBroker);
    final String consumerName = String.format("raw_kif_%s_%s", inputRegionBroker, topicPartition);

    // Create consumer adapter with proper context
    final PubSubConsumerAdapterContext consumerContext =
        new PubSubConsumerAdapterContext.Builder().setPubSubBrokerAddress(inputRegionBroker)
            .setVeniceProperties(configWithSsl)
            .setPubSubTopicRepository(topicRepository)
            .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(configWithSsl))
            .setConsumerName(consumerName)
            .build();
    final PubSubConsumerAdapter pubSubConsumer =
        PubSubClientsFactory.createConsumerFactory(configWithSsl).create(consumerContext);

    boolean shouldUseLocallyBuiltIndexAsOffset = configWithSsl.getBoolean(
        PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX,
        DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX);

    // Create key transformer on the executor side. ChunkKeyValueTransformerImpl is not Serializable
    // (it contains a RecordDeserializer), so it must be created here rather than stored as a field.
    ChunkKeyValueTransformer transformer = null;
    if (isChunkingEnabled) {
      String keySchemaString = jobConfig.getString(VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, "");
      if (!keySchemaString.isEmpty()) {
        Schema keySchema = new Schema.Parser().parse(keySchemaString);
        transformer = new ChunkKeyValueTransformerImpl(keySchema);
      }
    }

    SparkPubSubInputPartitionReader reader = new SparkPubSubInputPartitionReader(
        inputPartition,
        pubSubConsumer,
        regionName,
        shouldUseLocallyBuiltIndexAsOffset,
        isChunkingEnabled,
        transformer);
    LOGGER.info(
        "Created SparkPubSubInputPartitionReader for topic-partition: {} with consumer: {} to read from region: {} (chunking={})",
        topicPartition,
        consumerName,
        regionName,
        isChunkingEnabled);
    return reader;
  }

  // Make it explicit that this reader does not support columnar reads.
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
