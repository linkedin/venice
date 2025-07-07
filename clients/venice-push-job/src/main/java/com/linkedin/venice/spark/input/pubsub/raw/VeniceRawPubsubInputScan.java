package com.linkedin.venice.spark.input.pubsub.raw;

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
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;


public class VeniceRawPubsubInputScan implements Scan, Batch {
  private static final Logger LOGGER = LogManager.getLogger(VeniceRawPubsubInputScan.class);
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

      // TODO: this needs to be resilient to some level of PubSub Metadata failrues.
      // surround by retries from retryUtils or something ?
      List<PubSubTopicPartitionInfo> listOfPartitions = pubSubConsumer.partitionsFor(pubSubTopic);
      List<VeniceBasicPubsubInputPartition> veniceInputPartitions = new ArrayList<>();

      for (PubSubTopicPartitionInfo partition: listOfPartitions) {
        PubSubTopicPartition topicPartition = partition.getTopicPartition();
        PubSubPosition beginningPosition = pubSubConsumer.beginningPosition(topicPartition, Duration.ofMillis(1000));
        PubSubPosition endPosition = pubSubConsumer.endPosition(topicPartition);
        long beginningOffset = beginningPosition.getNumericOffset();
        long endOffset = endPosition.getNumericOffset();
        long offsetRange = endOffset - beginningOffset;

        if (offsetRange <= 0L) {
          // If the offset range is zero or negative, we skip this partition.
          // This can happen if the topic is empty or if the beginning position is equal to the end position.
          LOGGER.warn(
              "Topic-partition {}-{} is of length zero or negative. Will not process it. Beginning offset: {}, End offset: {}",
              topicName,
              topicPartition.getPartitionNumber(),
              beginningOffset,
              endOffset);
        } else {
          LOGGER.info(
              "Topic-partition {}-{} is of length: {}. Beginning offset: {}, End offset: {}",
              topicName,
              topicPartition.getPartitionNumber(),
              offsetRange,
              beginningOffset,
              endOffset);
          veniceInputPartitions
              .add(new VeniceBasicPubsubInputPartition(regionName, topicPartition, beginningPosition, endPosition));
        }
      }

      return veniceInputPartitions.toArray(new InputPartition[0]);
    } catch (Exception e) {
      throw new VeniceException("Could not breakdown Pubsub topic into input partitions.", e);
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
