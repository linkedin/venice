package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * We borrowed some idea from the open-sourced attic-crunch lib:
 * https://github.com/apache/attic-crunch/blob/master/crunch-kafka/src/main/java/org/apache/crunch/kafka/record/KafkaInputFormat.java
 *
 * This {@link InputFormat} implementation is used to read data off a Kafka topic.
 */
public class KafkaInputFormat implements InputFormat<KafkaInputMapperKey, KafkaInputMapperValue> {
  private static final Logger LOGGER = LogManager.getLogger(KafkaInputFormat.class);

  /**
   * The default max records per mapper, and if there are more records in one topic partition, it will be
   * consumed by multiple mappers in parallel.
   * BTW, this calculation is not accurate since it is purely based on offset, and the topic
   * being consumed could have log compaction enabled.
   */
  public static final long DEFAULT_KAFKA_INPUT_MAX_RECORDS_PER_MAPPER = 5000000L;
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  protected static TopicManager createTopicManager(JobConf config) {
    VeniceProperties consumerProperties = KafkaInputUtils.getConsumerProperties(config);
    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> consumerProperties)
            .setPubSubTopicRepository(TOPIC_REPOSITORY)
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(consumerProperties))
            .setPubSubAdminAdapterFactory(PubSubClientsFactory.createAdminFactory(consumerProperties))
            .setPubSubConsumerAdapterFactory(PubSubClientsFactory.createConsumerFactory(consumerProperties))
            .setTopicMetadataFetcherThreadPoolSize(1)
            .setTopicMetadataFetcherConsumerPoolSize(1)
            .setVeniceComponent(VeniceComponent.PUSH_JOB)
            .build();
    return new TopicManagerRepository(topicManagerContext, config.get(KAFKA_INPUT_BROKER_URL)).getLocalTopicManager();
  }

  protected static Map<PubSubTopicPartition, PubSubPosition> getEndPositions(
      TopicManager topicManager,
      PubSubTopic topic) {
    return RetryUtils.executeWithMaxAttempt(
        () -> topicManager.getEndPositionsForTopic(topic),
        10,
        Duration.ofMinutes(1),
        Collections.singletonList(Exception.class));
  }

  protected static Map<PubSubTopicPartition, PubSubPosition> getStartPositions(
      TopicManager topicManager,
      PubSubTopic topic) {
    return RetryUtils.executeWithMaxAttempt(
        () -> topicManager.getStartPositionsForTopic(topic),
        10,
        Duration.ofMinutes(1),
        Collections.singletonList(Exception.class));
  }

  protected static long getPositionsDiff(
      TopicManager topicManager,
      PubSubTopicPartition topicPartition,
      PubSubPosition start,
      PubSubPosition end) {
    return RetryUtils.executeWithMaxAttempt(
        () -> topicManager.diffPosition(topicPartition, end, start),
        10,
        Duration.ofMinutes(1),
        Collections.singletonList(Exception.class));
  }

  /**
   * Split the topic according to the topic partition size and the allowed max record per mapper.
   * {@param numSplits} is not being used in this function.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    long maxRecordsPerSplit =
        job.getLong(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, DEFAULT_KAFKA_INPUT_MAX_RECORDS_PER_MAPPER);
    if (maxRecordsPerSplit < 1L) {
      throw new IllegalArgumentException(
          "Invalid " + KAFKA_INPUT_MAX_RECORDS_PER_MAPPER + " value [" + maxRecordsPerSplit + "]");
    }

    return getSplitsByRecordsPerSplit(job, maxRecordsPerSplit);
  }

  public InputSplit[] getSplitsByRecordsPerSplit(JobConf job, long maxRecordsPerSplit) {
    PubSubTopic topic = TOPIC_REPOSITORY.getTopic(job.get(KAFKA_INPUT_TOPIC));
    TopicManager topicManager = createTopicManager(job);

    Map<PubSubTopicPartition, PubSubPosition> startPositions = getStartPositions(topicManager, topic);
    Map<PubSubTopicPartition, PubSubPosition> endPositions = getEndPositions(topicManager, topic);
    if (startPositions.isEmpty() || endPositions.isEmpty()) {
      return new KafkaInputSplit[0];
    }
    if (startPositions.size() != endPositions.size()) {
      throw new IllegalStateException(
          "Start positions and end positions size mismatch for topic: " + topic.getName() + ". Start positions: "
              + startPositions.size() + ", End positions: " + endPositions.size());
    }

    List<InputSplit> splits = new ArrayList<>(startPositions.size());
    // create one split for each topic partition
    for (Map.Entry<PubSubTopicPartition, PubSubPosition> entry: startPositions.entrySet()) {
      PubSubTopicPartition topicPartition = entry.getKey();
      PubSubPosition startPosition = entry.getValue();
      PubSubPosition endPosition = endPositions.get(topicPartition);

      if (endPosition == null) {
        throw new IllegalStateException(
            "End offset for topic partition " + topicPartition + " is not found for topic: " + topic.getName());
      }

      // If the start offset equals the end offset, it means there are no records to read.
      if (startPosition.equals(endPosition)) {
        LOGGER.info(
            "Skipping topic-partition {} as it has no records to read (start position: {}, end position: {}).",
            topicPartition,
            startPosition,
            endPosition);
        continue;
      }
      long recordsDiff = getPositionsDiff(topicManager, topicPartition, startPosition, endPosition);
      splits.add(new KafkaInputSplit(TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, recordsDiff));
      LOGGER.info(
          "Created split for topic-partition: {} with start position: {} and end position: {}.",
          topicPartition,
          startPosition,
          endPosition);
    }

    return splits.toArray(new KafkaInputSplit[splits.size()]);
  }

  @Override
  public RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter) {
    DataWriterTaskTracker taskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    return new KafkaInputRecordReader(split, job, taskTracker);
  }

  public RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter,
      PubSubConsumerAdapter consumer) {
    DataWriterTaskTracker taskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    return new KafkaInputRecordReader(split, job, taskTracker, consumer);
  }
}
