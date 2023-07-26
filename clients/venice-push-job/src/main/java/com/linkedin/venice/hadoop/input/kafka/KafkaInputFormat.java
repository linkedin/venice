package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kafka.common.TopicPartition;


/**
 * We borrowed some idea from the open-sourced attic-crunch lib:
 * https://github.com/apache/attic-crunch/blob/master/crunch-kafka/src/main/java/org/apache/crunch/kafka/record/KafkaInputFormat.java
 *
 * This {@link InputFormat} implementation is used to read data off a Kafka topic.
 */
public class KafkaInputFormat implements InputFormat<KafkaInputMapperKey, KafkaInputMapperValue> {
  /**
   * The default max records per mapper, and if there are more records in one topic partition, it will be
   * consumed by multiple mappers in parallel.
   * BTW, this calculation is not accurate since it is purely based on offset, and the topic
   * being consumed could have log compaction enabled.
   */
  public static final long DEFAULT_KAFKA_INPUT_MAX_RECORDS_PER_MAPPER = 5000000L;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  protected Map<TopicPartition, Long> getLatestOffsets(JobConf config) {
    VeniceProperties consumerProperties = KafkaInputUtils.getConsumerProperties(config);
    try (TopicManagerRepository topicManagerRepository = TopicManagerRepository.builder()
        .setPubSubProperties(k -> consumerProperties)
        .setLocalKafkaBootstrapServers(config.get(KAFKA_INPUT_BROKER_URL))
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setPubSubAdminAdapterFactory(new ApacheKafkaAdminAdapterFactory())
        .setPubSubConsumerAdapterFactory(new ApacheKafkaConsumerAdapterFactory())
        .build()) {
      try (TopicManager topicManager = topicManagerRepository.getTopicManager()) {
        String topic = config.get(KAFKA_INPUT_TOPIC);
        Map<Integer, Long> latestOffsets = topicManager.getTopicLatestOffsets(pubSubTopicRepository.getTopic(topic));
        Map<TopicPartition, Long> partitionOffsetMap = new HashMap<>(latestOffsets.size());
        latestOffsets.forEach(
            (partitionId, latestOffset) -> partitionOffsetMap
                .put(new TopicPartition(topic, partitionId), latestOffset));
        return partitionOffsetMap;
      }
    }
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
    Map<TopicPartition, Long> latestOffsets = getLatestOffsets(job);
    List<InputSplit> splits = new LinkedList<>();
    latestOffsets.forEach((topicPartition, end) -> {

      /**
       * Chop up any excessively large partitions into multiple splits for more balanced map task durations. This will
       * also exclude any partitions with no records to read (where the start offset equals the end offset).
       */
      long splitStart = 0;
      while (splitStart < end) {
        long splitEnd = Math.min(splitStart + maxRecordsPerSplit, end);
        splits.add(new KafkaInputSplit(topicPartition.topic(), topicPartition.partition(), splitStart, splitEnd));
        splitStart = splitEnd;
      }
    });

    return splits.toArray(new KafkaInputSplit[splits.size()]);
  }

  @Override
  public RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter) {
    return new KafkaInputRecordReader(split, job, reporter);
  }

  public RecordReader<KafkaInputMapperKey, KafkaInputMapperValue> getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter,
      PubSubConsumerAdapter consumer) {
    return new KafkaInputRecordReader(split, job, reporter, consumer);
  }
}
