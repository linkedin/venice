package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitPlanner;
import java.util.List;
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
   * Split the topic according to the topic partition size and the allowed max record per mapper.
   * {@param numSplits} is not being used in this function.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) {
    VeniceProperties veniceProperties = KafkaInputUtils.getConsumerProperties(job);
    return getSplits(veniceProperties);
  }

  public KafkaInputSplit[] getSplits(VeniceProperties veniceProperties) {
    PubSubSplitPlanner planner = new PubSubSplitPlanner();
    List<PubSubPartitionSplit> planned = planner.plan(veniceProperties);
    KafkaInputSplit[] splits = new KafkaInputSplit[planned.size()];

    int index = 0;
    for (PubSubPartitionSplit pubSubSplit: planned) {
      KafkaInputSplit split = new KafkaInputSplit(pubSubSplit);
      splits[index++] = split;
      LOGGER.info("Created split: {}", split);
    }

    LOGGER.info("Created {} splits for topic: {}", splits.length, veniceProperties.getString(KAFKA_INPUT_TOPIC));
    return splits;
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
