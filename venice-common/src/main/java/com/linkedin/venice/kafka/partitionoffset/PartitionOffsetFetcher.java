package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.annotation.Threadsafe;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


@Threadsafe
public interface PartitionOffsetFetcher extends Closeable {

  Map<Integer, Long> getTopicLatestOffsets(String topic);

  long getPartitionLatestOffset(String topic, int partition);

  long getPartitionLatestOffsetAndRetry(String topic, int partition, int retries);

  Map<Integer, Long> getTopicOffsetsByTime(String topic, long timestamp);

  long getPartitionOffsetByTime(String topic, int partition, long timestamp);

  long getPartitionOffsetByTimeWithRetry(String topic, int partition, long timestamp, int maxAttempt, Duration delay);

  long getLatestProducerTimestampAndRetry(String topic, int partition, int retries);

  List<PartitionInfo> partitionsFor(String topic);

  long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp);

  @Override
  void close();
}
