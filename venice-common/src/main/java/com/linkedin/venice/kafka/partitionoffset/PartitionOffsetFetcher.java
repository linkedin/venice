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

  /**
   * Get the producer timestamp of the last data message (non-control message) in the given topic partition. In other
   * words, if the last message in a topic partition is a control message, this method should keep looking at its previous
   * message(s) until it finds one that is not a control message and gets its producer timestamp.
   * @param topic
   * @param partition
   * @param retries
   * @return producer timestamp
   */
  long getProducerTimestampOfLastDataRecord(String topic, int partition, int retries);

  List<PartitionInfo> partitionsFor(String topic);

  long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp);

  @Override
  void close();
}
