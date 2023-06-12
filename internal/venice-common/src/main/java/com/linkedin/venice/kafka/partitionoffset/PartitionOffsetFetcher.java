package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import java.io.Closeable;
import java.util.List;


@Threadsafe
public interface PartitionOffsetFetcher extends Closeable {
  Int2LongMap getTopicLatestOffsets(PubSubTopic topic);

  long getPartitionLatestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries);

  long getPartitionEarliestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries);

  long getPartitionOffsetByTime(PubSubTopicPartition pubSubTopicPartition, long timestamp);

  /**
   * Get the producer timestamp of the last data message (non-control message) in the given topic partition. In other
   * words, if the last message in a topic partition is a control message, this method should keep looking at its previous
   * message(s) until it finds one that is not a control message and gets its producer timestamp.
   * @param pubSubTopicPartition
   * @param retries
   * @return producer timestamp
   */
  long getProducerTimestampOfLastDataRecord(PubSubTopicPartition pubSubTopicPartition, int retries);

  List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic);

  long getOffsetByTimeIfOutOfRange(PubSubTopicPartition pubSubTopicPartition, long timestamp);

  @Override
  void close();
}
