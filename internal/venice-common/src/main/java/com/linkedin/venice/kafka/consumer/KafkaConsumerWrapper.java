package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import java.io.Closeable;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


// TODO: Delete this interface in the future.
public interface KafkaConsumerWrapper extends AutoCloseable, Closeable {
  void subscribe(String topic, int partition, long lastReadOffset);

  void unSubscribe(String topic, int partition);

  void batchUnsubscribe(Set<TopicPartition> topicPartitionSet);

  void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException;

  void close();

  ConsumerRecords<byte[], byte[]> poll(long timeoutMs);

  /**
   * @return True if this consumer has subscribed any topic partition at all and vice versa.
   */
  boolean hasAnySubscription();

  boolean hasSubscription(String topic, int partition);

  void pause(String topic, int partition);

  void resume(String topic, int partition);

  Set<TopicPartition> getAssignment();

  /**
   * Get consuming offset lag for a topic partition
   * @return an offset lag of zero or above if a valid lag was collected by the consumer, or -1 otherwise
   */
  default long getOffsetLag(String topic, int partition) {
    return -1;
  }

  /**
   * Get the latest offset for a topic partition
   * @return an the latest offset (zero or above) if a offset was collected by the consumer, or -1 otherwise
   */
  default long getLatestOffset(String topic, int partition) {
    return -1;
  }
}
