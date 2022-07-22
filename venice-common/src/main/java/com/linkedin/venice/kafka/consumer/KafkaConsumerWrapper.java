package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


public interface KafkaConsumerWrapper extends AutoCloseable, Closeable {
  void subscribe(String topic, int partition, long lastReadOffset);

  void unSubscribe(String topic, int partition);

  void batchUnsubscribe(Set<TopicPartition> topicPartitionSet);

  void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException;

  void close();

  default void close(Set<String> topics) {
    close();
  }

  ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeoutMs);

  /**
   * @return True if this consumer has subscribed any topic partition at all and vice versa.
   */
  boolean hasAnySubscription();

  /**
   * @return True if this consumer has subscribed any topic in the given topic set {@param topics} and false otherwise.
   */
  boolean hasSubscribedAnyTopic(Set<String> topics);

  boolean hasSubscription(String topic, int partition);

  Map<TopicPartition,Long> beginningOffsets(List<TopicPartition> topicPartitions);

  Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions);

  void assign(Collection<TopicPartition> topicPartitions);

  void seek(TopicPartition topicPartition, long nextOffset);

  void pause(String topic, int partition);

  void resume(String topic, int partition);

  Set<TopicPartition> paused();

  Set<TopicPartition> getAssignment();

  /**
   * Get consuming offset lag for a topic partition
   * @param topic
   * @param partition
   * @return Nothing if there is no offset lag or no valid lag for the topic partition tracked by the consumer instance.
   *         Something if there is a valid (positive or zero) offset lag for the topic partition
   */
  default OptionalLong getOffsetLag(String topic, int partition) {
    return OptionalLong.empty();
  }
}
