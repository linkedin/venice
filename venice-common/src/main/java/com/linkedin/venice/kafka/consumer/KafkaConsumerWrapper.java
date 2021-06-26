package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


public interface KafkaConsumerWrapper extends AutoCloseable {
  void subscribe(String topic, int partition, long lastReadOffset);

  void unSubscribe(String topic, int partition);

  void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException;

  void close();

  default void close(Set<String> topics) {
    close();
  }

  ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeoutMs);

  boolean hasSubscription();

  default boolean hasSubscription(Set<String> topics) {
    return hasSubscription();
  }

  boolean hasSubscription(String topic, int partition);

  Map<TopicPartition,Long> beginningOffsets(List<TopicPartition> topicPartitions);

  Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions);

  void assign(List<TopicPartition> topicPartitions);

  void seek(TopicPartition topicPartition, long nextOffset);

  void pause(String topic, int partition);

  void resume(String topic, int partition);

  Set<TopicPartition> paused();

  Set<TopicPartition> getAssignment();

  default Optional<Long> getLatestOffset(String topic, int partition) { return Optional.empty(); }
}
