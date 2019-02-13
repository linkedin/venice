package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


public interface KafkaConsumerWrapper extends AutoCloseable {
  void subscribe(String topic, int partition, OffsetRecord offset);

  void unSubscribe(String topic, int partition);

  void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException;

  void close();

  ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeout);

  boolean hasSubscription();

  Map<String, List<PartitionInfo>> listTopics();

  Map<TopicPartition,Long> beginningOffsets(List<TopicPartition> topicPartitions);

  Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions);

  void assign(List<TopicPartition> topicPartitions);

  void seek(TopicPartition topicPartition, long nextOffset);
}
