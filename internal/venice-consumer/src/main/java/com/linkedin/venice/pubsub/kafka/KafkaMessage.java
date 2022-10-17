package com.linkedin.venice.pubsub.kafka;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class KafkaMessage<K, V> implements PubSubMessage<K, V, KafkaTopicPartition, Long> {
  private final K key;
  private final V value;
  private final KafkaTopicPartition topicPartition;
  private final long offset;
  private final long timestamp;

  KafkaMessage(ConsumerRecord<K, V> kafkaRecord, KafkaTopicPartition topicPartition) {
    this(kafkaRecord.key(), kafkaRecord.value(), topicPartition, kafkaRecord.offset(), kafkaRecord.timestamp());
  }

  KafkaMessage(K key, V value, KafkaTopicPartition topicPartition, long offset, long timestamp) {
    this.key = key;
    this.value = value;
    this.topicPartition = topicPartition;
    this.offset = offset;
    this.timestamp = timestamp;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public KafkaTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public Long getOffset() {
    return offset;
  }

  @Override
  public long getPubSubMessageTime() {
    return timestamp;
  }
}
