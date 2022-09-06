package com.linkedin.davinci.kafka.consumer;

public interface TopicPartitionConsumerFunction {
  void execute(String topic, int partition);
}
