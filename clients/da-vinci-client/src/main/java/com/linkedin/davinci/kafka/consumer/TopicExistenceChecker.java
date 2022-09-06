package com.linkedin.davinci.kafka.consumer;

public interface TopicExistenceChecker {
  boolean checkTopicExists(String topic);
}
