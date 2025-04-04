package com.linkedin.davinci.kafka.consumer;

public interface StaleTopicChecker {
  boolean shouldTopicExist(String topic);
}
