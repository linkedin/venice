package com.linkedin.davinci.kafka.consumer;

public class KafkaStoreIngestionServiceWithTopicWiseTest extends KafkaStoreIngestionServiceTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }
}
