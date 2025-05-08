package com.linkedin.davinci.kafka.consumer;

@Deprecated
public class KafkaStoreIngestionServiceWithTopicWiseTest extends KafkaStoreIngestionServiceTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }
}
