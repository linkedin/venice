package com.linkedin.davinci.kafka.consumer;

public class KafkaStoreIngestionServiceWithPartitionWiseTest extends KafkaStoreIngestionServiceTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }
}
