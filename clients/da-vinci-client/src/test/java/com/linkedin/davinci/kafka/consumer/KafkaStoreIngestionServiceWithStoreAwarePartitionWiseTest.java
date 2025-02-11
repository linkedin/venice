package com.linkedin.davinci.kafka.consumer;

public class KafkaStoreIngestionServiceWithStoreAwarePartitionWiseTest extends KafkaStoreIngestionServiceTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.STORE_AWARE_PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }
}
