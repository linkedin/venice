package com.linkedin.davinci.kafka.consumer;

public class SITWithSAwarePWiseWithoutBufferAfterLeaderTest extends StoreIngestionTaskTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.STORE_AWARE_PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  protected boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return false;
  }
}
