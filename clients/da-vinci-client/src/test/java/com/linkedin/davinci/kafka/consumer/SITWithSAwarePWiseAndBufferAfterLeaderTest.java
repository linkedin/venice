package com.linkedin.davinci.kafka.consumer;

public class SITWithSAwarePWiseAndBufferAfterLeaderTest extends StoreIngestionTaskTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.STORE_AWARE_PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  protected boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return true;
  }

  @Override
  protected boolean isAaWCParallelProcessingEnabled() {
    return true;
  }
}
