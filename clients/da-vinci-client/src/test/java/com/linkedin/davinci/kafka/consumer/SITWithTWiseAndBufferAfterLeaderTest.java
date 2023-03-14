package com.linkedin.davinci.kafka.consumer;

public class SITWithTWiseAndBufferAfterLeaderTest extends StoreIngestionTaskTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  protected boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return true;
  }
}
