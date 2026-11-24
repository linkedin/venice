package com.linkedin.davinci.kafka.consumer;

import org.testng.annotations.Test;


public class SITWithPWiseAndBufferAfterLeaderTest extends StoreIngestionTaskTest {
  @Test
  public void testReplicaDropBurstSharesOneConsumerDetach() throws Exception {
    verifyReplicaDropBurstSharesOneConsumerDetach();
  }

  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  protected boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return true;
  }

  @Override
  protected boolean isAaWCParallelProcessingEnabled() {
    return true;
  }
}
