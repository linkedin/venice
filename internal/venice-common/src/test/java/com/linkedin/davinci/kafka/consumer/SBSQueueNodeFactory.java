package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;


/**
 * Simple utility class to get access to the package-private classes in {@link StoreBufferService} and their
 * constructors.
 */
public class SBSQueueNodeFactory {
  public static StoreBufferService.QueueNode queueNode(
      DefaultPubSubMessage consumerRecord,
      StoreIngestionTask ingestionTask,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    return new StoreBufferService.QueueNode(consumerRecord, ingestionTask, kafkaUrl, beforeProcessingRecordTimestampNs);
  }

  public static StoreBufferService.LeaderQueueNode leaderQueueNode(
      DefaultPubSubMessage consumerRecord,
      StoreIngestionTask ingestionTask,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs,
      LeaderProducedRecordContext leaderProducedRecordContext) {
    return new StoreBufferService.LeaderQueueNode(
        consumerRecord,
        ingestionTask,
        kafkaUrl,
        beforeProcessingRecordTimestampNs,
        leaderProducedRecordContext);
  }

  public static Class<?> queueNodeClass() {
    return StoreBufferService.QueueNode.class;
  }

  public static Class<?> leaderQueueNodeClass() {
    return StoreBufferService.LeaderQueueNode.class;
  }

  private SBSQueueNodeFactory() {
    // Utility class
  }
}
