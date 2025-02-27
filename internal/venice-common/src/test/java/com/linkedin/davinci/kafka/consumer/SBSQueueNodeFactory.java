package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;


/**
 * Simple utility class to get access to the package-private classes in {@link StoreBufferService} and their
 * constructors.
 */
public class SBSQueueNodeFactory {
  public static StoreBufferService.QueueNode queueNode(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> consumerRecord,
      StoreIngestionTask ingestionTask,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) {
    return new StoreBufferService.QueueNode(consumerRecord, ingestionTask, kafkaUrl, beforeProcessingRecordTimestampNs);
  }

  public static StoreBufferService.LeaderQueueNode leaderQueueNode(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> consumerRecord,
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
