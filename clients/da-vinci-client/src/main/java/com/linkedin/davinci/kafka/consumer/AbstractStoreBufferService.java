package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.service.AbstractVeniceService;


/**
 * Abstract class capturing the responsibilities of drainers threads doing store ingestion.
 */
public abstract class AbstractStoreBufferService extends AbstractVeniceService {
  public abstract void putConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException;

  /**
   * This method will wait for all the messages to be processed (persisted to disk) that are already
   * queued up to drainer till now.
   */
  public abstract void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition topicPartition)
      throws InterruptedException;

  public abstract int getDrainerCount();

  public abstract long getDrainerQueueMemoryUsage(int index);

  public abstract long getTotalMemoryUsage();

  public abstract long getTotalRemainingMemory();

  public abstract long getMaxMemoryUsagePerDrainer();

  public abstract long getMinMemoryUsagePerDrainer();
}
