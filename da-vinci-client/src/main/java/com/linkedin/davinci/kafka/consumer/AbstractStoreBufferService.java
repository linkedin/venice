package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.service.AbstractVeniceService;


/**
 * Abstract class capturing the responsibilities of drainers threads doing store ingestion.
 */
public abstract class AbstractStoreBufferService extends AbstractVeniceService {

  public abstract void putConsumerRecord(VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> consumerRecordWrapper,
      StoreIngestionTask ingestionTask, LeaderProducedRecordContext leaderProducedRecordContext) throws InterruptedException;

  public abstract void drainBufferedRecordsFromTopicPartition(String topic, int partition) throws InterruptedException;

  public abstract int getDrainerCount();

  public abstract long getDrainerQueueMemoryUsage(int index);

  public abstract long getTotalMemoryUsage();

  public abstract long getTotalRemainingMemory();

  public abstract long getMaxMemoryUsagePerDrainer();

  public abstract long getMinMemoryUsagePerDrainer();
}

