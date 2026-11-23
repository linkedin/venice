package com.linkedin.davinci.external;

import com.linkedin.davinci.kafka.consumer.AbstractStoreBufferService;
import com.linkedin.davinci.kafka.consumer.LeaderProducedRecordContext;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.concurrent.CompletableFuture;


/** An external implementation of only the pre-time-checkpoint public contract. */
public class LegacyStoreBufferService extends AbstractStoreBufferService {
  @Override
  public void putConsumerRecord(
      DefaultPubSubMessage record,
      StoreIngestionTask task,
      LeaderProducedRecordContext context,
      int partition,
      String kafkaUrl,
      long timestampNs) {
  }

  @Override
  public void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition partition, long timeoutMs) {
  }

  @Override
  public long getTotalMemoryUsage() {
    return 0;
  }

  @Override
  public long getTotalRemainingMemory() {
    return 0;
  }

  @Override
  public long getMaxMemoryUsagePerDrainer() {
    return 0;
  }

  @Override
  public long getMinMemoryUsagePerDrainer() {
    return 0;
  }

  @Override
  public CompletableFuture<Void> execSyncOffsetCommandAsync(PubSubTopicPartition partition, StoreIngestionTask task) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> execSyncGlobalRtDivAsync(PubSubTopicPartition partition, StoreIngestionTask task) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> execSyncOffsetFromSnapshotAsync(
      PubSubTopicPartition partition,
      PartitionTracker snapshot,
      CompletableFuture<Void> preceding,
      StoreIngestionTask task) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean startInner() {
    return true;
  }

  @Override
  public void stopInner() {
  }
}
