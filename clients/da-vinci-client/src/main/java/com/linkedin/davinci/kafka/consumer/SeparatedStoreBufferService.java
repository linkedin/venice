package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This store buffer services maintains two separate drainer queues for store ingestions.
 * For the sorted messages, Venice SN could use SSTFileWriter to ingest into RocksDB, and the performance is constant and stable.
 * But for the unsorted messages, RocksDB behavior is not constant because of RocksDB compaction and sometimes write compute.
 * Since there are very different characteristics, it will be helpful to decouple these two types of ingestions to avoid one blocking the other.
 */
public class SeparatedStoreBufferService extends AbstractStoreBufferService {
  private static final Logger LOGGER = LogManager.getLogger(SeparatedStoreBufferService.class);
  protected final StoreBufferService sortedStoreBufferServiceDelegate;
  protected final StoreBufferService unsortedStoreBufferServiceDelegate;
  private final int sortedPoolSize;
  private final int unsortedPoolSize;

  SeparatedStoreBufferService(VeniceServerConfig serverConfig, MetricsRepository metricsRepository) {
    this(
        serverConfig.getDrainerPoolSizeSortedInput(),
        serverConfig.getDrainerPoolSizeUnsortedInput(),
        new StoreBufferService(
            serverConfig.getDrainerPoolSizeSortedInput(),
            serverConfig.getStoreWriterBufferMemoryCapacity(),
            serverConfig.getStoreWriterBufferNotifyDelta(),
            serverConfig.isStoreWriterBufferAfterLeaderLogicEnabled(),
            metricsRepository,
            true),
        new StoreBufferService(
            serverConfig.getDrainerPoolSizeUnsortedInput(),
            serverConfig.getStoreWriterBufferMemoryCapacity(),
            serverConfig.getStoreWriterBufferNotifyDelta(),
            serverConfig.isStoreWriterBufferAfterLeaderLogicEnabled(),
            metricsRepository,
            false));
    LOGGER.info(
        "Created separated store buffer service with {} sorted drainers and {} unsorted drainers queues with capacity of {}",
        sortedPoolSize,
        unsortedPoolSize,
        serverConfig.getStoreWriterBufferMemoryCapacity());
  }

  /** For tests */
  SeparatedStoreBufferService(
      int sortedPoolSize,
      int unsortedPoolSize,
      StoreBufferService sortedStoreBufferServiceDelegate,
      StoreBufferService unsortedStoreBufferServiceDelegate) {
    this.sortedPoolSize = sortedPoolSize;
    this.unsortedPoolSize = unsortedPoolSize;
    this.sortedStoreBufferServiceDelegate = sortedStoreBufferServiceDelegate;
    this.unsortedStoreBufferServiceDelegate = unsortedStoreBufferServiceDelegate;
  }

  @Override
  public void putConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException {
    StoreBufferService chosenSBS =
        ingestionTask.isHybridMode() ? unsortedStoreBufferServiceDelegate : sortedStoreBufferServiceDelegate;
    chosenSBS.putConsumerRecord(
        consumerRecord,
        ingestionTask,
        leaderProducedRecordContext,
        partition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
  }

  @Override
  public void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition topicPartition) throws InterruptedException {
    sortedStoreBufferServiceDelegate.drainBufferedRecordsFromTopicPartition(topicPartition);
    unsortedStoreBufferServiceDelegate.drainBufferedRecordsFromTopicPartition(topicPartition);
  }

  @Override
  public CompletableFuture<Void> execSyncOffsetCommandAsync(
      PubSubTopicPartition topicPartition,
      StoreIngestionTask ingestionTask) throws InterruptedException {
    StoreBufferService chosenSBS =
        ingestionTask.isHybridMode() ? unsortedStoreBufferServiceDelegate : sortedStoreBufferServiceDelegate;
    return chosenSBS.execSyncOffsetCommandAsync(topicPartition, ingestionTask);
  }

  @Override
  public boolean startInner() throws Exception {
    sortedStoreBufferServiceDelegate.startInner();
    unsortedStoreBufferServiceDelegate.startInner();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    sortedStoreBufferServiceDelegate.stopInner();
    unsortedStoreBufferServiceDelegate.stopInner();
  }

  public long getTotalMemoryUsage() {
    return unsortedStoreBufferServiceDelegate.getTotalMemoryUsage()
        + sortedStoreBufferServiceDelegate.getTotalMemoryUsage();
  }

  public long getTotalRemainingMemory() {
    return unsortedStoreBufferServiceDelegate.getTotalRemainingMemory()
        + sortedStoreBufferServiceDelegate.getTotalRemainingMemory();
  }

  public long getMaxMemoryUsagePerDrainer() {
    return unsortedStoreBufferServiceDelegate.getMaxMemoryUsagePerDrainer()
        + sortedStoreBufferServiceDelegate.getMaxMemoryUsagePerDrainer();
  }

  public long getMinMemoryUsagePerDrainer() {
    return sortedStoreBufferServiceDelegate.getMinMemoryUsagePerDrainer()
        + unsortedStoreBufferServiceDelegate.getMinMemoryUsagePerDrainer();
  }
}
