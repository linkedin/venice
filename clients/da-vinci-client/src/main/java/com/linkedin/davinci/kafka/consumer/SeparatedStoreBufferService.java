package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import io.tehuti.metrics.MetricsRepository;
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
  protected final StoreBufferService sortedServiceDelegate;
  protected final StoreBufferService unsortedServiceDelegate;
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
            metricsRepository),
        new StoreBufferService(
            serverConfig.getDrainerPoolSizeUnsortedInput(),
            serverConfig.getStoreWriterBufferMemoryCapacity(),
            serverConfig.getStoreWriterBufferNotifyDelta(),
            serverConfig.isStoreWriterBufferAfterLeaderLogicEnabled(),
            metricsRepository));
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
      StoreBufferService sortedServiceDelegate,
      StoreBufferService unsortedServiceDelegate) {
    this.sortedPoolSize = sortedPoolSize;
    this.unsortedPoolSize = unsortedPoolSize;
    this.sortedServiceDelegate = sortedServiceDelegate;
    this.unsortedServiceDelegate = unsortedServiceDelegate;
  }

  @Override
  public void putConsumerRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException {
    StoreBufferService chosenSBS = ingestionTask.isHybridMode() ? unsortedServiceDelegate : sortedServiceDelegate;
    chosenSBS.putConsumerRecord(
        consumerRecord,
        ingestionTask,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestampNs);
  }

  @Override
  public void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition topicPartition) throws InterruptedException {
    sortedServiceDelegate.drainBufferedRecordsFromTopicPartition(topicPartition);
    unsortedServiceDelegate.drainBufferedRecordsFromTopicPartition(topicPartition);
  }

  @Override
  public boolean startInner() throws Exception {
    sortedServiceDelegate.startInner();
    unsortedServiceDelegate.startInner();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    sortedServiceDelegate.stopInner();
    unsortedServiceDelegate.stopInner();
  }

  public long getTotalMemoryUsage() {
    return unsortedServiceDelegate.getTotalMemoryUsage() + sortedServiceDelegate.getTotalMemoryUsage();
  }

  public long getTotalRemainingMemory() {
    return unsortedServiceDelegate.getTotalRemainingMemory() + sortedServiceDelegate.getTotalRemainingMemory();
  }

  public long getMaxMemoryUsagePerDrainer() {
    return unsortedServiceDelegate.getMaxMemoryUsagePerDrainer() + sortedServiceDelegate.getMaxMemoryUsagePerDrainer();
  }

  public long getMinMemoryUsagePerDrainer() {
    return sortedServiceDelegate.getMinMemoryUsagePerDrainer() + unsortedServiceDelegate.getMinMemoryUsagePerDrainer();
  }
}
