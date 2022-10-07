package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
  private final Map<String, Boolean> topicToSortedIngestionMode = new VeniceConcurrentHashMap<>();

  SeparatedStoreBufferService(VeniceServerConfig serverConfig) {
    this.sortedPoolSize = serverConfig.getDrainerPoolSizeSortedInput();
    this.unsortedPoolSize = serverConfig.getDrainerPoolSizeUnsortedInput();
    this.sortedServiceDelegate = new StoreBufferService(
        sortedPoolSize,
        serverConfig.getStoreWriterBufferMemoryCapacity(),
        serverConfig.getStoreWriterBufferNotifyDelta());
    this.unsortedServiceDelegate = new StoreBufferService(
        unsortedPoolSize,
        serverConfig.getStoreWriterBufferMemoryCapacity(),
        serverConfig.getStoreWriterBufferNotifyDelta());
    LOGGER.info(
        "Created separated store buffer service with {} sorted drainers and {} unsorted drainers queues with capacity of {}",
        sortedPoolSize,
        unsortedPoolSize,
        serverConfig.getStoreWriterBufferMemoryCapacity());
  }

  @Override
  public void putConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int subPartition,
      String kafkaUrl,
      long beforeProcessingRecordTimestamp) throws InterruptedException {
    PartitionConsumptionState partitionConsumptionState = ingestionTask.getPartitionConsumptionState(subPartition);
    boolean sortedInput = false;
    if (partitionConsumptionState != null) {
      boolean currentState;

      // there is could be cases the following flag does not represent actual message's ingestion state as control
      // message
      // which updates the `isDeferredWrite` flag may not yet be processed. This might cause inefficiency but not
      // logical incorrectness.
      sortedInput = partitionConsumptionState.isDeferredWrite();
      if (topicToSortedIngestionMode.containsKey(consumerRecord.topic())) {
        currentState = topicToSortedIngestionMode.get(consumerRecord.topic());
        // If there is a change in deferredWrite mode, drain the buffers
        if (currentState != sortedInput) {
          LOGGER.info(
              "Switching drainer buffer for topic {} to use {}",
              consumerRecord.topic(),
              sortedInput ? "sorted queue." : "unsorted queue.");
          drainBufferedRecordsFromTopicPartition(consumerRecord.topic(), partitionConsumptionState.getPartition());
          topicToSortedIngestionMode.put(consumerRecord.topic(), sortedInput);
        }
      } else {
        topicToSortedIngestionMode.put(consumerRecord.topic(), sortedInput);
      }
    }
    if (sortedInput) {
      sortedServiceDelegate.putConsumerRecord(
          consumerRecord,
          ingestionTask,
          leaderProducedRecordContext,
          subPartition,
          kafkaUrl,
          beforeProcessingRecordTimestamp);
    } else {
      unsortedServiceDelegate.putConsumerRecord(
          consumerRecord,
          ingestionTask,
          leaderProducedRecordContext,
          subPartition,
          kafkaUrl,
          beforeProcessingRecordTimestamp);
    }
  }

  @Override
  public void drainBufferedRecordsFromTopicPartition(String topic, int partition) throws InterruptedException {
    sortedServiceDelegate.drainBufferedRecordsFromTopicPartition(topic, partition);
    unsortedServiceDelegate.drainBufferedRecordsFromTopicPartition(topic, partition);
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

  public int getDrainerCount() {
    return unsortedServiceDelegate.getDrainerCount() + sortedServiceDelegate.getDrainerCount();
  }

  /**
   * This get called from StoreBufferServiceStats to track the mem usage in a drainer,
   * in separated drainer case, we can assume first sortedPoolSize indices belong to sortedPoolSize
   * and indices more than sortedPoolSize belong to unsorted ingestions.
   * @param index
   * @return
   */
  public long getDrainerQueueMemoryUsage(int index) {
    if (index < sortedPoolSize) {
      return sortedServiceDelegate.getDrainerQueueMemoryUsage(index);
    }
    return unsortedServiceDelegate.getDrainerQueueMemoryUsage(index - sortedPoolSize);
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
