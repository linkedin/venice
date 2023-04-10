package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
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
  private final Map<PubSubTopic, Boolean> topicToSortedIngestionMode = new VeniceConcurrentHashMap<>();

  SeparatedStoreBufferService(VeniceServerConfig serverConfig) {
    this(
        serverConfig.getDrainerPoolSizeSortedInput(),
        serverConfig.getDrainerPoolSizeUnsortedInput(),
        new StoreBufferService(
            serverConfig.getDrainerPoolSizeSortedInput(),
            serverConfig.getStoreWriterBufferMemoryCapacity(),
            serverConfig.getStoreWriterBufferNotifyDelta(),
            serverConfig.isStoreWriterBufferAfterLeaderLogicEnabled()),
        new StoreBufferService(
            serverConfig.getDrainerPoolSizeUnsortedInput(),
            serverConfig.getStoreWriterBufferMemoryCapacity(),
            serverConfig.getStoreWriterBufferNotifyDelta(),
            serverConfig.isStoreWriterBufferAfterLeaderLogicEnabled()));
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
    PartitionConsumptionState partitionConsumptionState = ingestionTask.getPartitionConsumptionState(subPartition);
    boolean sortedInput = false;
    if (partitionConsumptionState != null) {

      // there is could be cases the following flag does not represent actual message's ingestion state as control
      // message
      // which updates the `isDeferredWrite` flag may not yet be processed. This might cause inefficiency but not
      // logical incorrectness.
      sortedInput = partitionConsumptionState.isDeferredWrite();
      Boolean currentState = topicToSortedIngestionMode.get(consumerRecord.getTopicPartition().getPubSubTopic());

      if (currentState != null) {
        // If there is a change in deferredWrite mode, drain the buffers
        if (currentState != sortedInput) {
          LOGGER.info(
              "Switching drainer buffer for topic {} to use {}",
              consumerRecord.getTopicPartition().getPubSubTopic().getName(),
              sortedInput ? "sorted queue." : "unsorted queue.");
          PubSubTopicPartition pubSubTopicPartition =
              consumerRecord.getTopicPartition().getPartitionNumber() != subPartition
                  ? new PubSubTopicPartitionImpl(consumerRecord.getTopicPartition().getPubSubTopic(), subPartition)
                  : consumerRecord.getTopicPartition();
          drainBufferedRecordsFromTopicPartition(pubSubTopicPartition);
          topicToSortedIngestionMode.put(consumerRecord.getTopicPartition().getPubSubTopic(), sortedInput);
        }
      } else {
        topicToSortedIngestionMode.put(consumerRecord.getTopicPartition().getPubSubTopic(), sortedInput);
      }
    }
    StoreBufferService chosenSBS = sortedInput ? sortedServiceDelegate : unsortedServiceDelegate;
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
