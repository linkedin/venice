package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * This store buffer services maintains two separate drainer queues for store ingestions.
 * For the sorted messages, Venice SN could use SSTFileWriter to ingest into RocksDB, and the performance is constant and stable.
 * But for the unsorted messages, RocksDB behavior is not constant because of RocksDB compaction and sometimes write compute.
 * Since there are very different characteristics, it will be helpful to decouple these two types of ingestions to avoid one blocking the other.
 */
public class SeparatedStoreBufferService extends AbstractStoreBufferService {
  private final StoreBufferService sortedServiceDelegate;
  private final StoreBufferService unsortedServiceDelegate;
  private final Map<String, Boolean> topicToSortedIngestionMode = new VeniceConcurrentHashMap<>();

  SeparatedStoreBufferService(VeniceServerConfig serverConfig) {
    this.sortedServiceDelegate = new StoreBufferService(serverConfig.getDrainerPoolSizeSortedInput(),
        serverConfig.getStoreWriterBufferMemoryCapacity(), serverConfig.getStoreWriterBufferNotifyDelta());
    this.unsortedServiceDelegate = new StoreBufferService(serverConfig.getDrainerPoolSizeUnsortedInput(),
        serverConfig.getStoreWriterBufferMemoryCapacity(), serverConfig.getStoreWriterBufferNotifyDelta());
  }

  @Override
  public void putConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      StoreIngestionTask ingestionTask, ProducedRecord producedRecord) throws InterruptedException {
    Optional<PartitionConsumptionState> partitionConsumptionState = ingestionTask.getPartitionConsumptionState(consumerRecord.partition());

    boolean sortedInput = false;
    if (partitionConsumptionState.isPresent()) {
      boolean currentState;

      // there is could be cases the following flag does not represent actual message's ingestion state as control message
      // which updates the `isDeferredWrite` flag may not yet be processed. This might cause inefficiency but not logical incorrectness.
      sortedInput = partitionConsumptionState.get().isDeferredWrite();
      if (topicToSortedIngestionMode.containsKey(consumerRecord.topic())) {
        currentState = topicToSortedIngestionMode.get(consumerRecord.topic());
        // If there is a change in deferredWrite mode, drain the buffers
        if (currentState != sortedInput) {
          drainBufferedRecordsFromTopicPartition(consumerRecord.topic(), partitionConsumptionState.get().getPartition());
          topicToSortedIngestionMode.put(consumerRecord.topic(), sortedInput);
        }
      } else {
        topicToSortedIngestionMode.put(consumerRecord.topic(), sortedInput);
      }
    }
    if (sortedInput) {
      sortedServiceDelegate.putConsumerRecord(consumerRecord, ingestionTask, producedRecord);
    } else {
      unsortedServiceDelegate.putConsumerRecord(consumerRecord, ingestionTask, producedRecord);
    }
  }

  @Override
  public void drainBufferedRecordsFromTopicPartition(String topic, int partition) throws InterruptedException{
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

  public long getDrainerQueueMemoryUsage(int index) {
    return unsortedServiceDelegate.getDrainerQueueMemoryUsage(index) + sortedServiceDelegate.getDrainerQueueMemoryUsage(index);
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
