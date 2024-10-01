package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class is used to process the AA/WC messages in parallel to address the long-tail partition lagging issues.
 * For the AA/WC message handling, the consumption is not the bottleneck, but the processing overhead, and
 * even with a single consumer, with {@link IngestionBatchProcessor}, we hope we can utilize the full node's
 * resources to speed up the leader ingestion.
 */
public class IngestionBatchProcessor {
  interface ProcessingFunction {
    PubSubMessageProcessedResult apply(
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
        PartitionConsumptionState partitionConsumptionState,
        int partition,
        String kafkaUrl,
        int kafkaClusterId,
        long beforeProcessingRecordTimestampNs,
        long beforeProcessingBatchRecordsTimestampMs);
  }

  private final String storeVersionName;
  private final String storeName;
  private final int version;
  private final ExecutorService batchProcessingThreadPool;
  private final KeyLevelLocksManager lockManager;
  private final boolean isWriteComputationEnabled;
  private final boolean isActiveActiveReplicationEnabled;
  private final ProcessingFunction processingFunction;
  private final AggVersionedIngestionStats aggVersionedIngestionStats;
  private final HostLevelIngestionStats hostLevelIngestionStats;

  public IngestionBatchProcessor(
      String storeVersionName,
      ExecutorService batchProcessingThreadPool,
      KeyLevelLocksManager lockManager,
      ProcessingFunction processingFunction,
      boolean isWriteComputationEnabled,
      boolean isActiveActiveReplicationEnabled,
      AggVersionedIngestionStats aggVersionedIngestionStats,
      HostLevelIngestionStats hostLevelIngestionStats) {
    this.storeVersionName = storeVersionName;
    this.batchProcessingThreadPool = batchProcessingThreadPool;
    this.lockManager = lockManager;
    this.processingFunction = processingFunction;
    this.isWriteComputationEnabled = isWriteComputationEnabled;
    this.isActiveActiveReplicationEnabled = isActiveActiveReplicationEnabled;
    this.aggVersionedIngestionStats = aggVersionedIngestionStats;
    this.hostLevelIngestionStats = hostLevelIngestionStats;

    this.storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    this.version = Version.parseVersionFromKafkaTopicName(storeVersionName);
  }

  /**
   * When {@link #lockManager} is not null, this function will try to lock all the keys
   * (except Control Messages) passed by the params.
   */
  public List<ReentrantLock> lockKeys(List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records) {
    if (lockManager != null) {
      List<ReentrantLock> locks = new ArrayList<>(records.size());
      records.forEach(r -> {
        if (!r.getKey().isControlMessage()) {
          ReentrantLock lock = lockManager.acquireLockByKey(ByteArrayKey.wrap(r.getKey().getKey()));
          locks.add(lock);
          lock.lock();
        }
      });
      return locks;
    }
    return Collections.emptyList();
  }

  public void unlockKeys(List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records, List<ReentrantLock> locks) {
    if (lockManager != null) {
      locks.forEach(lock -> lock.unlock());
      records.forEach(r -> {
        if (!r.getKey().isControlMessage()) {
          lockManager.releaseLock(ByteArrayKey.wrap(r.getKey().getKey()));
        }
      });
    }
  }

  public static boolean isAllMessagesFromRTTopic(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records) {
    for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record: records) {
      if (!record.getTopicPartition().getPubSubTopic().isRealTime()) {
        return false;
      }
    }
    return true;
  }

  public List<PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long>> process(
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      PartitionConsumptionState partitionConsumptionState,
      int partition,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs) {
    long currentTimestampInNs = System.nanoTime();
    if (records.isEmpty()) {
      return Collections.emptyList();
    }
    AtomicBoolean isAllMessagesFromRTTopic = new AtomicBoolean(true);
    List<PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long>> resultList =
        new ArrayList<>(records.size());
    records.forEach(r -> {
      resultList.add(new PubSubMessageProcessedResultWrapper<>(r));
      if (!r.getTopicPartition().getPubSubTopic().isRealTime()) {
        isAllMessagesFromRTTopic.set(false);
      }
    });
    if (!isWriteComputationEnabled && !isActiveActiveReplicationEnabled) {
      return resultList;
    }
    // Only handle records from the real-time topic
    if (!isAllMessagesFromRTTopic.get()) {
      return resultList;
    }

    /**
     * We would like to process the messages belonging to the same key sequentially to avoid race conditions.
     */
    int totalNumOfRecords = 0;
    Map<ByteArrayKey, List<PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long>>> keyGroupMap =
        new HashMap<>(records.size());

    for (PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> r: resultList) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message = r.getMessage();
      if (!message.getKey().isControlMessage()) {
        ByteArrayKey byteArrayKey = ByteArrayKey.wrap(message.getKey().getKey());
        keyGroupMap.computeIfAbsent(byteArrayKey, (ignored) -> new ArrayList<>()).add(r);
        totalNumOfRecords++;
      }
    }
    aggVersionedIngestionStats
        .recordBatchProcessingRequest(storeName, version, totalNumOfRecords, System.currentTimeMillis());
    hostLevelIngestionStats.recordBatchProcessingRequest(totalNumOfRecords);

    List<CompletableFuture<Void>> futureList = new ArrayList<>(keyGroupMap.size());
    keyGroupMap.forEach((ignored, recordsWithTheSameKey) -> {
      futureList.add(CompletableFuture.runAsync(() -> {
        recordsWithTheSameKey.forEach(recordWithTheSameKey -> {
          recordWithTheSameKey.setProcessedResult(
              processingFunction.apply(
                  recordWithTheSameKey.getMessage(),
                  partitionConsumptionState,
                  partition,
                  kafkaUrl,
                  kafkaClusterId,
                  beforeProcessingRecordTimestampNs,
                  beforeProcessingBatchRecordsTimestampMs));
        });
      }, batchProcessingThreadPool));
    });
    try {
      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).get();
      double requestLatency = LatencyUtils.getElapsedTimeFromNSToMS(currentTimestampInNs);
      aggVersionedIngestionStats
          .recordBatchProcessingLatency(storeName, version, requestLatency, System.currentTimeMillis());
      hostLevelIngestionStats.recordBatchProcessingRequestLatency(requestLatency);
    } catch (Exception e) {
      aggVersionedIngestionStats.recordBatchProcessingRequestError(storeName, version);
      hostLevelIngestionStats.recordBatchProcessingRequestError();
      throw new VeniceException(
          "Failed to execute the batch processing for " + storeVersionName + " partition: "
              + partitionConsumptionState.getPartition(),
          e);
    }

    return resultList;
  }
}
