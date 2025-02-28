package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class is used to process the AA/WC messages in parallel to address the long-tail partition lagging issues.
 * For the AA/WC message handling, the consumption is not the bottleneck, but the processing overhead, and
 * even with a single consumer, with {@link IngestionBatchProcessor}, we hope we can utilize the full node's
 * resources to speed up the leader ingestion.
 */
public class IngestionBatchProcessor {
  private static final TreeMap EMPTY_TREE_MAP = new TreeMap();

  interface ProcessingFunction {
    PubSubMessageProcessedResult apply(
        DefaultPubSubMessage consumerRecord,
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

  // For testing
  KeyLevelLocksManager getLockManager() {
    return this.lockManager;
  }

  /**
   * When {@link #lockManager} is not null, this function will try to lock all the keys
   * (except Control Messages) passed by the params.
   */
  public NavigableMap<ByteArrayKey, ReentrantLock> lockKeys(List<DefaultPubSubMessage> records) {
    if (lockManager != null) {
      /**
       * Need to use a {@link TreeMap} to make sure the locking will be executed in a deterministic order, otherwise
       * deadlock can happen.
       * Considering there could be multiple consumers, which are executing this function concurrently, and if they
       * are trying to lock the same set of keys with different orders, deadlock can happen.
       */
      TreeMap<ByteArrayKey, ReentrantLock> keyLockMap = new TreeMap<>();
      records.forEach(r -> {
        if (!r.getKey().isControlMessage()) {
          keyLockMap.computeIfAbsent(ByteArrayKey.wrap(r.getKey().getKey()), k -> lockManager.acquireLockByKey(k));
        }
      });
      keyLockMap.forEach((k, v) -> v.lock());
      return keyLockMap;
    }
    return Collections.emptyNavigableMap();
  }

  public void unlockKeys(NavigableMap<ByteArrayKey, ReentrantLock> keyLockMap) {
    if (lockManager != null) {
      keyLockMap.descendingMap().forEach((key, lock) -> {
        lock.unlock();
        lockManager.releaseLock(key);
      });
    }
  }

  public static boolean isAllMessagesFromRTTopic(Iterable<DefaultPubSubMessage> records) {
    for (DefaultPubSubMessage record: records) {
      if (!record.getTopicPartition().getPubSubTopic().isRealTime()) {
        return false;
      }
    }
    return true;
  }

  public List<PubSubMessageProcessedResultWrapper> process(
      List<DefaultPubSubMessage> records,
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
    boolean isAllMessagesFromRTTopic = true;
    List<PubSubMessageProcessedResultWrapper> resultList = new ArrayList<>(records.size());
    /**
     * We would like to process the messages belonging to the same key sequentially to avoid race conditions.
     */
    int totalNumOfRecords = 0;
    Map<ByteArrayKey, List<PubSubMessageProcessedResultWrapper>> keyGroupMap = new HashMap<>(records.size());

    for (DefaultPubSubMessage message: records) {
      if (!message.getTopicPartition().getPubSubTopic().isRealTime()) {
        isAllMessagesFromRTTopic = false;
      }
      PubSubMessageProcessedResultWrapper resultWrapper = new PubSubMessageProcessedResultWrapper(message);
      resultList.add(resultWrapper);
      if (!message.getKey().isControlMessage() && isAllMessagesFromRTTopic) {
        ByteArrayKey byteArrayKey = ByteArrayKey.wrap(message.getKey().getKey());
        keyGroupMap.computeIfAbsent(byteArrayKey, (ignored) -> new ArrayList<>()).add(resultWrapper);
        totalNumOfRecords++;
      }
    }
    if (!isWriteComputationEnabled && !isActiveActiveReplicationEnabled) {
      return resultList;
    }
    // Only handle records from the real-time topic
    if (!isAllMessagesFromRTTopic) {
      return resultList;
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
