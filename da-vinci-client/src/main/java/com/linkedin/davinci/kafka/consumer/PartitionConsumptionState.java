package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.SubPartitionStatus;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;


/**
 * This class is used to maintain internal state for consumption of each partition.
 */
public class PartitionConsumptionState {
  private final int partition;
  private final int amplificationFactor;
  private final int userPartition;
  private final boolean hybrid;
  private final boolean isIncrementalPushEnabled;
  private final IncrementalPushPolicy incrementalPushPolicy;
  private OffsetRecord offsetRecord;
  /** whether the ingestion of current partition is deferred-write. */
  private boolean deferredWrite;
  private boolean errorReported;
  private boolean lagCaughtUp;
  private boolean completionReported;
  private boolean isSubscribed;
  private LeaderFollowerStateType leaderFollowerState;

  /**
   * Only used in L/F model. Check if the partition has released the latch.
   * In L/F ingestion task, Optionally, the state model holds a latch that
   * is used to determine when it should transit from offline to follower.
   * The latch is only placed if the Helix resource is serving the read traffic.
   *
   * See {@link LeaderFollowerPartitionStateModel} for the
   * details why we need latch for certain resources.
   */
  private boolean isLatchReleased = false;

  /**
   * This future is completed in drainer thread after persisting the associated record and offset to DB.
   */
  private volatile Future<Void> lastLeaderPersistFuture = null;

  /**
   * In-memory cache for the TopicSwitch in {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState};
   * make sure to keep the in-memory state and StoreVersionState in sync.
   */
  private TopicSwitch topicSwitch = null;

  /**
   * The following priorities are used to store the progress of processed records since it is not efficient to
   * update offset db for every record.
   */
  private int processedRecordNum;
  private int processedRecordSize;
  private long processedRecordSizeSinceLastSync;

  /**
   * An in-memory state to track whether the leader consumer is consuming from remote or not; it will be updated with
   * correct value during ingestion.
   */
  private boolean consumeRemotely;

  private Optional<CheckSum> expectedSSTFileChecksum;

  private long latestMessageConsumptionTimestampInMs;

  /**
   * An in-memory state to record the ingestion start time of this partition; in O/O state model, the push timeout clock
   * is reset whenever a new state transition from BOOTSTRAP to ONLINE happens; to achieve a parity feature in L/F state
   * model, the push timeout clock will also reset when the partition consumption state is created for a new partition
   * and check timeout in the consumption state check function which runs regularly:
   * {@link LeaderFollowerStoreIngestionTask#checkLongRunningTaskState()}
   */
  private final long consumptionStartTimeInMs;

  /**
   * This hash map will keep a temporary mapping between a key and it's value.
   * get {@link #getTransientRecord(byte[])} and put {@link #setTransientRecord(long, byte[], byte[], int, int, int)}
   * operation on this map will be invoked from from kafka consumer thread.
   * delete {@link #mayRemoveTransientRecord(long, byte[])} operation will be invoked from drainer thread after persisting it in DB.
   * because of the properties of the above operations the caller is guaranteed to get the latest value for a key either from
   * this map or from the DB.
   */
  private final ConcurrentMap<ByteBuffer, TransientRecord> transientRecordMap = new VeniceConcurrentHashMap<>();

  /**
   * In-memory hash set which keeps track of all previous status this sub-partition has reported. It is the in-memory
   * cache of the previousStatuses field in {@link com.linkedin.venice.kafka.protocol.state.PartitionState} inside
   * {@link OffsetRecord}.
   * The reason to have this HashSet is to maintain correctness and efficiency for the sub-partition status report
   * condition checking. The previousStatus is a generated CharSequence to CharSequence map and we need to iterate over
   * all the records in it to in order to compare with incoming status string. Without explicit locking, this iteration
   * might throw {@link java.util.ConcurrentModificationException} in multi-thread environments.
   */
  private final Set<SubPartitionStatus> previousStatusSet = VeniceConcurrentHashMap.newKeySet();

  /**
   * This field is used to track whether the last queued record has been fully processed or not.
   * For Leader role, it is redundant from {@literal ProducedRecord#persistedToDBFuture} since it is tracking
   * the completeness end to end, since this field won't be set.
   * This field is useful for O/B/O model or for follower role since the server doesn't need to produce to local Kafka.
   */
  private CompletableFuture<Void> lastQueuedRecordPersistedFuture;

  /**
   * An in-memory state to track whether leader should skip processing the Kafka message. Leader will skip only if the
   * flag is set to true. For example, leader in remote fabric will skip SOBR after EOP in remote VT.
   */
  private boolean skipKafkaMessage = false;

  /**
   * This is an in-memory only map which will track the consumed offset from each kafka cluster. Currently used in
   * measuring hybrid offset lag for each prod region during RT consumption.
   */
  private final ConcurrentMap<String, Long> consumedUpstreamRTOffsetMap = new VeniceConcurrentHashMap<>();

  //stores the SOP control message's producer timestamp.
  private long startOfPushTimestamp = 0;

  //stores the EOP control message's producer timestamp.
  private long endOfPushTimestamp = 0;

  public PartitionConsumptionState(int partition, int amplificationFactor, OffsetRecord offsetRecord, boolean hybrid,
    boolean isIncrementalPushEnabled, IncrementalPushPolicy incrementalPushPolicy) {
    this.partition = partition;
    this.amplificationFactor = amplificationFactor;
    this.userPartition = PartitionUtils.getUserPartition(partition, amplificationFactor);
    this.hybrid = hybrid;
    this.isIncrementalPushEnabled = isIncrementalPushEnabled;
    this.incrementalPushPolicy = incrementalPushPolicy;
    this.offsetRecord = offsetRecord;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.completionReported = false;
    this.isSubscribed = true;
    this.processedRecordNum = 0;
    this.processedRecordSize = 0;
    this.processedRecordSizeSinceLastSync = 0;
    this.leaderFollowerState = LeaderFollowerStateType.STANDBY;
    this.expectedSSTFileChecksum = Optional.empty();
    /**
     * Initialize the latest consumption time with current time; otherwise, it's 0 by default
     * and leader will be promoted immediately.
     */
    this.latestMessageConsumptionTimestampInMs = System.currentTimeMillis();
    this.consumptionStartTimeInMs = System.currentTimeMillis();

    // Restore previous status from offset record.
    for (CharSequence status : offsetRecord.getSubPartitionStatus().keySet()) {
      previousStatusSet.add(SubPartitionStatus.valueOf(status.toString()));
    }
  }

  public int getPartition() {
    return this.partition;
  }
  public int getUserPartition() {
    return userPartition;
  }
  public int getAmplificationFactor() {
    return this.amplificationFactor;
  }
  public void setOffsetRecord(OffsetRecord offsetRecord) {
    this.offsetRecord = offsetRecord;
  }
  public OffsetRecord getOffsetRecord() {
    return this.offsetRecord;
  }
  public void setDeferredWrite(boolean deferredWrite) {
    this.deferredWrite = deferredWrite;
  }
  public boolean isDeferredWrite() {
    return this.deferredWrite;
  }
  public boolean isStarted() {
    return this.offsetRecord.getLocalVersionTopicOffset() > 0;
  }
  public boolean isEndOfPushReceived() {
    return this.offsetRecord.isEndOfPushReceived();
  }
  public boolean isWaitingForReplicationLag() {
    return isEndOfPushReceived() && !lagCaughtUp;
  }
  public void lagHasCaughtUp() {
    this.lagCaughtUp = true;
  }
  public boolean isCompletionReported() {
    return completionReported;
  }
  public void completionReported() {
    this.completionReported = true;
  }
  public boolean isSubscribed() {
    return isSubscribed;
  }
  public void unsubscribe() {
    this.isSubscribed = false;
  }
  public boolean isLatchReleased() {
    return isLatchReleased;
  }
  public void releaseLatch() {
    this.isLatchReleased = true;
  }
  public void errorReported() {
    this.errorReported = true;
  }
  public boolean isErrorReported() {
    return this.errorReported;
  }
  public void incrementProcessedRecordNum() {
    ++this.processedRecordNum;
  }
  public boolean isComplete() {
    if (!isEndOfPushReceived()) {
      return false;
    }

    //for regular push store, receiving EOP is good to go
    return (!isIncrementalPushEnabled && !hybrid) || lagCaughtUp;
  }

  /**
   * This value is not reliable for Hybrid ingest.  Check its current usage before relying on it.
   * @return
   */
  public int getProcessedRecordNum() {
    return this.processedRecordNum;
  }
  public void resetProcessedRecordNum() {
    this.processedRecordNum = 0;
  }
  public void incrementProcessedRecordSize(int recordSize) {
    this.processedRecordSize += recordSize;
  }
  public int getProcessedRecordSize() {
    return this.processedRecordSize;
  }
  public void resetProcessedRecordSize() {
    this.processedRecordSize = 0;
  }
  public IncrementalPush getIncrementalPush() {
    return this.offsetRecord.getIncrementalPush();
  }
  public void setIncrementalPush(IncrementalPush ip) {
    this.offsetRecord.setIncrementalPush(ip);
  }

  public boolean isHybrid() {
    return hybrid;
  }

  public boolean isBatchOnly() {
    return !isHybrid() && !isIncrementalPushEnabled();
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("PartitionConsumptionState{")
        .append("partition=").append(partition)
        .append(", hybrid=").append(hybrid)
        .append(", offsetRecord=" ).append(offsetRecord)
        .append(", errorReported=").append(errorReported)
        .append(", started=").append(isStarted())
        .append(", lagCaughtUp=").append(lagCaughtUp)
        .append(", processedRecordNum=").append(processedRecordNum)
        .append(", processedRecordSize=").append(processedRecordSize)
        .append(", processedRecordSizeSinceLastSync=").append(processedRecordSizeSinceLastSync)
        .append(", leaderFollowerState=").append(leaderFollowerState)
        .append(", isIncrementalPushEnabled=").append(isIncrementalPushEnabled)
        .append(", incrementalPushPolicy=").append(incrementalPushPolicy)
        .append("}").toString();
  }

  public long getProcessedRecordSizeSinceLastSync() {
    return this.processedRecordSizeSinceLastSync;
  }
  public void incrementProcessedRecordSizeSinceLastSync(int recordSize) {
    this.processedRecordSizeSinceLastSync += recordSize;
  }
  public void resetProcessedRecordSizeSinceLastSync() {
    this.processedRecordSizeSinceLastSync = 0;
  }

  public boolean isIncrementalPushEnabled() {
    return isIncrementalPushEnabled;
  }

  public IncrementalPushPolicy getIncrementalPushPolicy() { return incrementalPushPolicy; }

  public void setLeaderFollowerState(LeaderFollowerStateType state) {
    this.leaderFollowerState = state;
  }

  public LeaderFollowerStateType getLeaderFollowerState() {
    return this.leaderFollowerState;
  }

  public void setLastLeaderPersistFuture(Future<Void> future) {
    this.lastLeaderPersistFuture = future;
  }

  public Future<Void> getLastLeaderPersistFuture() {
    return this.lastLeaderPersistFuture;
  }

  public CompletableFuture<Void> getLastQueuedRecordPersistedFuture() {
    return lastQueuedRecordPersistedFuture;
  }

  public void setLastQueuedRecordPersistedFuture(CompletableFuture<Void> lastQueuedRecordPersistedFuture) {
    this.lastQueuedRecordPersistedFuture = lastQueuedRecordPersistedFuture;
  }

  /**
   * Update the in-memory state for TopicSwitch whenever encounter a new TopicSwitch message or after a restart.
   */
  public void setTopicSwitch(TopicSwitch topicSwitch) {
    this.topicSwitch = topicSwitch;
  }

  public TopicSwitch getTopicSwitch() {
    return this.topicSwitch;
  }

  public void setConsumeRemotely(boolean isConsumingRemotely) {
    this.consumeRemotely = isConsumingRemotely;
  }

  public boolean consumeRemotely() {
    return consumeRemotely;
  }

  public void initializeExpectedChecksum() {
    this.expectedSSTFileChecksum = CheckSum.getInstance(CheckSumType.MD5);
  }

  public void finalizeExpectedChecksum() {
    this.expectedSSTFileChecksum = Optional.empty();
  }

  /**
   * Keep updating the checksum for key/value pair received from kafka PUT message.
   * If the checksum instance is not configured via {@link PartitionConsumptionState#initializeExpectedChecksum} then do nothing.
   * This api will keep the caller's code clean.
   * @param key
   * @param put
   */
  public void maybeUpdateExpectedChecksum(byte[] key, Put put) {
    if (!expectedSSTFileChecksum.isPresent()) {
      return;
    }
    expectedSSTFileChecksum.get().update(key);
    ByteBuffer putValue = put.putValue;
    expectedSSTFileChecksum.get().update(put.schemaId);
    expectedSSTFileChecksum.get().update(putValue.array(), putValue.position(), putValue.remaining());
  }

  public void resetExpectedChecksum() {
    expectedSSTFileChecksum.get().reset();
  }

  public byte[] getExpectedChecksum() {
    return expectedSSTFileChecksum.get().getCheckSum();
  }

  public long getLatestMessageConsumptionTimestampInMs() {
    return latestMessageConsumptionTimestampInMs;
  }

  public void setLatestMessageConsumptionTimestampInMs(long consumptionTimestampInMs) {
    this.latestMessageConsumptionTimestampInMs = consumptionTimestampInMs;
  }

  public long getConsumptionStartTimeInMs() {
    return consumptionStartTimeInMs;
  }

  public void setTransientRecord(long kafkaConsumedOffset, byte[] key, ByteBuffer replicationMetadata) {
    setTransientRecord(kafkaConsumedOffset, key, null, -1, -1, -1, replicationMetadata);
  }

  public void setTransientRecord(long kafkaConsumedOffset, byte[] key, byte[] value, int valueOffset, int valueLen, int valueSchemaId, ByteBuffer replicationMetadata) {
    transientRecordMap.put(ByteBuffer.wrap(key), new TransientRecord(value, valueOffset, valueLen, valueSchemaId, kafkaConsumedOffset, replicationMetadata));
  }

  public TransientRecord getTransientRecord(byte[] key) {
    return transientRecordMap.get(ByteBuffer.wrap(key));
  }

  /**
   * This operation is performed atomically to delete the record only when the provided sourceOffset matches.
   * @param key
   * @param kafkaConsumedOffset
   * @return
   */
  public TransientRecord mayRemoveTransientRecord(long kafkaConsumedOffset, byte[] key) {
    TransientRecord removed = transientRecordMap.computeIfPresent(ByteBuffer.wrap(key), (k, v) -> {
      if (v.kafkaConsumedOffset == kafkaConsumedOffset) {
        return null;
      } else {
        return v;
      }
    });
    return removed;
  }

  public int getSourceTopicPartition(String topic) {
    if (Version.isRealTimeTopic(topic)) {
      return getUserPartition();
    } else {
      return getPartition();
    }
  }

  public int getTransientRecordMapSize() {
    return transientRecordMap.size();
  }

  public boolean hasSubPartitionStatus(SubPartitionStatus subPartitionStatus) {
    return previousStatusSet.contains(subPartitionStatus);
  }

  public void recordSubPartitionStatus(SubPartitionStatus subPartitionStatus) {
    if (this.getOffsetRecord() != null) {
      this.getOffsetRecord().recordSubPartitionStatus(subPartitionStatus);
    }
    previousStatusSet.add(subPartitionStatus);
  }

  public boolean skipKafkaMessage() {
    return this.skipKafkaMessage;
  }

  public void setSkipKafkaMessage(boolean skipKafkaMessage) {
    this.skipKafkaMessage = skipKafkaMessage;
  }

  /**
   * This immutable class holds a association between a key and  value and the source offset of the consumed message.
   * The value could be either as received in kafka ConsumerRecord or it could be a write computed value.
   */
  public static class TransientRecord {
    private final byte[] value;
    private final int valueOffset;
    private final int valueLen;
    private final int valueSchemaId;
    private final long kafkaConsumedOffset;
    private final ByteBuffer replicationMetadata;

    TransientRecord(byte[] value, int valueOffset, int valueLen, int valueSchemaId, long kafkaConsumedOffset, ByteBuffer replicationMetadata) {
      this.value = value;
      this.valueOffset = valueOffset;
      this.valueLen = valueLen;
      this.valueSchemaId = valueSchemaId;
      this.kafkaConsumedOffset = kafkaConsumedOffset;
      this.replicationMetadata = replicationMetadata;
    }

    public byte[] getValue() {return value;}
    public int getValueOffset() {return valueOffset;}
    public int getValueLen() {return valueLen;}
    public int getValueSchemaId() {return valueSchemaId;}
    public ByteBuffer getReplicationMetadata() {return replicationMetadata;}

  }

  public void updateLeaderConsumedUpstreamRTOffset(String kafkaUrl, long offset) {
    consumedUpstreamRTOffsetMap.put(kafkaUrl, offset);
  }

  public long getLeaderConsumedUpstreamRTOffset(String kafkaUrl) {
    return consumedUpstreamRTOffsetMap.getOrDefault(kafkaUrl, 0L);
  }

  public void setStartOfPushTimestamp(long startOfPushTimestamp) {
    this.startOfPushTimestamp = startOfPushTimestamp;
  }

  public long getStartOfPushTimestamp() {return startOfPushTimestamp;}

  public void setEndOfPushTimestamp(long endOfPushTimestamp) {
    this.endOfPushTimestamp = endOfPushTimestamp;
  }

  public long getEndOfPushTimestamp() {return endOfPushTimestamp;}
}
