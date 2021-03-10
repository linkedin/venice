package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;


/**
 * This class is used to maintain internal state for consumption of each partition.
 */
public class PartitionConsumptionState {
  private final int partition;
  private final boolean hybrid;
  private final boolean isIncrementalPushEnabled;
  private OffsetRecord offsetRecord;
  /** whether the ingestion of current partition is deferred-write. */
  private boolean deferredWrite;
  private boolean errorReported;
  private boolean lagCaughtUp;
  private boolean completionReported;
  private LeaderFollowerStateType leaderState;

  /**
   * Only used in L/F model. Check if the partition has released the latch.
   * In L/F ingestion task, Optionally, the state model holds a latch that
   * is used to determine when it should transit from offline to follower.
   * The latch is only placed if the Helix resource is serving the read traffic.
   *
   * See {@link com.linkedin.davinci.helix.LeaderFollowerParticipantModel} for the
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
   * This hash map will keep a temporary mapping between a key and it's value.
   * get {@link #getTransientRecord(byte[])} and put {@link ##setTransientRecord(long, byte[], byte[], int, int, int)}
   * operation on this map will be invoked from from kafka consumer thread.
   *
   * delete {@link #mayRemoveTransientRecord(long, byte[])} operation will be invoked from drainer thread after persisting it in DB.
   *
   * because of the properties of the above operations the caller is gurranted to get the latest value for a key either from
   * this map or from the DB.
   */
  private ConcurrentMap<ByteBuffer, TransientRecord> transientRecordMap = new VeniceConcurrentHashMap<>();


  public PartitionConsumptionState(int partition, OffsetRecord offsetRecord, boolean hybrid, boolean isIncrementalPushEnabled) {
    this.partition = partition;
    this.hybrid = hybrid;
    this.isIncrementalPushEnabled = isIncrementalPushEnabled;
    this.offsetRecord = offsetRecord;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.completionReported = false;
    this.processedRecordNum = 0;
    this.processedRecordSize = 0;
    this.processedRecordSizeSinceLastSync = 0;
    this.leaderState = LeaderFollowerStateType.STANDBY;
    this.expectedSSTFileChecksum = Optional.empty();
    /**
     * Initialize the latest consumption time with current time; otherwise, it's 0 by default
     * and leader will be promoted immediately.
     */
    latestMessageConsumptionTimestampInMs = System.currentTimeMillis();
  }

  public int getPartition() {
    return this.partition;
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
    return this.offsetRecord.getOffset() > 0;
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
    return "PartitionConsumptionState{" +
        "partition=" + partition +
        ", hybrid=" + hybrid +
        ", offsetRecord=" + offsetRecord +
        ", errorReported=" + errorReported +
        ", started=" + isStarted() +
        ", lagCaughtUp=" + lagCaughtUp +
        ", processedRecordNum=" + processedRecordNum +
        ", processedRecordSize=" + processedRecordSize +
        ", processedRecordSizeSinceLastSync=" + processedRecordSizeSinceLastSync +
        '}';
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

  public void setLeaderState(LeaderFollowerStateType state) {
    this.leaderState = state;
  }

  public LeaderFollowerStateType getLeaderState() {
    return this.leaderState;
  }

  public void setLastLeaderPersistFuture(Future<Void> future) {
    this.lastLeaderPersistFuture = future;
  }

  public Future<Void> getLastLeaderPersistFuture() {
    return this.lastLeaderPersistFuture;
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

  public void setTransientRecord(long kafkaConsumedOffset, byte[] key) {
    setTransientRecord(kafkaConsumedOffset, key, null, -1, -1, -1);
  }

  public void setTransientRecord(long kafkaConsumedOffset, byte[] key, byte[] value, int valueOffset, int valueLen, int valueSchemaId) {
    transientRecordMap.put(ByteBuffer.wrap(key), new TransientRecord(value, valueOffset, valueLen, valueSchemaId, kafkaConsumedOffset));
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

  public int getTransientRecordMapSize() {
    return transientRecordMap.size();
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

    TransientRecord(byte[] value, int valueOffset, int valueLen, int valueSchemaId, long kafkaConsumedOffset) {
      this.value = value;
      this.valueOffset = valueOffset;
      this.valueLen = valueLen;
      this.valueSchemaId = valueSchemaId;
      this.kafkaConsumedOffset = kafkaConsumedOffset;
    }

    public byte[] getValue() {return value;}
    public int getValueOffset() {return valueOffset;}
    public int getValueLen() {return valueLen;}
    public int getValueSchemaId() {return valueSchemaId;}

  }
}
