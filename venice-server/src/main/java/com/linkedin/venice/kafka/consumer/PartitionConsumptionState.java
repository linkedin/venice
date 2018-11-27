package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.offsets.OffsetRecord;

/**
 * This class is used to maintain internal state for consumption of each partition.
 */
class PartitionConsumptionState {
  private final int partition;
  private final boolean hybrid;
  private final boolean isIncrementalPushEnabled;
  private OffsetRecord offsetRecord;
  /** whether the ingestion of current partition is deferred-write. */
  private boolean deferredWrite;
  private boolean errorReported;
  private boolean lagCaughtUp;
  private boolean completionReported;
  /**
   * The following priorities are used to store the progress of processed records since it is not efficient to
   * update offset db for every record.
   */
  private int processedRecordNum;
  private int processedRecordSize;
  private long processedRecordSizeSinceLastSync;

  /**
   * The last time that the offset record is flushed to disk.
   *
   * There are 2 metrics: processed record size since last sync and the passed time since last sync;
   * whichever meets the threshold first, we flush the offset record to disk.
   */
  private long timestampOfLastSync;

  private long lastTimeOfSourceTopicOffsetLookup;
  private long sourceTopicMaxOffset;

  public void setSourceTopicMaxOffset(long sourceTopicMaxOffset) {
    this.sourceTopicMaxOffset = sourceTopicMaxOffset;
  }

  public long getSourceTopicMaxOffset() {

    return sourceTopicMaxOffset;
  }

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
    this.timestampOfLastSync = System.currentTimeMillis();
    this.lastTimeOfSourceTopicOffsetLookup = -1;
    this.sourceTopicMaxOffset = -1;
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
  public long getLastTimeOfSourceTopicOffsetLookup() {
    return lastTimeOfSourceTopicOffsetLookup;
  }
  public void setLastTimeOfSourceTopicOffsetLookup(long lastTimeOfSourceTopicOffsetLookup) {
    this.lastTimeOfSourceTopicOffsetLookup = lastTimeOfSourceTopicOffsetLookup;
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
        ", timestampOfLastSync=" + timestampOfLastSync +
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

  public long getTimestampOfLastSync() {
    return this.timestampOfLastSync;
  }

  public void resetTimestampOfLastSync() {
    this.timestampOfLastSync = System.currentTimeMillis();
  }

  public boolean isIncrementalPushEnabled() {
    return isIncrementalPushEnabled;
  }
}
