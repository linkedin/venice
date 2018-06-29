package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.offsets.OffsetRecord;

/**
 * This class is used to maintain internal state for consumption of each partition.
 */
class PartitionConsumptionState {
  private final int partition;
  private final boolean hybrid;
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

  private long lastTimeOfSourceTopicOffsetLookup;
  private long sourceTopicMaxOffset;

  public void setSourceTopicMaxOffset(long sourceTopicMaxOffset) {
    this.sourceTopicMaxOffset = sourceTopicMaxOffset;
  }

  public long getSourceTopicMaxOffset() {

    return sourceTopicMaxOffset;
  }

  public PartitionConsumptionState(int partition, OffsetRecord offsetRecord, boolean hybrid) {
    this.partition = partition;
    this.hybrid = hybrid;
    this.offsetRecord = offsetRecord;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.completionReported = false;
    this.processedRecordNum = 0;
    this.processedRecordSize = 0;
    this.processedRecordSizeSinceLastSync = 0;
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
  public boolean isComplete() {
    return isEndOfPushReceived() && lagCaughtUp;
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
}
