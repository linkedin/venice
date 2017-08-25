package com.linkedin.venice.kafka.consumer;

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
  private int processedRecordNumSinceLastSync;

  public PartitionConsumptionState(int partition, OffsetRecord offsetRecord, boolean hybrid) {
    this.partition = partition;
    this.hybrid = hybrid;
    this.offsetRecord = offsetRecord;
    this.errorReported = false;
    this.lagCaughtUp = false;
    this.completionReported = false;
    this.processedRecordNum = 0;
    this.processedRecordSize = 0;
    this.processedRecordNumSinceLastSync = 0;
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
    return isEndOfPushReceived() && (hybrid == lagCaughtUp);
  }
  public boolean isWaitingForReplicationLag() {
    return isEndOfPushReceived() && hybrid && !lagCaughtUp;
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
  public void incrProcessedRecordNum() {
    ++this.processedRecordNum;
  }
  public int getProcessedRecordNum() {
    return this.processedRecordNum;
  }
  public void resetProcessedRecordNum() {
    this.processedRecordNum = 0;
  }
  public void incrProcessedRecordSize(int recordSize) {
    this.processedRecordSize += recordSize;
  }
  public int getProcessedRecordSize() {
    return this.processedRecordSize;
  }
  public void resetProcessedRecordSize() {
    this.processedRecordSize = 0;
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
        (hybrid ? ", lagCaughtUp=" + lagCaughtUp : "") +
        ", processedRecordNum=" + processedRecordNum +
        ", processedRecordSize=" + processedRecordSize +
        '}';
  }
  public int getProcessedRecordNumSinceLastSync() {
    return this.processedRecordNumSinceLastSync;
  }
  public void incrProcessedRecordNumSinceLastSync() {
    ++this.processedRecordNumSinceLastSync;
  }
  public void resetProcessedRecordNumSinceLastSync() {
    this.processedRecordNumSinceLastSync = 0;
  }
}
