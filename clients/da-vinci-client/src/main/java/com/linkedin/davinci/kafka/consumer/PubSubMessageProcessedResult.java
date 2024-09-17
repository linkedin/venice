package com.linkedin.davinci.kafka.consumer;

public class PubSubMessageProcessedResult {
  private final MergeConflictResultWrapper mergeConflictResultWrapper;
  private final WriteComputeResultWrapper writeComputeResultWrapper;

  public PubSubMessageProcessedResult(MergeConflictResultWrapper mergeConflictResultWrapper) {
    this.mergeConflictResultWrapper = mergeConflictResultWrapper;
    this.writeComputeResultWrapper = null;
  }

  public PubSubMessageProcessedResult(WriteComputeResultWrapper writeComputeResultWrapper) {
    this.writeComputeResultWrapper = writeComputeResultWrapper;
    this.mergeConflictResultWrapper = null;
  }

  public MergeConflictResultWrapper getMergeConflictResultWrapper() {
    return mergeConflictResultWrapper;
  }

  public WriteComputeResultWrapper getWriteComputeResultWrapper() {
    return writeComputeResultWrapper;
  }
}
