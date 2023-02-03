package com.linkedin.davinci.kafka.consumer;

/** Measurable wrapper for exception, partition id pair */
class PartitionExceptionInfo {
  private final Exception e;
  private final int partitionId;
  private final boolean replicaCompleted;

  public PartitionExceptionInfo(Exception e, int partitionId, boolean replicaCompleted) {
    this.e = e;
    this.partitionId = partitionId;
    this.replicaCompleted = replicaCompleted;
  }

  public Exception getException() {
    return e;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public boolean isReplicaCompleted() {
    return replicaCompleted;
  }
}
