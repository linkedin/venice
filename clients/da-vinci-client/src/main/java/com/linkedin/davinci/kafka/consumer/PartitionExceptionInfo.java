package com.linkedin.davinci.kafka.consumer;

/** Measurable wrapper for exception, partition id pair */
class PartitionExceptionInfo {
  private final Exception e;
  private final int partitionId;
  private final boolean replicaCompleted;
  /** The PubSub broker URL where the exception originated, or null if unknown. */
  private final String pubSubExceptionSourceUrl;

  public PartitionExceptionInfo(Exception e, int partitionId, boolean replicaCompleted) {
    this(e, partitionId, replicaCompleted, null);
  }

  public PartitionExceptionInfo(
      Exception e,
      int partitionId,
      boolean replicaCompleted,
      String pubSubExceptionSourceUrl) {
    this.e = e;
    this.partitionId = partitionId;
    this.replicaCompleted = replicaCompleted;
    this.pubSubExceptionSourceUrl = pubSubExceptionSourceUrl;
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

  public String getPubSubExceptionSourceUrl() {
    return pubSubExceptionSourceUrl;
  }
}
