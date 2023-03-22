package com.linkedin.venice.meta;

import java.util.List;


public class UncompletedPartition {
  private int partitionId;
  private List<UncompletedReplica> uncompletedReplicas;

  public UncompletedPartition() {
  }

  public UncompletedPartition(int partitionId, List<UncompletedReplica> uncompletedReplicas) {
    this.partitionId = partitionId;
    this.uncompletedReplicas = uncompletedReplicas;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public List<UncompletedReplica> getUncompletedReplicas() {
    return uncompletedReplicas;
  }

  public void setUncompletedReplicas(List<UncompletedReplica> uncompletedReplicas) {
    this.uncompletedReplicas = uncompletedReplicas;
  }

  @Override
  public String toString() {
    return "UncompletedPartition{" + "partitionId=" + partitionId + ", uncompletedReplicas=" + uncompletedReplicas
        + "}";
  }
}
