package com.linkedin.venice.metadata;

/**
 * A struct which stores the address to a certain node/partition pair.
 * Created by clfung on 9/17/14.
 */
public class KeyAddress {
  private int nodeId;
  private int partitionId;

  public KeyAddress(int nodeId, int partitionId) {
    this.nodeId = nodeId;
    this.partitionId = partitionId;
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

}
