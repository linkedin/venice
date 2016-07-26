package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Instance;


/**
 * Replica is the basic unit to distribute data, replica is belong to a partition and running in a instance.
 */
public class Replica {
  private final Instance instance;
  private final int partitionId;
  private String status;

  public Replica(Instance instance, int partitionId) {
    this.instance = instance;
    this.partitionId = partitionId;
  }

  public Instance getInstance() {
    return instance;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
