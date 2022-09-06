package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.common.Measurable;


/** Measurable wrapper for exception, partition id pair */
class PartitionExceptionInfo implements Measurable {
  private final Exception e;
  private final int partitionId;

  public PartitionExceptionInfo(Exception e, int partitionId) {
    this.e = e;
    this.partitionId = partitionId;
  }

  public Exception getException() {
    return e;
  }

  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public int getSize() {
    int size = 4;
    if (e != null && e.getMessage() != null) {
      size += e.getMessage().length();
    }
    return size;
  }
}
