package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.partitioner.VenicePartitioner;


public class PartitionerPair {
  private final VenicePartitioner venicePartitioner;
  private final int partitionCount;

  public PartitionerPair(VenicePartitioner venicePartitioner, int partitionCount) {
    this.venicePartitioner = venicePartitioner;
    this.partitionCount = partitionCount;
  }

  public VenicePartitioner getVenicePartitioner() {
    return venicePartitioner;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  @Override
  public final int hashCode() {
    final int PRIME = 31;
    int result = 1;
    result = PRIME * result + venicePartitioner.hashCode();
    result = PRIME * result + partitionCount;
    return result;
  }

  @Override
  public final boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof PartitionerPair)) {
      return false;
    }
    final PartitionerPair partitionerPair = (PartitionerPair) obj;
    return venicePartitioner.equals(partitionerPair.venicePartitioner)
        && partitionCount == partitionerPair.partitionCount;
  }
}
