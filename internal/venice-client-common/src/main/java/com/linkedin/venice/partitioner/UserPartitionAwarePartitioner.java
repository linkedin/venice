package com.linkedin.venice.partitioner;

import java.nio.ByteBuffer;
import java.util.function.IntUnaryOperator;


/**
 * UserPartitionAwarePartitioner takes a partitioner and amplification factor as input params.
 * When partitioning, the partitioner is run twice in a row, once to determine the application partition,
 * and a second time to determine the sub-partition.
 *
 * Algorithm:
 *  subPartition = partition(key, userPartitions) * amplificationFactor + partition(key, amplificationFactor)
 *
 */
public class UserPartitionAwarePartitioner extends VenicePartitioner {
  private VenicePartitioner partitioner;
  private int amplificationFactor;

  public UserPartitionAwarePartitioner(VenicePartitioner partitioner, int amplificationFactor) {
    this.partitioner = partitioner;
    this.amplificationFactor = amplificationFactor;
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int subPartitionCount) {
    return getPartitionId((partitionCount -> partitioner.getPartitionId(keyBytes, partitionCount)), subPartitionCount);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int subPartitionCount) {
    return getPartitionId(
        (partitionCount -> partitioner.getPartitionId(keyByteBuffer, partitionCount)),
        subPartitionCount);
  }

  private int getPartitionId(IntUnaryOperator partitioner, int subPartitionCount) {
    if (subPartitionCount % amplificationFactor != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Sub-partition count %d is not a multiple of amplification factor %d.",
              subPartitionCount,
              amplificationFactor));
    }
    int userPartitionCount = subPartitionCount / amplificationFactor;
    int userPartition = partitioner.applyAsInt(userPartitionCount);
    int offset = partitioner.applyAsInt(amplificationFactor);
    return userPartition * amplificationFactor + offset;
  }
}
