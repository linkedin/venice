package com.linkedin.venice.partitioner;

import java.nio.ByteBuffer;

/**
 * UserPartitionAwarePartitioner takes a partitioner and amplicatitionFactor as input params.
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
  public int getPartitionId(byte[] keyBytes, int numSubPartitions) {
    return getPartitionId((numPartitions -> partitioner.getPartitionId(keyBytes, numPartitions)), numSubPartitions);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numSubPartitions) {
    return getPartitionId((numPartitions -> partitioner.getPartitionId(keyByteBuffer, numPartitions)), numSubPartitions);
  }

  private int getPartitionId(PartitionGetter partitionGetter, int numSubPartitions) {
    if (numSubPartitions % amplificationFactor != 0) {
      throw new IllegalArgumentException(
          String.format("numSubPartitions %d is not a multiple of amplificationFactor %d.",
              numSubPartitions, amplificationFactor));
    }
    int numUserPartitions = numSubPartitions / amplificationFactor;
    int userPartition = partitionGetter.get(numUserPartitions);
    int offset = partitionGetter.get(amplificationFactor);
    return userPartition * amplificationFactor + offset;
  }

  @FunctionalInterface
  private interface PartitionGetter {
    int get(int numPartitions);
  }
}
