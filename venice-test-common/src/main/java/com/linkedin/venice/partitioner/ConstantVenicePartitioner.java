package com.linkedin.venice.partitioner;

import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;


public class ConstantVenicePartitioner extends VenicePartitioner {
  public static final String CONSTANT_PARTITION = "constant.partition";
  private final int partitionId;

  public ConstantVenicePartitioner(VeniceProperties properties) {
    super(properties);
    partitionId = properties.getInt(CONSTANT_PARTITION);
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return partitionId;
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return getPartitionId(keyByteBuffer.array(), numPartitions);
  }
}
