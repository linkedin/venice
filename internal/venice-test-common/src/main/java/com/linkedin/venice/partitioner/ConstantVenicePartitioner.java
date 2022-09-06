package com.linkedin.venice.partitioner;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;


public class ConstantVenicePartitioner extends VenicePartitioner {
  public static final String CONSTANT_PARTITION = "constant.partition";
  private final int partitionId;

  public ConstantVenicePartitioner(VeniceProperties properties) {
    this(properties, null);
  }

  public ConstantVenicePartitioner(VeniceProperties properties, Schema schema) {
    super(properties, schema);
    partitionId = properties.getInt(CONSTANT_PARTITION);
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    if (numPartitions <= partitionId) {
      throw new VeniceException(String.format("numPartitions must be greater than %d.", partitionId));
    }
    return partitionId;
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return getPartitionId(keyByteBuffer.array(), numPartitions);
  }
}
