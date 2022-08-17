package com.linkedin.venice.unit.kafka;

import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;


/**
 * This {@link VenicePartitioner} implementation takes the first byte of the key,
 * and computes the partition as such:
 *
 * firstByte % numPartitions
 *
 * It is meant to be used in tests where we want to control the partition that a
 * key belongs to, but without needing to work out a proper input that yields the
 * desired partition when hashed by the
 * {@link com.linkedin.venice.partitioner.DefaultVenicePartitioner}.
 */
public class SimplePartitioner extends VenicePartitioner {
  public SimplePartitioner() {
    super();
  }

  public SimplePartitioner(VeniceProperties props) {
    this(props, null);
  }

  public SimplePartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  private int getPartitionId(byte firstByte, int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("numPartitions must be greater than 0!");
    }
    return firstByte % numPartitions;
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return getPartitionId(keyBytes[0], numPartitions);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    keyByteBuffer.mark();
    int partitionId = getPartitionId(keyByteBuffer.get(), numPartitions);
    keyByteBuffer.reset();
    return partitionId;
  }
}
