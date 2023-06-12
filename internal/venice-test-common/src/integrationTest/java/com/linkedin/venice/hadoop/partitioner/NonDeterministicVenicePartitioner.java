package com.linkedin.venice.hadoop.partitioner;

import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.avro.Schema;


/**
 * A class that sprays keys at random across every partitions.
 */
public class NonDeterministicVenicePartitioner extends VenicePartitioner {
  private Random random = new Random();

  public NonDeterministicVenicePartitioner() {
    super();
  }

  public NonDeterministicVenicePartitioner(VeniceProperties props) {
    super(props);
  }

  public NonDeterministicVenicePartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return random.nextInt(numPartitions);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return random.nextInt(numPartitions);
  }
}
