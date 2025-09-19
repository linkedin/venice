package com.linkedin.venice.view;

import com.linkedin.venice.partitioner.ComplexVenicePartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class TestValueBasedVenicePartitioner extends ComplexVenicePartitioner {
  public TestValueBasedVenicePartitioner() {
    super();
  }

  public TestValueBasedVenicePartitioner(VeniceProperties props) {
    super(props);
  }

  public TestValueBasedVenicePartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  @Override
  public int[] getPartitionId(byte[] keyBytes, GenericRecord value, int numPartitions) {
    int age = (Integer) value.get("age");
    if (age < 0) {
      return new int[0];
    } else if (age < 10) {
      int[] partitions = new int[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        partitions[i] = i;
      }
      return partitions;
    } else {
      int[] partition = new int[1];
      partition[0] = age % numPartitions;
      return partition;
    }
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return 0;
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return 0;
  }
}
