package com.linkedin.venice.hadoop.mapreduce.datawriter.reduce;

import com.linkedin.venice.partitioner.ComplexVenicePartitioner;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Dummy complex venice partitioner used for unit tests. Cannot use private static class because {@link PartitionUtils}
 * uses {@link ReflectUtils#loadClass(String)}.
 */
public class TestVeniceReducerComplexPartitioner extends ComplexVenicePartitioner {
  public TestVeniceReducerComplexPartitioner() {
    super();
  }

  public TestVeniceReducerComplexPartitioner(VeniceProperties props) {
    super(props);
  }

  public TestVeniceReducerComplexPartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  @Override
  public int[] getPartitionId(byte[] keyBytes, GenericRecord value, int numPartitions) {
    return new int[0];
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
