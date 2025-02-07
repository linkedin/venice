package com.linkedin.venice.view;

import com.linkedin.venice.partitioner.VeniceComplexPartitioner;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class TestValueBasedPartitioner extends VeniceComplexPartitioner {
  public TestValueBasedPartitioner() {
    super();
  }

  public TestValueBasedPartitioner(VeniceProperties props) {
    super(props);
  }

  public TestValueBasedPartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  @Override
  public Set<Integer> getPartitionId(Object value, int numPartitions) {
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      int age = (Integer) record.get("age");
      if (age < 0) {
        return Collections.emptySet();
      } else if (age < 10) {
        Set<Integer> integerSet = new HashSet<>();
        for (int i = 0; i < numPartitions; i++) {
          integerSet.add(i);
        }
        return integerSet;
      } else {
        return Collections.singleton(age % numPartitions);
      }
    } else {
      throw new IllegalArgumentException("Expecting value to be a GenericRecord");
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
