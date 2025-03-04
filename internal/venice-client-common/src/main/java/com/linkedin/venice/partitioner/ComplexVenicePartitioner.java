package com.linkedin.venice.partitioner;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Determines partitioning but offers more complex partitioning API that could partition not just based on "immutable"
 * fields of the value. In addition, it provides the option to partition a record to multiple partitions.
 */
public abstract class ComplexVenicePartitioner extends VenicePartitioner {
  public ComplexVenicePartitioner() {
    super();
  }

  public ComplexVenicePartitioner(VeniceProperties props) {
    super(props);
  }

  public ComplexVenicePartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  /**
   * A complex partitioner API that could be used in materialized views to partition based on value. The resulting
   * partition could also be an array of partition ids instead of just a single partition (one-to-many).
   * @param value that will be mapped to partition(s)
   * @param numPartitions of total available partitions
   * @return int array containing the partition id(s)
   */
  public abstract int[] getPartitionId(byte[] keyBytes, GenericRecord value, int numPartitions);

  @Override
  public VenicePartitionerType getPartitionerType() {
    return VenicePartitionerType.COMPLEX;
  }
}
