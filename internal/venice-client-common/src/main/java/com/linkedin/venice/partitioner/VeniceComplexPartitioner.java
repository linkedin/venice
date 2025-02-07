package com.linkedin.venice.partitioner;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Set;
import org.apache.avro.Schema;


/**
 * Determines partitioning but offers more complex partitioning API that could partition not just based on "immutable"
 * fields of the value. In addition, it provides the option to partition a record to multiple partitions.
 */
public abstract class VeniceComplexPartitioner extends VenicePartitioner {
  public VeniceComplexPartitioner() {
    super();
  }

  public VeniceComplexPartitioner(VeniceProperties props) {
    super(props);
  }

  public VeniceComplexPartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  /**
   * A complex partitioner API that could be used in materialized views to partition based on value. The resulting
   * partition could also be a set of partition Ids instead of just a single partition (one-to-many).
   * @param value that will be mapped to partition(s)
   * @param numPartitions of total available partitions
   * @return
   */
  public abstract Set<Integer> getPartitionId(Object value, int numPartitions);
}
