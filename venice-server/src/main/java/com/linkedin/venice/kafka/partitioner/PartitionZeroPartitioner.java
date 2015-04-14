package com.linkedin.venice.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Used for the ACK producer so we always route ACKs to the same partition...
 */
public class PartitionZeroPartitioner implements Partitioner {
  public PartitionZeroPartitioner(VerifiableProperties props) {
  }

  @Override
  public int partition(Object key, int numPartitions) {
    return 0;
  }
}