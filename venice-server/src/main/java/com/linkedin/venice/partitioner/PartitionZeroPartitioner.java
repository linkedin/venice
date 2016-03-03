package com.linkedin.venice.partitioner;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;


/**
 * Used for the ACK producer so we always route ACKs to the same partition...
 */
public class PartitionZeroPartitioner implements Partitioner {
  public PartitionZeroPartitioner() {
  }

  @Override
  public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
    return 0;
  }

  @Override
  public void close() {
    /* Not used but required for the interface. */
  }

  @Override
  public void configure(Map<String, ?> map) {
    /* Not used but required for the interface. */
  }
}