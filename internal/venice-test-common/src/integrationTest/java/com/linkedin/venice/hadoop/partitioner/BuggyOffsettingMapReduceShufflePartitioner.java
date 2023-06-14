package com.linkedin.venice.hadoop.partitioner;

import com.linkedin.venice.hadoop.VeniceMRPartitioner;
import org.apache.hadoop.io.BytesWritable;


/**
 * This {@link org.apache.hadoop.mapred.Partitioner} will route keys to the wrong partitions, but each
 * group of keys which belonged in one Kafka partition will end up entirely in another partition. This
 * cannot be detected simply by checking that the reducer is producing messages into just a single partition.
 */
public class BuggyOffsettingMapReduceShufflePartitioner extends VeniceMRPartitioner {
  @Override
  public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
    // Offset all partitions by 1.
    return (super.getPartition(key, value, numPartitions) + 1) % numPartitions;
  }
}
