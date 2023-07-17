package com.linkedin.venice.hadoop.partitioner;

import com.linkedin.venice.hadoop.VeniceMRPartitioner;
import com.linkedin.venice.hadoop.VeniceReducer;
import org.apache.hadoop.io.BytesWritable;


/**
 * This {@link org.apache.hadoop.mapred.Partitioner} will spray keys into all partitions (except the
 * highest one), which will result in reducers receiving keys for more than one Kafka partition. This
 * should be detected by the check in {@link VeniceReducer.ReducerProduceCallback}
 * which ensures that the task is writing to just a single partition.
 */
public class BuggySprayingMapReduceShufflePartitioner extends VeniceMRPartitioner {
  @Override
  public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
    // Truly wonderful what one fewer partition can achieve.
    return super.getPartition(key, value, numPartitions - 1);
  }
}
