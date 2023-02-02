package com.linkedin.venice.hadoop;

import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


/**
 * {@link VeniceMRPartitioner} will use the exact same partitioner: {@link DefaultVenicePartitioner} inside
 * {@link com.linkedin.venice.writer.VeniceWriter} to make sure the messages inside each reducer belong to
 * the same Kafka topic partition.
 */
public class VeniceMRPartitioner implements Partitioner<BytesWritable, BytesWritable> {
  protected VenicePartitioner venicePartitioner;

  public static final int EMPTY_KEY_LENGTH = 0;

  @Override
  public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
    if (key.getLength() == EMPTY_KEY_LENGTH) {
      // Special case, used only to ensure that all reducers are instantiated, even if starved of actual data.
      return ByteUtils.readInt(value.getBytes(), 0);
    }
    return getPartition(key, numPartitions);
  }

  protected int getPartition(BytesWritable key, int numPartitions) {
    return venicePartitioner.getPartitionId(key.getBytes(), 0, key.getLength(), numPartitions);
  }

  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);
    /**
     * Note: Here needs to use the exact same partitioner being used by {@link com.linkedin.venice.writer.VeniceWriter}.
     */
    this.venicePartitioner = PartitionUtils.getVenicePartitioner(props);
  }
}
