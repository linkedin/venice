package com.linkedin.venice.hadoop;

import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.log4j.Logger;

/**
 * {@link VeniceMRPartitioner} will use the exact same partitioner: {@link DefaultVenicePartitioner} inside
 * {@link com.linkedin.venice.writer.VeniceWriter} to make sure the messages inside each reducer belong to
 * the same Kafka topic partition.
 */
public class VeniceMRPartitioner implements Partitioner<BytesWritable, BytesWritable> {
  private static final Logger LOGGER = Logger.getLogger(VeniceMRPartitioner.class);

  private VenicePartitioner venicePartitioner;

  @Override
  public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
    int partitionId = venicePartitioner.getPartitionId(key.getBytes(), numPartitions);

    return partitionId;
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
