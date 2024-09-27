package com.linkedin.venice.spark.datawriter.partition;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;


/**
 * {@link VeniceSparkPartitioner} will use the exact same partitioner as the one that will be used inside
 * {@link com.linkedin.venice.writer.VeniceWriter} (e.g. {@link com.linkedin.venice.partitioner.DefaultVenicePartitioner})
 * to make sure the messages inside each partition belong to the same PubSub topic partition.
 */
public class VeniceSparkPartitioner extends Partitioner {
  private static final long serialVersionUID = 1L;
  private Broadcast<Properties> broadcastProperties;
  private transient VenicePartitioner venicePartitioner = null;
  private final int numPartitions;

  public VeniceSparkPartitioner(Broadcast<Properties> broadcastProperties, int numPartitions) {
    this.numPartitions = numPartitions;
    this.broadcastProperties = broadcastProperties;
    configurePartitioner();
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  private void configurePartitioner() {
    if (this.venicePartitioner != null) {
      return;
    }

    VeniceProperties props = new VeniceProperties(broadcastProperties.getValue());
    /**
     * Note: Here needs to use the exact same partitioner being used by {@link com.linkedin.venice.writer.VeniceWriter}.
     */
    this.venicePartitioner = PartitionUtils.getVenicePartitioner(props);

    int numPartitions = props.getInt(PARTITION_COUNT);

    if (numPartitions != this.numPartitions) {
      throw new VeniceException(
          "Partition count mismatch: " + numPartitions + " (from config) != " + this.numPartitions + " (from driver)");
    }
  }

  @Override
  public int getPartition(Object key) {
    if (!(key instanceof Row)) {
      throw new VeniceException("VeniceSparkPartitioner only supports Row keys");
    }

    // Since the partitioner is transient, it needs to be re-initialized in each task.
    configurePartitioner();

    Row row = (Row) key;
    return getPartition((byte[]) row.get(0), (byte[]) row.get(1), numPartitions);
  }

  private int getPartition(byte[] key, byte[] value, int numPartitions) {
    if (key.length == 0) {
      // Special case, used only to ensure that all PartitionWriters are instantiated, even if starved of actual data.
      return ByteUtils.readInt(value, 0);
    }
    return venicePartitioner.getPartitionId(key, numPartitions);
  }
}
