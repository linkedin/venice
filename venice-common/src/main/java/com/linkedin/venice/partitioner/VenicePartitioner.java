package com.linkedin.venice.partitioner;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;

/**
 * Determines partitioning, which is used for producing messages into the right
 * Kafka partitions and routing reads to the correct Venice storage nodes.
 *
 * N.B.: This is purposefully de-coupled from Kafka, so that the Router does
 *       not need to depend on Kafka.
 */
public abstract class VenicePartitioner {

    protected final VeniceProperties props; // available for sub-classes to use.

    public VenicePartitioner() {
        this(new VeniceProperties(new Properties()));
    }

    public VenicePartitioner(VeniceProperties props) {
        this.props = props;
    }

    /**
     * A function that returns the partitionId based on the key and partition count.
     *
     * @param keyBytes      - A key that will be mapped into a partition
     * @param numPartitions - The number of total partitions available in Kafka/storage
     * @return
     */
    public abstract int getPartitionId(byte[] keyBytes, int numPartitions);
}
