package com.linkedin.venice.partitioner;

import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
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

    /**
     * Implementors of this class can optionally provide an implementation of this function,
     * which would result in eliminating an instantiation of {@link ByteBuffer} in the case
     * where the provided offset and length do not map to the boundaries of the byte[]. This
     * is just a minor optimization.
     */
    public int getPartitionId(byte[] keyBytes, int offset, int length, int numPartitions) {
        if (0 != offset && keyBytes.length != length) {
            return getPartitionId(ByteBuffer.wrap(keyBytes, offset, length), numPartitions);
        }
        return getPartitionId(keyBytes, numPartitions);
    }

    public abstract int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions);
}
