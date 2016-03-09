package com.linkedin.venice.partitioner;

import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Props;

/**
 * Determines partitioning, which is used for producing messages into the right
 * Kafka partitions and routing reads to the correct Venice storage nodes.
 *
 * .
 */
public abstract class VenicePartitioner {

    protected final Props props; // available for sub-classes to use.

    /**
     * An abstraction on the standard Partitioner interface
     */
    public VenicePartitioner() {
        this(new Props());
    }

    public VenicePartitioner(Props props) {
        this.props = props;
    }

    /**
     * A function that returns the partitionId based on the key.
     * Note that this is based on the number of partitions.
     *
     * @param key           - A key that will be mapped into a partition
     * @param numPartitions - The number of total partitions available in Kafka/storage
     * @return
     */
    public int getPartitionId(KafkaKey key, int numPartitions) {
        OperationType opType = key.getOperationType();

        // Check if the key is for sending a control message
        if (opType == OperationType.BEGIN_OF_PUSH || opType == OperationType.END_OF_PUSH) {
            return ByteUtils.readInt(key.getKey(), 0);
        }
        return getPartitionIdImplementation(key, numPartitions);
    }

    protected abstract int getPartitionIdImplementation(KafkaKey key, int numPartitions);

}
