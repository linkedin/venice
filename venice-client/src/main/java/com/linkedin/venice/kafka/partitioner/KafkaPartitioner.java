package com.linkedin.venice.kafka.partitioner;

import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.utils.ByteUtils;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


/**
 * Custom Partitioner Class which is jointly used by Kafka and Venice.
 * Determines the appropriate partition for each message.
 */
public abstract class KafkaPartitioner implements Partitioner {

    /**
     * An abstraction on the standard Partitioner interface
     */
    public KafkaPartitioner(VerifiableProperties props) {
    }


    public int partition(Object key, int numPartitions) {
        KafkaKey kafkaKey = (KafkaKey) key;
        OperationType opType = kafkaKey.getOperationType();

        // Check if the key is for sending a control message
        if (opType == OperationType.BEGIN_OF_PUSH || opType == OperationType.END_OF_PUSH) {
            return ByteUtils.readInt(kafkaKey.getKey(), 0);
        }
        return getPartitionId(kafkaKey, numPartitions);
    }

    /**
     * A function that returns the partitionId based on the key.
     * Note that this is based on the number of partitions.
     *
     * @param key           - A key that will be mapped into a partition
     * @param numPartitions - The number of total partitions available in Kafka/storage
     * @return
     */
    public abstract int getPartitionId(KafkaKey key, int numPartitions);
}
