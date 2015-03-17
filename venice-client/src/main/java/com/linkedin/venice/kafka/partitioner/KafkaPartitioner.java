package com.linkedin.venice.kafka.partitioner;

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
  public KafkaPartitioner(VerifiableProperties props) {}

  /**
   * A consistent hashing algorithm that returns the partitionId based on the key
   * Note that this is based on the number of partitions
   *
   * @param key           - A string key that will be hashed into a partition
   * @param numPartitions - The number of total partitions available in Kafka/storage
   * @return The partitionId for which the given key is mapped to
   */
  public abstract int partition(Object key, int numPartitions);
}
