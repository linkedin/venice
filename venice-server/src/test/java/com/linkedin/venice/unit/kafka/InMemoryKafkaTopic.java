package com.linkedin.venice.unit.kafka;

import java.util.ArrayList;

/**
 * Maintains queues for each partition of an in-memory topic.
 *
 * @see InMemoryKafkaBroker
 */
class InMemoryKafkaTopic {
  private final ArrayList<InMemoryKafkaMessage>[] partitions;

  InMemoryKafkaTopic(int partitionCount) {
    if (partitionCount < 1) {
      throw new IllegalArgumentException(
          "Cannot create a " + InMemoryKafkaTopic.class.getSimpleName() + " with less than 1 partition.");
    }
    partitions = new ArrayList[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new ArrayList<>();
    }
  }

  /**
   * @param partition The partition in which to produce a message.
   * @param message The {@link InMemoryKafkaMessage} to produce into the partition.
   * @return the offset of the produced message
   * @throws IllegalArgumentException if the partition does not exist
   */
  synchronized long produce(int partition, InMemoryKafkaMessage message) throws IllegalArgumentException {
    checkPartitionCount(partition);
    ArrayList<InMemoryKafkaMessage> partitionQueue = partitions[partition];
    long nextOffset = partitionQueue.size();
    partitionQueue.add(message);
    return nextOffset;
  }

  /**
   * @param partition from which to consume
   * @param offset of the message to consume within the partition
   * @return a {@link InMemoryKafkaMessage} instance
   * @throws IllegalArgumentException if the partition or offset does not exist or if the offset is larger than {@link Integer#MAX_VALUE}
   */
  InMemoryKafkaMessage consume(int partition, long offset) throws IllegalArgumentException {
    checkPartitionCount(partition);
    ArrayList<InMemoryKafkaMessage> partitionQueue = partitions[partition];

    if (offset > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Offsets larger than " + Integer.MAX_VALUE + " are not supported by " + InMemoryKafkaTopic.class.getSimpleName());
    }

    try {
      return partitionQueue.get((int) offset);
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Partition " + partition + " in this topic does not contain offset " + offset, e);
    }
  }

  private void checkPartitionCount(int requestedPartition) throws IllegalArgumentException {
    int numberOfPartitions = getPartitionCount();
    if (numberOfPartitions <= requestedPartition) {
      throw new IllegalArgumentException(
          "This topic has " + numberOfPartitions + " partitions. Partition number " + requestedPartition + " does not exist.");
    }
  }

  int getPartitionCount() {
    return partitions.length;
  }
}
