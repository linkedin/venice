package com.linkedin.venice.unit.kafka;

import java.util.ArrayList;
import java.util.Optional;


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
   * @return Some {@link InMemoryKafkaMessage} instance, or the {@link Optional#empty()} instance if that partition is drained.
   * @throws IllegalArgumentException if the partition or offset does not exist
   */
  Optional<InMemoryKafkaMessage> consume(int partition, long offset) throws IllegalArgumentException {
    checkPartitionCount(partition);
    ArrayList<InMemoryKafkaMessage> partitionQueue = partitions[partition];

    if (offset > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Offsets larger than " + Integer.MAX_VALUE + " are not supported by "
              + InMemoryKafkaTopic.class.getSimpleName());
    }

    try {
      return Optional.of(partitionQueue.get((int) offset));
    } catch (IndexOutOfBoundsException e) {
      return Optional.empty();
    }
  }

  private void checkPartitionCount(int requestedPartition) throws IllegalArgumentException {
    int numberOfPartitions = getPartitionCount();
    if (numberOfPartitions <= requestedPartition) {
      throw new IllegalArgumentException(
          "This topic has " + numberOfPartitions + " partitions. Partition number " + requestedPartition
              + " does not exist.");
    }
  }

  int getPartitionCount() {
    return partitions.length;
  }

  Long getEndOffsets(int partition) {
    return (long) partitions[partition].size();
  }
}
