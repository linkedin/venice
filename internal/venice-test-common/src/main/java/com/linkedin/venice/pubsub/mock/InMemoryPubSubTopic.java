package com.linkedin.venice.pubsub.mock;

import java.util.ArrayList;
import java.util.Optional;


/**
 * Maintains queues for each partition of an in-memory topic.
 *
 * @see InMemoryPubSubBroker
 */
class InMemoryPubSubTopic {
  private final ArrayList<InMemoryPubSubMessage>[] partitions;

  InMemoryPubSubTopic(int partitionCount) {
    if (partitionCount < 1) {
      throw new IllegalArgumentException(
          "Cannot create a " + InMemoryPubSubTopic.class.getSimpleName() + " with less than 1 partition.");
    }
    partitions = new ArrayList[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new ArrayList<>();
    }
  }

  /**
   * @param partition The partition in which to produce a message.
   * @param message The {@link InMemoryPubSubMessage} to produce into the partition.
   * @return the offset of the produced message
   * @throws IllegalArgumentException if the partition does not exist
   */
  synchronized InMemoryPubSubPosition produce(int partition, InMemoryPubSubMessage message)
      throws IllegalArgumentException {
    checkPartitionCount(partition);
    ArrayList<InMemoryPubSubMessage> partitionQueue = partitions[partition];
    InMemoryPubSubPosition nextOffset = InMemoryPubSubPosition.of(partitionQueue.size());
    partitionQueue.add(message);
    return nextOffset;
  }

  /**
   * @param partition from which to consume
   * @param position the position from which to consume
   * @return Some {@link InMemoryPubSubMessage} instance, or the {@link Optional#empty()} instance if that partition is drained.
   * @throws IllegalArgumentException if the partition or offset does not exist
   */
  Optional<InMemoryPubSubMessage> consume(int partition, InMemoryPubSubPosition position)
      throws IllegalArgumentException {
    checkPartitionCount(partition);
    ArrayList<InMemoryPubSubMessage> partitionQueue = partitions[partition];

    long internalOffset = position.getInternalOffset();
    if (internalOffset > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Offsets larger than " + Integer.MAX_VALUE + " are not supported by "
              + InMemoryPubSubTopic.class.getSimpleName());
    }

    try {
      return Optional.of(partitionQueue.get((int) internalOffset));
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

  InMemoryPubSubPosition endPosition(int partition) {
    return InMemoryPubSubPosition.of(getEndOffsets(partition));
  }
}
