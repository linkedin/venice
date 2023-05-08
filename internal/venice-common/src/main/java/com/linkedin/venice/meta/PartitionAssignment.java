package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * This class represent the assignments of one resource including all of assigned partitions and expected number of
 * partitions.
 */
public class PartitionAssignment {
  private final String topic;
  private final Map<Integer, Partition> idToPartitionMap;
  private final Partition[] partitionsArrayIndexedById;

  public PartitionAssignment(String topic, int numberOfPartition) {
    this.topic = topic;
    if (numberOfPartition <= 0) {
      throw new VeniceException(
          "Expected number of partition should be larger than 0 for resource '" + topic + "'. Current value:"
              + numberOfPartition);
    }
    this.idToPartitionMap = new HashMap<>(numberOfPartition);
    this.partitionsArrayIndexedById = new Partition[numberOfPartition];
  }

  public Partition getPartition(int partitionId) {
    return this.partitionsArrayIndexedById[partitionId];
  }

  public void addPartition(Partition partition) {
    if (partition.getId() < 0 || partition.getId() >= getExpectedNumberOfPartitions()) {
      throw new VeniceException(
          "Invalid Partition id:" + partition.getId() + ". Partition id should be in the range of [0,"
              + getExpectedNumberOfPartitions() + "]");
    }
    this.idToPartitionMap.put(partition.getId(), partition);
    this.partitionsArrayIndexedById[partition.getId()] = partition;
  }

  public Collection<Partition> getAllPartitions() {
    return idToPartitionMap.values();
  }

  public int getExpectedNumberOfPartitions() {
    return this.partitionsArrayIndexedById.length;
  }

  public int getAssignedNumberOfPartitions() {
    return idToPartitionMap.size();
  }

  public boolean isMissingAssignedPartitions() {
    return getAssignedNumberOfPartitions() < getExpectedNumberOfPartitions();
  }

  public String getTopic() {
    return topic;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " {" + "\n\ttopic: " + topic + ", " + "\n\texpectedNumberOfPartitions: "
        + getExpectedNumberOfPartitions() + ", " + "\n\tidToPartitionMap: " + idToPartitionMap + "\n}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof PartitionAssignment) {
      return idToPartitionMap.equals(((PartitionAssignment) obj).idToPartitionMap);
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + topic.hashCode();
    result = 31 * result + idToPartitionMap.hashCode();
    result = 31 * result + getExpectedNumberOfPartitions();
    return result;
  }
}
