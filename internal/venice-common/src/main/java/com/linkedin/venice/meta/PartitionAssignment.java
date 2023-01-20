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
  /**
   * Expected number of partitions for this resource, it's a fixed value after resource is created. If this value is 0,
   * it means this resource has been deleted, so there is not any partition should be assigned to.
   */
  private final int expectedNumberOfPartitions;
  // TODO will remove the Partition class in the next change, instead, use map of <partitionId,List<Replica>> and expose
  // TODO the methods like getInstance(partitionId), getAllPartitionIds() from PartitionAssignment class.
  private final Map<Integer, Partition> idToPartitionMap;

  public PartitionAssignment(String topic, int numberOfPartition) {
    this.topic = topic;
    if (numberOfPartition <= 0) {
      throw new VeniceException(
          "Expected number of partition should be larger than 0 for resource '" + topic + "'. Current value:"
              + numberOfPartition);
    }
    this.expectedNumberOfPartitions = numberOfPartition;
    idToPartitionMap = new HashMap<>();
  }

  public Partition getPartition(int partitionId) {
    return idToPartitionMap.get(partitionId);
  }

  public void addPartition(Partition partition) {
    if (partition.getId() < 0 || partition.getId() >= expectedNumberOfPartitions) {
      throw new VeniceException(
          "Invalid Partition id:" + partition.getId() + ". Partition id should be in the range of [0,"
              + expectedNumberOfPartitions + "]");
    }
    idToPartitionMap.put(partition.getId(), partition);
  }

  public void removePartition(int partitionId) {
    idToPartitionMap.remove(partitionId);
  }

  public Collection<Partition> getAllPartitions() {
    return idToPartitionMap.values();
  }

  public int getExpectedNumberOfPartitions() {
    return expectedNumberOfPartitions;
  }

  public int getAssignedNumberOfPartitions() {
    return idToPartitionMap.size();
  }

  public boolean isMissingAssignedPartitions() {
    return getAssignedNumberOfPartitions() < expectedNumberOfPartitions;
  }

  public String getTopic() {
    return topic;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " {" + "\n\ttopic: " + topic + ", " + "\n\texpectedNumberOfPartitions: "
        + expectedNumberOfPartitions + ", " + "\n\tidToPartitionMap: " + idToPartitionMap.toString() + "\n}";
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
    result = 31 * result + expectedNumberOfPartitions;
    return result;
  }
}
