package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import javax.validation.constraints.NotNull;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A wrapper class that holds all the partitions for each resource(Store+Version) which are assigned to this Venice
 * node.
 *
 * 1.resourceNameToPartitionIdsMap - Is a map where key is a resource name and the value is a set of
 * partition
 * ids.
 */
public class PartitionAssignmentRepository {
  private static final Logger logger = Logger.getLogger(PartitionAssignmentRepository.class.getName());

  private final Map<String, Set<Integer>> resourceNameToPartitionIdsMap;


  public PartitionAssignmentRepository() {
    resourceNameToPartitionIdsMap = new HashMap<>();
  }

  // Read operations

  /**
   * Given the store name and nodeId get the list of venice logical partitions served
   *
   * @param resourceName the resource name to look up
   *
   * @return set of logical partitions owned by this node for resource @resourceName
   */
  public synchronized Set<Integer> getLogicalPartitionIds(String resourceName)
      throws VeniceException {
    if (resourceNameToPartitionIdsMap.containsKey(resourceName)) {
      //Assumes all nodes have some partitions for any given resource.
      return resourceNameToPartitionIdsMap.get(resourceName);
    } else {
      String errorMessage = "Store name '" + resourceName + "' in this node does not exist!";
      logger.warn(errorMessage);
      throw new VeniceException(errorMessage);
    }
  }

  // write operations

  /**
   * Set the Partition assignment for this resource. Updates all the three views in this repository atomically.
   *
   * @param resourceName      resource name to add or update
   * @param logicalPartitions Set representing assignment of logical partitions for this resource
   */
  public synchronized void setAssignment(@NotNull String resourceName, @NotNull Set<Integer> logicalPartitions)
      throws VeniceException {
    if (logicalPartitions.isEmpty()) {
      String errorMessage = "The partitions set assigned should not be empty.";
      logger.warn(errorMessage);
      throw new VeniceException(errorMessage);
    }
    resourceNameToPartitionIdsMap.put(resourceName, logicalPartitions);
  }

  public synchronized void deleteAssignment(@NotNull String resourceName)
      throws VeniceException {
    //update the first view
    resourceNameToPartitionIdsMap.remove(resourceName);
  }

  public synchronized void addPartition(String resourceName, int partition){
    if (!resourceNameToPartitionIdsMap.containsKey(resourceName)) {
      resourceNameToPartitionIdsMap.put(resourceName, new HashSet<>());
    }
    Set<Integer> partitions = resourceNameToPartitionIdsMap.get(resourceName);
    partitions.add(partition);
  }
}
