package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * A wrapper class that holds all the partition to node assignments for each store in Venice.
 *
 * 1.storeNameToNodeIdAndPartitionIdsMap - Is a Concurrent map where key is a store name and the value is
 * another map where key is node id and value is a set of partition ids associated with that node id.
 */
public class PartitionNodeAssignmentRepository {
  private static final Logger logger = Logger.getLogger(PartitionNodeAssignmentRepository.class.getName());

  private final ConcurrentMap<String, Map<Integer, Set<Integer>>> storeNameToNodeIdAndPartitionIdsMap;


  public PartitionNodeAssignmentRepository() {
    storeNameToNodeIdAndPartitionIdsMap = new ConcurrentHashMap<String, Map<Integer, Set<Integer>>>();
  }

  // Read operations

  /**
   * Given the store name and nodeId get the list of venice logical partitions served
   *
   * @param storeName the storename to look up
   * @param nodeId the node id to search for
   * @return set of logical partitions owned by the node @nodeId for store @storeName
   */
  public Set<Integer> getLogicalPartitionIds(String storeName, int nodeId)
      throws VeniceException {
    if (storeNameToNodeIdAndPartitionIdsMap.containsKey(storeName)) {
      //Assumes all nodes have some partitions for any given store.
      return storeNameToNodeIdAndPartitionIdsMap.get(storeName).get(nodeId);
    } else {
      String errorMessage = "Store name '" + storeName + "' in node: " + nodeId + " does not exist!";
      logger.warn(errorMessage);
      return new HashSet<Integer>();
    }
  }

  // write operations

  /**
   * Set the Partition to Node assignment for this store. Updates all the three views in this repository atomically.
   *
   * @param storeName storename to add or update
   * @param nodeToLogicalPartitionsMap Map representing assignment of logical partitions to each node for this store
   */
  public synchronized void setAssignment(String storeName, Map<Integer, Set<Integer>> nodeToLogicalPartitionsMap)
      throws VeniceException {
    String errorMessage;
    if (nodeToLogicalPartitionsMap == null) {
      errorMessage = "Node to partition assignment cannot be null!";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }

    if (storeName == null) {
      errorMessage = "Store name cannot be null!";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }

    storeNameToNodeIdAndPartitionIdsMap.put(storeName, nodeToLogicalPartitionsMap);

  }

  public synchronized void deleteAssignment(String storeName)
      throws VeniceException {
    if (storeName == null) {
      String errorMessage = "Store name cannot be null!";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }
    //update the first view
    storeNameToNodeIdAndPartitionIdsMap.remove(storeName);
  }

  public synchronized void addPartition(String storeName, int nodeId, int partition){
    if (!storeNameToNodeIdAndPartitionIdsMap.containsKey(storeName)) {
      storeNameToNodeIdAndPartitionIdsMap.put(storeName, new HashMap<Integer, Set<Integer>>());
    }
    Map<Integer, Set<Integer>> nodeToPartitions = storeNameToNodeIdAndPartitionIdsMap.get(storeName);
    if (!nodeToPartitions.containsKey(nodeId)) {
      nodeToPartitions.put(nodeId, new HashSet<Integer>());
    }
    nodeToPartitions.get(nodeId).add(partition);
  }
}
