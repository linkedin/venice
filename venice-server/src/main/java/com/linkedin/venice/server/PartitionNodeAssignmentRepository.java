package com.linkedin.venice.server;

import com.linkedin.venice.partition.AbstractPartitionNodeAssignmentScheme;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;


/**
 * A wrapper class that holds all the partition to node assignments for each store in Venice.
 *
 * There are three views in this repository:
 * 1.storeNameToNodeIdAndPartitionIdsMap - Is a Concurrent map where key is a store name and the value is
 * another map where key is node id and value is a set of partition ids associated with that node id.
 * 2. nodeIdToStoreNameAndPartitionIdsMap - Is a Concurrent map where key is a node Id and the value is
 * another map where key is store name and value is a set of partition ids associated with that store name.
 * 3. storeNameToPartitionIdAndNodesIdsMap - Is a Concurrent map where key is a store name and the value is
 * another map where key is partition id and value is a set of node Ids serving that partition.
 *
 */
public class PartitionNodeAssignmentRepository {
  private static final Logger logger = Logger.getLogger(PartitionNodeAssignmentRepository.class.getName());

  private final ConcurrentMap<String, Map<Integer, Set<Integer>>> storeNameToNodeIdAndPartitionIdsMap;
  private final ConcurrentMap<Integer, Map<String, Set<Integer>>> nodeIdToStoreNameAndPartitionIdsMap;
  private final ConcurrentMap<String, Map<Integer, Set<Integer>>> storeNameToPartitionIdAndNodesIdsMap;

  public PartitionNodeAssignmentRepository() {
    storeNameToNodeIdAndPartitionIdsMap = new ConcurrentHashMap<String, Map<Integer, Set<Integer>>>();
    nodeIdToStoreNameAndPartitionIdsMap = new ConcurrentHashMap<Integer, Map<String, Set<Integer>>>();
    storeNameToPartitionIdAndNodesIdsMap = new ConcurrentHashMap<String, Map<Integer, Set<Integer>>>();
  }

  // Read operations

  /**
   * Given the store name and nodeId get the list of venice logical partitions served
   *
   * @param storeName the storename to look up
   * @param nodeId the node id to search for
   * @return set of logical partitions owned by the node @nodeId for store @storeName
   */
  public Set<Integer> getLogicalPartitionIds(String storeName, int nodeId) {
    if (storeNameToNodeIdAndPartitionIdsMap.containsKey(storeName)) {
      //Assumes all nodes have some partitions for any given store.
      return storeNameToNodeIdAndPartitionIdsMap.get(storeName).get(nodeId);
    } else {
      logger.error("store name '" + storeName + "' does not exist!");
      // TODO throw exception for non existing storename
      return null;
    }
  }

  /**
   * Given a Venice StoreName, get all nodes and the corresponding logical partitions hosted by them
   *
   *
   * @param storeName to search for
   * @return Map of <nodeId, corresponding logical partition ids >
   */
  public Map<Integer, Set<Integer>> getNodeToLogicalPartitionIdsMap(String storeName) {
    if (storeNameToNodeIdAndPartitionIdsMap.containsKey(storeName)) {
      return storeNameToNodeIdAndPartitionIdsMap.get(storeName);
    } else {
      logger.error("store name '" + storeName + "' does not exist!");
      // TODO throw exception for non existing storename
      return null;     // Need to remove this later
    }
  }

  /**
   * Given a node id, get the map of all stores and their corresponding logical partitions
   *
   * @param nodeId the node id to look up
   * @return Map of <storename, List<logical partition ids>>
   */
  public Map<String, Set<Integer>> getStoreToLogicalPartitionIdsMap(int nodeId) {
    if (nodeIdToStoreNameAndPartitionIdsMap.containsKey(nodeId)) {
      return nodeIdToStoreNameAndPartitionIdsMap.get(nodeId);
    } else {
      logger.error("node '" + nodeId + "' does not exist!");
      // TODO throw exception for non existing node id
      return null;     // Need to remove this later
    }
  }

  /**
   * Get Set of all nodes that listen to a specific partition from the propagating layer. Note that Topic name from
   * propagating layer is same as Venice StoreName.This will be later used by the reader to choose which node
   * to query the key for.
   *
   * @param storeName  to search for
   * @param logicalPartitionId PartitionId from the propagating layer for the given store
   * @return All node ids that subscribe to the @logicalPartitionId
   */
  public Set<Integer> getAllNodeIdsSubscribedToALogicalPartition(String storeName, int logicalPartitionId) {
    if (storeNameToPartitionIdAndNodesIdsMap.containsKey(storeName)) {
      Map<Integer, Set<Integer>> partitionIdToNodeMap = storeNameToPartitionIdAndNodesIdsMap.get(storeName);
      if (partitionIdToNodeMap.containsKey(logicalPartitionId)) {
        return partitionIdToNodeMap.get(logicalPartitionId);
      } else {
        logger.error("partition '" + logicalPartitionId + "' does not exist!");
        // TODO  throw exception for non existing partition id
        return null;     // Need to remove this later
      }
    } else {
      logger.error("store name '" + storeName + "' does not exist!");
      // TODO throw exception for non existing storename
      return null;     // Need to remove this later
    }
  }

  // write operations

  /**
   * Set the Partition to Node assignment for this store. Updates all the three views in this repository atomically.
   *
   * @param storeName storename to add or update
   * @param nodeToLogicalPartitionsMap Map representing assignment of logical partitions to each node for this store
   */
  public synchronized void setAssignment(String storeName, Map<Integer, Set<Integer>> nodeToLogicalPartitionsMap) {
    if (nodeToLogicalPartitionsMap == null) {
      logger.error("node to partition assignment cannot be null!");
      //TODO throw appropriate exception
      return;      // need to remove later based on exception handling
    }

    //update the first view
    storeNameToNodeIdAndPartitionIdsMap.put(storeName, nodeToLogicalPartitionsMap);

    //update the second view
    Map<String, Set<Integer>> storeNameToPartitionsMap;
    for (Integer nodeId : nodeToLogicalPartitionsMap.keySet()) {
      if (nodeIdToStoreNameAndPartitionIdsMap.containsKey(nodeId)) {
        storeNameToPartitionsMap = nodeIdToStoreNameAndPartitionIdsMap.get(nodeId);
        storeNameToPartitionsMap.put(storeName, nodeToLogicalPartitionsMap.get(nodeId));
        nodeIdToStoreNameAndPartitionIdsMap.put(nodeId, storeNameToPartitionsMap);
      } else {
        //TODO log errors and throw exception as needed
        // ignore?
      }
    }

    //update the third view
    Map<Integer, Set<Integer>> partitionToNodeIds = new HashMap<Integer, Set<Integer>>();
    for (Integer nodeId : nodeToLogicalPartitionsMap.keySet()) {
      Set<Integer> partitions = nodeToLogicalPartitionsMap.get(nodeId);
      for (Integer partition : partitions) {
        if (!partitionToNodeIds.containsKey(partition)) {
          partitionToNodeIds.put(partition, new HashSet<Integer>());
        }
        Set<Integer> nodeIdsList = partitionToNodeIds.get(partition);
        nodeIdsList.add(nodeId);
        partitionToNodeIds.put(partition, nodeIdsList);
      }
    }
    storeNameToPartitionIdAndNodesIdsMap.put(storeName, partitionToNodeIds);
  }

  public synchronized void deleteAssignment(String storeName) {
    if (storeName == null) {
      logger.error("store name cannot be null!");
      //TODO throw exception?
      return;
    }
    //update the first view
    storeNameToNodeIdAndPartitionIdsMap.remove(storeName);

    //update the second view
    for (Integer nodeId : nodeIdToStoreNameAndPartitionIdsMap.keySet()) {
      Map<String, Set<Integer>> storeNameToPartitionsMap = nodeIdToStoreNameAndPartitionIdsMap.get(nodeId);
      if (storeNameToPartitionsMap != null && storeNameToPartitionsMap.containsKey(storeName)) {
        storeNameToPartitionsMap.remove(storeName);
      }
      nodeIdToStoreNameAndPartitionIdsMap.put(nodeId, storeNameToPartitionsMap);
    }

    //update the third view
    storeNameToPartitionIdAndNodesIdsMap.remove(storeName);
  }
}
