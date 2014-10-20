package com.linkedin.venice.metadata;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class which stores the mappings of { Kafka partitionId -> List of nodeId }
 */
public class NodeCache {

  static final Logger logger = Logger.getLogger(NodeCache.class.getName());

  private static NodeCache instance = null;

  // Mapping of partitionId to nodeId list.
  private static Map<Integer, List<Integer>> nodeMap;

  public NodeCache() {
    nodeMap = new ConcurrentHashMap<Integer, List<Integer>>();
  }

  public static synchronized NodeCache getInstance() {

    if (null == instance) {
      instance = new NodeCache();
    }

    return instance;

  }

  /**
   * Clears the nodeCache of all data
   * */
  public void clear() {
    nodeMap = new HashMap<Integer, List<Integer>>();
  }

  /**
   * Given a partitionId, returns the master storage node.
   * @param partitionId - The partitionId to query for
   * @return The master nodeId
   * */
  public int getMasterNodeId(int partitionId) {

    // key already exists in cache
    if (nodeMap.containsKey(partitionId)) {
      return nodeMap.get(partitionId).get(0); // get the first value in list
    }

    return -1;
  }

  /**
   * Given a partitionId, returns a list of associated nodes.
   * @param partitionId - The partitionId to query for
   * @return A list of nodeIds, of which the first element is the master.
   *         If replication factor is 1, the list will be of length 1.
   * */
  public List<Integer> getNodeIds(int partitionId) {

    // key already exists in cache
    if (nodeMap.containsKey(partitionId)) {
      return nodeMap.get(partitionId);
    }

    return new ArrayList<Integer>();

  }

  /**
   * Registers a new partition -> node mapping in the cache
   * @param partitionId - partitionId, an integer value for the Kafka partition
   * @param newNodeIds - A list of nodes which are tied to the given partitionId
   * */
  public synchronized void registerNewMapping(int partitionId, List<Integer> newNodeIds) {

    // already registered
    if (nodeMap.containsKey(partitionId)) {

      List<Integer> currentNodeIds = nodeMap.get(partitionId);

      // value is not the same
      if (!currentNodeIds.equals(newNodeIds)) {
        logger.error("Key conflict on partitionId: " + partitionId);
        logger.error("Attempted to register nodeId list but found conflicting list in storage.");
        return;  // intentionally not throwing an exception here, as I imagine the client will be calling this blindly
      }

    }

    // add key to cache
    nodeMap.put(partitionId, newNodeIds);

  }

}
