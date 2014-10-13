package com.linkedin.venice.storage;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerException;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import com.linkedin.venice.metadata.NodeCache;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;


/**
 * A class for managing storage nodes and their locations
 * Mostly used for directing read traffic from client
 */
public class VeniceStorageManager {

  static final Logger logger = Logger.getLogger(VeniceStorageManager.class.getName());

  private static VeniceStorageManager instance = null;

  // Note: both nodes and partitions start from id = 0
  private Map<Integer, VeniceStorageNode> storeNodeMap = null; // Map which explicitly stores the nodes, based on NodeId
  private Set<Integer> partitionIdList = null; // Set of all unique partition ids

  private StorageType storageType = GlobalConfiguration.getStorageType();

  private final int replicationFactor = GlobalConfiguration.getNumStorageCopies();

  private static NodeCache nodeCache; // cache mapping of partition to node

  public VeniceStorageManager() {

    // initialize node variables
    storeNodeMap = new HashMap<Integer, VeniceStorageNode>();

    // initialize partition variables
    partitionIdList = new HashSet<Integer>();

    nodeCache = new NodeCache();

  }

  /**
   * Creates a new VeniceStorageNode, based on the current configuration
   * */
  private VeniceStorageNode createNewStoreNode(int nodeId) {

    VeniceStorageNode toReturn;

    // TODO: implement other storage solutions when available
    switch (storageType) {
      case MEMORY:
        toReturn = new InMemoryStorageNode(nodeId);
        break;

      case BDB:
        throw new UnsupportedOperationException("BDB storage not yet implemented");

      case VOLDEMORT:
        throw new UnsupportedOperationException("Voldemort storage not yet implemented");

      default:
        toReturn = new InMemoryStorageNode(nodeId);
        break;
    }

    return toReturn;

  }

  /**
   * Creates a new node in the registry
   * @param nodeId - The storage node id to be registered
   * */
  public synchronized void registerNewNode(int nodeId) throws VeniceStorageException {

    if (storeNodeMap.containsKey(nodeId)) {
      throw new VeniceStorageException("Attempting to add a nodeId which already exists: " + nodeId);
    }

    storeNodeMap.put(nodeId, createNewStoreNode(nodeId));

  }

  /**
   * Registers a new partitionId and adds all of its copies to its associated nodes
   * @param partitionId - the id of the partition to register
   * */
  public synchronized void registerNewPartition(int partitionId)
      throws VeniceStorageException, VeniceKafkaConsumerException {

    // use conversion algorithm to find nodeId
    List<Integer> nodeIds = calculateNodeId(partitionId);

    for (int nodeId : nodeIds) {
      VeniceStorageNode node = storeNodeMap.get(nodeId);
      node.addPartition(partitionId);
    }

    nodeCache.registerNewMapping(partitionId, nodeIds);
    partitionIdList.add(partitionId);

  }

  /**
   * Returns a value from the storage - this method to be deprecated
   * @param key - the key for the KV pair
   * @return The value received from Venice Storage
   * @throws VeniceStorageException if any nodes or partitions cannot be referenced
   */
  public Object readValue(String key) throws VeniceStorageException {

    // get partition from kafka
    KafkaPartitioner kp = new KafkaPartitioner(new VerifiableProperties());
    int partitionId = kp.partition(key, GlobalConfiguration.getNumKafkaPartitions());

    // returns -1 if not in cache
    int nodeId = nodeCache.getMasterNodeId(partitionId);

    // calculate and reassign nodeId
    if (-1 == nodeId) {

      logger.warn("Cache could not find value, recalculating mapping for partitionId: " + partitionId);
      nodeCache.registerNewMapping(partitionId, calculateNodeId(partitionId));
      nodeId = nodeCache.getMasterNodeId(partitionId);

    }

    // does not exist
    if (!storeNodeMap.containsKey(nodeId)) {
      throw new VeniceStorageException("NodeId does not exist: " + nodeId);
    }

    return storeNodeMap.get(nodeId).get(partitionId, key);

  }

  /**
   * Method that calculates the nodeId for a given partitionId and creates the partition if does not exist
   * Must be a deterministic method for partitionIds AND their replicas
   * @param partitionId - The Kafka partitionId to be used in calculation
   * @return A list of all the nodeIds associated with the given partitionId
   * */
  private List<Integer> calculateNodeId(int partitionId) throws VeniceStorageException {

    int numNodes = storeNodeMap.size();

    if (0 == numNodes) {
      throw new VeniceStorageException("Cannot calculate node id for partition because there are no nodes!");
    }

    // TODO: improve algorithm to provide true balancing
    List<Integer> nodeIds = new ArrayList<Integer>();
    for (int i = 0; i < replicationFactor; i++) {
      nodeIds.add((partitionId + i) % numNodes);
    }

    return nodeIds;

  }

}
