package com.linkedin.venice.storage;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.partitioner.KafkaPartitioner;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.metadata.NodeCache;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton class for managing storage nodes and their locations
 */
public class VeniceStorageManager {

  static final Logger logger = Logger.getLogger(VeniceStorageManager.class.getName());

  private static VeniceStorageManager instance = null;

  // Note: both nodes and partitions start from id = 0
  private Map<Integer, VeniceStorageNode> storeNodeMap = null; // Map which explicitly stores the nodes, based on NodeId
  private Set<Integer> partitionIdList = null; // Set of all unique partition ids

  private StorageType storageType = GlobalConfiguration.getStorageType();

  private final int replicationFactor = GlobalConfiguration.getNumStorageCopies();

  private static int nodeCount;
  private static NodeCache nodeCache; // cache mapping of partition to node

  /* Constructor: Cannot externally instantiate a singleton */
  private VeniceStorageManager() {

    // initialize node variables
    storeNodeMap = new ConcurrentHashMap<Integer, VeniceStorageNode>();
    nodeCount = GlobalConfiguration.getNumStorageNodes();

    // initialize partition variables
    partitionIdList = new HashSet<Integer>();

    nodeCache = NodeCache.getInstance();

  }

  /*
   * Return the instance of the VeniceStorageManager
   * */
  public static synchronized VeniceStorageManager getInstance() {

    if (null == instance) {
      instance = new VeniceStorageManager();
    }

    return instance;

  }

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
    nodeCount++;

  }

  /**
   * Registers a new partitionId and adds all of its copies to its associated nodes
   * */
  public synchronized void registerNewPartition(int partitionId) throws VeniceStorageException {

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
   * Returns a value from the storage
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
   * TODO: For performance, implement this method in a concurrent and thread safe way
   * Stores a value into the storage, given a partition id. If the partition is not cached, stores into cache as well.
   * @param partitionId - The partition to look in
   * @param key - the key for the KV pair
   * @param msg - A VeniceMessage to be added to storage
   * @throws VeniceStorageException if any nodes or partitions cannot be referenced
   * @throws VeniceMessageException if an invalid VeniceMessage is given
   */
  public void storeValue(int partitionId, String key, VeniceMessage msg)
      throws VeniceStorageException, VeniceMessageException {

    // check for invalid inputs
    if (null == msg) {
      throw new VeniceMessageException("Given null Venice Message.");
    }

    if (null == msg.getOperationType()) {
      throw new VeniceMessageException("Venice Message does not have operation type!");
    }

    if (!partitionIdList.contains(partitionId)) {
      throw new VeniceStorageException("Partition does not exist: " + partitionId);
    }

    // check in cache first, returns -1 if not in cache
    List<Integer> nodeIds = nodeCache.getNodeIds(partitionId);

    if (nodeIds.isEmpty()) {

      nodeIds = calculateNodeId(partitionId);
      nodeCache.registerNewMapping(partitionId, nodeIds);

    }

    // Before performing ANY puts, check that all the nodes are valid
    for (int nodeId : nodeIds) {

      // sanity check for existing node
      if (!storeNodeMap.containsKey(nodeId)) {
        throw new VeniceStorageException("No instance of node id: " + nodeId);
      }

    }

    // Perform a put for every replica
    for (int nodeId : nodeIds) {

      switch (msg.getOperationType()) {

        // adding new values
        case PUT:
          logger.info("Putting: " + key + ", " + msg.getPayload());
          storeNodeMap.get(nodeId).put(partitionId, key, msg.getPayload());
          break;

        // deleting values
        case DELETE:
          logger.info("Deleting: " + key);
          storeNodeMap.get(nodeId).delete(partitionId, key);
          break;

        // partial update
        case PARTIAL_PUT:
          throw new UnsupportedOperationException("Partial puts not yet implemented");

        // error
        default:
          throw new VeniceMessageException("Invalid operation type submitted: " + msg.getOperationType());
      }

    }

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
