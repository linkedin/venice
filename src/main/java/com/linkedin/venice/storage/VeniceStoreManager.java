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

/**
 * A singleton class for managing storage nodes and their locations
 * Created by clfung on 9/17/14.
 */
public class VeniceStoreManager {

  static final Logger logger = Logger.getLogger(VeniceStoreManager.class.getName());

  private static VeniceStoreManager instance = null;

  // Note: both nodes and partitions start from id = 0
  private Map<Integer, VeniceStoreNode> storeNodeMap = null; // Map which explicitly stores the nodes, based on NodeId
  private Set<Integer> partitionIdList = null; // Set of all unique partition ids

  private StoreType storeType = GlobalConfiguration.getStorageType();

  private final int replicationFactor = GlobalConfiguration.getNumStorageCopies();

  private static int nodeCount;
  private static NodeCache nodeCache; // cache mapping of partition to node

  /* Constructor: Cannot externally instantiate a singleton */
  private VeniceStoreManager() {

    // initialize node variables
    storeNodeMap = new HashMap<Integer, VeniceStoreNode>();
    nodeCount = GlobalConfiguration.getNumStorageNodes();

    // initialize partition variables
    partitionIdList = new HashSet<Integer>();

    nodeCache = NodeCache.getInstance();

  }

  /*
   * Return the instance of the VeniceStoreManager
   * */
  public static synchronized VeniceStoreManager getInstance() {

    if (null == instance) {
      instance = new VeniceStoreManager();
    }

    return instance;

  }

  private VeniceStoreNode createNewStoreNode(int nodeId) {

    VeniceStoreNode toReturn;

    // TODO: implement other storage solutions when available
    switch (storeType) {
      case MEMORY:
        toReturn = new InMemoryStoreNode(nodeId);
        break;

      case BDB:
        throw new UnsupportedOperationException("BDB storage not yet implemented");

      case VOLDEMORT:
        throw new UnsupportedOperationException("Voldemort storage not yet implemented");

      default:
        toReturn = new InMemoryStoreNode(nodeId);
        break;
    }

    return toReturn;

  }

  /**
   * Creates a new node in the registry
   * @param nodeId - The storage node id to be registered
   * */
  public synchronized void registerNewNode(int nodeId) {

    nodeCount++;
    storeNodeMap.put(nodeId, createNewStoreNode(nodeId));

  }

  /**
   * Registers a new partitionId and adds all of its copies to its associated nodes
   * */
  public synchronized void registerNewPartition(int partitionId) {

    // use conversion algorithm to find nodeId
    List<Integer> nodeIds = calculateNodeId(partitionId);

    for (int nodeId : nodeIds) {
      VeniceStoreNode node = storeNodeMap.get(nodeId);
      node.addPartition(partitionId);
    }

    nodeCache.registerNewMapping(partitionId, nodeIds);
    partitionIdList.add(partitionId);

  }

  /**
   * Returns a value from the storage
   * @param key - the key for the KV pair
   */
  public Object readValue(String key) {

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
      logger.error("NodeId does not exist: " + nodeId);
      return null;
    }

    return storeNodeMap.get(nodeId).get(partitionId, key);

  }

  /**
   * TODO: For performance, implement this method in a concurrent and thread safe way
   * Stores a value into the storage, given a partition id. If the partition is not cached, stores into cache as well.
   * @param partitionId - The partition to look in
   * @param key - the key for the KV pair
   * @param msg - A VeniceMessage to be added to storage
   * @return true if operation was successful
   */
  public boolean storeValue(int partitionId, String key, VeniceMessage msg) {

    // check for invalid inputs
    if (null == msg) {
      logger.error("Given null Venice Message.");
      return false;
    }

    if (null == msg.getOperationType()) {
      logger.error("Venice Message does not have operation type!");
      return false;
    }

    if (!partitionIdList.contains(partitionId)) {
      logger.error("Partition does not exist: " + partitionId);
      return false;
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
        logger.error("No instance of node id: " + nodeId);
        return false;
      }

    }

    // Perform a put for every replica
    for (int nodeId : nodeIds) {

      switch (msg.getOperationType()) {

        // adding new values
        case PUT:
          storeNodeMap.get(nodeId).put(partitionId, key, msg.getPayload());
          logger.info("Putting: " + key + ", " + msg.getPayload());
          break;

        // deleting values
        case DELETE:
          storeNodeMap.get(nodeId).delete(partitionId, key);
          logger.info("Deleting: " + key);
          break;

        // partial update
        case PARTIAL_PUT:
          throw new UnsupportedOperationException("Partial puts not yet implemented");

        // error
        default:
          logger.error("Invalid operation type submitted: " + msg.getOperationType());
          break;
      }

    }

    return true;

  }

  /**
   * Method that calculates the nodeId for a given partitionId and creates the partition if does not exist
   * Must be a deterministic method for partitionIds AND their replicas
   * */
  private List<Integer> calculateNodeId(int partitionId) {

    int numNodes = storeNodeMap.size();

    if (0 == numNodes) {
      logger.error("Cannot calculate node id for partition because there are no nodes!");
      return new ArrayList<Integer>();
    }

    // TODO: improve algorithm to provide true balancing
    List<Integer> nodeIds = new ArrayList<Integer>();
    for (int i = 0; i < replicationFactor; i++) {
      nodeIds.add((partitionId + i) % numNodes);
    }

    return nodeIds;

  }

}
