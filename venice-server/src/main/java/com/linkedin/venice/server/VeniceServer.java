package com.linkedin.venice.server;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerException;
import com.linkedin.venice.storage.InMemoryStorageNode;
import com.linkedin.venice.storage.StorageType;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.VeniceStorageNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Primary Venice Server Main class.
 */
public class VeniceServer {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceServer.class.getName());

  private static final String DEFAULT_TOPIC = "default_topic";

  private static StorageType storageType;

  // a temporary variable to store InMemory Nodes
  // NOTE: for any Non-InMemory instances, this will not be used, as each Venice instance should be its own Node.
  private static Map<Integer, VeniceStorageNode> storeNodeMap;

  public static void main(String args[]) {

    try {
      GlobalConfiguration.initializeFromFile(args[0]);
    } catch (Exception e) {
      logger.error(e.getMessage());
      System.exit(1);
    }

    initializeKakfaConsumer();
    initializeStorage();

  }

  public static void initializeKakfaConsumer() {

    // Start the service which provides partition connections to Kafka
    KafkaConsumerPartitionManager.initialize(DEFAULT_TOPIC,
        GlobalConfiguration.getBrokerList(), GlobalConfiguration.getKafkaBrokerPort());

  }

  /**
   *  Start the storage instance; includes physical storage and Kafka consumption objects
   */
  public static void initializeStorage() {

    storageType = GlobalConfiguration.getStorageType();

    try {

      switch (storageType) {

        case MEMORY:

          storeNodeMap = new HashMap<Integer, VeniceStorageNode>();

          // start nodes
          for (int n = 0; n < GlobalConfiguration.getNumStorageNodes(); n++) {
            storeNodeMap.put(n, createNewStoreNode(n));
          }

          // start partitions
          for (int p = 0; p < GlobalConfiguration.getNumKafkaPartitions(); p++) {
            registerNewPartition(p);
          }

          break;

        default:
          throw new UnsupportedOperationException("Only the In Memory implementation " +
                  "is supported in this version.");

      }

    } catch (VeniceStorageException e) {

      logger.error("Could not properly initialize the storage instance.");
      e.printStackTrace();
      shutdown();

    } catch (VeniceKafkaConsumerException e) {

      logger.error("Could not properly initialize Venice Kafka instance.");
      e.printStackTrace();
      shutdown();

    }

  }

  /**
   * Creates a new VeniceStorageNode, based on the current configuration
   * */
  private static VeniceStorageNode createNewStoreNode(int nodeId) {

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
   * Registers a new partitionId and adds all of its copies to its associated nodes
   * Creates a storage partition and a Kafka Consumer for the given partition, and ties it to the node
   * @param partitionId - the id of the partition to register
   * */
  public static void registerNewPartition(int partitionId)
          throws VeniceStorageException, VeniceKafkaConsumerException {

    // use conversion algorithm to find nodeId
    List<Integer> nodeIds = getNodeMappings(partitionId);

    for (int nodeId : nodeIds) {
      VeniceStorageNode node = storeNodeMap.get(nodeId);
      node.addPartition(partitionId);
    }

  }

  /**
   * Perform a "GET" on the Venice Storage
   * @param key - The key to query in storage
   * @return The value associated with the given key
   * */
  public Object readValue(String key) {
    throw new UnsupportedOperationException("Read protocol is not yet supported");
  }

  /**
   * Method that calculates the nodeId for a given partitionId and creates the partition if does not exist
   * Must be a deterministic method for partitionIds AND their replicas
   * @param partitionId - The Kafka partitionId to be used in calculation
   * @return A list of all the nodeIds associated with the given partitionId
   * */
  private static List<Integer> getNodeMappings(int partitionId) throws VeniceStorageException {

    int numNodes = storeNodeMap.size();

    if (0 == numNodes) {
      throw new VeniceStorageException("Cannot calculate node id for partition because there are no nodes!");
    }

    // TODO: improve algorithm to provide true balancing
    List<Integer> nodeIds = new ArrayList<Integer>();
    for (int i = 0; i < GlobalConfiguration.getNumStorageCopies(); i++) {
      nodeIds.add((partitionId + i) % numNodes);
    }

    return nodeIds;

  }

  /**
   * Method which closes VeniceServer, shuts down its resources, and exits the JVM.
   * */
  public static void shutdown() {
    System.exit(1);
  }

}
