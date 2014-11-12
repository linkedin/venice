package com.linkedin.venice.server;

import com.google.common.collect.ImmutableList;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.KafkaConsumerException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.InMemoryStorageNode;
import com.linkedin.venice.storage.StorageType;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.VeniceStorageNode;
import com.linkedin.venice.utils.Utils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


// TODO curate all comments later
public class VeniceServer {

  private static final Logger logger = Logger.getLogger(VeniceServer.class.getName());
  private final VeniceConfig veniceConfig;
  private final AtomicBoolean isStarted;

  private StorageType storageType;

  private final List<AbstractVeniceService> services;

  // a temporary variable to store InMemory Nodes
  // NOTE: for any Non-InMemory instances, this will not be used, as each
  // Venice instance should be its own Node.
  private static Map<Integer, VeniceStorageNode> storeNodeMap;

  public VeniceServer(VeniceConfig veniceConfig) {
    this.isStarted = new AtomicBoolean(false);
    this.veniceConfig = veniceConfig;
    this.storageType = veniceConfig.getStorageType();
    this.services = createServices();

		/*
     * TODO - 1. Consider distribution of storage across servers. How do the servers share the same
		 * config - For example in Voldemort we use cluster.xml and stores.xml. 
		 * 2. Check Hostnames like in Voldemort to make sure that local host and 
         * ips match up.
		 */
  }

  public boolean isStarted() {
    return isStarted.get();
  }

  private List<AbstractVeniceService> createServices() {
    /* Services are created in the order they must be started */
    List<AbstractVeniceService> services = new ArrayList<AbstractVeniceService>();

    // TODO create services for Kafka Consumer and Storage

    return ImmutableList.copyOf(services);
  }

  public void start()
      throws Exception {

    // TODO - Efficient way to lock java heap
    logger.info("Starting " + services.size() + " services.");
    long start = System.currentTimeMillis();
    for (AbstractVeniceService service : services) {
      service.start();
    }
    long end = System.currentTimeMillis();
    logger.info("Startup completed in " + (end - start) + " ms.");

    // <<<TODO - Seperate this logic LATER - START>>>>
    // Start the service which provides partition connections to Kafka
    KafkaConsumerPartitionManager.initialize(veniceConfig);
    try {
      switch (storageType) {
        case MEMORY:
          storeNodeMap = new HashMap<Integer, VeniceStorageNode>();
          // start nodes
          for (int n = 0; n < veniceConfig.getNumStorageNodes(); n++) {
            storeNodeMap.put(n, createNewStoreNode(n));
          }
          // start partitions
          for (int p = 0; p < veniceConfig.getNumKafkaPartitions(); p++) {
            registerNewPartition(p);
          }
          break;
        default:
          throw new UnsupportedOperationException(
              "Only the In Memory implementation " + "is supported in this version.");
      }
    } catch (VeniceStorageException e) {
      logger.error("Could not properly initialize the storage instance.");
      e.printStackTrace();
      shutdown();
    } catch (KafkaConsumerException e) {
      logger.error("Could not properly initialize Venice Kafka instance.");
      e.printStackTrace();
      shutdown();
    }
    // <<<TODO  - Seperate this logic LATER - END>>>>
  }

  /**
   * Method which closes VeniceServer, shuts down its resources, and exits the
   * JVM.
   * @throws Exception
   * */
  public void shutdown()
      throws Exception {
    List<Exception> exceptions = new ArrayList<Exception>();
    // TODO handle exceptions as necessary - Introduce Venice specific
    // Exceptions
    logger.info("Stopping services"); // TODO -
    // "Stopping services on Node: <node-id>"
    // - Need to get current ode id
    // information
		/* Stop in reverse order */
    for (AbstractVeniceService service : Utils.reversed(services)) {
      try {
        service.stop();
      } catch (Exception e) {
        exceptions.add(e);
        logger.error(e);
      }
    }
    logger.info("All services stopped"); // "All services stopped for Node:"
    // + <node-id>);

    if (exceptions.size() > 0) {
      throw exceptions.get(0);
    }

    // TODO - Efficient way to unlock java heap

    // TODO get rid of System.exit later
    System.exit(1);
  }

  public static void main(String args[])
      throws Exception {
    VeniceConfig veniceConfig = null;
    try {
      if (args.length == 0) {
        veniceConfig = VeniceConfig.loadFromEnvironmentVariable();
      } else if (args.length == 1) {
        veniceConfig = VeniceConfig.loadFromVeniceHome(args[0]);
      } else if(args.length == 2){
         veniceConfig = VeniceConfig.loadFromVeniceHome(args[0], args[1]);
      } else{
        Utils.croak("USAGE: java " + VeniceServer.class.getName() + " [venice_home_dir]  [venice_config_dir] ");
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
      Utils.croak("Error while loading configuration: " + e.getMessage());
    }
    final VeniceServer server = new VeniceServer(veniceConfig);
    if (!server.isStarted()) {
      server.start();
    }

    // TODO Add a shutdown hook ?
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
   * Registers a new partitionId and adds all of its copies to its associated
   * nodes Creates a storage partition and a Kafka Consumer for the given
   * partition, and ties it to the node
   *
   * @param partitionId
   *            - the id of the partition to register
   * */
  public void registerNewPartition(int partitionId)
      throws VeniceStorageException, KafkaConsumerException {
    // use conversion algorithm to find nodeId
    List<Integer> nodeIds = getNodeMappings(partitionId);
    for (int nodeId : nodeIds) {
      VeniceStorageNode node = storeNodeMap.get(nodeId);
      node.addPartition(partitionId);
    }
  }

  /**
   * Perform a "GET" on the Venice Storage
   *
   * @param key
   *            - The key to query in storage
   * @return The value associated with the given key
   * */
  public Object readValue(String key) {
    throw new UnsupportedOperationException("Read protocol is not yet supported");
  }

  /**
   * Method that calculates the nodeId for a given partitionId and creates the
   * partition if does not exist Must be a deterministic method for
   * partitionIds AND their replicas
   *
   * @param partitionId
   *            - The Kafka partitionId to be used in calculation
   * @return A list of all the nodeIds associated with the given partitionId
   * */
  private List<Integer> getNodeMappings(int partitionId)
      throws VeniceStorageException {
    // TODO Separate the partition assignment logic and make it pluggable.
    int numNodes = storeNodeMap.size();
    if (0 == numNodes) {
      throw new VeniceStorageException("Cannot calculate node id for partition because there are no nodes!");
    }
    // TODO: improve algorithm to provide true balancing
    List<Integer> nodeIds = new ArrayList<Integer>();
    for (int i = 0; i < veniceConfig.getNumStorageCopies(); i++) {
      nodeIds.add((partitionId + i) % numNodes);
    }
    return nodeIds;
  }
}
