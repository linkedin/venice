package com.linkedin.venice.server;

import com.google.common.collect.ImmutableList;
import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.helix.HelixParticipationService;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import com.linkedin.venice.listener.ListenerService;
import com.linkedin.venice.partition.AbstractPartitionNodeAssignmentScheme;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Set;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


// TODO curate all comments later
public class VeniceServer {

  private static final Logger logger = Logger.getLogger(VeniceServer.class.getName());
  private final VeniceConfigService veniceConfigService;
  private final AtomicBoolean isStarted;

  private final StoreRepository storeRepository;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  private AbstractPartitionNodeAssignmentScheme partitionNodeAssignmentScheme;

  private final List<AbstractVeniceService> services;

  public VeniceServer(VeniceConfigService veniceConfigService)
      throws Exception {
    this.isStarted = new AtomicBoolean(false);
    this.veniceConfigService = veniceConfigService;
    this.storeRepository = new StoreRepository();
    this.partitionNodeAssignmentRepository = new PartitionNodeAssignmentRepository();

    /*
     * TODO - 1. How do the servers share the same config - For example in Voldemort we use cluster.xml and stores.xml.
     * 2. Check Hostnames like in Voldemort to make sure that local host and ips match up.
     */

    //Populates the partitionToNodeAssignmentRepository
    this.assignPartitionToNodes();

    //create all services
    this.services = createServices();
  }

  /**
   * Assigns logical partitions to Node for each store based on the storage replication factor and total number of nodes
   * in the cluster. The scheme for this assignment is available in config.properties and is parsed by VeniceConfig.
   * When this method finishes the PartitionToNodeAssignmentRepository is populated which is then used by other services.
   */
  private void assignPartitionToNodes() {
    logger.info("Populating partition node assignment repository");
    String partitionNodeAssignmentSchemeClassName =
        veniceConfigService.getVeniceClusterConfig().getPartitionNodeAssignmentSchemeClassName();
    try {
      Class<?> AssignmentSchemeClass = ReflectUtils.loadClass(partitionNodeAssignmentSchemeClassName);
      partitionNodeAssignmentScheme = (AbstractPartitionNodeAssignmentScheme) ReflectUtils
          .callConstructor(AssignmentSchemeClass, new Class<?>[]{}, new Object[]{});
    } catch (IllegalStateException e) {
      String errorMessage =
          "Error loading Partition Node Assignment Class '" + partitionNodeAssignmentSchemeClassName + "'.";
      logger.error(errorMessage, e);
      throw e;
    }

    for (Map.Entry<String, VeniceStoreConfig> storeEntry : veniceConfigService.getAllStoreConfigs().entrySet()) {
      Map<Integer, Set<Integer>> nodeToLogicalPartitionIdsMap =
          partitionNodeAssignmentScheme.getNodeToLogicalPartitionsMap(storeEntry.getValue());
      partitionNodeAssignmentRepository.setAssignment(storeEntry.getKey(), nodeToLogicalPartitionIdsMap);
    }
  }

  /**
   * Instantiate all known services. Most of the services in this method intake:
   * 1. StoreRepository - that maps store to appropriate storage engine instance
   * 2. VeniceConfig - which contains configs related to this cluster
   * 3. StoreNameToConfigsMap - which contains store specific configs
   * 4. PartitionNodeAssignmentRepository - which contains how partitions for each store are mapped to nodes in the
   *    cluster
   *
   * @return
   */
  private List<AbstractVeniceService> createServices() {
    /* Services are created in the order they must be started */
    List<AbstractVeniceService> services = new ArrayList<AbstractVeniceService>();

    // create and add StorageService. storeRepository will be populated by StorageService,
    StorageService storageService =
        new StorageService(storeRepository, veniceConfigService, partitionNodeAssignmentRepository);
    services.add(storageService);

    //create and add KafkaSimpleConsumerService
    KafkaConsumerPerStoreService kafkaConsumerService =
        new KafkaConsumerPerStoreService(storeRepository, veniceConfigService);
    services.add(kafkaConsumerService);

    // Start venice participant service if Helix is enabled.
    VeniceClusterConfig clusterConfig = veniceConfigService.getVeniceClusterConfig();
    if(clusterConfig.isHelixEnabled()) {
      HelixParticipationService helixParticipationService = new HelixParticipationService(kafkaConsumerService
          , storeRepository, veniceConfigService, clusterConfig.getZookeeperAddress(), clusterConfig.getClusterName()
          , HelixUtils.convertNodeIdToHelixParticipantName(veniceConfigService.getVeniceServerConfig().getNodeId()));
      services.add(helixParticipationService);
    } else {
      // Note: Only required when NOT using Helix.
      kafkaConsumerService.consumeForPartitionNodeAssignmentRepository(partitionNodeAssignmentRepository);
    }

    //create and add ListenerServer for handling GET requests
    ListenerService listenerService =
        new ListenerService(storeRepository, veniceConfigService, partitionNodeAssignmentRepository);
    services.add(listenerService);


    /**
     * TODO Create an admin service later. The admin service will need both StorageService and KafkaSimpleConsumerService
     * passed on to it.
     *
     * To add a new store do this in order:
     * 1. Populate storeNameToConfigsMap
     * 2. Get the assignment plan from PartitionNodeAssignmentScheme and  populate the PartitionNodeAssignmentRepository
     * 3. call StorageService.openStore(..) to create the appropriate storage partitions
     * 4. call KafkaSimpleConsumerService.startConsumption(..) to create and start the consumer tasks for all kafka partitions.
     */

    return ImmutableList.copyOf(services);
  }

  public boolean isStarted() {
    return isStarted.get();
  }

  /**
   * Method which starts the services instantiate earlier
   *
   * @throws Exception
   */
  public void start()
      throws Exception {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new IllegalStateException("Service is already started!");
    }
    // TODO - Efficient way to lock java heap
    logger.info("Starting " + services.size() + " services.");
    long start = System.currentTimeMillis();
    for (AbstractVeniceService service : services) {
      service.start();
    }
    long end = System.currentTimeMillis();
    logger.info("Startup completed in " + (end - start) + " ms.");
  }

  /**
   * Method which closes VeniceServer, shuts down its resources, and exits the
   * JVM.
   * @throws Exception
   * */
  public void shutdown()
      throws Exception {
    List<Exception> exceptions = new ArrayList<Exception>();
    logger.info("Stopping all services on Node: " + veniceConfigService.getVeniceServerConfig().getNodeId());

    /* Stop in reverse order */

    synchronized (this) {
      if (!isStarted()) {
        logger.info("The server is already stopped, ignoring duplicate attempt.");
        return;
      }
      for (AbstractVeniceService service : Utils.reversed(services)) {
        try {
          service.stop();
        } catch (Exception e) {
          exceptions.add(e);
          logger.error("Exception in stopping service: " + service.getName(), e);
        }
      }
      logger.info("All services stopped"); // "All services stopped for Node:"
      // + <node-id>);

      if (exceptions.size() > 0) {
        throw exceptions.get(0);
      }
      isStarted.set(false);

      // TODO - Efficient way to unlock java heap
    }
  }

  public StoreRepository getStoreRepository() {
    return storeRepository;
  }

  public static void main(String args[])
      throws Exception {
    VeniceConfigService veniceConfigService = null;
    try {
      if (args.length == 0) {
        veniceConfigService = VeniceConfigService.loadFromEnvironmentVariable();
      } else if (args.length == 1) {
        veniceConfigService = new VeniceConfigService(args[0]);
      } else {
        Utils.croak("USAGE: java " + VeniceServer.class.getName() + "[venice_config_dir] ");
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
      e.printStackTrace();
      Utils.croak("Error while loading configuration: " + e.getMessage());
    }
    final VeniceServer server = new VeniceServer(veniceConfigService);
    if (!server.isStarted()) {
      server.start();
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (server.isStarted()) {
          try {
            server.shutdown();
          } catch (Exception e) {
            logger.error("Error shutting the server. ", e);
          }
        }
      }
    });
    Thread.currentThread().join();
  }
}
