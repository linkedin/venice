package com.linkedin.venice.server;

import com.google.common.collect.ImmutableList;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.partition.AbstractPartitionNodeAssignmentScheme;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


// TODO curate all comments later
public class VeniceServer {

  private static final Logger logger = Logger.getLogger(VeniceServer.class.getName());
  private final VeniceConfig veniceConfig;
  private final AtomicBoolean isStarted;

  private final StoreRepository storeRepository;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  private AbstractPartitionNodeAssignmentScheme partitionNodeAssignmentScheme;

  private final List<AbstractVeniceService> services;

  private final ConcurrentMap<String, Properties> storeNameToConfigsMap;

  public VeniceServer(VeniceConfig veniceConfig)
      throws Exception {
    this.isStarted = new AtomicBoolean(false);
    this.veniceConfig = veniceConfig;
    this.storeNameToConfigsMap = new ConcurrentHashMap<String, Properties>();
    this.storeRepository = new StoreRepository();
    this.partitionNodeAssignmentRepository = new PartitionNodeAssignmentRepository();

    //initialize all store configs
    this.initStoreConfigs();

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
   * Go over the list of configured stores and initialize
   * 1. store configs map
   *
   * Note that the individual configs include both storage and streaming layer (i.e. kafka) specific configs for each Venice store
   */
  private void initStoreConfigs()
      throws Exception {
    logger.info("Initializing store configs:");
    File storeConfigsDir =
        new File(veniceConfig.getConfigDirAbsolutePath() + File.separator + veniceConfig.STORE_CONFIGS_DIR_NAME);
    if (!Utils.isReadableDir(storeConfigsDir)) {
      String errorMessage =
          "Either the " + VeniceConfig.STORE_CONFIGS_DIR_NAME + " directory does not exist or is not readable.";
      logger.error(errorMessage);
      throw new Exception(errorMessage); // TODO later change this to appropriate Exception Type.
    }

    // Get all .properties file in config/STORES directory
    List<File> storeConfigurationFiles = Arrays.asList(storeConfigsDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".properties");
      }
    }));

    //parse the properties for each store
    for (File storeConfig : storeConfigurationFiles) {
      try {
        Properties prop = Utils.parseProperties(storeConfig);
        storeNameToConfigsMap.putIfAbsent(prop.getProperty("name"), prop);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Assigns logical partitions to Node for each store based on the storage replication factor and total number of nodes
   * in the cluster. The scheme for this assignment is available in config.properties and is parsed by VeniceConfig.
   * When this method finishes the PartitionToNodeAssignmentRepository is populated which is then used by other services.
   */
  private void assignPartitionToNodes()
      throws Exception {
    logger.info("Populating partition node assignment repository");
    String partitionNodeAssignmentSchemeClassName =
        veniceConfig.getPartitionNodeAssignmentSchemeClassMap(veniceConfig.getPartitionNodeAssignmentSchemeName());
    if (partitionNodeAssignmentSchemeClassName != null) {
      try {
        Class<?> AssignmentSchemeClass = ReflectUtils.loadClass(partitionNodeAssignmentSchemeClassName);
        partitionNodeAssignmentScheme = (AbstractPartitionNodeAssignmentScheme) ReflectUtils
            .callConstructor(AssignmentSchemeClass, new Class<?>[]{}, new Object[]{});
      } catch (IllegalStateException e) {
        String errorMessage =
            "Error loading Partition Node Assignment Class '" + partitionNodeAssignmentSchemeClassName + "'.";
        logger.error(errorMessage, e);
        throw new Exception(errorMessage); // TODO later change this to appropriate Exception Type.
      }
    } else {
      String erroMessage = "Unknown Partition Node Assignment Scheme: " + partitionNodeAssignmentSchemeClassName;
      logger.error(erroMessage);
      throw new Exception(erroMessage); // TODO later change this to appropriate Exception Type.
    }
    for (Map.Entry<String, Properties> storeEntry : storeNameToConfigsMap.entrySet()) {
      Map<Integer, Set<Integer>> nodeToLogicalPartitionIdsMap = partitionNodeAssignmentScheme
          .getNodeToLogicalPartitionsMap(storeEntry.getValue(), veniceConfig.getNumStorageNodes());
      partitionNodeAssignmentRepository.setAssignment(storeEntry.getKey(), nodeToLogicalPartitionIdsMap);
    }
  }

  /**
   * Instantiate all known services. Most of the services in this method intake:
   * 1. StoreRepositry - that maps store to appropriate storage engine instance
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
        new StorageService(storeRepository, veniceConfig, storeNameToConfigsMap, partitionNodeAssignmentRepository);
    services.add(storageService);

    //create and add KafkaConsumerService
    KafkaConsumerService kafkaConsumerService =
        new KafkaConsumerService(storeRepository, veniceConfig, storeNameToConfigsMap,
            partitionNodeAssignmentRepository);
    services.add(kafkaConsumerService);

    /**
     * TODO Create an admin service later. The admin service will need both StorageService and KafkaConsumerService
     * passed on to it.
     *
     * To add a new store do this in order:
     * 1. Get the assignment plan from PartitionNodeAssignmentScheme and  populate the PartitionNodeAssignmentRepository
     * 2. call StorageService.openStore(..) to create the appropriate storage partitions
     * 3. call KafkaConsumerService.startConsumption(..) to create and start the consumer tasks for all kafka partitions.
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
    logger.info("Stopping all services"); // TODO -"Stopping services on Node: <node-id>"
    // - Need to get current node id information
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
    VeniceConfig veniceConfig = null;
    try {
      if (args.length == 0) {
        veniceConfig = VeniceConfig.loadFromEnvironmentVariable();
      } else if (args.length == 1) {
        veniceConfig = VeniceConfig.loadFromVeniceHome(args[0]);
      } else if (args.length == 2) {
        veniceConfig = VeniceConfig.loadFromVeniceHome(args[0], args[1]);
      } else {
        Utils.croak("USAGE: java " + VeniceServer.class.getName() + " [venice_home_dir]  [venice_config_dir] ");
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
      e.printStackTrace();
      Utils.croak("Error while loading configuration: " + e.getMessage());
    }
    final VeniceServer server = new VeniceServer(veniceConfig);
    if (!server.isStarted()) {
      server.start();
    }

    // TODO Add a shutdown hook ?
  }
}
