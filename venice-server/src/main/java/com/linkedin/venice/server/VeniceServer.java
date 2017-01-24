package com.linkedin.venice.server;

import com.google.common.collect.ImmutableList;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixParticipationService;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.WhitelistAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import com.linkedin.venice.listener.ListenerService;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.offsets.BdbOffsetManager;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.ServerAggStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


// TODO curate all comments later
public class VeniceServer {

  private static final Logger logger = Logger.getLogger(VeniceServer.class);
  private final VeniceConfigLoader veniceConfigLoader;
  private final AtomicBoolean isStarted;

  private StorageService storageService;
  private BdbOffsetManager offSetService;
  private KafkaConsumerPerStoreService kafkaConsumerPerStoreService;

  private ReadOnlySchemaRepository schemaRepo;

  private final List<AbstractVeniceService> services;

  public final static String SERVER_SERVICE_NAME = "venice-server";

  public VeniceServer(VeniceConfigLoader veniceConfigLoader)
      throws VeniceException {
    this(veniceConfigLoader, TehutiUtils.getMetricsRepository(SERVER_SERVICE_NAME));
  }

  public VeniceServer(VeniceConfigLoader veniceConfigLoader, MetricsRepository  metricsRepository) {
    this.isStarted = new AtomicBoolean(false);
    this.veniceConfigLoader = veniceConfigLoader;
    if (!isServerInWhiteList(veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(),
                             veniceConfigLoader.getVeniceClusterConfig().getClusterName(),
                             veniceConfigLoader.getVeniceServerConfig().getListenerPort(),
                             veniceConfigLoader.getVeniceServerConfig().isServerWhiteLIstEnabled())) {
      throw new VeniceException(
          "Can not create a venice server because this server has not been added into white list.");
    }

    if (!veniceConfigLoader.getVeniceServerConfig().isAutoCreateDataPath()) {
      if (!directoryExists(veniceConfigLoader.getVeniceServerConfig().getDataBasePath())) {
        throw new VeniceException("Data directory: " + veniceConfigLoader.getVeniceServerConfig().getDataBasePath()
            + " does not exist and " + ConfigKeys.AUTOCREATE_DATA_PATH + " is set to false.  Cannot create server.");
      }
    }

    /*
     * TODO - 1. How do the servers share the same config - For example in Voldemort we use cluster.xml and stores.xml.
     * 2. Check Hostnames like in Voldemort to make sure that local host and ips match up.
     */

    //create all services
    this.services = createServices();

    ServerAggStats.init(metricsRepository, kafkaConsumerPerStoreService);
  }

  /**
   * Instantiate all known services. Most of the services in this method intake:
   * 1. StoreRepository - that maps store to appropriate storage engine instance
   * 2. VeniceConfig - which contains configs related to this cluster
   * 3. StoreNameToConfigsMap - which contains store specific configs
   * 4. PartitionAssignmentRepository - which contains how partitions for each store are mapped to nodes in the
   *    cluster
   *
   * @return
   */
  private List<AbstractVeniceService> createServices() {
    /* Services are created in the order they must be started */
    List<AbstractVeniceService> services = new ArrayList<AbstractVeniceService>();

    VeniceClusterConfig clusterConfig = veniceConfigLoader.getVeniceClusterConfig();

    // create and add StorageService. storeRepository will be populated by StorageService,
    storageService = new StorageService(veniceConfigLoader);
    services.add(storageService);

    // Create and add Offset Service.
    offSetService = new BdbOffsetManager(veniceConfigLoader.getVeniceClusterConfig());
    services.add(offSetService);

    // Create ReadOnlySchemaRepository
    schemaRepo = createSchemaRepository(clusterConfig);

    //create and add KafkaSimpleConsumerService
    this.kafkaConsumerPerStoreService =
        new KafkaConsumerPerStoreService(storageService.getStoreRepository(), veniceConfigLoader, offSetService, schemaRepo);

    // start venice participant service if Helix is enabled.
    if(clusterConfig.isHelixEnabled()) {
      HelixParticipationService helixParticipationService =
          new HelixParticipationService(kafkaConsumerPerStoreService, storageService, veniceConfigLoader,
              clusterConfig.getZookeeperAddress(), clusterConfig.getClusterName(),
              veniceConfigLoader.getVeniceServerConfig().getListenerPort());
      services.add(helixParticipationService);
      // Add kafka consumer service last so when shutdown the server, it will be stopped first to avoid the case
      // that helix is disconnected but consumption service try to send message by helix.
      services.add(kafkaConsumerPerStoreService);
    } else {
      // Note: Only required when NOT using Helix.
      throw new UnsupportedOperationException("Only Helix mode of operation is supported");
    }

    //create and add ListenerServer for handling GET requests
    ListenerService listenerService =
        new ListenerService(storageService.getStoreRepository(), offSetService, veniceConfigLoader);
    services.add(listenerService);


    /**
     * TODO Create an admin service later. The admin service will need both StorageService and KafkaSimpleConsumerService
     * passed on to it.
     *
     * To add a new store do this in order:
     * 1. Populate storeNameToConfigsMap
     * 2. Get the assignment plan from PartitionNodeAssignmentScheme and  populate the PartitionAssignmentRepository
     * 3. call StorageService.openStore(..) to create the appropriate storage partitions
     * 4. call KafkaSimpleConsumerService.startConsumption(..) to create and start the consumer tasks for all kafka partitions.
     */

    return ImmutableList.copyOf(services);
  }

  private ReadOnlySchemaRepository createSchemaRepository(VeniceClusterConfig clusterConfig) {
    ZkClient zkClient = new ZkClient(clusterConfig.getZookeeperAddress(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    String clusterName = clusterConfig.getClusterName();
    ReadOnlyStoreRepository storeRepo = new HelixReadOnlyStoreRepository(zkClient, adapter, clusterName);
    // Load existing store config and setup watches
    storeRepo.refresh();
    ReadOnlySchemaRepository schemaRepo = new HelixReadOnlySchemaRepository(storeRepo, zkClient, adapter, clusterName);
    schemaRepo.refresh();

    return schemaRepo;
  }

  public StorageService getStorageService(){
    if (isStarted()){
      return storageService;
    } else {
      throw new VeniceException("Cannot get storage service if server is not started");
    }
  }

  /**
   * @return true if the {@link VeniceServer} and all of its inner services are fully started
   *         false if the {@link VeniceServer} was not started or if any of its inner services
   *         are not finished starting.
   */
  public boolean isStarted() {
    return isStarted.get() && services.stream().allMatch(abstractVeniceService -> abstractVeniceService.isStarted());
  }

  /**
   * Method which starts the services instantiate earlier
   *
   * @throws Exception
   */
  public void start()
      throws VeniceException {
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
      throws VeniceException {
    List<Exception> exceptions = new ArrayList<Exception>();
    logger.info("Stopping all services ");

    /* Stop in reverse order */

    //TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
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
      logger.info("All services stopped");

      if (exceptions.size() > 0) {
        throw new VeniceException(exceptions.get(0));
      }
      isStarted.set(false);

      // TODO - Efficient way to unlock java heap
    }
  }

  protected boolean isServerInWhiteList(String zkAddress, String clusterName, int listenPort,
      boolean enableServerWhitelist) {
    if (!enableServerWhitelist) {
      logger.info("Check whitelist is disable, continue to start participant.");
      return true;
    }
    try (WhitelistAccessor accessor = new ZkWhitelistAccessor(zkAddress)) {
      // Note: If a server has been added in to the white list, then node is failed or shutdown by SRE. once it
      // start up again, it will automatically join the cluster because it already exists in the white list.
      String participantName = Utils.getHelixNodeIdentifier(listenPort);
      if (!accessor.isInstanceInWhitelist(clusterName, participantName)) {
        logger.info(participantName + " is not in the white list of " + clusterName + ", stop starting venice server");
        return false;
      } else {
        logger.info(participantName + " has been added into white list, continue to start participant.");
        return true;
      }
    } catch (Exception e) {
      String errorMsg = "Met error during checking white list.";
      logger.error(errorMsg, e);
      throw new VeniceException(errorMsg, e);
    }
  }

  protected static boolean directoryExists(String dataDirectory){
    File dir = new File(dataDirectory).getAbsoluteFile();
    return dir.isDirectory();
  }

  public static void main(String args[])
      throws Exception {
    VeniceConfigLoader veniceConfigService = null;
    try {
      if (args.length == 0) {
        veniceConfigService = VeniceConfigLoader.loadFromEnvironmentVariable();
      } else if (args.length == 1) {
        veniceConfigService = VeniceConfigLoader.loadFromConfigDirectory(args[0]);
      } else {
        Utils.croak("USAGE: java " + VeniceServer.class.getName() + " [venice_config_dir] ");
      }
    } catch (Exception e) {
      logger.error("Error starting Venice Server ", e);
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

  }
}
