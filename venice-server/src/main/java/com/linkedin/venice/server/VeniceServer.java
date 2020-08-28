package com.linkedin.venice.server;

import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.cleaner.LeakedResourceCleaner;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixParticipationService;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.WhitelistAccessor;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.listener.ListenerService;
import com.linkedin.venice.listener.StoreValueSchemasCacheService;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.AggRocksDBStats;
import com.linkedin.venice.stats.AggVersionedBdbStorageEngineStats;
import com.linkedin.venice.stats.AggVersionedStorageEngineStats;
import com.linkedin.venice.stats.DiskHealthStats;
import com.linkedin.venice.stats.RocksDBMemoryStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceJVMStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.storage.BdbStorageMetadataService;
import com.linkedin.venice.storage.DiskHealthCheckService;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.storage.StorageEngineMetadataService;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.utils.Utils;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;

import io.tehuti.metrics.MetricsRepository;

import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;


// TODO curate all comments later
public class VeniceServer {
  private static final Logger logger = Logger.getLogger(VeniceServer.class);

  public final static String SERVER_SERVICE_NAME = "venice-server";

  private final VeniceConfigLoader veniceConfigLoader;
  private final Optional<SSLEngineComponentFactory> sslFactory;
  private final Optional<StaticAccessController> accessController;
  private final Optional<ClientConfig> clientConfigForConsumer;
  private final AtomicBoolean isStarted;
  private final List<AbstractVeniceService> services;

  private StorageService storageService;
  private StorageMetadataService storageMetadataService;
  private StorageEngineMetadataService storageEngineMetadataService;
  private BdbStorageMetadataService bdbMetadataService;
  private KafkaStoreIngestionService kafkaStoreIngestionService;
  private HelixParticipationService helixParticipationService;
  private LeakedResourceCleaner leakedResourceCleaner;
  private DiskHealthCheckService diskHealthCheckService;
  private MetricsRepository metricsRepository;
  private ReadOnlyStoreRepository metadataRepo;
  private ReadOnlySchemaRepository schemaRepo;
  private ZkClient zkClient;
  private VeniceJVMStats jvmStats;

  public VeniceServer(VeniceConfigLoader veniceConfigLoader)
      throws VeniceException {
    this(veniceConfigLoader, TehutiUtils.getMetricsRepository(SERVER_SERVICE_NAME));
  }

  public VeniceServer(VeniceConfigLoader veniceConfigLoader, MetricsRepository  metricsRepository) {
    this(veniceConfigLoader, metricsRepository, Optional.empty(), Optional.empty(), Optional.empty());
  }

  public VeniceServer(
      VeniceConfigLoader veniceConfigLoader,
      MetricsRepository metricsRepository,
      Optional<SSLEngineComponentFactory> sslFactory, // TODO: Clean this up. We shouldn't use proprietary abstractions.
      Optional<StaticAccessController> accessController,
      Optional<ClientConfig> clientConfigForConsumer) {

    // force out any potential config errors using a wildcard store name
    veniceConfigLoader.getStoreConfig("");

    if (!isServerInWhiteList(
        veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(),
        veniceConfigLoader.getVeniceClusterConfig().getClusterName(),
        veniceConfigLoader.getVeniceServerConfig().getListenerPort(),
        veniceConfigLoader.getVeniceServerConfig().isServerWhitelistEnabled())) {
      throw new VeniceException("Can not create a venice server because this server has not been added into white list.");
    }

    this.isStarted = new AtomicBoolean(false);
    this.veniceConfigLoader = veniceConfigLoader;
    this.metricsRepository = metricsRepository;
    this.sslFactory = sslFactory;
    this.accessController = accessController;
    this.clientConfigForConsumer = clientConfigForConsumer;
    this.services = createServices();
  }

  /**
   * Instantiate all known services. Most of the services in this method intake:
   * 1. StorageEngineRepository - that maps store to appropriate storage engine instance
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

    // Create jvm metrics object
    jvmStats = new VeniceJVMStats(metricsRepository, "VeniceJVMStats");

    // Create and add Offset Service.
    VeniceClusterConfig clusterConfig = veniceConfigLoader.getVeniceClusterConfig();
    bdbMetadataService = new BdbStorageMetadataService(clusterConfig);
    services.add(bdbMetadataService);

    // Create ReadOnlyStore/SchemaRepository
    createHelixStoreAndSchemaRepository(clusterConfig, metricsRepository);

    // TODO: It would be cleaner to come up with a storage engine metric abstraction so we're not passing around so
    // many objects in constructors
    AggVersionedBdbStorageEngineStats bdbStorageEngineStats = new AggVersionedBdbStorageEngineStats(metricsRepository, metadataRepo);
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(metricsRepository, metadataRepo);
    boolean plainTableEnabled = veniceConfigLoader.getVeniceServerConfig().getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled();
    RocksDBMemoryStats rocksDBMemoryStats = veniceConfigLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled() ?
        new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", plainTableEnabled) : null;

    // create and add StorageService. storeRepository will be populated by StorageService,
    storageService = new StorageService(veniceConfigLoader, s -> storageMetadataService.clearStoreVersionState(s),
        bdbStorageEngineStats, storageEngineStats, rocksDBMemoryStats);

    if (veniceConfigLoader.getVeniceServerConfig().isRocksDBOffsetMetadataEnabled()) {
      logger.info("Server will start with RocksDB offset store enabled");
      storageEngineMetadataService = new StorageEngineMetadataService(storageService.getStorageEngineRepository());
      services.add(storageEngineMetadataService);
      storageMetadataService = storageEngineMetadataService;
    } else {
      storageMetadataService = bdbMetadataService;
    }

    services.add(storageService);

    // Create stats for RocksDB
    storageService.getRocksDBAggregatedStatistics().ifPresent( stat -> new AggRocksDBStats(metricsRepository, stat));

    Optional<SchemaReader> schemaReader = clientConfigForConsumer.map(cc -> ClientFactory.getSchemaReader(
        cc.setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())));

    // create and add KafkaSimpleConsumerService
    this.kafkaStoreIngestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        veniceConfigLoader,
        storageMetadataService,
        metadataRepo,
        schemaRepo,
        metricsRepository,
        null,
        schemaReader,
        clientConfigForConsumer);

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    this.diskHealthCheckService = new DiskHealthCheckService(
        serverConfig.isDiskHealthCheckServiceEnabled(),
        serverConfig.getDiskHealthCheckIntervalInMS(),
        serverConfig.getDiskHealthCheckTimeoutInMs(),
        serverConfig.getDataBasePath(), serverConfig.getSsdHealthCheckShutdownTimeMs());
    services.add(diskHealthCheckService);
    // create stats for disk health check service
    new DiskHealthStats(metricsRepository, diskHealthCheckService, "disk_health_check_service");

    //HelixParticipationService below creates a Helix manager and connects asynchronously below.  The listener service
    //needs a routing data repository that relies on a connected helix manager.  So we pass the listener service a future
    //that will be completed with a routing data repository once the manager connects.
    CompletableFuture<SafeHelixManager> managerFuture = new CompletableFuture<>();
    CompletableFuture<RoutingDataRepository> routingRepositoryFuture = managerFuture.thenApply(manager -> {
      RoutingDataRepository routingData = new HelixExternalViewRepository(manager);
      routingData.refresh();
      return routingData;
    });

    /**
     * Fast schema lookup implementation for read compute path.
     */
    StoreValueSchemasCacheService storeValueSchemasCacheService = new StoreValueSchemasCacheService(metadataRepo, schemaRepo);
    services.add(storeValueSchemasCacheService);

    // create and add ListenerServer for handling GET requests
    ListenerService listenerService = createListenerService(storageService.getStorageEngineRepository(), metadataRepo, storeValueSchemasCacheService,
        routingRepositoryFuture, kafkaStoreIngestionService, serverConfig, metricsRepository, sslFactory, accessController, diskHealthCheckService);
    services.add(listenerService);

    /**
     * Helix participator service should start last since we need to make sure current Storage Node is ready to take
     * read requests if it claims to be available in Helix.
     */
    this.helixParticipationService =
        new HelixParticipationService(kafkaStoreIngestionService, storageService, veniceConfigLoader, metadataRepo,
            metricsRepository, clusterConfig.getZookeeperAddress(), clusterConfig.getClusterName(),
            veniceConfigLoader.getVeniceServerConfig().getListenerPort(), managerFuture);
    services.add(helixParticipationService);

    // Add kafka consumer service last so when shutdown the server, it will be stopped first to avoid the case
    // that helix is disconnected but consumption service try to send message by helix.
    services.add(kafkaStoreIngestionService);

    /**
     * Resource cleanup service
     */
    if (serverConfig.isLeakedResourceCleanupEnabled()) {
      this.leakedResourceCleaner = new LeakedResourceCleaner(storageService.getStorageEngineRepository(), serverConfig.getLeakedResourceCleanUpIntervalInMS(),
          metadataRepo, kafkaStoreIngestionService, storageService, metricsRepository);
      services.add(leakedResourceCleaner);
    }

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

    return Collections.unmodifiableList(services);
  }

  private void createHelixStoreAndSchemaRepository(VeniceClusterConfig clusterConfig, MetricsRepository metricsRepository) {
    zkClient = ZkClientFactory.newZkClient(clusterConfig.getZookeeperAddress());
    zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "server-zk-client"));
    HelixAdapterSerializer adapter = new HelixAdapterSerializer();
    String clusterName = clusterConfig.getClusterName();
    this.metadataRepo = new HelixReadOnlyStoreRepository(zkClient, adapter, clusterName,
        clusterConfig.getRefreshAttemptsForZkReconnect(), clusterConfig.getRefreshIntervalForZkReconnectInMs());
    // Load existing store config and setup watches
    metadataRepo.refresh();

    this.schemaRepo = new HelixReadOnlySchemaRepository(metadataRepo, zkClient, adapter, clusterName,
        clusterConfig.getRefreshAttemptsForZkReconnect(), clusterConfig.getRefreshIntervalForZkReconnectInMs());
    schemaRepo.refresh();
  }

  public StorageService getStorageService() {
    if (isStarted()) {
      return storageService;
    } else {
      throw new VeniceException("Cannot get storage service if server is not started");
    }
  }

  public StorageMetadataService getStorageMetadataService() {
    if (isStarted()) {
      return storageMetadataService;
    } else {
      throw new VeniceException("Cannot get storage metadata service if server is not started");
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
  public void start() throws VeniceException {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new IllegalStateException("Service is already started!");
    }
    // TODO - Efficient way to lock java heap
    logger.info("Starting " + services.size() + " services.");
    long start = System.currentTimeMillis();
    for (AbstractVeniceService service : services) {
      // Skip the participant and ingestions service only after offset bootstrap.
      if (service == kafkaStoreIngestionService || service == helixParticipationService) {
        continue;
      }
      service.start();
    }

    /**
     * We need to add some delay here for router connection warming.
     * The new started server won't be serve online traffic right away because of router connection warming.
     * If server doesn't have extra delay here, Router might encounter availability issue.
     */
    try {
      Thread.sleep(veniceConfigLoader.getVeniceServerConfig().getRouterConnectionWarmingDelayMs());
    } catch (InterruptedException e) {
      throw new VeniceException("Got interrupted exception while delaying start for Router connection warming");
    }
    List<AbstractStorageEngine> allStorageEngines =
        storageService.getStorageEngineRepository().getAllLocalStorageEngines();
    if (veniceConfigLoader.getVeniceServerConfig().isRocksDBOffsetMetadataEnabled()) {
      for (AbstractStorageEngine storageEngine : allStorageEngines) {
        logger.info("Bootstrapping offset records from BDB to RocksDB for store : " + storageEngine.getName());
        storageEngine.bootStrapAndValidateOffsetRecordsFromBDB(bdbMetadataService);
      }
    } else {
      for (AbstractStorageEngine storageEngine : allStorageEngines) {
        /**
         * Following method checks if a store was previously migrated to use RocksDB but still trying to start
         * the server with config isRocksDBOffsetMetadataEnabled() set to false. For rolling back all the data
         * needs to be wiped clean
         */
        if (storageEngine.isMetadataMigrationCompleted()) {
          throw new VeniceException(String.format(
              "Store %s has previously migrated to RocksDB metadata offset store, but now its set "
                  + "(server.enable.rocksdb.offset.metadata = false) to use BDB. "
                  + "Please delete all data including offset for this store", storageEngine.getName()));
        }
      }
    }
    // start the helix participant service and ingestion service only after offset store bootstrap.
    kafkaStoreIngestionService.start();
    helixParticipationService.start();
    long end = System.currentTimeMillis();
    logger.info("Startup completed in " + (end - start) + " ms.");
  }

  /**
   * Method which closes VeniceServer, shuts down its resources, and exits the
   * JVM.
   * @throws Exception
   * */
  public void shutdown() throws VeniceException {
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

      metricsRepository.close();
      zkClient.close();

      // TODO - Efficient way to unlock java heap
    }
  }

  protected static boolean isServerInWhiteList(String zkAddress, String clusterName, int listenPort, boolean enableServerWhitelist) {
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

  protected VeniceConfigLoader getConfigLoader() {
    return veniceConfigLoader;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  protected ListenerService createListenerService(
      StorageEngineRepository storageEngineRepository,
      ReadOnlyStoreRepository storeMetadataRepository,
      ReadOnlySchemaRepository schemaRepository,
      CompletableFuture<RoutingDataRepository> routingRepository,
      MetadataRetriever metadataRetriever,
      VeniceServerConfig serverConfig,
      MetricsRepository metricsRepository,
      Optional<SSLEngineComponentFactory> sslFactory,
      Optional<StaticAccessController> accessController,
      DiskHealthCheckService diskHealthService) {
    return new ListenerService(
        storageEngineRepository, storeMetadataRepository, schemaRepository, routingRepository, metadataRetriever, serverConfig,
        metricsRepository, sslFactory, accessController, diskHealthService);
  }

  public static void main(String args[]) throws Exception {
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
    addShutdownHook(server);
  }

  private static void addShutdownHook(VeniceServer server) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> server.shutdown()));

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      logger.error("Unable to join thread in shutdown hook. ", e);
    }
  }
}
