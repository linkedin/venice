package com.linkedin.venice.server;

import com.linkedin.avro.fastserde.FastDeserializerGeneratorAccessor;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.helix.HelixParticipationService;
import com.linkedin.davinci.ingestion.main.MainIngestionStorageMetadataService;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.RemoteIngestionRepairService;
import com.linkedin.davinci.repository.VeniceMetadataRepositoryBuilder;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.stats.MetadataUpdateStats;
import com.linkedin.davinci.stats.RocksDBMemoryStats;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.MetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineMetadataService;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.cleaner.BackupVersionOptimizationService;
import com.linkedin.venice.cleaner.LeakedResourceCleaner;
import com.linkedin.venice.cleaner.ResourceReadUsageTracker;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.AllowlistAccessor;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkAllowlistAccessor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.listener.ListenerService;
import com.linkedin.venice.listener.ServerStoreAclHandler;
import com.linkedin.venice.listener.StoreValueSchemasCacheService;
import com.linkedin.venice.meta.IngestionMode;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StaticClusterInfoProvider;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.AggRocksDBStats;
import com.linkedin.venice.stats.BackupVersionOptimizationServiceStats;
import com.linkedin.venice.stats.DiskHealthStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceJVMStats;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class that represents the Venice server. It is responsible for maintaining
 * the lifecycle of multiple internal services including e.g. service start/stop
 * operations and making sure that it is done in the right order based on their
 * dependencies.
 */
public class VeniceServer {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServer.class);

  private final List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
  private static final String SERVER_SERVICE_NAME = "venice-server";

  private final VeniceConfigLoader veniceConfigLoader;
  private final Optional<SSLFactory> sslFactory;
  private final Optional<StaticAccessController> routerAccessController;
  private final Optional<DynamicAccessController> storeAccessController;
  private final Optional<ClientConfig> clientConfigForConsumer;
  private final AtomicBoolean isStarted;
  private final Lazy<List<AbstractVeniceService>> services;
  private StorageService storageService;
  private StorageMetadataService storageMetadataService;
  private StorageEngineMetadataService storageEngineMetadataService;
  private KafkaStoreIngestionService kafkaStoreIngestionService;
  private HelixParticipationService helixParticipationService;
  private LeakedResourceCleaner leakedResourceCleaner;
  private DiskHealthCheckService diskHealthCheckService;
  private MetricsRepository metricsRepository;
  private ReadOnlyStoreRepository metadataRepo;
  private ReadOnlySchemaRepository schemaRepo;
  private ReadOnlyLiveClusterConfigRepository liveClusterConfigRepo;
  private Optional<HelixReadOnlyZKSharedSchemaRepository> readOnlyZKSharedSchemaRepository;
  private ZkClient zkClient;
  /** Used by the metrics framework, even though static analysis cannot tell... */
  @SuppressWarnings("unused")
  private VeniceJVMStats jvmStats;
  private ICProvider icProvider;
  StorageEngineBackedCompressorFactory compressorFactory;

  /**
   * @see #VeniceServer(VeniceConfigLoader)
   */
  public VeniceServer(VeniceConfigLoader veniceConfigLoader) throws VeniceException {
    this(veniceConfigLoader, TehutiUtils.getMetricsRepository(SERVER_SERVICE_NAME));
  }

  /**
   * @see #VeniceServer(VeniceConfigLoader, MetricsRepository, Optional, Optional, Optional)
   */
  public VeniceServer(VeniceConfigLoader veniceConfigLoader, MetricsRepository metricsRepository) {
    this(veniceConfigLoader, metricsRepository, Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * @see #VeniceServer(VeniceConfigLoader, MetricsRepository, Optional, Optional, Optional, Optional, ICProvider)
   */
  public VeniceServer(
      VeniceConfigLoader veniceConfigLoader,
      MetricsRepository metricsRepository,
      Optional<SSLFactory> sslFactory,
      Optional<StaticAccessController> routerAccessController,
      Optional<ClientConfig> clientConfigForConsumer) {
    this(
        veniceConfigLoader,
        metricsRepository,
        sslFactory,
        routerAccessController,
        Optional.empty(),
        clientConfigForConsumer,
        null);
  }

  /**
   * @see #VeniceServer(VeniceConfigLoader, MetricsRepository, Optional, Optional, Optional, Optional, ICProvider, List<ServiceDiscoveryAnnouncer>)
   */
  public VeniceServer(
      VeniceConfigLoader veniceConfigLoader,
      MetricsRepository metricsRepository,
      Optional<SSLFactory> sslFactory,
      Optional<StaticAccessController> routerAccessController,
      Optional<DynamicAccessController> storeAccessController,
      Optional<ClientConfig> clientConfigForConsumer,
      ICProvider icProvider) {
    this(
        veniceConfigLoader,
        metricsRepository,
        sslFactory,
        routerAccessController,
        storeAccessController,
        clientConfigForConsumer,
        icProvider,
        Collections.emptyList());
  }

  /**
   * Allocates a new {@code VeniceServer} object.
   * @param veniceConfigLoader a config loader to load configs related to cluster and server.
   * @param metricsRepository a registry for reporting metrics.
   * @param sslFactory ssl certificate for enabling HTTP2.
   * @param routerAccessController  validates the accesses/requests from routers.
   * @param storeAccessController validates the accesses/requests from clients.
   * @param clientConfigForConsumer kafka client configurations.
   * @param icProvider Provide IC(invocation-context) to remote calls inside services.
   * @param serviceDiscoveryAnnouncers list of server discovery announcers
   * @see VeniceConfigLoader
   * @see ServerStoreAclHandler
   */

  public VeniceServer(
      VeniceConfigLoader veniceConfigLoader,
      MetricsRepository metricsRepository,
      Optional<SSLFactory> sslFactory,
      Optional<StaticAccessController> routerAccessController,
      Optional<DynamicAccessController> storeAccessController,
      Optional<ClientConfig> clientConfigForConsumer,
      ICProvider icProvider,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers) {

    // force out any potential config errors using a wildcard store name
    veniceConfigLoader.getStoreConfig("");

    if (!isServerInAllowList(
        veniceConfigLoader.getVeniceClusterConfig().getZookeeperAddress(),
        veniceConfigLoader.getVeniceClusterConfig().getClusterName(),
        veniceConfigLoader.getVeniceServerConfig().getListenerHostname(),
        veniceConfigLoader.getVeniceServerConfig().getListenerPort(),
        veniceConfigLoader.getVeniceServerConfig().isServerAllowlistEnabled())) {
      throw new VeniceException(
          "Can not create a venice server because this server has not been added into allowlist.");
    }

    this.isStarted = new AtomicBoolean(false);
    this.services = Lazy.of(() -> createServices());
    this.veniceConfigLoader = veniceConfigLoader;
    this.metricsRepository = metricsRepository;
    this.sslFactory = sslFactory;
    this.routerAccessController = routerAccessController;
    this.storeAccessController = storeAccessController;
    this.clientConfigForConsumer = clientConfigForConsumer;
    this.icProvider = icProvider;
    this.serviceDiscoveryAnnouncers = serviceDiscoveryAnnouncers;
  }

  /**
   * Instantiate all known services. Most of the services in this method intake:
   * 1. StorageEngineRepository - that maps store to appropriate storage engine instance
   * 2. VeniceConfig - which contains configs related to this cluster.
   * 3. StoreNameToConfigsMap - which contains store specific configs
   * 4. PartitionAssignmentRepository - which contains how partitions for each store are mapped to nodes in the
   *    cluster
   *
   * @return a list of services initiated by Venice server.
   */
  private List<AbstractVeniceService> createServices() {
    /* Services are created in the order they must be started */
    List<AbstractVeniceService> services = new ArrayList<>();

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();

    // Doing this at the very beginning, before any services get started, in case they leverage Fast-Avro
    FastDeserializerGeneratorAccessor.setFieldsPerPopulationMethod(serverConfig.getFastAvroFieldLimitPerMethod());

    // Create jvm metrics object
    jvmStats = new VeniceJVMStats(metricsRepository, "VeniceJVMStats");

    Optional<SchemaReader> partitionStateSchemaReader = clientConfigForConsumer.map(
        cc -> ClientFactory
            .getSchemaReader(cc.setStoreName(AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName()), icProvider));
    Optional<SchemaReader> storeVersionStateSchemaReader = clientConfigForConsumer.map(
        cc -> ClientFactory.getSchemaReader(
            cc.setStoreName(AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName()),
            icProvider));
    final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    partitionStateSchemaReader.ifPresent(partitionStateSerializer::setSchemaReader);
    final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    storeVersionStateSchemaReader.ifPresent(storeVersionStateSerializer::setSchemaReader);

    // Verify the current version of PARTITION_STATE and STORE_VERSION_STATE schema is registered in ZK before moving
    // ahead.
    if (serverConfig.isSchemaPresenceCheckEnabled()) {
      partitionStateSchemaReader.ifPresent(
          schemaReader -> new SchemaPresenceChecker(schemaReader, AvroProtocolDefinition.PARTITION_STATE)
              .verifySchemaVersionPresentOrExit());

      storeVersionStateSchemaReader.ifPresent(
          schemaReader -> new SchemaPresenceChecker(schemaReader, AvroProtocolDefinition.STORE_VERSION_STATE)
              .verifySchemaVersionPresentOrExit());
    }

    // Create and add Offset Service.
    VeniceClusterConfig clusterConfig = veniceConfigLoader.getVeniceClusterConfig();

    // Create ReadOnlyStore/SchemaRepository
    VeniceMetadataRepositoryBuilder veniceMetadataRepositoryBuilder = new VeniceMetadataRepositoryBuilder(
        veniceConfigLoader,
        clientConfigForConsumer.orElse(null),
        metricsRepository,
        icProvider,
        false);
    zkClient = veniceMetadataRepositoryBuilder.getZkClient();
    metadataRepo = veniceMetadataRepositoryBuilder.getStoreRepo();
    schemaRepo = veniceMetadataRepositoryBuilder.getSchemaRepo();
    liveClusterConfigRepo = veniceMetadataRepositoryBuilder.getLiveClusterConfigRepo();
    readOnlyZKSharedSchemaRepository = veniceMetadataRepositoryBuilder.getReadOnlyZKSharedSchemaRepository();

    // TODO: It would be cleaner to come up with a storage engine metric abstraction so we're not passing around so
    // many objects in constructors
    AggVersionedStorageEngineStats storageEngineStats = new AggVersionedStorageEngineStats(
        metricsRepository,
        metadataRepo,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
    boolean plainTableEnabled =
        veniceConfigLoader.getVeniceServerConfig().getRocksDBServerConfig().isRocksDBPlainTableFormatEnabled();
    RocksDBMemoryStats rocksDBMemoryStats = veniceConfigLoader.getVeniceServerConfig().isDatabaseMemoryStatsEnabled()
        ? new RocksDBMemoryStats(metricsRepository, "RocksDBMemoryStats", plainTableEnabled)
        : null;

    // Add extra safeguards here to ensure we have released RocksDB database locks before we initialize storage
    // services.
    IsolatedIngestionUtils.destroyLingeringIsolatedIngestionProcess(veniceConfigLoader);
    // Create and add StorageService. storeRepository will be populated by StorageService
    if (veniceConfigLoader.getVeniceServerConfig().getIngestionMode().equals(IngestionMode.ISOLATED)) {
      // Venice Server does not require bootstrap step, so there is no need to open and close all local storage engines.
      storageService = new StorageService(
          veniceConfigLoader,
          storageEngineStats,
          rocksDBMemoryStats,
          storeVersionStateSerializer,
          partitionStateSerializer,
          metadataRepo,
          false,
          false);
      LOGGER.info("Create {} for ingestion isolation.", MainIngestionStorageMetadataService.class.getName());
      MainIngestionStorageMetadataService ingestionStorageMetadataService = new MainIngestionStorageMetadataService(
          veniceConfigLoader.getVeniceServerConfig().getIngestionServicePort(),
          partitionStateSerializer,
          new MetadataUpdateStats(metricsRepository),
          veniceConfigLoader,
          storageService.getStoreVersionStateSyncer());
      services.add(ingestionStorageMetadataService);
      storageMetadataService = ingestionStorageMetadataService;
    } else {
      storageService = new StorageService(
          veniceConfigLoader,
          storageEngineStats,
          rocksDBMemoryStats,
          storeVersionStateSerializer,
          partitionStateSerializer,
          metadataRepo);
      storageEngineMetadataService =
          new StorageEngineMetadataService(storageService.getStorageEngineRepository(), partitionStateSerializer);
      services.add(storageEngineMetadataService);
      storageMetadataService = storageEngineMetadataService;
    }
    services.add(storageService);

    // Create stats for RocksDB
    storageService.getRocksDBAggregatedStatistics().ifPresent(stat -> new AggRocksDBStats(metricsRepository, stat));

    Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader = clientConfigForConsumer.map(
        cc -> ClientFactory.getSchemaReader(
            cc.setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()),
            icProvider));
    Optional<SchemaReader> metaSystemStoreSchemaReader = clientConfigForConsumer.map(
        cc -> ClientFactory.getSchemaReader(
            cc.setStoreName(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName()),
            icProvider));

    // verify the current version of the system schemas are registered in ZK before moving ahead
    if (serverConfig.isSchemaPresenceCheckEnabled()) {
      kafkaMessageEnvelopeSchemaReader.ifPresent(
          schemaReader -> new SchemaPresenceChecker(schemaReader, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE)
              .verifySchemaVersionPresentOrExit());

      metaSystemStoreSchemaReader.ifPresent(
          schemaReader -> new SchemaPresenceChecker(schemaReader, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE)
              .verifySchemaVersionPresentOrExit());
    }

    compressorFactory = new StorageEngineBackedCompressorFactory(storageMetadataService);

    /**
     * Build Ingestion Repair service.
     */
    RemoteIngestionRepairService remoteIngestionRepairService =
        new RemoteIngestionRepairService(serverConfig.getRemoteIngestionRepairSleepInterval());
    services.add(remoteIngestionRepairService);

    // HelixParticipationService below creates a Helix manager and connects asynchronously below. The listener service
    // needs a routing data repository that relies on a connected helix manager. So we pass the listener service a
    // future that will be completed with a routing data repository once the manager connects.
    CompletableFuture<SafeHelixManager> managerFuture = new CompletableFuture<>();
    CompletableFuture<RoutingDataRepository> routingRepositoryFuture = managerFuture.thenApply(manager -> {
      RoutingDataRepository routingData = new HelixExternalViewRepository(manager);
      routingData.refresh();
      return routingData;
    });

    CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture =
        managerFuture.thenApply(manager -> {
          HelixCustomizedViewOfflinePushRepository customizedView =
              new HelixCustomizedViewOfflinePushRepository(manager);
          customizedView.refresh();
          return customizedView;
        });

    CompletableFuture<HelixInstanceConfigRepository> helixInstanceFuture = managerFuture.thenApply(manager -> {
      HelixInstanceConfigRepository helixData = new HelixInstanceConfigRepository(manager, false);
      helixData.refresh();
      return helixData;
    });

    // create and add KafkaSimpleConsumerService
    this.kafkaStoreIngestionService = new KafkaStoreIngestionService(
        storageService.getStorageEngineRepository(),
        veniceConfigLoader,
        storageMetadataService,
        new StaticClusterInfoProvider(Collections.singleton(clusterConfig.getClusterName())),
        metadataRepo,
        schemaRepo,
        Optional.of(customizedViewFuture),
        Optional.of(helixInstanceFuture),
        liveClusterConfigRepo,
        metricsRepository,
        kafkaMessageEnvelopeSchemaReader,
        clientConfigForConsumer,
        partitionStateSerializer,
        readOnlyZKSharedSchemaRepository,
        icProvider,
        false,
        compressorFactory,
        Optional.empty(),
        false,
        remoteIngestionRepairService);
    this.kafkaStoreIngestionService.addMetaSystemStoreReplicaStatusNotifier();

    this.diskHealthCheckService = new DiskHealthCheckService(
        serverConfig.isDiskHealthCheckServiceEnabled(),
        serverConfig.getDiskHealthCheckIntervalInMS(),
        serverConfig.getDiskHealthCheckTimeoutInMs(),
        serverConfig.getDataBasePath(),
        serverConfig.getSsdHealthCheckShutdownTimeMs());
    services.add(diskHealthCheckService);
    // create stats for disk health check service
    new DiskHealthStats(metricsRepository, diskHealthCheckService, "disk_health_check_service");

    final Optional<ResourceReadUsageTracker> resourceReadUsageTracker;
    if (serverConfig.isOptimizeDatabaseForBackupVersionEnabled()) {
      BackupVersionOptimizationService backupVersionOptimizationService = new BackupVersionOptimizationService(
          metadataRepo,
          storageService.getStorageEngineRepository(),
          serverConfig.getOptimizeDatabaseForBackupVersionNoReadThresholdMS(),
          serverConfig.getOptimizeDatabaseServiceScheduleIntervalSeconds(),
          new BackupVersionOptimizationServiceStats(metricsRepository, "BackupVersionOptimizationService"));
      services.add(backupVersionOptimizationService);
      resourceReadUsageTracker = Optional.of(backupVersionOptimizationService);
    } else {
      resourceReadUsageTracker = Optional.empty();
    }
    /**
     * Fast schema lookup implementation for read compute path.
     */
    StoreValueSchemasCacheService storeValueSchemasCacheService =
        new StoreValueSchemasCacheService(metadataRepo, schemaRepo);
    services.add(storeValueSchemasCacheService);

    // create and add ListenerServer for handling GET requests
    ListenerService listenerService = createListenerService(
        storageService.getStorageEngineRepository(),
        metadataRepo,
        storeValueSchemasCacheService,
        routingRepositoryFuture,
        kafkaStoreIngestionService,
        serverConfig,
        metricsRepository,
        sslFactory,
        routerAccessController,
        storeAccessController,
        diskHealthCheckService,
        compressorFactory,
        resourceReadUsageTracker);
    services.add(listenerService);

    /**
     * Helix participator service should start last since we need to make sure current Storage Node is ready to take
     * read requests if it claims to be available in Helix.
     */
    this.helixParticipationService = new HelixParticipationService(
        kafkaStoreIngestionService,
        storageService,
        storageMetadataService,
        veniceConfigLoader,
        metadataRepo,
        metricsRepository,
        clusterConfig.getZookeeperAddress(),
        clusterConfig.getClusterName(),
        veniceConfigLoader.getVeniceServerConfig().getListenerPort(),
        veniceConfigLoader.getVeniceServerConfig().getListenerHostname(),
        managerFuture);
    services.add(helixParticipationService);

    // Add kafka consumer service last so when shutdown the server, it will be stopped first to avoid the case
    // that helix is disconnected but consumption service try to send message by helix.
    services.add(kafkaStoreIngestionService);

    /**
     * Resource cleanup service
     */
    if (serverConfig.isLeakedResourceCleanupEnabled()) {
      this.leakedResourceCleaner = new LeakedResourceCleaner(
          storageService.getStorageEngineRepository(),
          serverConfig.getLeakedResourceCleanUpIntervalInMS(),
          metadataRepo,
          kafkaStoreIngestionService,
          storageService,
          metricsRepository);
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

  public KafkaStoreIngestionService getKafkaStoreIngestionService() {
    if (isStarted()) {
      return kafkaStoreIngestionService;
    } else {
      throw new VeniceException("Cannot get kafka store ingestion service if server is not started");
    }
  }

  // This is for testing purpose.
  public HelixParticipationService getHelixParticipationService() {
    if (isStarted()) {
      return this.helixParticipationService;
    }
    throw new VeniceException("Cannot get helix participation service if server is not started");
  }

  /**
   * @return true if the {@link VeniceServer} and all of its inner services are fully started
   *         false if the {@link VeniceServer} was not started or if any of its inner services
   *         are not finished starting.
   */
  public boolean isStarted() {
    return isStarted.get() && services.isPresent()
        && services.get().stream().allMatch(AbstractVeniceService::isRunning);
  }

  /**
   * Method which starts the services instantiate earlier.
   * @throws Exception
   */
  public void start() throws VeniceException {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new IllegalStateException("Service is already started!");
    }
    /**
     * Move all the service creation here since {@link #storeAccessController} can only be initialized
     * in this function since internally it depends on d2 client to finish the initialization, and d2 client
     * is not started in the constructor.
     */
    List<AbstractVeniceService> veniceServiceList = this.services.get();
    LOGGER.info("Starting {} services.", veniceServiceList.size());
    long start = System.currentTimeMillis();

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

    for (AbstractVeniceService service: veniceServiceList) {
      service.start();
    }

    for (ServiceDiscoveryAnnouncer serviceDiscoveryAnnouncer: serviceDiscoveryAnnouncers) {
      LOGGER.info("Registering to service discovery: {}", serviceDiscoveryAnnouncer);
      serviceDiscoveryAnnouncer.register();
    }

    LOGGER.info("Startup completed in {} ms.", (System.currentTimeMillis() - start));
  }

  /**
   * Method which closes VeniceServer, shuts down its resources, and exits the
   * JVM.
   * @throws VeniceException
   * */
  public void shutdown() throws VeniceException {
    List<Exception> exceptions = new ArrayList<>();
    LOGGER.info("Stopping all services");

    /* Stop in reverse order */

    // TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    synchronized (this) {
      if (!isStarted()) {
        LOGGER.info("The server has been already stopped, ignoring reattempt.");
        return;
      }
      for (ServiceDiscoveryAnnouncer serviceDiscoveryAnnouncer: serviceDiscoveryAnnouncers) {
        LOGGER.info("Unregistering from service discovery: {}", serviceDiscoveryAnnouncer);
        try {
          serviceDiscoveryAnnouncer.unregister();
        } catch (RuntimeException e) {
          LOGGER.error("Service discovery announcer {} failed to unregister properly", serviceDiscoveryAnnouncer, e);
        }
      }

      for (AbstractVeniceService service: CollectionUtils.reversed(services.get())) {
        try {
          LOGGER.info("Stopping service: {}", service.getName());
          service.stop();
          LOGGER.info("Service: {} stopped.", service.getName());
        } catch (Exception e) {
          exceptions.add(e);
          LOGGER.error("Exception while stopping service: {}", service.getName(), e);
        }
      }
      LOGGER.info("All services have been stopped");

      compressorFactory.close();

      try {
        metricsRepository.close();
      } catch (Exception e) {
        exceptions.add(e);
        LOGGER.error("Exception while closing: {}", metricsRepository.getClass().getSimpleName(), e);
      }

      try {
        zkClient.close();
      } catch (Exception e) {
        exceptions.add(e);
        LOGGER.error("Exception while closing: {}", zkClient.getClass().getSimpleName(), e);
      }

      if (exceptions.size() > 0) {
        throw new VeniceException(exceptions.get(0));
      }
      isStarted.set(false);
    }
  }

  protected static boolean isServerInAllowList(
      String zkAddress,
      String clusterName,
      String hostname,
      int listenPort,
      boolean enableServerAllowlist) {
    if (!enableServerAllowlist) {
      LOGGER.info("Server allow list feature is disabled, hence skipping serverAllowlist checks");
      return true;
    }
    try (AllowlistAccessor accessor = new ZkAllowlistAccessor(zkAddress)) {
      /**
       * Note: If a server has been added in to the allowlist, then node is failed or shutdown by SRE. once it
       * starts up again, it will automatically join the cluster because it already exists in the allowlist.
       */
      String participantName = Utils.getHelixNodeIdentifier(hostname, listenPort);
      if (!accessor.isInstanceInAllowlist(clusterName, participantName)) {
        LOGGER.info("{} is not in the allowlist of {}, stop starting venice server", participantName, clusterName);
        return false;
      } else {
        LOGGER.info("{} has been added into the allowlist, continue to start participant.", participantName);
        return true;
      }
    } catch (Exception e) {
      String errorMsg = "Met error during checking allowlist.";
      LOGGER.error(errorMsg, e);
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
      Optional<SSLFactory> sslFactory,
      Optional<StaticAccessController> routerAccessController,
      Optional<DynamicAccessController> storeAccessController,
      DiskHealthCheckService diskHealthService,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ResourceReadUsageTracker> resourceReadUsageTracker) {
    return new ListenerService(
        storageEngineRepository,
        storeMetadataRepository,
        schemaRepository,
        routingRepository,
        metadataRetriever,
        serverConfig,
        metricsRepository,
        sslFactory,
        routerAccessController,
        storeAccessController,
        diskHealthService,
        compressorFactory,
        resourceReadUsageTracker);
  }

  public static void main(String args[]) throws Exception {
    VeniceConfigLoader veniceConfigService = null;
    try {
      if (args.length == 0) {
        veniceConfigService = VeniceConfigLoader.loadFromEnvironmentVariable();
      } else if (args.length == 1) {
        veniceConfigService = VeniceConfigLoader.loadFromConfigDirectory(args[0]);
      } else {
        Utils.exit("USAGE: java -jar venice-server-all.jar <server_config_directory_path>");
      }
    } catch (Exception e) {
      LOGGER.error("Error starting Venice Server ", e);
      Utils.exit("Error while loading configuration: " + e.getMessage());
    }

    final VeniceServer server = new VeniceServer(veniceConfigService);
    if (!server.isStarted()) {
      server.start();
    }
    addShutdownHook(server);
  }

  private static void addShutdownHook(VeniceServer server) {
    Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      LOGGER.error("Unable to join thread in shutdown hook. ", e);
    }
  }
}
