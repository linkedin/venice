package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.grpc.server.ClusterAdminOpsGrpcServiceImpl;
import com.linkedin.venice.controller.grpc.server.SchemaGrpcServiceImpl;
import com.linkedin.venice.controller.grpc.server.StoreGrpcServiceImpl;
import com.linkedin.venice.controller.grpc.server.interceptor.ControllerGrpcAuditLoggingInterceptor;
import com.linkedin.venice.controller.grpc.server.interceptor.ControllerGrpcSslSessionInterceptor;
import com.linkedin.venice.controller.grpc.server.interceptor.ParentControllerRegionValidationInterceptor;
import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controller.kafka.TopicCleanupServiceForParentController;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.controller.server.VeniceControllerGrpcServiceImpl;
import com.linkedin.venice.controller.server.VeniceControllerRequestHandler;
import com.linkedin.venice.controller.stats.ControllerMetricEntity;
import com.linkedin.venice.controller.stats.DeferredVersionSwapStats;
import com.linkedin.venice.controller.stats.TopicCleanupServiceStats;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.controller.systemstore.SystemStoreRepairService;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.grpc.VeniceGrpcServer;
import com.linkedin.venice.grpc.VeniceGrpcServerConfig;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.servicediscovery.AsyncRetryingServiceDiscoveryAnnouncer;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.system.store.ControllerClientBackedSystemSchemaInitializer;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.BlockingQueueType;
import com.linkedin.venice.utils.concurrent.ThreadPoolFactory;
import io.grpc.ServerInterceptor;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {
  private static final Logger LOGGER = LogManager.getLogger(VeniceController.class);
  private static final String CONTROLLER_GRPC_SERVER_THREAD_NAME = "ControllerGrpcServer";
  public static final String CONTROLLER_SERVICE_NAME = "venice-controller";
  public static final String CONTROLLER_SERVICE_METRIC_PREFIX = "controller";
  public static final Collection<MetricEntity> CONTROLLER_SERVICE_METRIC_ENTITIES =
      ModuleMetricEntityInterface.getUniqueMetricEntities(ControllerMetricEntity.class);

  // services
  private final VeniceControllerService controllerService;
  private final AdminSparkServer adminServer;
  private final AdminSparkServer secureAdminServer;
  private VeniceGrpcServer adminGrpcServer;
  private VeniceGrpcServer adminSecureGrpcServer;
  private final TopicCleanupService topicCleanupService;
  private final Optional<StoreBackupVersionCleanupService> storeBackupVersionCleanupService;

  private final Optional<DisabledPartitionEnablerService> disabledPartitionEnablerService;
  private final Optional<UnusedValueSchemaCleanupService> unusedValueSchemaCleanupService;

  private final Optional<StoreGraveyardCleanupService> storeGraveyardCleanupService;
  private final Optional<SystemStoreRepairService> systemStoreRepairService;

  private Optional<DeferredVersionSwapService> deferredVersionSwapService;

  private VeniceControllerRequestHandler secureRequestHandler;
  private VeniceControllerRequestHandler unsecureRequestHandler;
  private ThreadPoolExecutor grpcExecutor = null;

  private final boolean sslEnabled;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final MetricsRepository metricsRepository;
  private final List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
  private final AsyncRetryingServiceDiscoveryAnnouncer asyncRetryingServiceDiscoveryAnnouncer;
  private final Optional<DynamicAccessController> accessController;
  private final Optional<AuthorizerService> authorizerService;
  private final D2Client d2Client;
  private Map<String, D2Client> d2Clients;
  private final Optional<ClientConfig> routerClientConfig;
  private final Optional<ICProvider> icProvider;
  private final Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final PubSubClientsFactory pubSubClientsFactory;
  private final LogContext logContext;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
  private final Optional<List<VeniceVersionLifecycleEventListener>> versionLifecycleEventListeners;
  private final Optional<ExternalETLService> externalETLService;

  /**
   * Allocates a new {@code VeniceController} object.
   *
   * @param propertiesList
   *        config properties coming from {@link com.linkedin.venice.ConfigKeys}.
   * @param metricsRepository
   *        a metric repository to emit metrics.
   * @param serviceDiscoveryAnnouncers
   *        a list of {@code ServiceDiscoveryAnnouncer} for service discovery announcement. Can be empty.
   * @param accessController
   *        an optional {@link DynamicAccessController} for auth/auth. Deprecated, use authorizerService instead.
   * @param authorizerService
   *        an optional {@link AuthorizerService} for auth/auth.
   * @param d2Client
   *        a {@link D2Client} used for interacting with child controllers.
   * @param routerClientConfig
   *        an optional {@link ClientConfig} used for reading schema from routers.
   * @param icProvider
   *        an optional invocation-context provider class for calls between various deployable services.
   * @param externalSupersetSchemaGenerator
   *        an optional {@link SupersetSchemaGenerator} used for generating superset schema.
   */
  @Deprecated
  public VeniceController(
      List<VeniceProperties> propertiesList,
      MetricsRepository metricsRepository,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<DynamicAccessController> accessController,
      Optional<AuthorizerService> authorizerService,
      D2Client d2Client,
      Optional<ClientConfig> routerClientConfig,
      Optional<ICProvider> icProvider,
      Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator) {
    this(
        new VeniceControllerContext.Builder().setPropertiesList(propertiesList)
            .setMetricsRepository(metricsRepository)
            .setServiceDiscoveryAnnouncers(serviceDiscoveryAnnouncers)
            .setAccessController(accessController.orElse(null))
            .setAuthorizerService(authorizerService.orElse(null))
            .setD2Client(d2Client)
            .setRouterClientConfig(routerClientConfig.orElse(null))
            .setIcProvider(icProvider.orElse(null))
            .setExternalSupersetSchemaGenerator(externalSupersetSchemaGenerator.orElse(null))
            .build());
  }

  public VeniceController(VeniceControllerContext ctx) {
    this.multiClusterConfigs = new VeniceControllerMultiClusterConfig(ctx.getPropertiesList());
    this.logContext = multiClusterConfigs.getLogContext();
    this.metricsRepository = ctx.getMetricsRepository();
    this.serviceDiscoveryAnnouncers = ctx.getServiceDiscoveryAnnouncers();
    Optional<SSLConfig> sslConfig = multiClusterConfigs.getSslConfig();
    this.sslEnabled = sslConfig.isPresent() && sslConfig.get().isControllerSSLEnabled();
    this.accessController = Optional.ofNullable(ctx.getAccessController());
    this.authorizerService = Optional.ofNullable(ctx.getAuthorizerService());
    this.d2Client = ctx.getD2Client();
    this.d2Clients = ctx.getD2Clients();
    this.routerClientConfig = Optional.ofNullable(ctx.getRouterClientConfig());
    this.icProvider = Optional.ofNullable(ctx.getIcProvider());
    this.externalSupersetSchemaGenerator = Optional.ofNullable(ctx.getExternalSupersetSchemaGenerator());
    this.pubSubClientsFactory = multiClusterConfigs.getPubSubClientsFactory();
    this.pubSubPositionTypeRegistry = multiClusterConfigs.getPubSubPositionTypeRegistry();
    long serviceDiscoveryRegistrationRetryMS = multiClusterConfigs.getServiceDiscoveryRegistrationRetryMS();
    this.asyncRetryingServiceDiscoveryAnnouncer =
        new AsyncRetryingServiceDiscoveryAnnouncer(serviceDiscoveryAnnouncers, serviceDiscoveryRegistrationRetryMS);
    this.pubSubTopicRepository = multiClusterConfigs.getPubSubTopicRepository();
    this.versionLifecycleEventListeners = Optional.ofNullable(ctx.getVersionLifecycleEventListeners());
    this.externalETLService = Optional.ofNullable(ctx.getExternalETLService());
    this.controllerService = createControllerService();
    this.adminServer = createAdminServer(false);
    this.secureAdminServer = sslEnabled ? createAdminServer(true) : null;
    this.topicCleanupService = createTopicCleanupService();
    this.storeBackupVersionCleanupService = createStoreBackupVersionCleanupService();
    this.disabledPartitionEnablerService = createDisabledPartitionEnablerService();
    this.unusedValueSchemaCleanupService = createUnusedValueSchemaCleanupService();
    this.storeGraveyardCleanupService = createStoreGraveyardCleanupService();
    this.systemStoreRepairService = createSystemStoreRepairService();
    this.deferredVersionSwapService = createDeferredVersionSwapService();
    if (multiClusterConfigs.isGrpcServerEnabled()) {
      initializeGrpcServer();
    }

    // Run before enabling controller in helix so leadership won't hand back to this controller during schema requests.
    initializeSystemSchema(controllerService.getVeniceHelixAdmin());
  }

  private VeniceControllerService createControllerService() {
    VeniceControllerService veniceControllerService = new VeniceControllerService(
        multiClusterConfigs,
        metricsRepository,
        sslEnabled,
        multiClusterConfigs.getSslConfig(),
        accessController,
        authorizerService,
        d2Client,
        d2Clients,
        routerClientConfig,
        icProvider,
        externalSupersetSchemaGenerator,
        pubSubTopicRepository,
        pubSubClientsFactory,
        pubSubPositionTypeRegistry,
        versionLifecycleEventListeners,
        externalETLService);
    Admin admin = veniceControllerService.getVeniceHelixAdmin();
    if (multiClusterConfigs.isParent() && !(admin instanceof VeniceParentHelixAdmin)) {
      throw new VeniceException(
          "'VeniceParentHelixAdmin' is expected of the returned 'Admin' from 'VeniceControllerService#getVeniceHelixAdmin' in parent mode");
    }
    unsecureRequestHandler = new VeniceControllerRequestHandler(buildRequestHandlerDependencies(admin, false));
    if (sslEnabled) {
      secureRequestHandler = new VeniceControllerRequestHandler(buildRequestHandlerDependencies(admin, true));
    }
    return veniceControllerService;
  }

  AdminSparkServer createAdminServer(boolean secure) {
    return new AdminSparkServer(
        secure ? multiClusterConfigs.getAdminSecurePort() : multiClusterConfigs.getAdminPort(),
        controllerService.getVeniceHelixAdmin(),
        metricsRepository,
        multiClusterConfigs.getClusters(),
        secure || multiClusterConfigs.isControllerEnforceSSLOnly(),
        secure ? multiClusterConfigs.getSslConfig() : Optional.empty(),
        secure && multiClusterConfigs.adminCheckReadMethodForKafka(),
        secure ? accessController : Optional.empty(),
        multiClusterConfigs.getDisabledRoutes(),
        multiClusterConfigs.getCommonConfig().getJettyConfigOverrides(),
        multiClusterConfigs.getCommonConfig().isDisableParentRequestTopicForStreamPushes(),
        pubSubTopicRepository,
        secure ? secureRequestHandler : unsecureRequestHandler);
  }

  private TopicCleanupService createTopicCleanupService() {
    Admin admin = controllerService.getVeniceHelixAdmin();

    if (multiClusterConfigs.isParent()) {
      // TODO: Remove the following once ConcurrentPushDetectionStrategy.PARENT_VERSION_STATUS_ONLY is fully rolled out
      return new TopicCleanupServiceForParentController(
          admin,
          multiClusterConfigs,
          pubSubTopicRepository,
          new TopicCleanupServiceStats(metricsRepository),
          pubSubClientsFactory);
    } else {
      return new TopicCleanupService(
          admin,
          multiClusterConfigs,
          pubSubTopicRepository,
          new TopicCleanupServiceStats(metricsRepository),
          pubSubClientsFactory);
    }
  }

  private Optional<StoreBackupVersionCleanupService> createStoreBackupVersionCleanupService() {
    if (!multiClusterConfigs.isParent()) {
      Admin admin = controllerService.getVeniceHelixAdmin();
      return Optional
          .of(new StoreBackupVersionCleanupService((VeniceHelixAdmin) admin, multiClusterConfigs, metricsRepository));
    }
    return Optional.empty();
  }

  private Optional<DisabledPartitionEnablerService> createDisabledPartitionEnablerService() {
    if (!multiClusterConfigs.isParent()) {
      Admin admin = controllerService.getVeniceHelixAdmin();
      return Optional.of(new DisabledPartitionEnablerService((VeniceHelixAdmin) admin, multiClusterConfigs));
    }
    return Optional.empty();
  }

  private Optional<StoreGraveyardCleanupService> createStoreGraveyardCleanupService() {
    if (multiClusterConfigs.isParent()) {
      Admin admin = controllerService.getVeniceHelixAdmin();
      return Optional.of(new StoreGraveyardCleanupService((VeniceParentHelixAdmin) admin, multiClusterConfigs));
    }
    return Optional.empty();
  }

  private Optional<SystemStoreRepairService> createSystemStoreRepairService() {
    if (multiClusterConfigs.isParent()) {
      Admin admin = controllerService.getVeniceHelixAdmin();
      return Optional
          .of(new SystemStoreRepairService((VeniceParentHelixAdmin) admin, multiClusterConfigs, metricsRepository));
    }
    return Optional.empty();
  }

  private Optional<UnusedValueSchemaCleanupService> createUnusedValueSchemaCleanupService() {
    if (multiClusterConfigs.isParent()) {
      Admin admin = controllerService.getVeniceHelixAdmin();
      return Optional.of(new UnusedValueSchemaCleanupService(multiClusterConfigs, (VeniceParentHelixAdmin) admin));
    }
    return Optional.empty();
  }

  private Optional<DeferredVersionSwapService> createDeferredVersionSwapService() {
    if (multiClusterConfigs.isParent() && multiClusterConfigs.isDeferredVersionSwapServiceEnabled()) {
      Admin admin = controllerService.getVeniceHelixAdmin();
      return Optional.of(
          new DeferredVersionSwapService(
              (VeniceParentHelixAdmin) admin,
              multiClusterConfigs,
              new DeferredVersionSwapStats(metricsRepository),
              metricsRepository));
    }
    return Optional.empty();
  }

  // package-private for testing
  private void initializeGrpcServer() {
    LOGGER.info("Initializing gRPC server as it is enabled for the controller...");
    ParentControllerRegionValidationInterceptor parentControllerRegionValidationInterceptor =
        new ParentControllerRegionValidationInterceptor(controllerService.getVeniceHelixAdmin());
    List<ServerInterceptor> interceptors = new ArrayList<>(4);
    interceptors.add(new ControllerGrpcAuditLoggingInterceptor());
    interceptors.add(parentControllerRegionValidationInterceptor);

    VeniceControllerGrpcServiceImpl grpcService = new VeniceControllerGrpcServiceImpl(unsecureRequestHandler);
    StoreGrpcServiceImpl storeGrpcServiceGrpc = new StoreGrpcServiceImpl(
        unsecureRequestHandler.getStoreRequestHandler(),
        unsecureRequestHandler.getControllerAccessManager());
    SchemaGrpcServiceImpl schemaGrpcService =
        new SchemaGrpcServiceImpl(unsecureRequestHandler.getSchemaRequestHandler());
    ClusterAdminOpsGrpcServiceImpl clusterAdminOpsGrpcService = new ClusterAdminOpsGrpcServiceImpl(
        unsecureRequestHandler.getClusterAdminOpsRequestHandler(),
        unsecureRequestHandler.getControllerAccessManager());
    grpcExecutor = ThreadPoolFactory.createThreadPool(
        multiClusterConfigs.getGrpcServerThreadCount(),
        CONTROLLER_GRPC_SERVER_THREAD_NAME,
        multiClusterConfigs.getLogContext(),
        Integer.MAX_VALUE,
        BlockingQueueType.LINKED_BLOCKING_QUEUE);

    adminGrpcServer = new VeniceGrpcServer(
        new VeniceGrpcServerConfig.Builder().setPort(multiClusterConfigs.getAdminGrpcPort())
            .addService(grpcService)
            .addService(storeGrpcServiceGrpc)
            .addService(schemaGrpcService)
            .addService(clusterAdminOpsGrpcService)
            .setExecutor(grpcExecutor)
            .setInterceptors(interceptors)
            .build());

    if (sslEnabled) {
      interceptors.add(new ControllerGrpcSslSessionInterceptor());
      SSLFactory sslFactory = SslUtils.getSSLFactory(
          multiClusterConfigs.getSslConfig().get().getSslProperties(),
          multiClusterConfigs.getSslFactoryClassName());
      VeniceControllerGrpcServiceImpl secureGrpcService = new VeniceControllerGrpcServiceImpl(secureRequestHandler);
      StoreGrpcServiceImpl secureStoreGrpcService = new StoreGrpcServiceImpl(
          secureRequestHandler.getStoreRequestHandler(),
          secureRequestHandler.getControllerAccessManager());
      SchemaGrpcServiceImpl secureSchemaGrpcService =
          new SchemaGrpcServiceImpl(secureRequestHandler.getSchemaRequestHandler());
      ClusterAdminOpsGrpcServiceImpl secureClusterAdminOpsGrpcService = new ClusterAdminOpsGrpcServiceImpl(
          secureRequestHandler.getClusterAdminOpsRequestHandler(),
          secureRequestHandler.getControllerAccessManager());
      adminSecureGrpcServer = new VeniceGrpcServer(
          new VeniceGrpcServerConfig.Builder().setPort(multiClusterConfigs.getAdminSecureGrpcPort())
              .addService(secureGrpcService)
              .addService(secureStoreGrpcService)
              .addService(secureSchemaGrpcService)
              .addService(secureClusterAdminOpsGrpcService)
              .setExecutor(grpcExecutor)
              .setSslFactory(sslFactory)
              .setInterceptors(interceptors)
              .build());
    }
  }

  // package-private for testing
  ControllerRequestHandlerDependencies buildRequestHandlerDependencies(Admin admin, boolean secure) {
    ControllerRequestHandlerDependencies.Builder builder =
        new ControllerRequestHandlerDependencies.Builder().setAdmin(admin)
            .setMetricsRepository(metricsRepository)
            .setClusters(multiClusterConfigs.getClusters())
            .setDisabledRoutes(multiClusterConfigs.getDisabledRoutes())
            .setVeniceProperties(multiClusterConfigs.getCommonConfig().getJettyConfigOverrides())
            .setDisableParentRequestTopicForStreamPushes(
                multiClusterConfigs.getCommonConfig().isDisableParentRequestTopicForStreamPushes())
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setSslConfig(secure ? multiClusterConfigs.getSslConfig().orElse(null) : null)
            .setSslEnabled(secure)
            .setCheckReadMethodForKafka(secure && multiClusterConfigs.adminCheckReadMethodForKafka())
            .setAccessController(secure ? accessController.orElse(null) : null)
            .setEnforceSSL(secure || multiClusterConfigs.isControllerEnforceSSLOnly());
    return builder.build();
  }

  /**
   * Causes venice controller and its associated services to begin execution.
   */
  public void start() {
    LOGGER.info(
        "Starting controller: {} for clusters: {} with ZKAddress: {}",
        multiClusterConfigs.getControllerName(),
        multiClusterConfigs.getClusters(),
        multiClusterConfigs.getZkAddress());
    controllerService.start();
    adminServer.start();
    if (sslEnabled) {
      secureAdminServer.start();
    }

    topicCleanupService.start();
    storeBackupVersionCleanupService.ifPresent(AbstractVeniceService::start);
    storeGraveyardCleanupService.ifPresent(AbstractVeniceService::start);
    unusedValueSchemaCleanupService.ifPresent(AbstractVeniceService::start);
    systemStoreRepairService.ifPresent(AbstractVeniceService::start);
    disabledPartitionEnablerService.ifPresent(AbstractVeniceService::start);
    deferredVersionSwapService.ifPresent(AbstractVeniceService::start);
    // register with service discovery at the end
    asyncRetryingServiceDiscoveryAnnouncer.register();
    if (adminGrpcServer != null) {
      adminGrpcServer.start();
    }
    if (adminSecureGrpcServer != null) {
      adminSecureGrpcServer.start();
    }
    LOGGER.info("Controller is started.");
  }

  private void initializeSystemSchema(Admin admin) {
    String systemStoreCluster = multiClusterConfigs.getSystemSchemaClusterName();
    VeniceControllerClusterConfig systemStoreClusterConfig =
        multiClusterConfigs.getControllerConfig(systemStoreCluster);
    if (!multiClusterConfigs.isParent() && systemStoreClusterConfig.isSystemSchemaInitializationAtStartTimeEnabled()) {
      String regionName = systemStoreClusterConfig.getRegionName();
      String childControllerUrl = systemStoreClusterConfig.getChildControllerUrl(regionName);
      String d2ServiceName = systemStoreClusterConfig.getD2ServiceName();
      String d2ZkHost = systemStoreClusterConfig.getChildControllerD2ZkHost(regionName);
      Optional<D2Client> regionD2Client = Optional.ofNullable(d2Clients == null ? null : d2Clients.get(regionName));
      boolean sslOnly = systemStoreClusterConfig.isControllerEnforceSSLOnly();
      if (multiClusterConfigs.isZkSharedMetaSystemSchemaStoreAutoCreationEnabled()) {
        ControllerClientBackedSystemSchemaInitializer metaSystemStoreSchemaInitializer =
            new ControllerClientBackedSystemSchemaInitializer(
                AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
                systemStoreCluster,
                AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema(),
                VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS,
                true,
                ((VeniceHelixAdmin) admin).getSslFactory(),
                childControllerUrl,
                d2ServiceName,
                regionD2Client,
                d2ZkHost,
                sslOnly);
        metaSystemStoreSchemaInitializer.execute();
      }
      ControllerClientBackedSystemSchemaInitializer kmeSchemaInitializer =
          new ControllerClientBackedSystemSchemaInitializer(
              AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE,
              systemStoreCluster,
              null,
              null,
              false,
              ((VeniceHelixAdmin) admin).getSslFactory(),
              childControllerUrl,
              d2ServiceName,
              regionD2Client,
              d2ZkHost,
              sslOnly);
      kmeSchemaInitializer.execute();
    }
  }

  /**
   * Causes venice controller and its associated services to stop executing.
   */
  public void stop() {
    // unregister from service discovery first
    asyncRetryingServiceDiscoveryAnnouncer.unregister();
    // TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    systemStoreRepairService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    storeGraveyardCleanupService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    unusedValueSchemaCleanupService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    storeBackupVersionCleanupService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    disabledPartitionEnablerService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    deferredVersionSwapService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    if (adminGrpcServer != null) {
      adminGrpcServer.stop();
    }
    if (adminSecureGrpcServer != null) {
      adminSecureGrpcServer.stop();
    }
    if (grpcExecutor != null) {
      LOGGER.info("Shutting down gRPC executor");
      grpcExecutor.shutdown();
    }
    Utils.closeQuietlyWithErrorLogged(topicCleanupService);
    Utils.closeQuietlyWithErrorLogged(secureAdminServer);
    Utils.closeQuietlyWithErrorLogged(adminServer);
    Utils.closeQuietlyWithErrorLogged(controllerService);
  }

  /**
   * @return the Venice controller service.
   */
  public VeniceControllerService getVeniceControllerService() {
    return controllerService;
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      Utils.exit("USAGE: java -jar venice-controller-all.jar <cluster_config_file_path> <controller_config_file_path>");
    }
    run(args[0], args[1], true);
  }

  public static void run(String clusterConfigFilePath, String controllerConfigFilePath, boolean joinThread) {

    VeniceProperties controllerProps = null;
    String zkAddress = null;
    try {
      VeniceProperties clusterProps = Utils.parseProperties(clusterConfigFilePath);
      VeniceProperties controllerBaseProps = Utils.parseProperties(controllerConfigFilePath);

      controllerProps =
          new PropertyBuilder().put(clusterProps.toProperties()).put(controllerBaseProps.toProperties()).build();
      zkAddress = controllerProps.getString(ZOOKEEPER_ADDRESS);
    } catch (Exception e) {
      String errorMessage = "Can not load configuration from file.";
      LOGGER.error(errorMessage, e);
      Utils.exit(errorMessage + e.getMessage());
    }
    D2Client d2Client = D2ClientFactory.getD2Client(zkAddress, Optional.empty());
    String regionName = RegionUtils.getLocalRegionName(controllerProps, false);
    Map<String, D2Client> d2Clients = Collections.singletonMap(regionName, d2Client);
    VeniceController controller = new VeniceController(
        new VeniceControllerContext.Builder().setPropertiesList(Collections.singletonList(controllerProps))
            .setServiceDiscoveryAnnouncers(new ArrayList<>())
            .setD2Clients(d2Clients)
            .setD2Client(d2Client)
            .build());
    controller.start();
    addShutdownHook(controller, zkAddress);
    if (joinThread) {
      try {
        Thread.currentThread().join();
      } catch (InterruptedException e) {
        LOGGER.error("Unable to join thread in shutdown hook. ", e);
      }
    }
  }

  private static void addShutdownHook(VeniceController controller, String zkAddress) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      controller.stop();
      D2ClientFactory.release(zkAddress);
    }));
  }

  // helper method to aid in testing
  AdminSparkServer getSecureAdminServer() {
    return secureAdminServer;
  }

  VeniceGrpcServer getAdminSecureGrpcServer() {
    return adminSecureGrpcServer;
  }

  AdminSparkServer getAdminServer() {
    return adminServer;
  }

  VeniceGrpcServer getAdminGrpcServer() {
    return adminGrpcServer;
  }

  Optional<StoreBackupVersionCleanupService> getStoreBackupVersionCleanupService() {
    return storeBackupVersionCleanupService;
  }

  Optional<DisabledPartitionEnablerService> getDisabledPartitionEnablerService() {
    return disabledPartitionEnablerService;
  }

  Optional<UnusedValueSchemaCleanupService> getUnusedValueSchemaCleanupService() {
    return unusedValueSchemaCleanupService;
  }

  Optional<StoreGraveyardCleanupService> getStoreGraveyardCleanupService() {
    return storeGraveyardCleanupService;
  }
}
