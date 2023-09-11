package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.authentication.AuthenticationServiceUtils.buildAuthenticationService;
import static com.linkedin.venice.authorization.AuthorizerServiceUtils.buildAuthorizerService;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controller.kafka.TopicCleanupServiceForParentController;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.controller.systemstore.SystemStoreRepairService;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.system.store.ControllerClientBackedSystemSchemaInitializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {
  private static final Logger LOGGER = LogManager.getLogger(VeniceController.class);

  // services
  private VeniceControllerService controllerService;
  private AdminSparkServer adminServer;
  private AdminSparkServer secureAdminServer;
  private TopicCleanupService topicCleanupService;
  private Optional<StoreBackupVersionCleanupService> storeBackupVersionCleanupService;
  private Optional<StoreGraveyardCleanupService> storeGraveyardCleanupService;
  private Optional<SystemStoreRepairService> systemStoreRepairService;

  private final boolean sslEnabled;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final MetricsRepository metricsRepository;
  private final List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers;
  private final Optional<DynamicAccessController> accessController;
  private final Optional<AuthenticationService> authenticationService;
  private final Optional<AuthorizerService> authorizerService;
  private final D2Client d2Client;
  private final Optional<ClientConfig> routerClientConfig;
  private final Optional<ICProvider> icProvider;
  private final Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  /**
   * This constructor is being used in integration test.
   *
   * @see #VeniceController(List, MetricsRepository, List, Optional, Optional, D2Client, Optional, Optional, Optional, Optional)
   */
  public VeniceController(
      List<VeniceProperties> propertiesList,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService,
      D2Client d2Client) {
    this(
        propertiesList,
        TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME),
        serviceDiscoveryAnnouncers,
        Optional.empty(),
        authenticationService,
        authorizerService,
        d2Client,
        Optional.empty());
  }

  public VeniceController(
      List<VeniceProperties> propertiesList,
      MetricsRepository metricsRepository,
      List<ServiceDiscoveryAnnouncer> serviceDiscoveryAnnouncers,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService,
      D2Client d2Client,
      Optional<ClientConfig> routerClientConfig) {
    this(
        propertiesList,
        metricsRepository,
        serviceDiscoveryAnnouncers,
        accessController,
        authenticationService,
        authorizerService,
        d2Client,
        routerClientConfig,
        Optional.empty(),
        Optional.empty());
  }

  private final PubSubClientsFactory pubSubClientsFactory;
  static final String CONTROLLER_SERVICE_NAME = "venice-controller";

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
      Optional<AuthenticationService> authenticationService,
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
    this.metricsRepository = ctx.getMetricsRepository();
    this.serviceDiscoveryAnnouncers = ctx.getServiceDiscoveryAnnouncers();
    Optional<SSLConfig> sslConfig = multiClusterConfigs.getSslConfig();
    this.sslEnabled = sslConfig.isPresent() && sslConfig.get().isControllerSSLEnabled();
    this.accessController = Optional.ofNullable(ctx.getAccessController());
    this.authorizerService = Optional.ofNullable(ctx.getAuthorizerService());
    this.authenticationService = Optional.ofNullable(ctx.getAuthenticationService());
    this.d2Client = ctx.getD2Client();
    this.routerClientConfig = Optional.ofNullable(ctx.getRouterClientConfig());
    this.icProvider = Optional.ofNullable(ctx.getIcProvider());
    this.externalSupersetSchemaGenerator = Optional.ofNullable(ctx.getExternalSupersetSchemaGenerator());
    this.pubSubClientsFactory = Objects.requireNonNull(ctx.getPubSubClientsFactory(), "PubSubClientsFactory is null");
    createServices();
  }

  private void createServices() {
    controllerService = new VeniceControllerService(
        multiClusterConfigs,
        metricsRepository,
        sslEnabled,
        multiClusterConfigs.getSslConfig(),
        accessController,
        authorizerService,
        d2Client,
        routerClientConfig,
        icProvider,
        externalSupersetSchemaGenerator,
        pubSubTopicRepository,
        pubSubClientsFactory);

    adminServer = new AdminSparkServer(
        // no need to pass the hostname, we are binding to all the addresses
        multiClusterConfigs.getAdminPort(),
        controllerService.getVeniceHelixAdmin(),
        metricsRepository,
        multiClusterConfigs.getClusters(),
        multiClusterConfigs.isControllerEnforceSSLOnly(),
        Optional.empty(),
        false,
        Optional.empty(),
        authenticationService,
        authorizerService,
        multiClusterConfigs.getDisabledRoutes(),
        multiClusterConfigs.getCommonConfig().getJettyConfigOverrides(),
        // TODO: Builder pattern or just pass the config object here?
        multiClusterConfigs.getCommonConfig().isDisableParentRequestTopicForStreamPushes(),
        pubSubTopicRepository);
    if (sslEnabled) {
      /**
       * SSL enabled AdminSparkServer uses a different port number than the regular service.
       */
      secureAdminServer = new AdminSparkServer(
          multiClusterConfigs.getAdminSecurePort(),
          controllerService.getVeniceHelixAdmin(),
          metricsRepository,
          multiClusterConfigs.getClusters(),
          true,
          multiClusterConfigs.getSslConfig(),
          multiClusterConfigs.adminCheckReadMethodForKafka(),
          accessController,
          authenticationService,
          authorizerService,
          multiClusterConfigs.getDisabledRoutes(),
          multiClusterConfigs.getCommonConfig().getJettyConfigOverrides(),
          multiClusterConfigs.getCommonConfig().isDisableParentRequestTopicForStreamPushes(),
          pubSubTopicRepository);
    }
    storeBackupVersionCleanupService = Optional.empty();
    storeGraveyardCleanupService = Optional.empty();
    systemStoreRepairService = Optional.empty();
    Admin admin = controllerService.getVeniceHelixAdmin();
    if (multiClusterConfigs.isParent()) {
      topicCleanupService =
          new TopicCleanupServiceForParentController(admin, multiClusterConfigs, pubSubTopicRepository);
      if (!(admin instanceof VeniceParentHelixAdmin)) {
        throw new VeniceException(
            "'VeniceParentHelixAdmin' is expected of the returned 'Admin' from 'VeniceControllerService#getVeniceHelixAdmin' in parent mode");
      }
      storeGraveyardCleanupService =
          Optional.of(new StoreGraveyardCleanupService((VeniceParentHelixAdmin) admin, multiClusterConfigs));
      LOGGER.info("StoreGraveyardCleanupService is enabled");
      if (multiClusterConfigs.getCommonConfig().isParentSystemStoreRepairServiceEnabled()) {
        systemStoreRepairService = Optional
            .of(new SystemStoreRepairService((VeniceParentHelixAdmin) admin, multiClusterConfigs, metricsRepository));
        LOGGER.info("SystemStoreRepairServiceEnabled is enabled");
      }
    } else {
      topicCleanupService = new TopicCleanupService(admin, multiClusterConfigs, pubSubTopicRepository);
      if (!(admin instanceof VeniceHelixAdmin)) {
        throw new VeniceException(
            "'VeniceHelixAdmin' is expected of the returned 'Admin' from 'VeniceControllerService#getVeniceHelixAdmin' in child mode");
      }
      storeBackupVersionCleanupService =
          Optional.of(new StoreBackupVersionCleanupService((VeniceHelixAdmin) admin, multiClusterConfigs));
      LOGGER.info("StoreBackupVersionCleanupService is enabled");
    }
    // Run before enabling controller in helix so leadership won't hand back to this controller during schema requests.
    initializeSystemSchema(controllerService.getVeniceHelixAdmin());
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
    systemStoreRepairService.ifPresent(AbstractVeniceService::start);
    // register with service discovery at the end
    serviceDiscoveryAnnouncers.forEach(serviceDiscoveryAnnouncer -> {
      serviceDiscoveryAnnouncer.register();
      LOGGER.info("Registered to service discovery: {}", serviceDiscoveryAnnouncer);
    });
    LOGGER.info("Controller is started.");
  }

  private void initializeSystemSchema(Admin admin) {
    String systemStoreCluster = multiClusterConfigs.getSystemSchemaClusterName();
    VeniceControllerConfig systemStoreClusterConfig = multiClusterConfigs.getControllerConfig(systemStoreCluster);
    if (!multiClusterConfigs.isParent() && multiClusterConfigs.isZkSharedMetaSystemSchemaStoreAutoCreationEnabled()
        && systemStoreClusterConfig.isSystemSchemaInitializationAtStartTimeEnabled()) {
      String regionName = systemStoreClusterConfig.getRegionName();
      ControllerClientBackedSystemSchemaInitializer metaSystemStoreSchemaInitializer =
          new ControllerClientBackedSystemSchemaInitializer(
              AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
              systemStoreCluster,
              AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema(),
              VeniceSystemStoreUtils.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS,
              true,
              ((VeniceHelixAdmin) admin).getSslFactory(),
              systemStoreClusterConfig.getChildControllerUrl(regionName),
              systemStoreClusterConfig.getChildControllerD2ServiceName(),
              systemStoreClusterConfig.getChildControllerD2ZkHost(regionName),
              systemStoreClusterConfig.isControllerEnforceSSLOnly());
      metaSystemStoreSchemaInitializer.execute();
    }
  }

  /**
   * Causes venice controller and its associated services to stop executing.
   */
  public void stop() {
    // unregister from service discovery first
    serviceDiscoveryAnnouncers.forEach(serviceDiscoveryAnnouncer -> {
      serviceDiscoveryAnnouncer.unregister();
      LOGGER.info("Unregistered from service discovery: {}", serviceDiscoveryAnnouncer);
    });
    // TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    systemStoreRepairService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    storeGraveyardCleanupService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    storeBackupVersionCleanupService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    Utils.closeQuietlyWithErrorLogged(topicCleanupService);
    Utils.closeQuietlyWithErrorLogged(secureAdminServer);
    Utils.closeQuietlyWithErrorLogged(adminServer);
    Utils.closeQuietlyWithErrorLogged(controllerService);
    authenticationService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    authorizerService.ifPresent(Utils::closeQuietlyWithErrorLogged);
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

    Optional<AuthenticationService> authenticationService = buildAuthenticationService(controllerProps);
    Optional<AuthorizerService> authorizerService = buildAuthorizerService(controllerProps);

    D2Client d2Client = D2ClientFactory.getD2Client(zkAddress, Optional.empty());
    VeniceController controller = new VeniceController(
        new VeniceControllerContext.Builder()
            .setPropertiesList(Arrays.asList(new VeniceProperties[] { controllerProps }))
            .setServiceDiscoveryAnnouncers(new ArrayList<>())
            .setD2Client(d2Client)
            .setAuthenticationService(authenticationService.orElse(null))
            .setAuthorizerService(authorizerService.orElse(null))
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
}
