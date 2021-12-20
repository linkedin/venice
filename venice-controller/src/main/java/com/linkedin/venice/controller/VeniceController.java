package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controller.kafka.TopicCleanupServiceForParentController;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.KafkaClientStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {

  private static final Logger logger = LogManager.getLogger(VeniceController.class);

  //services
  private VeniceControllerService controllerService;
  private AdminSparkServer adminServer;
  private AdminSparkServer secureAdminServer;
  private TopicCleanupService topicCleanupService;
  private Optional<StoreBackupVersionCleanupService> storeBackupVersionCleanupService;

  private final boolean sslEnabled;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final MetricsRepository metricsRepository;
  private final List<D2Server> d2ServerList;
  private final Optional<DynamicAccessController> accessController;
  private final Optional<AuthorizerService> authorizerService;
  private final D2Client d2Client;
  private final Optional<ClientConfig> routerClientConfig;
  private final static String CONTROLLER_SERVICE_NAME = "venice-controller";

  // This constructor is being used in integration test
  public VeniceController(List<VeniceProperties> propertiesList, List<D2Server> d2ServerList, Optional<AuthorizerService> authorizerService, D2Client d2Client) {
    this(propertiesList, TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME), d2ServerList, Optional.empty(), authorizerService, d2Client, Optional.empty());
  }

  public VeniceController(VeniceProperties props, MetricsRepository metricsRepository, List<D2Server> d2ServerList,
      Optional<DynamicAccessController> accessController, Optional<AuthorizerService> authorizerService,
      D2Client d2Client,  Optional<ClientConfig> routerClientConfig) {
    this(Collections.singletonList(props), metricsRepository, d2ServerList, accessController,
        authorizerService, d2Client, routerClientConfig);
  }

  public VeniceController(List<VeniceProperties> propertiesList, MetricsRepository metricsRepository, List<D2Server> d2ServerList,
      Optional<DynamicAccessController> accessController, Optional<AuthorizerService> authorizerService, D2Client d2Client,
      Optional<ClientConfig> routerClientConfig) {
    this.multiClusterConfigs = new VeniceControllerMultiClusterConfig(propertiesList);
    this.metricsRepository = metricsRepository;
    this.d2ServerList = d2ServerList;
    Optional<SSLConfig> sslConfig = multiClusterConfigs.getSslConfig();
    this.sslEnabled = sslConfig.isPresent() && sslConfig.get().isControllerSSLEnabled();
    this.accessController = accessController;
    this.authorizerService = authorizerService;
    this.d2Client = d2Client;
    this.routerClientConfig = routerClientConfig;

    createServices();
    KafkaClientStats.registerKafkaClientStats(metricsRepository, "KafkaClientStats", Optional.empty());
  }

  private void createServices() {
    controllerService = new VeniceControllerService(multiClusterConfigs, metricsRepository, sslEnabled,
        multiClusterConfigs.getSslConfig(), accessController, authorizerService, d2Client, routerClientConfig);
    adminServer = new AdminSparkServer(
        multiClusterConfigs.getAdminPort(),
        controllerService.getVeniceHelixAdmin(),
        metricsRepository,
        multiClusterConfigs.getClusters(),
        multiClusterConfigs.isControllerEnforceSSLOnly(),
        Optional.empty(),
        false,
        Optional.empty(),
        multiClusterConfigs.getDisabledRoutes(),
        multiClusterConfigs.getCommonConfig().getJettyConfigOverrides());
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
          multiClusterConfigs.getDisabledRoutes(),
          multiClusterConfigs.getCommonConfig().getJettyConfigOverrides());
    }
    storeBackupVersionCleanupService = Optional.empty();
    if (multiClusterConfigs.isParent()) {
      topicCleanupService = new TopicCleanupServiceForParentController(controllerService.getVeniceHelixAdmin(), multiClusterConfigs);
    } else {
      topicCleanupService = new TopicCleanupService(controllerService.getVeniceHelixAdmin(), multiClusterConfigs);
      Admin admin = controllerService.getVeniceHelixAdmin();
      if (!(admin instanceof VeniceHelixAdmin)) {
        throw new VeniceException("'VeniceHelixAdmin' is expected of the returned 'Admin' from 'VeniceControllerService#getVeniceHelixAdmin' in child mode");
      }
      storeBackupVersionCleanupService = Optional.of(new StoreBackupVersionCleanupService((VeniceHelixAdmin)admin, multiClusterConfigs));
      logger.info("StoreBackupVersionCleanupService is enabled");
    }
  }

  public void start() {
    logger.info(
        "Starting controller: " + multiClusterConfigs.getControllerName() + " for clusters: " + multiClusterConfigs
            .getClusters().toString() + " with ZKAddress: " + multiClusterConfigs.getZkAddress());
    controllerService.start();
    adminServer.start();
    if (sslEnabled) {
      secureAdminServer.start();
    }
    topicCleanupService.start();
    storeBackupVersionCleanupService.ifPresent( s -> s.start());
    // start d2 service at the end
    d2ServerList.forEach( d2Server -> {
      d2Server.forceStart();
      logger.info("Started d2 announcer: " + d2Server);
    });
    logger.info("Controller is started.");
  }


  public void stop(){
    // stop d2 service first
    d2ServerList.forEach( d2Server -> {
      d2Server.notifyShutdown();
      logger.info("Stopped d2 announcer: " + d2Server);
    });
    //TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    Utils.closeQuietlyWithErrorLogged(topicCleanupService);
    storeBackupVersionCleanupService.ifPresent(Utils::closeQuietlyWithErrorLogged);
    Utils.closeQuietlyWithErrorLogged(adminServer);
    Utils.closeQuietlyWithErrorLogged(secureAdminServer);
    Utils.closeQuietlyWithErrorLogged(controllerService);
  }

  public VeniceControllerService getVeniceControllerService() {
    return controllerService;
  }
}
