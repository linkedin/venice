package com.linkedin.venice.controller;

import com.linkedin.d2.server.factory.D2Server;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controller.kafka.TopicCleanupServiceForParentController;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.Logger;


/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {

  private static final Logger logger = Logger.getLogger(VeniceController.class);

  //services
  VeniceControllerService controllerService;
  AdminSparkServer adminServer;
  AdminSparkServer secureAdminServer;
  TopicCleanupService topicCleanupService;

  private final boolean sslEnabled;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final MetricsRepository metricsRepository;
  private final List<D2Server> d2ServerList;
  private final Optional<DynamicAccessController> accessController;

  private final static String CONTROLLER_SERVICE_NAME = "venice-controller";

  // This constructor is being used in local mode
  public VeniceController(VeniceProperties props) {
    this(props, TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME), Collections.emptyList(), Optional.empty());
  }

  // This constructor is being used in integration test
  public VeniceController(List<VeniceProperties> propertiesList, List<D2Server> d2ServerList) {
    this(propertiesList, TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME), d2ServerList, Optional.empty());
  }

  public VeniceController(VeniceProperties props, MetricsRepository metricsRepository, List<D2Server> d2ServerList, Optional<DynamicAccessController> accessController) {
    this(Arrays.asList(new VeniceProperties[]{props}), metricsRepository, d2ServerList, accessController);
  }

  public VeniceController(List<VeniceProperties> propertiesList, MetricsRepository metricsRepository, List<D2Server> d2ServerList, Optional<DynamicAccessController> accessController) {
    this.multiClusterConfigs = new VeniceControllerMultiClusterConfig(propertiesList);
    this.metricsRepository = metricsRepository;
    this.d2ServerList = d2ServerList;
    Optional<SSLConfig> sslConfig = multiClusterConfigs.getSslConfig();
    this.sslEnabled = sslConfig.isPresent() && sslConfig.get().isControllerSSLEnabled();
    this.accessController = accessController;
    createServices();
  }


  public void createServices(){
    controllerService = new VeniceControllerService(multiClusterConfigs, metricsRepository, sslEnabled, multiClusterConfigs.getSslConfig(), accessController);
    adminServer = new AdminSparkServer(
        multiClusterConfigs.getAdminPort(),
        controllerService.getVeniceHelixAdmin(),
        metricsRepository,
        multiClusterConfigs.getClusters(),
        Optional.empty(),
        false,
        Optional.empty());
    if (sslEnabled) {
      /**
       * SSL enabled AdminSparkServer uses a different port number than the regular service.
       */
      secureAdminServer = new AdminSparkServer(
          multiClusterConfigs.getAdminSecurePort(),
          controllerService.getVeniceHelixAdmin(),
          metricsRepository, multiClusterConfigs.getClusters(),
          multiClusterConfigs.getSslConfig(),
          multiClusterConfigs.adminCheckReadMethodForKafka(),
          accessController);
    }
    if (multiClusterConfigs.isParent()) {
      topicCleanupService = new TopicCleanupServiceForParentController(controllerService.getVeniceHelixAdmin(), multiClusterConfigs);
    } else {
      topicCleanupService = new TopicCleanupService(controllerService.getVeniceHelixAdmin(), multiClusterConfigs);
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
    AbstractVeniceService.stopIfNotNull(topicCleanupService);
    AbstractVeniceService.stopIfNotNull(adminServer);
    AbstractVeniceService.stopIfNotNull(secureAdminServer);
    AbstractVeniceService.stopIfNotNull(controllerService);
  }

  public VeniceControllerService getVeniceControllerService() {
    return controllerService;
  }

  public static void main(String args[]) {
    if(args.length < 2) {
      Utils.croak("USAGE: java " + VeniceController.class.getName()
          + " [cluster_config_file_path] [controller_config_file_path] ");
    }
    VeniceProperties controllerProps = null;
    try {
      VeniceProperties clusterProps = Utils.parseProperties(args[0]);
      VeniceProperties controllerBaseProps = Utils.parseProperties(args[1]);

      controllerProps = new PropertyBuilder()
              .put(clusterProps.toProperties())
              .put(controllerBaseProps.toProperties())
              .build();
    } catch (Exception e) {
      String errorMessage = "Can not load configuration from file.";
      logger.error(errorMessage, e);
      Utils.croak(errorMessage+e.getMessage());
    }

    VeniceController controller = new VeniceController(controllerProps);
    controller.start();
    addShutdownHook(controller);
  }

  private static void addShutdownHook(VeniceController controller) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        controller.stop();
      }
    });
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      logger.error("Unable to join thread in shutdown hook. ", e);
    }
  }

}