package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.kafka.TopicMonitor;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {

  private static final Logger logger = Logger.getLogger(VeniceController.class);

  //services
  VeniceControllerService controllerService;
  AdminSparkServer adminServer;
  TopicMonitor topicMonitor;

  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final MetricsRepository metricsRepository;

  private final static String CONTROLLER_SERVICE_NAME = "venice-controller";

  public VeniceController(VeniceProperties props) {
    this(props, TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME));
  }

  public VeniceController(List<VeniceProperties> propertiesList) {
    multiClusterConfigs = new VeniceControllerMultiClusterConfig(propertiesList);
    metricsRepository = TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME);
    createServices();
  }

  public VeniceController(VeniceProperties props, MetricsRepository metricsRepository) {
    this(Arrays.asList(new VeniceProperties[]{props}), metricsRepository);
  }

  public VeniceController(List<VeniceProperties> propertiesList, MetricsRepository metricsRepository) {
    multiClusterConfigs = new VeniceControllerMultiClusterConfig(propertiesList);
    this.metricsRepository = metricsRepository;
    createServices();
  }

  public void createServices(){
    controllerService = new VeniceControllerService(multiClusterConfigs, metricsRepository);
    adminServer = new AdminSparkServer(
        multiClusterConfigs.getAdminPort(),
        controllerService.getVeniceHelixAdmin(),
        metricsRepository,
        multiClusterConfigs.getClusters());
    // TODO: disable TopicMonitor in Corp cluster for now.
    // If we decide to continue to use TopicMonitor for version creation, we need to update the existing VeniceParentHelixAdmin to support it
    if (!multiClusterConfigs.isParent())
    {
      topicMonitor =
          new TopicMonitor(controllerService.getVeniceHelixAdmin(), multiClusterConfigs.getTopicMonitorPollIntervalMs(),
              controllerService.getVeniceHelixAdmin().getVeniceConsumerFactory());
    }
  }

  public void start() {
    logger.info(
        "Starting controller: " + multiClusterConfigs.getControllerName() + " for clusters: " + multiClusterConfigs
            .getClusters().toString() + " with ZKAddress: " + multiClusterConfigs.getZkAddress());
    controllerService.start();
    adminServer.start();
    if (null != topicMonitor) {
      topicMonitor.start();
    }
    logger.info("Controller is started.");
  }


  public void stop(){
    //TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    AbstractVeniceService.stopIfNotNull(topicMonitor);
    AbstractVeniceService.stopIfNotNull(adminServer);
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