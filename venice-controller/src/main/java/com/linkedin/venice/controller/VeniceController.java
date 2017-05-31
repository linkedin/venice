package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.TopicMonitor;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;

import javax.validation.constraints.NotNull;

/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {

  private static final Logger logger = Logger.getLogger(VeniceController.class);

  //services
  VeniceControllerService controllerService;
  AdminSparkServer adminServer;
  TopicMonitor topicMonitor;

  private final VeniceControllerConfig config;
  private final MetricsRepository metricsRepository;

  private final static String CONTROLLER_SERVICE_NAME = "venice-controller";

  public VeniceController(VeniceProperties props) {
    this(props, TehutiUtils.getMetricsRepository(CONTROLLER_SERVICE_NAME));
  }

  public VeniceController(VeniceProperties props, MetricsRepository metricsRepository){
    config = new VeniceControllerConfig(props);

    this.metricsRepository = metricsRepository;

    createServices();
  }

  public void createServices(){
    controllerService = new VeniceControllerService(config, metricsRepository);
    adminServer = new AdminSparkServer(
        config.getAdminPort(),
        controllerService.getVeniceHelixAdmin(),
        metricsRepository);
    // TODO: disable TopicMonitor in Corp cluster for now.
    // If we decide to continue to use TopicMonitor for version creation, we need to update the existing VeniceParentHelixAdmin to support it
    if (!config.isParent())
    {
      topicMonitor = new TopicMonitor(
          controllerService.getVeniceHelixAdmin(),
          config.getClusterName(),
          config.getReplicaFactor(),
          config.getTopicMonitorPollIntervalMs());
    }
  }

  public void start(){
    logger.info("Starting controller: " + config.getControllerName() + " for cluster: " + config.getClusterName()
        + " with ZKAddress: " + config.getZkAddress());
    logger.info("sdwu's log. New controller version");
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