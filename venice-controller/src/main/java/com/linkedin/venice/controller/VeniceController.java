package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.TopicMonitor;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
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

  private final VeniceControllerConfig config;

  public VeniceController(VeniceProperties props){
    config = new VeniceControllerConfig(props);
    createServices();
  }

  public void createServices(){
    controllerService = new VeniceControllerService(config);
    adminServer = new AdminSparkServer(
        config.getAdminPort(),
        controllerService.getVeniceHelixAdmin());
    topicMonitor = new TopicMonitor(
        controllerService.getVeniceHelixAdmin(),
        config.getClusterName(),
        config.getReplicaFactor(),
        10, TimeUnit.SECONDS); /* poll every 10 sec, TODO: configurable, long poll makes tests slower */
  }

  public void start(){
    logger.info("Starting controller: " + config.getControllerName() + " for cluster: " + config.getClusterName()
        + " with ZKAddress: " + config.getZkAddress());
    controllerService.start();
    adminServer.start();
    topicMonitor.start();
    logger.info("Controller is started.");
  }


  public void stop(){
    //TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    AbstractVeniceService.stopIfNotNull(topicMonitor);
    AbstractVeniceService.stopIfNotNull(adminServer);
    AbstractVeniceService.stopIfNotNull(controllerService);
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