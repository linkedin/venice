package com.linkedin.venice.controller;

import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Props;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {

  private static final Logger logger = Logger.getLogger(VeniceController.class.getName());

  //services
  VeniceControllerService controllerService;
  AdminSparkServer adminServer;

  private final VeniceControllerConfig config;

  public VeniceController(Props props){
    config = new VeniceControllerConfig(props);
    createServices();
  }

  public void createServices(){
    controllerService = new VeniceControllerService(config);
    adminServer = new AdminSparkServer(config.getAdminPort(), controllerService.getVeniceHelixAdmin());
  }

  public void start(){
    logger.info("Starting controller: " + config.getControllerName() + " for cluster: " + config.getClusterName()
        + " with ZKAddress: " + config.getZkAddress());
    controllerService.start();
    adminServer.start();
    logger.info("Controller is started.");
  }


  public void stop(){
    //TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
    AbstractVeniceService.stopIfNotNull(adminServer);
    AbstractVeniceService.stopIfNotNull(controllerService);
  }

  public static void main(String args[]) {
    if(args.length < 2) {
      Utils.croak("USAGE: java " + VeniceController.class.getName()
          + " [cluster_config_file_path] [controller_config_file_path] ");
    }
    Props controllerProps = null;
    try {
      Props clusterProps = Utils.parseProperties(args[0]);
      controllerProps = clusterProps.mergeWithProperties(Utils.parseProperties(args[1]));
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