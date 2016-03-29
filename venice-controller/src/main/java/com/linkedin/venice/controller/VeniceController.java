package com.linkedin.venice.controller;

import com.linkedin.venice.controller.server.AdminSparkServer;
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

  private static List<AbstractVeniceService> services;

  private final VeniceControllerConfig config;

  public VeniceController(Props props){
    config = new VeniceControllerConfig(props);
  }

  public void createServices(){
     /* Services are created in the order they must be started */
    services = new ArrayList<AbstractVeniceService>();

    VeniceControllerService controllerService = new VeniceControllerService(config);
    services.add(controllerService);

    AdminSparkServer adminServer = new AdminSparkServer(config.getAdminPort(), config.getClusterName(), controllerService.getVeniceHelixAdmin());
    services.add(adminServer);
  }

  public void start(){
    logger.info("Starting controller: " + config.getControllerName() + " for cluster: " + config.getClusterName()
        + " with ZKAddress: " + config.getZkAddress());
    for (AbstractVeniceService service: services) {
      try {
        service.start();
      } catch (Exception e) {
        logger.error("Error starting the service: " + service.getName(), e);
        System.exit(1);
      }
    }
    addShutdownHook();
    logger.info("Controller is started.");
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
    controller.createServices();
    controller.start();
  }

  private static void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        //TODO: we may want a dependency structure so we ensure services are shutdown in the correct order.
        for (AbstractVeniceService service : Utils.reversed(services)) {
          logger.info("Stopping controller service: " + service.getName());
          try {
            if (service != null) {
              service.stop();
            }
          } catch (Exception e) {
            logger.error("Unable to stop service: " + service.getName(), e);
          }
        }
      }
    });
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      logger.error("Unable to join thread in shutdown hook. ", e);
    }
  }

}