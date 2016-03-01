package com.linkedin.venice.controller;

import com.linkedin.venice.controller.server.AdminServer;
import com.linkedin.venice.service.AbstractVeniceService;
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

  public static void main(String args[]) {
    if(args.length < 1) {
      Utils.croak("USAGE: java " + VeniceController.class.getName()
          + " [config_file_path] ");
    }

    VeniceControllerClusterConfig config = new VeniceControllerClusterConfig(args[0]);
    logger.info("Starting controller: " + config.getControllerName() + " for cluster: " + config.getClusterName()
        + " with ZKAddress: " + config.getZkAddress());

    /* Services are created in the order they must be started */
    services = new ArrayList<AbstractVeniceService>();

    VeniceControllerService controllerService = new VeniceControllerService(config);
    services.add(controllerService);
    //TODO: controller config so we can configure the port
    AdminServer adminServer = new AdminServer(8078, config.getClusterName(), controllerService.getVeniceHelixAdmin());
    services.add(adminServer);

    for (AbstractVeniceService service: services) {
      try {
        service.start();
      } catch (Exception e) {
        logger.error("Error starting the service: " + service.getName(), e);
        System.exit(1);
      }
    }
    addShutdownHook();
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