package com.linkedin.venice.controller;

import com.linkedin.venice.config.StoreConfigsLoader;
import com.linkedin.venice.config.VeniceStorePartitionInformation;
import com.linkedin.venice.controller.server.AdminServer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Venice Controller to manage the cluster. Internally wraps Helix Controller.
 */
public class VeniceController {

  private static final Logger logger = Logger.getLogger(VeniceController.class.getName());

  private static String clusterName;
  private static String zkAddress;
  private static String controllerName;
  private static Map<String, VeniceStorePartitionInformation> storeToPartitionInformationMap;
  private static List<AbstractVeniceService> services;

  public static void main(String args[]) {
    if(args.length < 3) {
      Utils.croak("USAGE: java " + VeniceController.class.getName()
          + " [helix_cluster_name] [zookeeper_address] [controller_name] ");
    }

    clusterName = args[0];
    zkAddress = args[1];
    controllerName = args[2];
    storeToPartitionInformationMap = null;

    logger.info("Starting controller: " + controllerName + " for cluster: " + clusterName + " with ZKAddress: " + zkAddress);

    try {
      storeToPartitionInformationMap = StoreConfigsLoader.loadFromEnvironmentVariable();
    } catch (Exception e) {
      logger.error("Unable to load store configuration from Environment. ", e);
      System.exit(1);
    }

    /* Services are created in the order they must be started */
    services = new ArrayList<AbstractVeniceService>();

    VeniceControllerService controllerService = new VeniceControllerService(zkAddress, clusterName, controllerName, storeToPartitionInformationMap);
    services.add(controllerService);
    //TODO: controller config so we can configure the port
    AdminServer adminServer = new AdminServer(8078, clusterName, controllerService.getVeniceHelixAdmin());
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

        for (AbstractVeniceService service : services) {
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