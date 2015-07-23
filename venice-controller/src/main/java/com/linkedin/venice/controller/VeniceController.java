package com.linkedin.venice.controller;

import com.linkedin.venice.config.StoreConfigsLoader;
import com.linkedin.venice.config.VeniceStorePartitionInformation;
import com.linkedin.venice.utils.Utils;
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
  private static VeniceControllerService controllerService;

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

    controllerService = new VeniceControllerService(zkAddress, clusterName, controllerName, storeToPartitionInformationMap);

    try {
      controllerService.start();
    } catch (Exception e) {
      logger.error("Error starting the Controller Service. ", e);
      System.exit(1);
    }
    addShutdownHook();
  }

  private static void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        logger.info("Disconnecting Helix Standalone Controller.");
        try {
          if (controllerService != null) {
            controllerService.stop();
          }
        } catch (Exception e) {
          logger.error("Unable to stop controller service. ", e);
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