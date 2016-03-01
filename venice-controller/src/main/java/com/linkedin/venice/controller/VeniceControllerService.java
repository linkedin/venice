package com.linkedin.venice.controller;

import com.linkedin.venice.service.AbstractVeniceService;
import org.apache.log4j.Logger;

/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(VeniceControllerService.class.getName());
  private static final String VENICE_CONTROLLER_SERVICE_NAME = "venice-controller-service";

  private final Admin admin;

  private final VeniceControllerClusterConfig config;

  public VeniceControllerService(VeniceControllerClusterConfig config) {
    super(VENICE_CONTROLLER_SERVICE_NAME);
    this.config = config;
    this.admin = new VeniceHelixAdmin(config.getControllerName(),config.getZkAddress(),config.getKafkaZkAddress());
  }

  @Override
  public void startInner() {
    admin.start(config.getClusterName(), config);
    logger.info("Start cluster:" + config.getClusterName());
    addDefaultStoresAsHelixResources();
  }

  @Override
  public void stopInner() {
    admin.stop(config.getClusterName());
    logger.info("Stop cluster:" + config.getClusterName());
  }

  /**
   * Adds Venice stores as a Helix Resource.
   * <p>
   * Add some default stores loaded from configuration file for test purpose. If store already exist, do not add it
   * again. If store already has version-1, do not add version again.
   */
  private void addDefaultStoresAsHelixResources() {
    for (String storeName : config.getStores()) {
      admin.addStore(config.getClusterName(), storeName, "venice-dev");
      admin.addVersion(config.getClusterName(), storeName, 1);
    }
  }

  public Admin getVeniceHelixAdmin(){
    return admin;
  }
}
