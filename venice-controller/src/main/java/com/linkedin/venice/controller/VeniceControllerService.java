package com.linkedin.venice.controller;

import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import org.apache.log4j.Logger;

/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(VeniceControllerService.class);
  private static final String VENICE_CONTROLLER_SERVICE_NAME = "venice-controller-service";

  private final Admin admin;

  private final VeniceControllerClusterConfig config;

  public VeniceControllerService(VeniceControllerConfig config) {
    super(VENICE_CONTROLLER_SERVICE_NAME);
    this.config = config;
    this.admin = new VeniceHelixAdmin(config);
  }

  @Override
  public void startInner() {
    admin.start(config.getClusterName());
    logger.info("start cluster:" + config.getClusterName());
  }

  @Override
  public void stopInner() {
    admin.stop(config.getClusterName());
    logger.info("Stop cluster:" + config.getClusterName());
  }

  public Admin getVeniceHelixAdmin(){
    return admin;
  }
}
