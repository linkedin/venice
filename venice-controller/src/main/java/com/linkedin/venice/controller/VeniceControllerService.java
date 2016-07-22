package com.linkedin.venice.controller;

import com.linkedin.venice.service.AbstractVeniceService;
import org.apache.log4j.Logger;

/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(VeniceControllerService.class);

  private final Admin admin;

  private final VeniceControllerClusterConfig config;

  public VeniceControllerService(VeniceControllerConfig config) {
    this.config = config;
    this.admin = new VeniceHelixAdmin(config);
  }

  @Override
  public boolean startInner() {
    admin.start(config.getClusterName());
    logger.info("start cluster:" + config.getClusterName());

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
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
