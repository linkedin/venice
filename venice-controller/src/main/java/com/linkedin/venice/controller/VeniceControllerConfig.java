package com.linkedin.venice.controller;

import com.linkedin.venice.utils.VeniceProperties;


/**
 * Configuration which is specific to a Venice controller.
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {
  private static final String ADMIN_PORT = "admin.port";

  private int adminPort;

  public VeniceControllerConfig(VeniceProperties props) {
    super(props);
    checkProperties(props);
  }

  private void checkProperties(VeniceProperties props) {
    adminPort = props.getInt(ADMIN_PORT);
  }

  public int getAdminPort() {
    return adminPort;
  }
}
