package com.linkedin.venice.controller;

import com.linkedin.venice.utils.Props;


/**
 * Configuration which is specific to a Venice controller.
 */
public class VeniceControllerConfig extends VeniceControllerClusterConfig {
  private static final String ADMIN_PORT = "admin.port";

  private int adminPort;

  public VeniceControllerConfig(Props props) {
    super(props);
    checkProperties(props);
  }

  private void checkProperties(Props props) {
    adminPort = props.getInt(ADMIN_PORT);
  }

  public int getAdminPort() {
    return adminPort;
  }
}
