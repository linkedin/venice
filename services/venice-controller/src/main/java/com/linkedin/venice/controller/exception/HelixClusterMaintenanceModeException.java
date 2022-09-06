package com.linkedin.venice.controller.exception;

import com.linkedin.venice.exceptions.VeniceException;


public class HelixClusterMaintenanceModeException extends VeniceException {
  public HelixClusterMaintenanceModeException(String clusterName) {
    super(
        "Cluster: " + clusterName + " in maintenance mode, so no new resource allocation is allowed. "
            + "Please reach out to Venice team if it doesn't recover soon");
  }
}
