package com.linkedin.venice.controllerapi.request;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;


/**
 * Base class for request objects used in controller endpoints.
 *
 * Extend this class to ensure required parameters are validated in the constructor of the extending class.
 * This class is intended for use on both the client and server sides.
 * All required parameters should be passed to and validated within the constructor of the extending class.
 */
public class ControllerRequest {
  protected String clusterName;
  protected String storeName;

  public ControllerRequest(String clusterName) {
    this.clusterName = validateParam(clusterName, CLUSTER);
    this.storeName = null;
  }

  public ControllerRequest(String clusterName, String storeName) {
    this.clusterName = validateParam(clusterName, CLUSTER);
    this.storeName = validateParam(storeName, STORE_NAME);
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }

  public static String validateParam(String param, String paramName) {
    if (param == null || param.isEmpty()) {
      throw new IllegalArgumentException("The request is missing the " + paramName + ", which is a mandatory field.");
    }
    return param;
  }
}
