package com.linkedin.venice.controllerapi.request;

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
    this.clusterName = validateParam(clusterName, "Cluster name");
    this.storeName = null;
  }

  public ControllerRequest(String clusterName, String storeName) {
    this.clusterName = validateParam(clusterName, "Cluster name");
    this.storeName = validateParam(storeName, "Store name");
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }

  public static String validateParam(String param, String paramName) {
    if (param == null || param.isEmpty()) {
      throw new IllegalArgumentException(paramName + " is missing in the request. It is mandatory");
    }
    return param;
  }
}
