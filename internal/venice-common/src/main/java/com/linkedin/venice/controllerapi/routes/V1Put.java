package com.linkedin.venice.controllerapi.routes;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;

import com.linkedin.venice.controllerapi.ControllerApiConstants;


public enum V1Put implements V1Route {
  STORE("/v1/cluster/%s/store/%s", new String[] { CLUSTER, NAME }, new String[] { OWNER }),
  CURRENT_VERSION("/v1/cluster/%s/store/%s/current_version", new String[] { CLUSTER, NAME }, new String[] { VERSION }),
  FROZEN(
      "/v1/cluster/%s/store/%s/frozen", new String[] { CLUSTER, NAME }, new String[] { ControllerApiConstants.FROZEN }
  );

  private final String pathFormat;
  private final String rawPath;
  private final String[] pathParams;
  private final String[] bodyParams;

  V1Put(String pathFormat, String[] pathParams, String[] bodyParams) {
    this.pathFormat = pathFormat;
    this.pathParams = pathParams;
    this.bodyParams = bodyParams;
    this.rawPath = V1Route.rawPath(pathFormat, pathParams);
  }

  @Override
  public String getRawPath() {
    return rawPath;
  }

  @Override
  public String getPathFormat() {
    return pathFormat;
  }

  @Override
  public String[] getPathParams() {
    return pathParams;
  }

  public String[] getBodyParams() {
    return bodyParams;
  }
}
