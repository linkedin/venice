package com.linkedin.venice.controllerapi.routes;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;

import com.linkedin.venice.controllerapi.ControllerApiConstants;


public enum V1Delete implements V1Route {
  VERSION("/v1/cluster/%s/store/%s/version/%s", new String[] { CLUSTER, NAME, ControllerApiConstants.VERSION }), // AKA
                                                                                                                 // kill
                                                                                                                 // job
  STORE("/v1/cluster/%s/store/%s", new String[] { CLUSTER, NAME }); // Not implemented

  private final String pathFormat;
  private final String rawPath;
  private final String[] pathParams;

  V1Delete(String pathFormat, String[] pathParams) {
    this.pathFormat = pathFormat;
    this.pathParams = pathParams;
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
}
