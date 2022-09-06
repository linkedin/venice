package com.linkedin.venice.controllerapi.routes;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_ID;

import com.linkedin.venice.controllerapi.ControllerApiConstants;


/**
 * Controller API version 1 HTTP GET routes
 */
public enum V1Get implements V1Route {
  STORE("/v1/cluster/%s/store/%s", new String[] { CLUSTER, NAME }),
  STORES("/v1/cluster/%s/store", new String[] { CLUSTER }),
  KEY_SCHEMA("/v1/cluster/%s/store/%s/key_schema", new String[] { CLUSTER, NAME }),
  VALUE_SCHEMAS("/v1/cluster/%s/store/%s/value_schema", new String[] { CLUSTER, NAME }),
  VALUE_SCHEMA("/v1/cluster/%s/store/%s/value_schema/%s", new String[] { CLUSTER, NAME, SCHEMA_ID }),
  VERSIONS("/v1/cluster/%s/store/%s/version", new String[] { CLUSTER, NAME }),
  VERSION("/v1/cluster/%s/store/%s/version/%s", new String[] { CLUSTER, NAME, ControllerApiConstants.VERSION }), // AKA
                                                                                                                 // job
                                                                                                                 // status
  CURRENT_VERSION("/v1/cluster/%s/store/%s/version/current", new String[] { CLUSTER, NAME });

  private final String pathFormat;
  private final String rawPath;
  private final String[] pathParams;

  V1Get(String pathFormat, String[] pathParams) {
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
