package com.linkedin.venice.controllerapi.routes;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_SIZE;

import com.linkedin.venice.controllerapi.ControllerApiConstants;


public enum V1Post implements V1Route {
  STORE(
      "/v1/cluster/%s/store", new String[] { CLUSTER },
      new String[] { NAME, OWNER, KEY_SCHEMA, ControllerApiConstants.VALUE_SCHEMA }
  ),
  VALUE_SCHEMA(
      "/v1/cluster/%s/store/%s/val_schema", new String[] { CLUSTER, NAME },
      new String[] { ControllerApiConstants.VALUE_SCHEMA }
  ), VERSION("/v1/cluster/%s/store/%s/version", new String[] { CLUSTER, NAME }, new String[] { STORE_SIZE });

  private final String pathFormat;
  private final String rawPath;
  private final String[] pathParams;
  private final String[] bodyParams;

  V1Post(String pathFormat, String[] pathParams, String[] bodyParams) {
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
