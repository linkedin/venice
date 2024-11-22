package com.linkedin.venice.controller.transport;

import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;


/**
 * A container enum to provide information for {@link ControllerGrpcTransportClient} about the physical routes and type
 * information about the request and response for gRPC implementations of controller API endpoints.
 *
 * Authors of controller API, will need to create a new route along with the type information for the
 * {@link ControllerGrpcTransportClient} to automatically support the newer APIs from
 * {@link com.linkedin.venice.controllerapi.ControllerClient}
 */
public enum GrpcRoute {
  CREATE_STORE("createStore", CreateStoreGrpcRequest.class, CreateStoreGrpcResponse.class);

  private final String method;
  private final Class<?> requestType;

  private final Class<?> responseType;

  <T, D> GrpcRoute(String method, Class<T> requestType, Class<D> responseType) {
    this.method = method;
    this.requestType = requestType;
    this.responseType = responseType;
  }

  public String getMethod() {
    return method;
  }

  public Class<?> getRequestType() {
    return requestType;
  }

  public Class<?> getResponseType() {
    return responseType;
  }
}
