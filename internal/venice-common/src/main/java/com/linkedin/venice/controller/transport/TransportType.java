package com.linkedin.venice.controller.transport;

/**
 * Modes of transport protocol currently supported by {@link com.linkedin.venice.controllerapi.ControllerClient}
 * Please refer to {@link RouteUtils#ROUTE_MAP} to check if the controller API is supported for all the transport types.
 */
public enum TransportType {
  HTTP, GRPC
}
