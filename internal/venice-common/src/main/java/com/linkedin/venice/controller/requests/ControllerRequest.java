package com.linkedin.venice.controller.requests;

import com.linkedin.venice.controllerapi.ControllerRoute;


/**
 * A data model to represent controller requests that are exposed through CLIs and Rest Spec to end users. Authors
 * of new controller APIs will need to extend this class and plug in appropropriate fields needed for the new API.
 *
 * Additionally, authors need to define converters for the new APIs to be able to work with newer transport agnostic
 * request model. Refer to {@link com.linkedin.venice.controller.converters.GrpcConvertersRegistry} and
 * {@link com.linkedin.venice.controller.converters.HttpConvertersRegistry} to register converters for new request types
 *
 * For routing, the newer APIs need to define routes in {@link ControllerRoute} and
 * {@link com.linkedin.venice.controller.transport.GrpcRoute} for the {@link com.linkedin.venice.controllerapi.ControllerClient}
 * to dispatch requests in transport agnostic model.
 */
public abstract class ControllerRequest {
  private static final long DEFAULT_TIMEOUT_MS = 30000L; // 10 seconds
  private static final int DEFAULT_MAX_RETRIES = 5;
  private final ControllerRoute route;
  private final String clusterName;

  private final long timeoutMs;

  private final int maxRetries;

  public ControllerRequest(String clusterName, ControllerRoute route) {
    this(clusterName, route, DEFAULT_TIMEOUT_MS, DEFAULT_MAX_RETRIES);
  }

  public ControllerRequest(String clusterName, ControllerRoute route, long timeoutMs, int maxRetries) {
    this.clusterName = clusterName;
    this.route = route;
    this.timeoutMs = timeoutMs;
    this.maxRetries = maxRetries;
  }

  public String getClusterName() {
    return clusterName;
  }

  public ControllerRoute getRoute() {
    return route;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  static abstract class Builder<T extends Builder<?>> {
    String clusterName;

    public T setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return getThis();
    }

    abstract T getThis();
  }
}
