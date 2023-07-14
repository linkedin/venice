package com.linkedin.venice.grpc;

import io.grpc.BindableService;
import io.grpc.InsecureServerCredentials;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import java.util.Collections;
import java.util.List;


public class VeniceGrpcServerConfig {
  private final int port;
  private final ServerCredentials credentials;
  private final BindableService service;
  private final boolean hasInterceptors;
  private final List<? extends ServerInterceptor> interceptors;

  private VeniceGrpcServerConfig(Builder builder) {
    port = builder.port;
    credentials = builder.credentials;
    service = builder.service;
    hasInterceptors = builder.interceptors != null && !builder.interceptors.isEmpty();
    interceptors = builder.interceptors;
  }

  public int getPort() {
    return port;
  }

  public ServerCredentials getCredentials() {
    return credentials;
  }

  public BindableService getService() {
    return service;
  }

  public boolean getHasInteceptors() {
    return hasInterceptors;
  }

  public List<? extends ServerInterceptor> getInterceptors() {
    return interceptors;
  }

  @Override
  public String toString() {
    return "VeniceGrpcServerConfig{" + "port=" + port + ", service=" + service + "}";
  }

  public static class Builder {
    private Integer port;
    private ServerCredentials credentials;
    private BindableService service;
    private List<? extends ServerInterceptor> interceptors;

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setCredentials(ServerCredentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public Builder setService(BindableService service) {
      this.service = service;
      return this;
    }

    public Builder setInterceptors(List<? extends ServerInterceptor> interceptors) {
      this.interceptors = interceptors;
      return this;
    }

    public Builder setInterceptor(ServerInterceptor interceptor) {
      this.interceptors = Collections.singletonList(interceptor);
      return this;
    }

    public VeniceGrpcServerConfig build() {
      verifyAndAddDefaults();
      return new VeniceGrpcServerConfig(this);
    }

    public void verifyAndAddDefaults() {
      if (port == null) {
        throw new IllegalArgumentException("Port must be set");
      }
      if (credentials == null) {
        credentials = InsecureServerCredentials.create();
      }
      if (service == null) {
        throw new IllegalArgumentException("Service must be set");
      }
      if (interceptors == null) {
        interceptors = Collections.emptyList();
      }
    }
  }
}
