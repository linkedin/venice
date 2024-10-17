package com.linkedin.venice.grpc;

import com.linkedin.venice.security.SSLFactory;
import io.grpc.BindableService;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


public class VeniceGrpcServerConfig {
  private final int port;
  private final ServerCredentials credentials;
  private final BindableService service;
  private final List<? extends ServerInterceptor> interceptors;
  private final SSLFactory sslFactory;
  private final Executor executor;

  private VeniceGrpcServerConfig(Builder builder) {
    port = builder.port;
    credentials = builder.credentials;
    service = builder.service;
    interceptors = builder.interceptors;
    sslFactory = builder.sslFactory;
    executor = builder.executor;
  }

  public int getPort() {
    return port;
  }

  public ServerCredentials getCredentials() {
    return credentials;
  }

  public Executor getExecutor() {
    return executor;
  }

  public BindableService getService() {
    return service;
  }

  public List<? extends ServerInterceptor> getInterceptors() {
    return interceptors;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
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
    private SSLFactory sslFactory;
    private int numThreads;
    private Executor executor;

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

    public Builder setSslFactory(SSLFactory sslFactory) {
      this.sslFactory = sslFactory;
      return this;
    }

    public Builder setNumThreads(int numThreads) {
      this.numThreads = numThreads;
      return this;
    }

    public Builder setExecutor(Executor executor) {
      this.executor = executor;
      return this;
    }

    public VeniceGrpcServerConfig build() {
      verifyAndAddDefaults();
      return new VeniceGrpcServerConfig(this);
    }

    private void verifyAndAddDefaults() {
      if (port == null) {
        throw new IllegalArgumentException("Port must be set");
      }
      if (service == null) {
        throw new IllegalArgumentException("Service must be set");
      }
      if (interceptors == null) {
        interceptors = Collections.emptyList();
      }
      if (numThreads <= 0 && executor == null) {
        throw new IllegalArgumentException("Either numThreads or executor must be set");
      }

      if (executor == null) {
        executor = Executors.newFixedThreadPool(numThreads);
      }
    }
  }
}
