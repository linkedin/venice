package com.linkedin.venice.grpc;

import com.linkedin.venice.security.SSLFactory;
import io.grpc.BindableService;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


public class VeniceGrpcServerConfig {
  private final int port;
  private final ServerCredentials credentials;
  private final List<BindableService> services;
  private final List<? extends ServerInterceptor> interceptors;
  private final SSLFactory sslFactory;
  private final Executor executor;

  private VeniceGrpcServerConfig(Builder builder) {
    port = builder.port;
    credentials = builder.credentials;
    services = builder.services;
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

  public List<BindableService> getServices() {
    return services;
  }

  public List<? extends ServerInterceptor> getInterceptors() {
    return interceptors;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }

  @Override
  public String toString() {
    return "VeniceGrpcServerConfig{" + "port=" + port + ", services=" + services + "}";
  }

  public static class Builder {
    private Integer port;
    private ServerCredentials credentials;
    private final List<BindableService> services = new ArrayList<>(4);
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

    public Builder addService(BindableService service) {
      this.services.add(service);
      return this;
    }

    public Builder setServices(List<BindableService> services) {
      this.services.addAll(services);
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
        throw new IllegalArgumentException("Port value is required to create the gRPC server but was not provided.");
      }
      if (services.isEmpty()) {
        throw new IllegalArgumentException("Service value is required to create the gRPC server but was not provided.");
      }
      if (numThreads <= 0 && executor == null) {
        throw new IllegalArgumentException(
            "gRPC server creation requires a valid number of threads (numThreads > 0) or a non-null executor.");
      }
      if (interceptors == null) {
        interceptors = Collections.emptyList();
      }
      if (executor == null) {
        executor = Executors.newFixedThreadPool(numThreads);
      }
    }
  }
}
