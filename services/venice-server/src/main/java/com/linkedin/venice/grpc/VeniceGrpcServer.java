package com.linkedin.venice.grpc;

import com.linkedin.venice.exceptions.VeniceException;
import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceGrpcServer {
  private static final Logger LOGGER = LogManager.getLogger(VeniceGrpcServer.class);
  private final Server server;
  private final int port;
  private final VeniceGrpcServerConfig config;

  public VeniceGrpcServer(VeniceGrpcServerConfig config) {
    port = config.getPort();
    this.config = config;
    server = Grpc.newServerBuilderForPort(config.getPort(), config.getCredentials())
        // .executor(...) TODO: experiment with server config w.r.t. custom executor for optimizing performance
        .addService(ServerInterceptors.intercept(config.getService(), config.getInterceptors()))
        .build();

  }

  public void start() throws VeniceException {
    try {
      server.start();
    } catch (IOException exception) {
      LOGGER.error(
          "Failed to start gRPC Server for service {} on port {}",
          config.getService().getClass().getSimpleName(),
          port,
          exception);
      throw new VeniceException("Unable to start gRPC server", exception);
    }
  }

  public boolean isShutdown() {
    return server.isShutdown();
  }

  public boolean isTerminated() {
    return server.isTerminated();
  }

  public void stop() {
    if (server != null && !server.isShutdown()) {
      server.shutdown();
    }
  }
}
