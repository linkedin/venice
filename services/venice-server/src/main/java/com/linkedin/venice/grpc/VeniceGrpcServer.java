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

  public VeniceGrpcServer(VeniceGrpcServerConfig config) {
    server = Grpc.newServerBuilderForPort(config.getPort(), config.getCredentials())
        .addService(ServerInterceptors.intercept(config.getService(), config.getInterceptors()))
        .build();
  }

  public void start() {
    try {
      server.start();
    } catch (IOException exception) {
      LOGGER.error(
          "Failed to start gRPC Server for service {} on port {}",
          server.getServices(),
          server.getPort(),
          exception);
      throw new VeniceException("Unable to start gRPC server", exception);
    }
  }

  public void stop() {
    if (server != null && !server.isShutdown()) {
      server.shutdown();
    }
  }
}
