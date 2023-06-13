package com.linkedin.venice.grpc;

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
    } catch (IllegalStateException | IOException illegalStateException) {
      LOGGER.error(
          "Failed to start {} on port {}",
          VeniceGrpcServer.class.getSimpleName(),
          server.getPort(),
          illegalStateException);
      throw new RuntimeException(illegalStateException);
    }
  }

  public void stop() {
    if (server != null && !server.isShutdown()) {
      server.shutdown();
    }
  }
}
