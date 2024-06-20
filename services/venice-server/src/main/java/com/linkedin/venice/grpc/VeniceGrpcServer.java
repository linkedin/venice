package com.linkedin.venice.grpc;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptors;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceGrpcServer {
  private static final Logger LOGGER = LogManager.getLogger(VeniceGrpcServer.class);
  private final Server server;
  private final int port;
  private final SSLFactory sslFactory;
  // protected for testing purposes
  protected ServerCredentials credentials;
  private final Executor executor;
  private final VeniceGrpcServerConfig config;

  public VeniceGrpcServer(VeniceGrpcServerConfig config) {
    port = config.getPort();
    sslFactory = config.getSslFactory();
    executor = config.getExecutor();

    this.config = config;

    initServerCredentials();

    server = Grpc.newServerBuilderForPort(config.getPort(), credentials)
        .executor(executor) // TODO: experiment with different executors for best performance
        .addService(ServerInterceptors.intercept(config.getService(), config.getInterceptors()))
        .addService(ProtoReflectionService.newInstance())
        .build();
  }

  private void initServerCredentials() {
    if (sslFactory == null && config.getCredentials() == null) {
      LOGGER.info("Creating gRPC server with insecure credentials");
      credentials = InsecureServerCredentials.create();
      return;
    }

    if (config.getCredentials() != null) {
      LOGGER.info("Creating gRPC server with custom credentials");
      credentials = config.getCredentials();
      return;
    }

    try {
      credentials = TlsServerCredentials.newBuilder()
          .keyManager(GrpcUtils.getKeyManagers(sslFactory))
          .trustManager(GrpcUtils.getTrustManagers(sslFactory))
          .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
          .build();
    } catch (UnrecoverableKeyException | KeyStoreException | CertificateException | IOException
        | NoSuchAlgorithmException e) {
      LOGGER.error("Failed to initialize secure server credentials for gRPC Server");
      throw new VeniceException("Unable to create credentials with SSLFactory for gRPC server", e);
    }
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
