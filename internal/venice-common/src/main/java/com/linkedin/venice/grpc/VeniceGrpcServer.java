package com.linkedin.venice.grpc;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
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
  boolean isSecure;
  private final Executor executor;
  private final VeniceGrpcServerConfig config;

  public VeniceGrpcServer(VeniceGrpcServerConfig config) {
    this.port = config.getPort();
    this.sslFactory = config.getSslFactory();
    this.executor = config.getExecutor();
    this.config = config;
    initServerCredentials();
    ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort(port, credentials)
        .executor(executor)
        .addService(ProtoReflectionService.newInstance());

    List<BindableService> services = config.getServices();
    for (BindableService service: services) {
      serverBuilder.addService(service);
    }
    List<? extends ServerInterceptor> interceptors = config.getInterceptors();
    for (ServerInterceptor interceptor: interceptors) {
      serverBuilder.intercept(interceptor);
    }
    server = serverBuilder.build();
  }

  private void initServerCredentials() {
    if (sslFactory == null && config.getCredentials() == null) {
      LOGGER.info("Creating gRPC server with insecure credentials on port: {}", port);
      credentials = InsecureServerCredentials.create();
      isSecure = false;
      return;
    }

    if (config.getCredentials() != null) {
      LOGGER.debug("Creating gRPC server with custom credentials");
      credentials = config.getCredentials();
      isSecure = !(credentials instanceof InsecureServerCredentials);
      return;
    }

    try {
      credentials = TlsServerCredentials.newBuilder()
          .keyManager(GrpcUtils.getKeyManagers(sslFactory))
          .trustManager(GrpcUtils.getTrustManagers(sslFactory))
          .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
          .build();
      isSecure = !(credentials instanceof InsecureServerCredentials);
    } catch (UnrecoverableKeyException | KeyStoreException | CertificateException | IOException
        | NoSuchAlgorithmException e) {
      LOGGER.error("Failed to initialize secure server credentials for gRPC Server");
      throw new VeniceException("Unable to create credentials with SSLFactory for gRPC server", e);
    }
  }

  public void start() throws VeniceException {
    try {
      server.start();
      LOGGER.info(
          "Started gRPC server for services: {} on port: {} isSecure: {}",
          config.getServices(),
          port,
          isSecure());
    } catch (IOException exception) {
      LOGGER.error("Failed to start gRPC server for services: {} on port: {}", config.getServices(), port, exception);
      throw new VeniceException("Unable to start gRPC server", exception);
    }
  }

  public boolean isRunning() {
    return !server.isShutdown();
  }

  public boolean isTerminated() {
    return server.isTerminated();
  }

  private boolean isSecure() {
    return isSecure;
  }

  public void stop() {
    LOGGER.info(
        "Shutting down gRPC server for services: {} on port: {} isSecure: {}",
        config.getServices(),
        port,
        isSecure());
    if (server != null && !server.isShutdown()) {
      server.shutdown();
    }
  }

  public Server getServer() {
    return server;
  }
}
