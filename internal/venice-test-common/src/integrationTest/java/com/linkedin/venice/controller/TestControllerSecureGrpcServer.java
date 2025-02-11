package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.grpc.ControllerGrpcConstants.GRPC_CONTROLLER_CLIENT_DETAILS;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils;
import com.linkedin.venice.controller.grpc.server.GrpcControllerClientDetails;
import com.linkedin.venice.controller.grpc.server.interceptor.ControllerGrpcSslSessionInterceptor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.grpc.VeniceGrpcServer;
import com.linkedin.venice.grpc.VeniceGrpcServerConfig;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import io.grpc.ChannelCredentials;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerSecureGrpcServer {
  private static final Logger LOGGER = LogManager.getLogger(TestControllerSecureGrpcServer.class);

  private VeniceGrpcServer grpcSecureServer;
  private int grpcSecureServerPort;
  private SSLFactory sslFactory;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() {
    sslFactory = SslUtils.getVeniceLocalSslFactory();
    grpcSecureServerPort = TestUtils.getFreePort();
    VeniceGrpcServerConfig grpcSecureServerConfig =
        new VeniceGrpcServerConfig.Builder().addService(new VeniceControllerGrpcSecureServiceTestImpl())
            .setPort(grpcSecureServerPort)
            .setNumThreads(2)
            .setSslFactory(sslFactory)
            .setInterceptor(new ControllerGrpcSslSessionInterceptor())
            .build();
    grpcSecureServer = new VeniceGrpcServer(grpcSecureServerConfig);
    grpcSecureServer.start();
  }

  @AfterClass
  public void tearDownClass() {
    if (grpcSecureServer != null) {
      grpcSecureServer.stop();
    }
  }

  public static class VeniceControllerGrpcSecureServiceTestImpl extends VeniceControllerGrpcServiceImplBase {
    @Override
    public void discoverClusterForStore(
        DiscoverClusterGrpcRequest request,
        StreamObserver<DiscoverClusterGrpcResponse> responseObserver) {
      ControllerGrpcServerUtils
          .handleRequest(VeniceControllerGrpcServiceGrpc.getDiscoverClusterForStoreMethod(), () -> {
            GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(Context.current());
            LOGGER.info("Client details: {}", clientDetails);
            // If any of the following fields are null, then that means there is some regression in the code.
            // Make sure Context.current() is called in gRPC thread to get the correct client details.
            if (clientDetails == null) {
              throw new VeniceException("Client details is null");
            }
            if (clientDetails.getClientCertificate() == null) {
              throw new VeniceUnauthorizedAccessException("Client cert is null");
            }
            LOGGER.info("Client cert: {}", clientDetails.getClientCertificate().getSubjectX500Principal());
            if (clientDetails.getClientAddress() == null) {
              throw new VeniceException("Client address is null");
            }
            LOGGER.info("Client address: {}", clientDetails.getClientAddress());
            return DiscoverClusterGrpcResponse.newBuilder()
                .setClusterName("test-cluster")
                .setStoreName(request.getStoreName())
                .build();
          }, responseObserver, null, request.getStoreName());
    }
  }

  @Test
  public void testSslCertificatePropagationByGrpcInterceptor() {
    String serverAddress = String.format("localhost:%d", grpcSecureServerPort);
    ChannelCredentials credentials = GrpcUtils.buildChannelCredentials(sslFactory);
    ManagedChannel channel = Grpc.newChannelBuilder(serverAddress, credentials).build();
    VeniceControllerGrpcServiceBlockingStub blockingStub = VeniceControllerGrpcServiceGrpc.newBlockingStub(channel);
    DiscoverClusterGrpcRequest request = DiscoverClusterGrpcRequest.newBuilder().setStoreName("test-store").build();
    DiscoverClusterGrpcResponse response = blockingStub.discoverClusterForStore(request);
    assertEquals(response.getClusterName(), "test-cluster");
    assertEquals(response.getStoreName(), "test-store");
  }
}
