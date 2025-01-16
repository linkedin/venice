package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.server.grpc.ControllerGrpcSslSessionInterceptor.GRPC_CONTROLLER_CLIENT_DETAILS;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.server.grpc.ControllerGrpcSslSessionInterceptor;
import com.linkedin.venice.controller.server.grpc.GrpcControllerClientDetails;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.grpc.VeniceGrpcServer;
import com.linkedin.venice.grpc.VeniceGrpcServerConfig;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import io.grpc.ChannelCredentials;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerSecureGrpcServer {
  private VeniceGrpcServer grpcSecureServer;
  private int grpcSecureServerPort;
  private SSLFactory sslFactory;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() {
    sslFactory = SslUtils.getVeniceLocalSslFactory();
    grpcSecureServerPort = TestUtils.getFreePort();
    VeniceGrpcServerConfig grpcSecureServerConfig =
        new VeniceGrpcServerConfig.Builder().setService(new VeniceControllerGrpcSecureServiceTestImpl())
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

  public static class VeniceControllerGrpcSecureServiceTestImpl
      extends VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase {
    @Override
    public void discoverClusterForStore(
        DiscoverClusterGrpcRequest request,
        io.grpc.stub.StreamObserver<DiscoverClusterGrpcResponse> responseObserver) {
      GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(Context.current());
      if (clientDetails.getClientCertificate() == null) {
        throw new RuntimeException("Client cert is null");
      }
      if (clientDetails.getClientAddress() == null) {
        throw new RuntimeException("Client address is null");
      }
      DiscoverClusterGrpcResponse discoverClusterGrpcResponse =
          DiscoverClusterGrpcResponse.newBuilder().setClusterName("test-cluster").build();
      responseObserver.onNext(discoverClusterGrpcResponse);
      responseObserver.onCompleted();
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
  }
}
