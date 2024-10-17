package com.linkedin.venice.grpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import io.grpc.InsecureServerCredentials;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceGrpcServerTest {
  private static final int NUM_THREADS = 2;
  private VeniceGrpcServer grpcServer;
  private VeniceGrpcServerConfig.Builder serverConfig;

  @BeforeMethod
  void setUp() {
    serverConfig = new VeniceGrpcServerConfig.Builder().setPort(TestUtils.getFreePort())
        .setService(new VeniceControllerGrpcServiceTestImpl())
        .setNumThreads(NUM_THREADS);
  }

  @Test
  void startServerSuccessfully() {
    grpcServer = new VeniceGrpcServer(serverConfig.build());

    grpcServer.start();
    assertNotNull(grpcServer.getServer());
    assertTrue(grpcServer.isRunning());
    assertFalse(grpcServer.isTerminated());

    grpcServer.stop();
    assertFalse(grpcServer.isRunning());
  }

  @Test
  void startServerThrowVeniceException() {
    VeniceGrpcServer firstServer = new VeniceGrpcServer(serverConfig.build());
    firstServer.start();
    grpcServer = new VeniceGrpcServer(serverConfig.build());
    try {
      grpcServer.start();
    } catch (Exception e) {
      assertEquals(e.getClass(), VeniceException.class);
      assertFalse(grpcServer.isTerminated());
    }

    firstServer.stop();
  }

  @Test
  void testServerShutdown() throws InterruptedException {
    grpcServer = new VeniceGrpcServer(serverConfig.build());
    grpcServer.start();

    Thread.sleep(500);

    grpcServer.stop();
    assertFalse(grpcServer.isRunning());

    Thread.sleep(500);

    assertTrue(grpcServer.isTerminated());
  }

  @Test
  void testServerWithSSL() {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();

    serverConfig.setSslFactory(sslFactory);
    grpcServer = new VeniceGrpcServer(serverConfig.build());
    ServerCredentials serverCredentials = grpcServer.credentials;

    assertTrue(serverCredentials instanceof TlsServerCredentials);

    serverConfig.setSslFactory(null);
    grpcServer = new VeniceGrpcServer(serverConfig.build());
    serverCredentials = grpcServer.credentials;

    assertFalse(serverCredentials instanceof TlsServerCredentials);
    assertTrue(serverCredentials instanceof InsecureServerCredentials);
  }

  public static class VeniceControllerGrpcServiceTestImpl
      extends VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase {
  }
}
