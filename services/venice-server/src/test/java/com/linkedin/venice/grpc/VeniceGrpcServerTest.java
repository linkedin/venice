package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.HttpChannelInitializer;
import com.linkedin.venice.listener.grpc.VeniceReadServiceImpl;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceGrpcServerTest {
  private VeniceGrpcServer grpcServer;
  private VeniceGrpcServerConfig.Builder serverConfig;
  private HttpChannelInitializer initializer;

  @BeforeMethod
  void setUp() {
    initializer = mock(HttpChannelInitializer.class);
    serverConfig = new VeniceGrpcServerConfig.Builder().setPort(TestUtils.getFreePort())
        .setService(new VeniceReadServiceImpl(initializer));
  }

  @Test
  void startServerSuccessfully() {
    grpcServer = new VeniceGrpcServer(serverConfig.build());

    grpcServer.start();
    assertFalse(grpcServer.isTerminated());

    grpcServer.stop();
    assertTrue(grpcServer.isShutdown());
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
    assertTrue(grpcServer.isShutdown());

    Thread.sleep(500);

    assertTrue(grpcServer.isTerminated());
  }

  @Test
  void testServerWithSSL() {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();

    serverConfig.setSslFactory(sslFactory);
    grpcServer = new VeniceGrpcServer(serverConfig.build());
    grpcServer.stop();
  }
}
