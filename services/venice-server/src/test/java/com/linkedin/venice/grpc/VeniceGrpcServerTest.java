package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.grpc.VeniceReadServiceImpl;
import com.linkedin.venice.utils.TestUtils;
import io.grpc.InsecureServerCredentials;
import java.util.Collections;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class VeniceGrpcServerTest {
  private VeniceGrpcServer grpcServer;
  private VeniceGrpcServerConfig serverConfig;

  @BeforeTest
  void setUp() {
    serverConfig = mock(VeniceGrpcServerConfig.class);
    when(serverConfig.getCredentials()).thenReturn(InsecureServerCredentials.create());
    when(serverConfig.getService()).thenReturn(mock(VeniceReadServiceImpl.class));
    when(serverConfig.getInterceptors()).thenReturn(Collections.emptyList());
  }

  @AfterTest
  void teardown() {
    grpcServer.stop();
  }

  @Test
  void startServerSuccessfully() {
    when(serverConfig.getPort()).thenReturn(TestUtils.getFreePort());

    grpcServer = new VeniceGrpcServer(serverConfig);

    grpcServer.start();
    assertFalse(grpcServer.isTerminated());
  }

  @Test
  void startServerThrowVeniceException() {
    int port = TestUtils.getFreePort();
    when(serverConfig.getPort()).thenReturn(port);
    VeniceGrpcServer firstServer = new VeniceGrpcServer(serverConfig);
    firstServer.start();
    grpcServer = new VeniceGrpcServer(serverConfig);
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
    when(serverConfig.getPort()).thenReturn(TestUtils.getFreePort());
    grpcServer = new VeniceGrpcServer(serverConfig);
    grpcServer.start();

    Thread.sleep(500);

    grpcServer.stop();
    assertTrue(grpcServer.isShutdown());

    Thread.sleep(500);

    assertTrue(grpcServer.isTerminated());
  }
}
