package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.grpc.BindableService;
import io.grpc.InsecureServerCredentials;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import org.testng.annotations.Test;


public class VeniceGrpcServerConfigTest {
  @Test
  public void testDefaults() {
    VeniceGrpcServerConfig config =
        new VeniceGrpcServerConfig.Builder().setPort(8080).setService(mock(BindableService.class)).build();

    assertEquals(config.getPort(), 8080);
    assertTrue(config.getCredentials() instanceof InsecureServerCredentials);
    assertEquals(config.getInterceptors().size(), 0);
  }

  @Test
  public void testCustomCredentials() {
    VeniceGrpcServerConfig config = new VeniceGrpcServerConfig.Builder().setPort(8080)
        .setService(mock(BindableService.class))
        .setCredentials(mock(ServerCredentials.class))
        .build();

    assertNotNull(config.getCredentials());
  }

  @Test
  public void testInterceptor() {
    ServerInterceptor interceptor = mock(ServerInterceptor.class);
    VeniceGrpcServerConfig config = new VeniceGrpcServerConfig.Builder().setPort(8080)
        .setService(mock(BindableService.class))
        .setInterceptor(interceptor)
        .build();

    assertEquals(config.getInterceptors().size(), 1);
    assertEquals(config.getInterceptors().get(0), interceptor);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNoService() {
    new VeniceGrpcServerConfig.Builder().setPort(8080).build();
  }
}
