package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.security.SSLFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class GrpcClientConfigTest {
  private static final Map<String, String> NETTY_SERVER_TO_GRPC_SERVER_ADDRESS =
      ImmutableMap.of("testserver:1690", "localhost:23900");

  private static final int PORT = 23900;

  @Test
  public void testBuilder() {
    Client r2Client = mock(Client.class);
    SSLFactory sslFactory = mock(SSLFactory.class);

    GrpcClientConfig config = new GrpcClientConfig.Builder().setR2Client(r2Client)
        .setNettyServerToGrpcAddress(NETTY_SERVER_TO_GRPC_SERVER_ADDRESS)
        .setSSLFactory(sslFactory)
        .setPort(PORT)
        .build();

    assertEquals(config.getR2Client(), r2Client);
    assertEquals(config.getNettyServerToGrpcAddress(), NETTY_SERVER_TO_GRPC_SERVER_ADDRESS);
    assertEquals(config.getSslFactory(), sslFactory);
    assertEquals(config.getPort(), PORT);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testMissingR2Client() {
    new GrpcClientConfig.Builder().setNettyServerToGrpcAddress(new HashMap<>())
        .setSSLFactory(mock(SSLFactory.class))
        .build();
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testMissingAddressMap() {
    new GrpcClientConfig.Builder().setR2Client(mock(Client.class)).setSSLFactory(mock(SSLFactory.class)).build();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testBuildWithNoPortOrMap() {
    new GrpcClientConfig.Builder().setR2Client(mock(Client.class))
        .setSSLFactory(mock(SSLFactory.class))
        .setNettyServerToGrpcAddress(Collections.emptyMap())
        .build();
  }

  @Test(dataProvider = "port-address-combination")
  public void testBuildSucceedsWithEitherPortOrMap(int port, Map<String, String> nettyServerToGrpcAddress) {
    GrpcClientConfig config = new GrpcClientConfig.Builder().setR2Client(mock(Client.class))
        .setSSLFactory(mock(SSLFactory.class))
        .setNettyServerToGrpcAddress(nettyServerToGrpcAddress)
        .setPort(port)
        .build();

    assertEquals(config.getPort(), port);
    assertEquals(config.getNettyServerToGrpcAddress(), nettyServerToGrpcAddress);
  }

  @DataProvider(name = "port-address-combination")
  public static Object[][] generatePortAddressForClientConfig() {
    return new Object[][] { { 0, NETTY_SERVER_TO_GRPC_SERVER_ADDRESS }, { PORT, Collections.emptyMap() },
        { PORT, NETTY_SERVER_TO_GRPC_SERVER_ADDRESS } };
  }
}
