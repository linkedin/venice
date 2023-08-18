package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.security.SSLFactory;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class GrpcClientConfigTest {
  @Test
  public void testBuilder() {
    Client r2Client = mock(Client.class);
    Map<String, String> addressMap = new HashMap<>();
    addressMap.put("server1", "localhost:5000");

    SSLFactory sslFactory = mock(SSLFactory.class);

    GrpcClientConfig config = new GrpcClientConfig.Builder().setR2Client(r2Client)
        .setNettyServerToGrpcAddressMap(addressMap)
        .setSSLFactory(sslFactory)
        .build();

    assertEquals(config.getR2Client(), r2Client);
    assertEquals(config.getNettyServerToGrpcAddressMap(), addressMap);
    assertEquals(config.getSslFactory(), sslFactory);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMissingR2Client() {
    new GrpcClientConfig.Builder().setNettyServerToGrpcAddressMap(new HashMap<>())
        .setSSLFactory(mock(SSLFactory.class))
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMissingAddressMap() {
    new GrpcClientConfig.Builder().setR2Client(mock(Client.class)).setSSLFactory(mock(SSLFactory.class)).build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyAddressMap() {
    new GrpcClientConfig.Builder().setR2Client(mock(Client.class))
        .setNettyServerToGrpcAddressMap(new HashMap<>())
        .setSSLFactory(mock(SSLFactory.class))
        .build();
  }

}
