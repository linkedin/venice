package com.linkedin.venice.fastclient.transport;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.exceptions.VeniceException;
import io.grpc.ManagedChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GrpcTransportClientTest {
  private ManagedChannel mockChannel;

  private Map<String, String> addrToGrpcMap;
  private GrpcTransportClient client;
  private Map<String, ManagedChannel> serverGrpcServers;

  private String serverAddr = "localhost:8080";
  private String grpcPort = "8081";

  @BeforeTest
  public void setUp() {
    mockChannel = mock(ManagedChannel.class);

    addrToGrpcMap = new HashMap<>();
    addrToGrpcMap.put(serverAddr, grpcPort);

    client = new GrpcTransportClient(addrToGrpcMap, mock(R2TransportClient.class));

    serverGrpcServers = spy(client.serverGrpcChannels);
    when(serverGrpcServers.get(any())).thenReturn(mockChannel);
  }

  @Test(expectedExceptions = com.linkedin.venice.exceptions.VeniceException.class)
  public void testGet() throws Exception {
    CompletableFuture<TransportClientResponse> response =
        client.get("https://" + serverAddr + "/storage/fake-store-name_v1/1/keyalue", Collections.emptyMap());

    try {
      response.get(); // will not be able to complete gRPC exception
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException("failed to get response from server", e);
    } finally {
      client.close();
    }
  }

  @Test(expectedExceptions = java.util.concurrent.ExecutionException.class)
  public void testBatchGet() throws Exception {
    CompletableFuture<TransportClientResponse> response =
        client.post("https://" + serverAddr + "/storage/somerandomstuff_v1/1/supsup", new byte[0]);
    response.get(); // will not be able to complete gRPC exception
    client.close();
  }

  @Test(expectedExceptions = com.linkedin.venice.exceptions.VeniceException.class)
  public void testNonExistentGrpcPort() throws Exception {
    GrpcTransportClient client = new GrpcTransportClient(addrToGrpcMap, mock(R2TransportClient.class));

    client.get("https://localhost:1000/storage/fake-store-name/1", Collections.emptyMap());
    client.close();
  }

}
