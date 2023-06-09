package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.integration.utils.MockVeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.TestMockTime;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestD2ServiceDiscovery {
  @Test
  public void testD2ServiceDiscovery() {
    String storeName = "test";
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    try (ZkServerWrapper zk = ServiceFactory.getZkServer()) {
      // Start a mock server which will serve for the d2 service.
      try (MockVeniceRouterWrapper router =
          ServiceFactory.getMockVeniceRouter(zk.getAddress(), false, new Properties())) {
        // Set up client config to use the d2 service that router serving for
        clientConfig.setD2ServiceName(router.getRouterD2Service()).setVeniceURL(zk.getAddress());
        // Find the d2 service for that store.
        try (D2TransportClient client = new D2TransportClient(router.getRouterD2Service(), clientConfig)) {
          String d2ServiceName = new D2ServiceDiscovery().find(client, storeName).getD2Service();
          Assert.assertEquals(
              d2ServiceName,
              router.getD2ServiceNameForCluster(router.getClusterName()),
              "Should find the correct d2 service associated with the given cluster.");
        }
      }
    }
  }

  @DataProvider(name = "exceptionProvider")
  public static Object[][] exceptionProvider() {
    return new Object[][] { { new TimeoutException("Fake timeout") },
        { new ExecutionException(new RuntimeException("Fake execution exception")) } };
  }

  @Test(dataProvider = "exceptionProvider", expectedExceptions = ServiceDiscoveryException.class, expectedExceptionsMessageRegExp = "Failed to find d2 service for test after 10 attempts")
  public void testRetry(Exception e) throws InterruptedException, ExecutionException, TimeoutException {
    D2TransportClient mockTransportClient = mock(D2TransportClient.class);
    CompletableFuture<TransportClientResponse> mockFuture = mock(CompletableFuture.class);
    doThrow(e).when(mockFuture).get(anyLong(), any());
    doReturn(mockFuture).when(mockTransportClient).get(anyString(), any());

    new D2ServiceDiscovery(new TestMockTime()).find(mockTransportClient, "test");
  }
}
