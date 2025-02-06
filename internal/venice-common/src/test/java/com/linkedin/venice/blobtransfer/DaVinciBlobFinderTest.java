package com.linkedin.venice.blobtransfer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DaVinciBlobFinderTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciBlobFinderTest.class);
  private AbstractAvroStoreClient storeClient;
  private DaVinciBlobFinder daVinciBlobFinder;
  private static final String storeName = "testStore";
  private static final int version = 1;
  private static final int partition = 1;

  @BeforeMethod
  public void setUp() {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName);

    storeClient = mock(AbstractAvroStoreClient.class);
    daVinciBlobFinder = spy(new DaVinciBlobFinder(clientConfig));

    Mockito.doReturn(storeName).when(storeClient).getStoreName();
  }

  @Test
  public void testDiscoverBlobPeers_Success() {
    Mockito.doReturn(storeClient).when(daVinciBlobFinder).getStoreClient(storeName);

    String responseBodyJson =
        "{\"error\":false,\"errorMessage\":\"\",\"discoveryResult\":[\"host1\",\"host2\",\"host3\"]}";
    byte[] responseBody = responseBodyJson.getBytes(StandardCharsets.UTF_8);
    TransportClientResponse mockResponse = new TransportClientResponse(0, null, responseBody);

    CompletableFuture<byte[]> futureResponse = CompletableFuture.completedFuture(mockResponse.getBody());
    when(storeClient.getRaw(anyString())).thenReturn(futureResponse);

    BlobPeersDiscoveryResponse response = daVinciBlobFinder.discoverBlobPeers(storeName, version, partition);
    List<String> hostNames = response.getDiscoveryResult();
    Assert.assertNotNull(hostNames);
    Assert.assertEquals(3, hostNames.size());
  }

  @Test
  public void testDiscoverBlobPeers_CallsTransportClientWithCorrectURI() {
    Mockito.doReturn(storeClient).when(daVinciBlobFinder).getStoreClient(storeName);
    String responseBodyJson =
        "{\"error\":false,\"errorMessage\":\"\",\"discoveryResult\":[\"host1\",\"host2\",\"host3\"]}";
    byte[] responseBody = responseBodyJson.getBytes(StandardCharsets.UTF_8);
    TransportClientResponse mockResponse = new TransportClientResponse(0, null, responseBody);

    CompletableFuture<byte[]> futureResponse = CompletableFuture.completedFuture(mockResponse.getBody());
    when(storeClient.getRaw(anyString())).thenReturn(futureResponse);

    daVinciBlobFinder.discoverBlobPeers(storeName, version, partition);

    // Capture the argument passed to transportClient.get
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(storeClient).getRaw(argumentCaptor.capture());

    String expectedUri = String
        .format("blob_discovery?store_name=%s&store_version=%d&store_partition=%d", storeName, version, partition);
    assertEquals(expectedUri, argumentCaptor.getValue());
  }

  @Test
  public void testDiscoverBlobPeers_ContentDeserializationError() throws Exception {
    Mockito.doReturn(storeClient).when(daVinciBlobFinder).getStoreClient(storeName);

    String responseBodyJson = "{\"error\":true,\"errorMessage\":\"some error\",\"discoveryResult\":[]}";
    byte[] responseBody = responseBodyJson.getBytes(StandardCharsets.UTF_8);
    TransportClientResponse mockResponse = new TransportClientResponse(0, null, responseBody);

    CompletableFuture<byte[]> futureResponse = CompletableFuture.completedFuture(mockResponse.getBody());
    when(storeClient.getRaw(anyString())).thenReturn(futureResponse);

    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    ObjectMapper mockMapper = spy(mapper);
    doThrow(new IOException("Test Exception")).when(mockMapper)
        .readValue(responseBody, BlobPeersDiscoveryResponse.class);

    BlobPeersDiscoveryResponse response = daVinciBlobFinder.discoverBlobPeers(storeName, version, partition);
    assertEquals(0, response.getDiscoveryResult().size());
    assertEquals(response.getErrorMessage(), "some error");
    assertTrue(response.isError());
  }

  @Test
  public void testDiscoverBlobPeers_ClientWithIncorrectUri() {
    Mockito.doReturn(storeClient).when(daVinciBlobFinder).getStoreClient(storeName);

    CompletableFuture<byte[]> futureResponse = new CompletableFuture<>();
    futureResponse.completeExceptionally(new RuntimeException("Test Exception"));
    when(storeClient.getRaw(anyString())).thenReturn(futureResponse);

    BlobPeersDiscoveryResponse response = daVinciBlobFinder.discoverBlobPeers(storeName, version, partition);

    assertTrue(response.isError());
    assertEquals(
        response.getErrorMessage(),
        "Error finding DVC peers for blob transfer in store: testStore, version: 1, partition: 1");
  }

  @Test
  public void testGetStoreClient() throws IOException {
    // set up the transport client provider used to initialize the store client
    TransportClient transportClient1 = mock(TransportClient.class);
    TransportClient transportClient2 = mock(TransportClient.class);
    Function<ClientConfig, TransportClient> clientConfigTransportClientFunction = (clientConfig) -> {
      if (clientConfig.getStoreName().equals(storeName)) {
        return transportClient1;
      } else if (clientConfig.getStoreName().equals("storeName2")) {
        return transportClient2;
      } else {
        // Create TransportClient the regular way
        return null;
      }
    };
    ClientFactory.setUnitTestMode();
    ClientFactory.setTransportClientProvider(clientConfigTransportClientFunction);

    // ClientConfig is initialized with storeName
    AbstractAvroStoreClient storeClient = daVinciBlobFinder.getStoreClient(storeName);
    Assert.assertNotNull(storeClient);
    Assert.assertEquals(storeClient.getStoreName(), storeName);

    // Even if the daVinciBlobFinder is initialized at the beginning with "storeName", the getStoreClient
    // method should be able to return a store client for "storeName2"
    AbstractAvroStoreClient storeClient2 = daVinciBlobFinder.getStoreClient("storeName2");
    Assert.assertNotNull(storeClient2);
    Assert.assertEquals(storeClient2.getStoreName(), "storeName2");

    daVinciBlobFinder.close();
    // verify that the store client is closed
    verify(transportClient1).close();
    verify(transportClient2).close();
  }
}
