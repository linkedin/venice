package com.linkedin.venice.blobtransfer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DvcBlobFinderTest {
  private TransportClient transportClient;
  private DvcBlobFinder dvcBlobFinder;
  private static final String routerUrl = "http://localhost:8080";
  private static final String storeName = "testStore";
  private static final int version = 1;
  private static final int partition = 1;

  @BeforeMethod
  public void setUp() {
    transportClient = mock(TransportClient.class);
    dvcBlobFinder = new DvcBlobFinder(transportClient, routerUrl);
  }

  @Test
  public void testDiscoverBlobPeers_Success() throws Exception {
    String responseBodyJson =
        "{\"error\":false,\"errorMessage\":\"\",\"discoveryResult\":[\"host1\",\"host2\",\"host3\"]}";
    byte[] responseBody = responseBodyJson.getBytes(StandardCharsets.UTF_8);
    TransportClientResponse mockResponse = new TransportClientResponse(0, null, responseBody);

    CompletableFuture<TransportClientResponse> futureResponse = CompletableFuture.completedFuture(mockResponse);
    when(transportClient.get(anyString())).thenReturn(futureResponse);

    BlobPeersDiscoveryResponse response = dvcBlobFinder.discoverBlobPeers(storeName, version, partition);
    assertEquals(3, response.getDiscoveryResult().size());
  }

  @Test
  public void testDiscoverBlobPeers_CallsTransportClientWithCorrectURI() throws Exception {
    String responseBodyJson =
        "{\"error\":false,\"errorMessage\":\"\",\"discoveryResult\":[\"host1\",\"host2\",\"host3\"]}";
    byte[] responseBody = responseBodyJson.getBytes(StandardCharsets.UTF_8);
    TransportClientResponse mockResponse = new TransportClientResponse(0, null, responseBody);

    CompletableFuture<TransportClientResponse> futureResponse = CompletableFuture.completedFuture(mockResponse);
    when(transportClient.get(anyString())).thenReturn(futureResponse);

    dvcBlobFinder.discoverBlobPeers(storeName, version, partition);

    // Capture the argument passed to transportClient.get
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(transportClient).get(argumentCaptor.capture());

    String expectedUri = String.format(
        "%s/TYPE_BLOB_DISCOVERY?store_name=%s&store_version=%d&store_partition=%d",
        routerUrl,
        storeName,
        version,
        partition);
    assertEquals(expectedUri, argumentCaptor.getValue());
  }

  @Test
  public void testDiscoverBlobPeers_IOException() throws Exception {
    String responseBodyJson = "{\"error\":true,\"errorMessage\":\"some error\",\"discoveryResult\":[]}";
    byte[] responseBody = responseBodyJson.getBytes(StandardCharsets.UTF_8);
    TransportClientResponse mockResponse = new TransportClientResponse(0, null, responseBody);

    CompletableFuture<TransportClientResponse> futureResponse = CompletableFuture.completedFuture(mockResponse);
    when(transportClient.get(anyString())).thenReturn(futureResponse);

    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    ObjectMapper mockMapper = spy(mapper);
    doThrow(new IOException("Test Exception")).when(mockMapper)
        .readValue(responseBody, BlobPeersDiscoveryResponse.class);

    BlobPeersDiscoveryResponse response = dvcBlobFinder.discoverBlobPeers(storeName, version, partition);
    assertEquals(0, response.getDiscoveryResult().size());
    assertEquals(response.getErrorMessage(), "some error");
    assertTrue(response.isError());
  }

  @Test
  public void testDiscoverBlobPeers_Exceptionally() {
    CompletableFuture<TransportClientResponse> futureResponse = new CompletableFuture<>();
    futureResponse.completeExceptionally(new RuntimeException("Test Exception"));
    when(transportClient.get(anyString())).thenReturn(futureResponse);

    BlobPeersDiscoveryResponse response = dvcBlobFinder.discoverBlobPeers(storeName, version, partition);

    assertTrue(response.isError());
    assertEquals(
        response.getErrorMessage(),
        "Error finding DVC peers for blob transfer in store: testStore, version: 1, partition: 1");
  }
}
