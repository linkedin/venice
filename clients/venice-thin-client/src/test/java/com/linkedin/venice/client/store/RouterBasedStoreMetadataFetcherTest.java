package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.store.RouterBasedStoreMetadataFetcher.TYPE_STORES;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterBasedStoreMetadataFetcherTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private TransportClient mockTransportClientWith(MultiStoreResponse response) throws Exception {
    TransportClient mockClient = Mockito.mock(TransportClient.class);
    byte[] responseBytes = OBJECT_MAPPER.writeValueAsBytes(response);
    TransportClientResponse transportResponse = Mockito.mock(TransportClientResponse.class);
    Mockito.doReturn(responseBytes).when(transportResponse).getBody();
    CompletableFuture<TransportClientResponse> mockFuture = CompletableFuture.completedFuture(transportResponse);
    Mockito.doReturn(mockFuture).when(mockClient).get(Mockito.eq(TYPE_STORES), Mockito.anyMap());
    return mockClient;
  }

  @Test
  public void testGetAllStoreNames() throws Exception {
    MultiStoreResponse multiStoreResponse = new MultiStoreResponse();
    multiStoreResponse.setStores(new String[] { "store_a", "store_b", "store_c" });

    TransportClient mockClient = mockTransportClientWith(multiStoreResponse);
    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Set<String> storeNames = fetcher.getAllStoreNames();

    Assert.assertEquals(storeNames.size(), 3);
    Assert.assertTrue(storeNames.contains("store_a"));
    Assert.assertTrue(storeNames.contains("store_b"));
    Assert.assertTrue(storeNames.contains("store_c"));
    Mockito.verify(mockClient, Mockito.times(1)).get(Mockito.eq(TYPE_STORES), Mockito.anyMap());
  }

  @Test
  public void testGetAllStoreNamesReturnsEmptyWhenNullStores() throws Exception {
    MultiStoreResponse multiStoreResponse = new MultiStoreResponse();
    multiStoreResponse.setStores(null);

    TransportClient mockClient = mockTransportClientWith(multiStoreResponse);
    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Set<String> storeNames = fetcher.getAllStoreNames();

    Assert.assertTrue(storeNames.isEmpty());
  }

  @Test
  public void testGetAllStoreNamesThrowsOnRouterError() throws Exception {
    MultiStoreResponse errorResponse = new MultiStoreResponse();
    errorResponse.setError("Internal server error");

    TransportClient mockClient = mockTransportClientWith(errorResponse);
    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testGetAllStoreNamesThrowsOnNullResponse() throws Exception {
    TransportClient mockClient = Mockito.mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> mockFuture = CompletableFuture.completedFuture(null);
    Mockito.doReturn(mockFuture).when(mockClient).get(Mockito.eq(TYPE_STORES), Mockito.anyMap());

    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testGetAllStoreNamesThrowsOnNullResponseBody() throws Exception {
    TransportClient mockClient = Mockito.mock(TransportClient.class);
    TransportClientResponse transportResponse = Mockito.mock(TransportClientResponse.class);
    Mockito.doReturn(null).when(transportResponse).getBody();
    CompletableFuture<TransportClientResponse> mockFuture = CompletableFuture.completedFuture(transportResponse);
    Mockito.doReturn(mockFuture).when(mockClient).get(Mockito.eq(TYPE_STORES), Mockito.anyMap());

    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testGetAllStoreNamesThrowsOnTransportFailure() throws Exception {
    TransportClient mockClient = Mockito.mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new ExecutionException("transport error", new RuntimeException()));
    Mockito.doReturn(failedFuture).when(mockClient).get(Mockito.eq(TYPE_STORES), Mockito.anyMap());

    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testGetAllStoreNamesThrowsOnTimeout() throws Exception {
    TransportClient mockClient = Mockito.mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> stalledFuture = new CompletableFuture<>();
    stalledFuture.completeExceptionally(new TimeoutException("timed out"));
    Mockito.doReturn(stalledFuture).when(mockClient).get(Mockito.eq(TYPE_STORES), Mockito.anyMap());

    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testClose() throws IOException {
    TransportClient mockClient = Mockito.mock(TransportClient.class);
    RouterBasedStoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);
    fetcher.close();
    Mockito.verify(mockClient, Mockito.times(1)).close();
  }
}
