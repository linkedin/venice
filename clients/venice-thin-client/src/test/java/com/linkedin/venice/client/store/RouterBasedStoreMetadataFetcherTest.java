package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.store.RouterBasedStoreMetadataFetcher.TYPE_STORES;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterBasedStoreMetadataFetcherTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private AbstractAvroStoreClient mockClientWith(MultiStoreResponse response) throws Exception {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    CompletableFuture<byte[]> mockFuture = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(response)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw(TYPE_STORES);
    return mockClient;
  }

  @Test
  public void testGetAllStoreNames() throws Exception {
    MultiStoreResponse multiStoreResponse = new MultiStoreResponse();
    multiStoreResponse.setStores(new String[] { "store_a", "store_b", "store_c" });

    AbstractAvroStoreClient mockClient = mockClientWith(multiStoreResponse);
    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Set<String> storeNames = fetcher.getAllStoreNames();

    Assert.assertEquals(storeNames.size(), 3);
    Assert.assertTrue(storeNames.contains("store_a"));
    Assert.assertTrue(storeNames.contains("store_b"));
    Assert.assertTrue(storeNames.contains("store_c"));
    Mockito.verify(mockClient, Mockito.times(1)).getRaw(TYPE_STORES);
  }

  @Test
  public void testGetAllStoreNamesReturnsEmptyWhenNullStores() throws Exception {
    MultiStoreResponse multiStoreResponse = new MultiStoreResponse();
    multiStoreResponse.setStores(null);

    AbstractAvroStoreClient mockClient = mockClientWith(multiStoreResponse);
    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Set<String> storeNames = fetcher.getAllStoreNames();

    Assert.assertTrue(storeNames.isEmpty());
  }

  @Test
  public void testGetAllStoreNamesThrowsOnRouterError() throws Exception {
    MultiStoreResponse errorResponse = new MultiStoreResponse();
    errorResponse.setError("Internal server error");

    AbstractAvroStoreClient mockClient = mockClientWith(errorResponse);
    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testGetAllStoreNamesThrowsOnNullResponse() throws Exception {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    CompletableFuture<byte[]> mockFuture = CompletableFuture.completedFuture(null);
    Mockito.doReturn(mockFuture).when(mockClient).getRaw(TYPE_STORES);

    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }

  @Test
  public void testGetAllStoreNamesThrowsOnTransportFailure() throws Exception {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    CompletableFuture<byte[]> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new ExecutionException("transport error", new RuntimeException()));
    Mockito.doReturn(failedFuture).when(mockClient).getRaw(TYPE_STORES);

    StoreMetadataFetcher fetcher = new RouterBasedStoreMetadataFetcher(mockClient);

    Assert.assertThrows(VeniceException.class, fetcher::getAllStoreNames);
  }
}
