package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class RetriableStoreClientTest {
  private StatTrackingStoreClient<String, Object> mockStoreClient;

  @BeforeTest
  public void setUp() {
    mockStoreClient = mock(StatTrackingStoreClient.class);

    String storeName = Utils.getUniqueString("store");
    doReturn(storeName).when(mockStoreClient).getStoreName();
  }

  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.complete(mockReturnObject);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;
    });

    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key", Optional.empty(), 1234).get());
  }

  @Test
  public void testGetRetry() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> mockInnerFuture1 = new CompletableFuture();
    mockInnerFuture1.complete(mockReturnObject);

    mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    mockInnerFuture1.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });

    doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).get(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key").get());
  }

  @Test
  public void testGetRetryWithBackOffAndCount() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> mockInnerFuture1 = new CompletableFuture();
    mockInnerFuture1.complete(mockReturnObject);

    mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    mockInnerFuture1.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });

    // Verify retry count and retry backoff can be configured
    doReturn(mockInnerFuture).doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).get(any());
    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName())
            .setRetryCount(3)
            .setRetryBackOffInMs(100));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key").get());
  }

  @Test
  public void testMultiGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Map<String, String> result = new HashMap<>();
    Set<String> keySet = new HashSet<>();
    String keyPrefix = "key_";
    for (int i = 0; i < 5; ++i) {
      result.put(keyPrefix + i, "value_" + i);
    }
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    mockInnerFuture.complete(result);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });

    doReturn(mockInnerFuture).when(mockStoreClient).batchGet(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(result, retriableStoreClient.batchGet(keySet).get());
  }

  @Test
  public void testMultiGetRetry() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Map<String, String> result = new HashMap<>();
    Set<String> keySet = new HashSet<>();
    String keyPrefix = "key_";
    for (int i = 0; i < 5; ++i) {
      result.put(keyPrefix + i, "value_" + i);
    }
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> mockInnerFuture1 = new CompletableFuture();
    mockInnerFuture1.complete(result);

    mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    mockInnerFuture1.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;
    });

    doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).batchGet(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(result, retriableStoreClient.batchGet(keySet).get());
  }

  @Test
  public void testGetWithException() throws InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    mockInnerFuture.completeExceptionally(
        new VeniceClientHttpException("Inner mock exception", HttpResponseStatus.BAD_REQUEST.code()));
    doReturn(mockInnerFuture).when(mockStoreClient).get(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    try {
      retriableStoreClient.get("key").get();
      Assert.fail("ExecutionException should be thrown");
    } catch (ExecutionException e) {
      System.out.println(e);
      // expected
    }
  }
}
