package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.utils.TestUtils;
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

import static com.linkedin.venice.client.store.InternalAvroStoreClient.*;
import static org.mockito.Mockito.*;

public class RetriableStoreClientTest {
  private InternalAvroStoreClient<String, Object> mockStoreClient;

  @BeforeTest
  public void setUp() {
    mockStoreClient = mock(InternalAvroStoreClient.class);

    String storeName = TestUtils.getUniqueString("store");
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

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key", Optional.empty(), 1234).get());
  }

  @Test
  public void testGetRetry() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object>  mockInnerFuture1 = new CompletableFuture();
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

    doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).get(any(), any(), anyLong());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
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

    doReturn(mockInnerFuture).when(mockStoreClient).batchGet(any(), any(), anyLong());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(mockStoreClient,
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
    CompletableFuture<Object>  mockInnerFuture1 = new CompletableFuture();
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

    doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).batchGet(any(), any(), anyLong());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(result, retriableStoreClient.batchGet(keySet).get());
  }

  @Test
  public void testGetWithException() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException("Inner mock exception", HttpResponseStatus.BAD_REQUEST.code()));
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
        handleStoreExceptionInternally(throwable);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });
    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(mockStoreClient,
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
