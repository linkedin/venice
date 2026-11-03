package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
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
  public void testThrowExceptionOnSameExecutors() {
    ClientConfig cc = ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName());
    ExecutorService testExecutor = Executors.newFixedThreadPool(1);
    cc.setRetryExecutor(testExecutor);
    cc.setDeserializationExecutor(testExecutor);
    try {
      assertThrows(VeniceClientException.class, () -> new RetriableStoreClient<>(mockStoreClient, cc));
    } finally {
      testExecutor.shutdownNow();
    }
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

  @Test
  public void testGetRawDelegatesToInnerClient() {
    String requestPath = "key_schema/" + mockStoreClient.getStoreName();
    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    retriableStoreClient.getRaw(requestPath);
    verify(mockStoreClient).getRaw(requestPath);
  }

  private RetriableStoreClient<String, Object> buildRealChain(
      InternalAvroStoreClient<String, Object> inner,
      MetricsRepository metricsRepository) {
    String storeName = "realChainStore";
    doReturn(storeName).when(inner).getStoreName();
    ClientConfig config = ClientConfig.defaultGenericClientConfig(storeName)
        .setMetricsRepository(metricsRepository)
        .setRetryOnAllErrors(true)
        .setRetryCount(1);
    return new RetriableStoreClient<>(new StatTrackingStoreClient<>(inner, config), config);
  }

  private double metricValue(MetricsRepository repo, String suffix) {
    String metricName = ".realChainStore" + suffix;
    Assert.assertNotNull(repo.metrics().get(metricName), "Missing metric: " + metricName);
    return repo.metrics().get(metricName).value();
  }

  private void assertNoAppTimeoutMetrics(MetricsRepository repo) {
    Assert.assertTrue(Double.isNaN(metricValue(repo, "--client_future_timeout.Avg")));
    Assert.assertEquals(metricValue(repo, "--app_timed_out_request.OccurrenceRate"), 0d);
  }

  @Test
  public void testRealStatChainRecordsSingleGetOnSuccess() throws Exception {
    InternalAvroStoreClient<String, Object> inner = mock(InternalAvroStoreClient.class);
    Object value = new Object();
    doReturn(CompletableFuture.completedFuture(value)).when(inner).get(any(), any(), anyLong());
    MetricsRepository repo = new MetricsRepository();
    RetriableStoreClient<String, Object> client = buildRealChain(inner, repo);

    Assert.assertSame(client.get("key").get(), value);
    Assert.assertTrue(metricValue(repo, "--healthy_request.OccurrenceRate") > 0d);
    assertNoAppTimeoutMetrics(repo);
  }

  @DataProvider(name = "singleGetValues")
  public Object[][] singleGetValues() {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put("first", "value");
    mapValue.put("second", "value");
    return new Object[][] { { new Object(), 1 }, { null, 0 }, { Collections.emptyMap(), 1 }, { mapValue, 1 } };
  }

  @Test(dataProvider = "singleGetValues")
  public void testRealStatChainRecordsSingleGetOnRetryWithoutCallerTimeout(Object value, int expectedKeyCount)
      throws Exception {
    InternalAvroStoreClient<String, Object> inner = mock(InternalAvroStoreClient.class);
    CompletableFuture<Object> failed = new CompletableFuture<>();
    failed.completeExceptionally(new VeniceClientHttpException(503));
    doReturn(failed).doReturn(CompletableFuture.completedFuture(value)).when(inner).get(any(), any(), anyLong());
    MetricsRepository repo = new MetricsRepository();
    RetriableStoreClient<String, Object> client = buildRealChain(inner, repo);

    Assert.assertSame(client.get("key").get(), value);
    Assert.assertTrue(metricValue(repo, "--healthy_request.OccurrenceRate") > 0d);
    Assert.assertTrue(metricValue(repo, "--unhealthy_request.OccurrenceRate") > 0d);
    Assert.assertEquals(metricValue(repo, "--success_request_key_count.Avg"), (double) expectedKeyCount);
    assertNoAppTimeoutMetrics(repo);
  }

  @DataProvider(name = "readFailures")
  public Object[][] readFailures() {
    return DataProviderUtils.allPermutationGenerator(
        DataProviderUtils.BOOLEAN,
        DataProviderUtils.BOOLEAN,
        new Object[] { new VeniceClientRateExceededException(), new VeniceSerializationException("Invalid test value"),
            new CompletionException(new VeniceSerializationException("Invalid test value")) });
  }

  @Test(dataProvider = "readFailures")
  public void testReadExceptionsPreservedThroughDecorators(
      boolean statTrackingEnabled,
      boolean retryEnabled,
      Throwable exception) throws Exception {
    InternalAvroStoreClient<String, Object> inner = mock(InternalAvroStoreClient.class);
    doReturn("realChainStore").when(inner).getStoreName();
    CompletableFuture<Object> getFuture = new CompletableFuture<>();
    getFuture.completeExceptionally(exception);
    CompletableFuture<byte[]> rawFuture = new CompletableFuture<>();
    rawFuture.completeExceptionally(exception);
    doReturn(getFuture).when(inner).get(any(), any(), anyLong());
    doReturn(rawFuture).when(inner).getRaw(any(), any(), anyLong());
    MetricsRepository repo = new MetricsRepository();
    ClientConfig config = ClientConfig.defaultGenericClientConfig("realChainStore")
        .setMetricsRepository(repo)
        .setStatTrackingEnabled(statTrackingEnabled)
        .setRetryOnRouterError(retryEnabled);
    InternalAvroStoreClient<String, Object> trackingClient =
        statTrackingEnabled ? new StatTrackingStoreClient<>(inner, config) : new LoggingTrackingStoreClient<>(inner);
    InternalAvroStoreClient<String, Object> client =
        retryEnabled ? new RetriableStoreClient<>(trackingClient, config) : trackingClient;

    CompletableFuture<?>[] results = { client.get("key"), client.getRaw("key_schema/realChainStore") };
    for (CompletableFuture<?> result: results) {
      Throwable actualException = result.handle((value, throwable) -> throwable).get();
      if (exception instanceof VeniceClientException || exception instanceof CompletionException) {
        Assert.assertSame(actualException, exception);
      } else {
        Assert.assertTrue(actualException instanceof VeniceClientException);
        Assert.assertSame(actualException.getCause(), exception);
      }
    }
    verify(inner).get(any(), any(), anyLong());
    verify(inner).getRaw(any(), any(), anyLong());
    if (statTrackingEnabled) {
      Assert.assertTrue(metricValue(repo, "--unhealthy_request.OccurrenceRate") > 0d);
      Assert.assertTrue(metricValue(repo, "_schema_reader--unhealthy_request.OccurrenceRate") > 0d);
    }
  }

  @Test(timeOut = 30000)
  public void testRealStatChainRetryInternalWaitTimeoutNotCountedAsAppTimeout() throws Exception {
    InternalAvroStoreClient<String, Object> inner = mock(InternalAvroStoreClient.class);
    CompletableFuture<Object> failed = new CompletableFuture<>();
    failed.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> neverCompletes = new CompletableFuture<>();
    doReturn(failed).doReturn(neverCompletes).when(inner).get(any(), any(), anyLong());
    MetricsRepository repo = new MetricsRepository();
    RetriableStoreClient<String, Object> client = buildRealChain(inner, repo);

    CompletableFuture<Object> result = client.get("key");
    try {
      result.get(20, TimeUnit.SECONDS);
      Assert.fail("The internal retry wait should time out");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof TimeoutException, "Expected the internal retry timeout");
    } finally {
      neverCompletes.cancel(false);
    }
    Assert.assertTrue(metricValue(repo, "--request_retry_count.OccurrenceRate") > 0d);
    assertNoAppTimeoutMetrics(repo);
  }

  @Test
  public void testRealStatChainRecordsGetRaw() throws Exception {
    InternalAvroStoreClient<String, Object> inner = mock(InternalAvroStoreClient.class);
    byte[] bytes = new byte[] { 1 };
    doReturn(CompletableFuture.completedFuture(bytes)).when(inner).getRaw(any(), any(), anyLong());
    MetricsRepository repo = new MetricsRepository();
    RetriableStoreClient<String, Object> client = buildRealChain(inner, repo);

    Assert.assertSame(client.getRaw("key_schema/realChainStore").get(), bytes);
    Assert.assertTrue(metricValue(repo, "_schema_reader--healthy_request.OccurrenceRate") > 0d);
  }
}
