package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.*;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.utils.TestUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RetriableAvroGenericStoreClientTest {
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private static final int LONG_TAIL_RETRY_THRESHOLD_IN_MS = 100; // 100ms
  private static final String VALUE_RESPONSE = "test_value";

  private TimeoutProcessor timeoutProcessor;

  @BeforeClass
  public void setUp() {
    timeoutProcessor = new TimeoutProcessor(null, true, 1);
  }

  @AfterClass
  public void tearDown() throws InterruptedException {
    timeoutProcessor.shutdownNow();
    timeoutProcessor.awaitTermination(10, TimeUnit.SECONDS);
    TestUtils.shutdownExecutor(scheduledExecutor);
  }

  private InternalAvroStoreClient prepareDispatchingClient(
      boolean originalRequestThrowException,
      long originalRequestDelayMs,
      boolean retryRequestThrowException,
      long retryRequestDelayMs) {
    return new InternalAvroStoreClient() {
      private int requestCnt = 0;

      @Override
      public void start() throws VeniceClientException {
      }

      @Override
      public void close() {
      }

      @Override
      public String getStoreName() {
        return null;
      }

      @Override
      public Schema getKeySchema() {
        return null;
      }

      @Override
      public Schema getLatestValueSchema() {
        return null;
      }

      @Override
      protected CompletableFuture get(GetRequestContext requestContext, Object key) throws VeniceClientException {
        InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
        doReturn(timeoutProcessor).when(instanceHealthMonitor).getTimeoutProcessor();
        requestContext.instanceHealthMonitor = instanceHealthMonitor;

        ++requestCnt;
        if (requestCnt == 1) {
          // Mock the original request
          final CompletableFuture originalRequestFuture = new CompletableFuture();
          scheduledExecutor.schedule(() -> {
            if (originalRequestThrowException) {
              originalRequestFuture.completeExceptionally(new VeniceClientException("Original request exception"));
            } else {
              originalRequestFuture.complete(VALUE_RESPONSE);
            }
          }, originalRequestDelayMs, TimeUnit.MILLISECONDS);
          return originalRequestFuture;
        } else if (requestCnt == 2) {
          // Mock the retry request
          final CompletableFuture retryRequestFuture = new CompletableFuture();
          scheduledExecutor.schedule(() -> {
            if (retryRequestThrowException) {
              retryRequestFuture.completeExceptionally(new VeniceClientException("Retry request exception"));
            } else {
              retryRequestFuture.complete(VALUE_RESPONSE);
            }
          }, retryRequestDelayMs, TimeUnit.MILLISECONDS);
          return retryRequestFuture;
        } else {
          throw new VeniceClientException("Unexpected request cnt: " + requestCnt);
        }
      }

      @Override
      protected CompletableFuture<Map> batchGet(BatchGetRequestContext requestContext, Set keys)
          throws VeniceClientException {
        return null;
      }

      @Override
      protected void streamingBatchGet(BatchGetRequestContext requestContext, Set keys, StreamingCallback callback) {
      }

      @Override
      protected CompletableFuture<VeniceResponseMap> streamingBatchGet(
          BatchGetRequestContext requestContext,
          Set keys) {
        return null;
      }
    };
  }

  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    ClientConfig clientConfig = mock(ClientConfig.class);
    doReturn(true).when(clientConfig).isLongTailRetryEnabledForSingleGet();
    doReturn((int) TimeUnit.MILLISECONDS.toMicros(LONG_TAIL_RETRY_THRESHOLD_IN_MS)).when(clientConfig)
        .getLongTailRetryThresholdForSingletGetInMicroSeconds();
    String value;
    GetRequestContext getRequestContext;

    // Original request is faster than retry threshold.
    RetriableAvroGenericStoreClient<String, String> retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 2),
        clientConfig);
    getRequestContext = new GetRequestContext();
    value = retriableClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency is higher than retry threshold, but still faster than retry request
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 50),
        clientConfig);
    getRequestContext = new GetRequestContext();
    value = retriableClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency is higher than retry threshold, but slower than retry request
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2),
        clientConfig);
    getRequestContext = new GetRequestContext();
    value = retriableClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    final GetRequestContext finalGetRequestContext1 = getRequestContext;
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(finalGetRequestContext1.retryWin));

    // Original request fails and retry succeeds.
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, false, LONG_TAIL_RETRY_THRESHOLD_IN_MS),
        clientConfig);
    getRequestContext = new GetRequestContext();
    value = retriableClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    Assert.assertTrue(getRequestContext.errorRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.longTailRetryRequestTriggered);
    final GetRequestContext finalGetRequestContext2 = getRequestContext;
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(finalGetRequestContext2.retryWin));

    // Original request latency exceeds the retry threshold, and the retry fails.
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(false, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0),
        clientConfig);
    getRequestContext = new GetRequestContext();
    value = retriableClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency exceeds the retry threshold, and both the original request and the retry fails.
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0),
        clientConfig);
    getRequestContext = new GetRequestContext();
    try {
      retriableClient.get(getRequestContext, "test_key").get();
      Assert.fail("An ExecutionException should be thrown here");
    } catch (ExecutionException e) {
      // expected
    }
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency is lower than the retry threshold, and both the original request and the retry fails.
    retriableClient = new RetriableAvroGenericStoreClient<>(prepareDispatchingClient(true, 0, true, 0), clientConfig);
    getRequestContext = new GetRequestContext();
    try {
      retriableClient.get(getRequestContext, "test_key").get();
      Assert.fail("An ExecutionException should be thrown here");
    } catch (ExecutionException e) {
      // expected
    }
    Assert.assertTrue(getRequestContext.errorRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(getRequestContext.retryWin);
  }
}
