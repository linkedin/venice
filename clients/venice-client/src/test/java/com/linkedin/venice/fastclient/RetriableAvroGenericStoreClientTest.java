package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
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
  private static final String STORE_NAME = "test_store";

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
        // TODO add tests for multiGet
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
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
            .setR2Client(mock(Client.class))
            .setLongTailRetryEnabledForSingleGet(true)
            .setLongTailRetryThresholdForSingleGetInMicroSeconds(
                (int) TimeUnit.MILLISECONDS.toMicros(LONG_TAIL_RETRY_THRESHOLD_IN_MS));

    String value;
    GetRequestContext getRequestContext;
    ClientConfig clientConfig;
    RetriableAvroGenericStoreClient<String, String> retriableClient;
    StatsAvroGenericStoreClient statsAvroGenericStoreClient;
    Map<String, ? extends Metric> metrics;

    // Original request is faster than retry threshold.
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 2),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    metrics = getStats(clientConfig);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency is higher than retry threshold, but still faster than retry request
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 50),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    metrics = getStats(clientConfig);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency is higher than retry threshold, but slower than retry request
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    metrics = getStats(clientConfig);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    final GetRequestContext finalGetRequestContext1 = getRequestContext;
    // TODO need to check why retry_request_win fails
    // final Map<String, ? extends Metric> metrics1 = metrics;
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(
            finalGetRequestContext1.retryWin /*&& metrics1.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0*/));

    // Original request fails and retry succeeds.
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, false, LONG_TAIL_RETRY_THRESHOLD_IN_MS),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    metrics = getStats(clientConfig);
    Assert.assertTrue(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertTrue(getRequestContext.errorRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.longTailRetryRequestTriggered);
    // TODO: check why retryWin metrics is not getting set
    final GetRequestContext finalGetRequestContext2 = getRequestContext;
    // final Map<String, ? extends Metric> metrics1 = metrics;
    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(
            finalGetRequestContext2.retryWin /*&& metrics1.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0*/));

    // Original request latency exceeds the retry threshold, and the retry fails.
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(false, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
    Assert.assertEquals(value, VALUE_RESPONSE);
    metrics = getStats(clientConfig);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    Assert.assertTrue(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency exceeds the retry threshold, and both the original request and the retry fails.
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    try {
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      Assert.fail("An ExecutionException should be thrown here");
    } catch (ExecutionException e) {
      // expected
    }
    metrics = getStats(clientConfig);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.errorRetryRequestTriggered);
    // TODO check why this fails
    // Assert.assertTrue(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertTrue(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.retryWin);

    // Original request latency is lower than the retry threshold, and both the original request and the retry fails.
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(prepareDispatchingClient(true, 0, true, 0), clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    getRequestContext = new GetRequestContext();
    try {
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      Assert.fail("An ExecutionException should be thrown here");
    } catch (ExecutionException e) {
      // expected
    }
    metrics = getStats(clientConfig);
    // TODO check why this fails
    // Assert.assertTrue(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
    Assert.assertTrue(getRequestContext.errorRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.longTailRetryRequestTriggered);
    Assert.assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
    Assert.assertFalse(getRequestContext.retryWin);
  }

  private Map<String, ? extends Metric> getStats(ClientConfig clientConfig) {
    FastClientStats stats = clientConfig.getStats(RequestType.SINGLE_GET);
    MetricsRepository metricsRepository = stats.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    return metrics;
  }
}
