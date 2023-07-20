package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This class add tests for {@link RetriableAvroGenericStoreClient#get} and
 * {@link RetriableAvroGenericStoreClient#batchGet}
 */

public class RetriableAvroGenericStoreClientTest {
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_SECOND;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private static final int LONG_TAIL_RETRY_THRESHOLD_IN_MS = 100; // 100ms
  private static final String SINGLE_GET_VALUE_RESPONSE = "test_value";
  private static final String STORE_NAME = "test_store";
  private static final Set<String> BATCH_GET_KEYS = new HashSet<>();
  private static final Map<String, String> BATCH_GET_VALUE_RESPONSE = new HashMap<>();

  private TimeoutProcessor timeoutProcessor;
  private ClientConfig.ClientConfigBuilder clientConfigBuilder;
  private GetRequestContext getRequestContext;
  private BatchGetRequestContext batchGetRequestContext;
  private ClientConfig clientConfig;
  private RetriableAvroGenericStoreClient<String, String> retriableClient;
  private StatsAvroGenericStoreClient statsAvroGenericStoreClient;
  private Map<String, ? extends Metric> metrics;

  @BeforeClass
  public void setUp() {
    timeoutProcessor = new TimeoutProcessor(null, true, 1);
    clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(
            (int) TimeUnit.MILLISECONDS.toMicros(LONG_TAIL_RETRY_THRESHOLD_IN_MS))
        .setLongTailRetryEnabledForBatchGet(true)
        .setLongTailRetryThresholdForBatchGetInMicroSeconds(
            (int) TimeUnit.MILLISECONDS.toMicros(LONG_TAIL_RETRY_THRESHOLD_IN_MS))
        .setUseStreamingBatchGetAsDefault(true);
    BATCH_GET_KEYS.add("test_key_1");
    BATCH_GET_KEYS.add("test_key_2");
    BATCH_GET_VALUE_RESPONSE.put("test_key_1", "test_value_1");
    BATCH_GET_VALUE_RESPONSE.put("test_key_2", "test_value_2");
  }

  @AfterClass
  public void tearDown() throws InterruptedException {
    timeoutProcessor.shutdownNow();
    timeoutProcessor.awaitTermination(10, TimeUnit.SECONDS);
    TestUtils.shutdownExecutor(scheduledExecutor);
  }

  /**
   * Mocking the dispatchingClient
   */
  private InternalAvroStoreClient prepareDispatchingClient(
      boolean originalRequestThrowException,
      long originalRequestDelayMs,
      boolean retryRequestThrowException,
      long retryRequestDelayMs,
      ClientConfig clientConfig) {
    return new DispatchingAvroGenericStoreClient(null, clientConfig) {
      private int requestCnt = 0;

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
              originalRequestFuture.complete(SINGLE_GET_VALUE_RESPONSE);
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
              retryRequestFuture.complete(SINGLE_GET_VALUE_RESPONSE);
            }
          }, retryRequestDelayMs, TimeUnit.MILLISECONDS);
          return retryRequestFuture;
        } else {
          throw new VeniceClientException("Unexpected request cnt: " + requestCnt);
        }
      }

      @Override
      protected void streamingBatchGet(BatchGetRequestContext requestContext, Set keys, StreamingCallback callback) {
        InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
        doReturn(timeoutProcessor).when(instanceHealthMonitor).getTimeoutProcessor();
        requestContext.instanceHealthMonitor = instanceHealthMonitor;

        ++requestCnt;
        if (requestCnt == 1) {
          // Mock the original request
          scheduledExecutor.schedule(() -> {
            if (originalRequestThrowException) {
              callback.onCompletion(Optional.of(new VeniceClientException("Original request exception")));
            } else {
              BATCH_GET_KEYS.forEach(key -> {
                callback.onRecordReceived(key, BATCH_GET_VALUE_RESPONSE.get(key));
              });
              callback.onCompletion(Optional.empty());
            }
          }, originalRequestDelayMs, TimeUnit.MILLISECONDS);
        } else if (requestCnt == 2) {
          // Mock the retry request
          scheduledExecutor.schedule(() -> {
            if (retryRequestThrowException) {
              callback.onCompletion(Optional.of(new VeniceClientException("Retry request exception")));
            } else {
              BATCH_GET_KEYS.forEach(key -> {
                callback.onRecordReceived(key, BATCH_GET_VALUE_RESPONSE.get(key));
              });
              callback.onCompletion(Optional.empty());
            }
          }, retryRequestDelayMs, TimeUnit.MILLISECONDS);
        } else {
          throw new VeniceClientException("Unexpected request cnt: " + requestCnt);
        }
      }

      @Override
      protected CompletableFuture<VeniceResponseMap> streamingBatchGet(
          BatchGetRequestContext requestContext,
          Set keys) {
        throw new VeniceClientException("Implementation not added");
      }
    };
  }

  private Map<String, ? extends Metric> getStats(ClientConfig clientConfig) {
    return getStats(clientConfig, RequestType.SINGLE_GET);
  }

  private Map<String, ? extends Metric> getStats(ClientConfig clientConfig, RequestType requestType) {
    FastClientStats stats = clientConfig.getStats(requestType);
    MetricsRepository metricsRepository = stats.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    return metrics;
  }

  /**
   * Original request is faster than retry threshold.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithoutTriggeringLongTailRetry(boolean batchGet) throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 2,
            clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      validateSingleGetMetrics(false, false, false, false);
    } else {
      validateBatchGetMetrics(false, false, false);
    }
  }

  /**
   * Original request latency is higher than retry threshold, but still faster than retry request
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndOriginalWins(boolean batchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 50,
            clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      validateSingleGetMetrics(false, false, true, false);
    } else {
      validateBatchGetMetrics(false, true, false);
    }
  }

  /**
   * Original request latency is higher than retry threshold and slower than the retry request
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndRetryWins(boolean batchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2,
            clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      validateSingleGetMetrics(false, false, true, true);
    } else {
      validateBatchGetMetrics(false, true, true);
    }
  }

  /**
   * Original request fails and retry succeeds.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringErrorRetryAndRetryWins(boolean batchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, false, LONG_TAIL_RETRY_THRESHOLD_IN_MS, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      validateSingleGetMetrics(false, true, false, true);
    } else {
      validateBatchGetMetrics(false, true, true);
    }
  }

  /**
   * Original request latency exceeds the retry threshold but succeeds and the retry fails.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndRetryFails(boolean batchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(false, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      validateSingleGetMetrics(false, false, true, false);
    } else {
      validateBatchGetMetrics(false, true, false);
    }
  }

  /**
   * Original request latency exceeds the retry threshold, and both the original request and the retry fails.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndBothFailsV1(boolean batchGet)
      throws InterruptedException, ExecutionException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true,
     *  but requestContext values should be checked.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    if (!batchGet) {
      validateSingleGetMetrics(true, false, true, false);
    } else {
      validateBatchGetMetrics(true, true, false);
    }
  }

  /**
   * Original request latency is lower than the retry threshold, and both the original request and the retry fails.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndBothFailsV2(boolean batchGet)
      throws InterruptedException, ExecutionException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient =
        new RetriableAvroGenericStoreClient<>(prepareDispatchingClient(true, 0, true, 0, clientConfig), clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true,
     *  but requestContext values should be checked.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    if (!batchGet) {
      validateSingleGetMetrics(true, true, false, false);
    } else {
      validateBatchGetMetrics(true, true, false);
    }
  }

  /**
   * @param bothOriginalAndRetryFails In this case, get and batchGet both will throw veniceClientException
   *                                  and most metrics are not incremented, but requestContext values should
   *                                  be checked. Check {@link StatsAvroGenericStoreClient#recordRequestMetrics}
   *                                  for more details.
   * @param errorRetry request is retried because the original request results in exception
   * @param longTailRetry request is retried because the original request is taking more time
   * @param retryWin retry request wins
   */
  private void validateSingleGetMetrics(
      boolean bothOriginalAndRetryFails,
      boolean errorRetry,
      boolean longTailRetry,
      boolean retryWin) throws ExecutionException, InterruptedException {
    getRequestContext = new GetRequestContext();
    try {
      String value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      if (bothOriginalAndRetryFails) {
        fail("An ExecutionException should be thrown here");
      }
      assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
    } catch (ExecutionException e) {
      if (!bothOriginalAndRetryFails) {
        throw e;
      }
    }

    metrics = getStats(clientConfig);
    assertEquals(metrics.get("." + STORE_NAME + "--request_key_count.Max").value(), 1.0);

    if (errorRetry || longTailRetry) {
      if (!bothOriginalAndRetryFails) {
        assertEquals(metrics.get("." + STORE_NAME + "--retry_request_key_count.Max").value(), 1.0);
      } else {
        assertFalse(metrics.get("." + STORE_NAME + "--retry_request_key_count.Max").value() > 0);
      }
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--retry_request_key_count.Max").value() > 0);
    }
    if (errorRetry) {
      if (!bothOriginalAndRetryFails) {
        assertTrue(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
      } else {
        assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
      }
      assertTrue(getRequestContext.errorRetryRequestTriggered);
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.errorRetryRequestTriggered);
    }

    if (longTailRetry) {
      if (!bothOriginalAndRetryFails) {
        assertTrue(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
      } else {
        assertFalse(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
      }
      assertTrue(getRequestContext.longTailRetryRequestTriggered);
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.longTailRetryRequestTriggered);
    }

    if (retryWin) {
      assertTrue(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
      assertEquals(metrics.get("." + STORE_NAME + "--retry_request_success_key_count.Max").value(), 1.0);
      assertTrue(getRequestContext.retryWin);
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--retry_request_success_key_count.Max").value() > 0);
      assertFalse(getRequestContext.retryWin);
    }
  }

  /**
   * @param bothOriginalAndRetryFails In this case, get and batchGet both will throw veniceClientException
   *                                  and most metrics are not incremented, but requestContext values should
   *                                  be checked. Check {@link StatsAvroGenericStoreClient#recordRequestMetrics}
   *                                  for more details.
   * @param longTailRetry request is retried because the original request is taking more time
   * @param retryWin retry request wins
   */
  private void validateBatchGetMetrics(boolean bothOriginalAndRetryFails, boolean longTailRetry, boolean retryWin)
      throws ExecutionException, InterruptedException {
    batchGetRequestContext = new BatchGetRequestContext<>();
    try {
      Map<String, String> value =
          (Map<String, String>) statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();

      if (bothOriginalAndRetryFails) {
        fail("An ExecutionException should be thrown here");
      }
      assertEquals(value, BATCH_GET_VALUE_RESPONSE);
    } catch (ExecutionException e) {
      if (!bothOriginalAndRetryFails) {
        throw e;
      }
    }
    metrics = getStats(clientConfig, RequestType.MULTI_GET);

    assertEquals(metrics.get("." + STORE_NAME + "--multiget_request_key_count.Max").value(), 2.0);

    if (longTailRetry) {
      if (!bothOriginalAndRetryFails) {
        assertTrue(metrics.get("." + STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate").value() > 0);
        assertTrue(metrics.get("." + STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
        assertEquals(metrics.get("." + STORE_NAME + "--multiget_retry_request_key_count.Max").value(), 2.0);
      } else {
        assertFalse(metrics.get("." + STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate").value() > 0);
        assertFalse(metrics.get("." + STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
        assertFalse(metrics.get("." + STORE_NAME + "--multiget_retry_request_key_count.Max").value() > 0);
      }
      assertTrue(batchGetRequestContext.longTailRetryTriggered);
      assertEquals(batchGetRequestContext.numberOfKeysSentInRetryRequest, 2);
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--multiget_long_tail_retry_request.OccurrenceRate").value() > 0);
      assertFalse(batchGetRequestContext.longTailRetryTriggered);
      assertFalse(metrics.get("." + STORE_NAME + "--multiget_retry_request_key_count.Rate").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--multiget_retry_request_key_count.Max").value() > 0);
      assertFalse(batchGetRequestContext.numberOfKeysSentInRetryRequest > 0);
    }

    if (retryWin) {
      assertTrue(metrics.get("." + STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
      assertEquals(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get(), 2);
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--multiget_retry_request_success_key_count.Rate").value() > 0);
      assertFalse(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get() > 0);
    }
  }
}
