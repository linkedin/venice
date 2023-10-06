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
import org.testng.annotations.DataProvider;
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
  private static final Map<String, String> BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE = new HashMap<>();

  private TimeoutProcessor timeoutProcessor;
  private ClientConfig.ClientConfigBuilder clientConfigBuilder;
  private GetRequestContext getRequestContext;
  private BatchGetRequestContext batchGetRequestContext;
  private ClientConfig clientConfig;
  private RetriableAvroGenericStoreClient<String, String> retriableClient;
  private StatsAvroGenericStoreClient statsAvroGenericStoreClient;
  private Map<String, ? extends Metric> metrics;

  @DataProvider(name = "FastClient-Single-Get-MultiGet-And-Streaming-MultiGet")
  public Object[][] twoBoolean() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean batchGet = (boolean) permutation[0];
      boolean streamingBatchGet = (boolean) permutation[1];
      if (!batchGet) {
        if (streamingBatchGet) {
          return false;
        }
      }
      return true;
    },
        DataProviderUtils.BOOLEAN, // batchGet
        DataProviderUtils.BOOLEAN); // streamingBatchGet
  }

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
    BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE.put("test_key_2", "test_value_2");
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
      boolean keyNotFound,
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
              if (keyNotFound) {
                originalRequestFuture.complete(null);
              } else {
                originalRequestFuture.complete(SINGLE_GET_VALUE_RESPONSE);
              }
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
              if (keyNotFound) {
                retryRequestFuture.complete(null);
              } else {
                retryRequestFuture.complete(SINGLE_GET_VALUE_RESPONSE);
              }
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
                if (key.equals("test_key_1") && keyNotFound) {
                  callback.onRecordReceived(key, null);
                } else {
                  callback.onRecordReceived(key, BATCH_GET_VALUE_RESPONSE.get(key));
                }
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
                if (key.equals("test_key_1") && keyNotFound) {
                  callback.onRecordReceived(key, null);
                } else {
                  callback.onRecordReceived(key, BATCH_GET_VALUE_RESPONSE.get(key));
                }
              });
              callback.onCompletion(Optional.empty());
            }
          }, retryRequestDelayMs, TimeUnit.MILLISECONDS);
        } else {
          throw new VeniceClientException("Unexpected request cnt: " + requestCnt);
        }
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

  private void testSingleGetAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean errorRetry,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound) throws ExecutionException, InterruptedException {
    getRequestContext = new GetRequestContext(false);
    try {
      String value = (String) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      if (bothOriginalAndRetryFails) {
        fail("An ExecutionException should be thrown here");
      }
      if (keyNotFound) {
        assertEquals(value, null);
      } else {
        assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      if (!bothOriginalAndRetryFails) {
        throw e;
      }
    }

    validateMetrics(false, errorRetry, longTailRetry, retryWin);
  }

  private void testBatchGetAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound) throws ExecutionException, InterruptedException {
    batchGetRequestContext = new BatchGetRequestContext<>(false);
    try {
      Map<String, String> value =
          (Map<String, String>) statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();

      if (bothOriginalAndRetryFails) {
        fail("An ExecutionException should be thrown here");
      }

      if (keyNotFound) {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE);
      } else {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      if (!bothOriginalAndRetryFails) {
        throw e;
      }
    }

    validateMetrics(true, false, longTailRetry, retryWin);
  }

  private void testStreamingBatchGetAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound) throws ExecutionException, InterruptedException {
    batchGetRequestContext = new BatchGetRequestContext<>(true);
    try {
      Map<String, String> value =
          (Map<String, String>) statsAvroGenericStoreClient.streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();

      if (bothOriginalAndRetryFails) {
        fail("An ExecutionException should be thrown here");
      }

      if (keyNotFound) {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE);
      } else {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      if (!bothOriginalAndRetryFails) {
        throw e;
      }
    }

    validateMetrics(true, false, longTailRetry, retryWin);
  }

  /**
   * Note that DispatchingAvroGenericStoreClient is mocked in this test and so the counters
   * @param errorRetry request is retried because the original request results in exception. Only applicable
   *                   for single gets.
   * @param longTailRetry request is retried because the original request is taking more time
   * @param retryWin retry request wins
   */
  private void validateMetrics(boolean batchGet, boolean errorRetry, boolean longTailRetry, boolean retryWin) {
    metrics = getStats(clientConfig);
    String metricsPrefix = "." + STORE_NAME + (batchGet ? "--multiget_streaming_" : "--");
    double expectedKeyCount = batchGet ? 2.0 : 1.0;

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertTrue(metrics.get(metricsPrefix + "request.OccurrenceRate").value() > 0);
    });
    assertEquals(metrics.get(metricsPrefix + "request_key_count.Max").value(), expectedKeyCount);

    if (errorRetry || longTailRetry) {
      assertTrue(metrics.get(metricsPrefix + "retry_request_key_count.Rate").value() > 0);
      assertEquals(metrics.get(metricsPrefix + "retry_request_key_count.Max").value(), expectedKeyCount);
    } else {
      assertFalse(metrics.get(metricsPrefix + "retry_request_key_count.Rate").value() > 0);
      assertFalse(metrics.get(metricsPrefix + "retry_request_key_count.Max").value() > 0);
    }

    // errorRetry is only for single gets
    if (!batchGet) {
      if (errorRetry) {
        assertTrue(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
        assertTrue(getRequestContext.errorRetryRequestTriggered);
      } else {
        assertFalse(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
        assertFalse(getRequestContext.errorRetryRequestTriggered);
      }
    }

    // longTailRetry is for both single and batch gets
    if (longTailRetry) {
      assertTrue(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
      if (batchGet) {
        assertTrue(batchGetRequestContext.longTailRetryTriggered);
        assertEquals(batchGetRequestContext.numberOfKeysSentInRetryRequest, (int) expectedKeyCount);
      } else {
        assertTrue(getRequestContext.longTailRetryRequestTriggered);
      }
    } else {
      assertFalse(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
      if (batchGet) {
        assertFalse(batchGetRequestContext.longTailRetryTriggered);
        assertFalse(batchGetRequestContext.numberOfKeysSentInRetryRequest > 0);
      } else {
        assertFalse(getRequestContext.longTailRetryRequestTriggered);
      }
    }

    if (retryWin) {
      assertTrue(metrics.get(metricsPrefix + "retry_request_win.OccurrenceRate").value() > 0);
      assertEquals(metrics.get(metricsPrefix + "retry_request_success_key_count.Max").value(), expectedKeyCount);
      if (batchGet) {
        assertTrue(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get() > 0);
      } else {
        assertTrue(getRequestContext.retryWin);
      }
    } else {
      assertFalse(metrics.get(metricsPrefix + "retry_request_win.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricsPrefix + "retry_request_success_key_count.Max").value() > 0);
      if (batchGet) {
        assertFalse(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get() > 0);
      } else {
        assertFalse(getRequestContext.retryWin);
      }
    }
  }

  /**
   * Original request is faster than retry threshold.
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithoutTriggeringLongTailRetry(boolean batchGet, boolean keyNotFound)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 2,
            keyNotFound,
            clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      testSingleGetAndValidateMetrics(false, false, false, false, keyNotFound);
    } else {
      testBatchGetAndValidateMetrics(false, false, false, keyNotFound);
    }
  }

  /**
   * Original request latency is higher than retry threshold, but still faster than retry request
   */
  @Test(dataProvider = "FastClient-Single-Get-MultiGet-And-Streaming-MultiGet", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndOriginalWins(boolean batchGet, boolean streamingBatchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 50,
            false,
            clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      testSingleGetAndValidateMetrics(false, false, true, false, false);
    } else {
      if (streamingBatchGet) {
        testStreamingBatchGetAndValidateMetrics(false, true, false, false);
      } else {
        testBatchGetAndValidateMetrics(false, true, false, false);
      }
    }
  }

  /**
   * Original request latency is higher than retry threshold and slower than the retry request
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndRetryWins(boolean batchGet, boolean keyNotFound)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS * 10,
            false,
            LONG_TAIL_RETRY_THRESHOLD_IN_MS / 2,
            keyNotFound,
            clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      testSingleGetAndValidateMetrics(false, false, true, true, keyNotFound);
    } else {
      testBatchGetAndValidateMetrics(false, true, true, keyNotFound);
    }
  }

  /**
   * Original request fails and retry succeeds.
   */
  @Test(dataProvider = "FastClient-Single-Get-MultiGet-And-Streaming-MultiGet", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringErrorRetryAndRetryWins(boolean batchGet, boolean streamingBatchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, false, LONG_TAIL_RETRY_THRESHOLD_IN_MS, false, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      testSingleGetAndValidateMetrics(false, true, false, true, false);
    } else {
      if (streamingBatchGet) {
        testStreamingBatchGetAndValidateMetrics(false, true, true, false);
      } else {
        testBatchGetAndValidateMetrics(false, true, true, false);
      }
    }
  }

  /**
   * Original request latency exceeds the retry threshold but succeeds and the retry fails.
   */
  @Test(dataProvider = "FastClient-Single-Get-MultiGet-And-Streaming-MultiGet", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndRetryFails(boolean batchGet, boolean streamingBatchGet)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(false, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0, false, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      testSingleGetAndValidateMetrics(false, false, true, false, false);
    } else {
      if (streamingBatchGet) {
        testStreamingBatchGetAndValidateMetrics(false, true, false, false);
      } else {
        testBatchGetAndValidateMetrics(false, true, false, false);
      }
    }
  }

  /**
   * Original request latency exceeds the retry threshold, and both the original request and the retry fails.
   */
  @Test(dataProvider = "FastClient-Single-Get-MultiGet-And-Streaming-MultiGet", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndBothFailsV1(boolean batchGet, boolean streamingBatchGet)
      throws InterruptedException, ExecutionException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 10 * LONG_TAIL_RETRY_THRESHOLD_IN_MS, true, 0, false, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true,
     *  but requestContext values should be checked.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    if (!batchGet) {
      testSingleGetAndValidateMetrics(true, false, true, false, false);
    } else {
      if (streamingBatchGet) {
        testStreamingBatchGetAndValidateMetrics(true, true, false, false);
      } else {
        testBatchGetAndValidateMetrics(true, true, false, false);
      }
    }
  }

  /**
   * Original request latency is lower than the retry threshold, and both the original request and the retry fails.
   */
  @Test(dataProvider = "FastClient-Single-Get-MultiGet-And-Streaming-MultiGet", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndBothFailsV2(boolean batchGet, boolean streamingBatchGet)
      throws InterruptedException, ExecutionException {
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, true, 0, false, clientConfig),
        clientConfig);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true,
     *  but requestContext values should be checked.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    if (!batchGet) {
      testSingleGetAndValidateMetrics(true, true, false, false, false);
    } else {
      if (streamingBatchGet) {
        testStreamingBatchGetAndValidateMetrics(true, true, false, false);
      } else {
        testBatchGetAndValidateMetrics(true, true, false, false);
      }
    }
  }
}
