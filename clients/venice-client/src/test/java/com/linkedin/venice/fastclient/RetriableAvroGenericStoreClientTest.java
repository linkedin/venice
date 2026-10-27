package com.linkedin.venice.fastclient;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;
import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.schema.Utils.loadSchemaFileAsString;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * This class add tests for {@link RetriableAvroGenericStoreClient#get} and
 * {@link RetriableAvroGenericStoreClient#batchGet}
 */

public class RetriableAvroGenericStoreClientTest {
  private static final int TEST_TIMEOUT = 15 * Time.MS_PER_SECOND;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private static final long LONG_TAIL_RETRY_THRESHOLD_IN_MS = 100L;// 100ms for single get
  // Batch get uses dynamic thresholds: "1-5:15,6-20:30,21-150:50,151-500:100,501-:500"
  // For 2 keys (BATCH_GET_KEYS size), threshold is 15ms (from "1-5:15" range)
  private static final long BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS = 15L;
  // Minimum delay for the "slow" original request in retry tests. On loaded CI machines, the
  // TimeoutProcessor (which fires the retry) can be delayed by hundreds of milliseconds. Using
  // thresholdMs * 10 gives only 150ms for batch-get (threshold=15ms), which is not enough headroom.
  // This constant ensures the original request is always slow enough for the retry to fire first.
  private static final long MIN_SLOW_REQUEST_DELAY_MS = 500L;
  private static final Schema STORE_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileAsString("TestRecord.avsc"));
  private static final RandomRecordGenerator rrg = new RandomRecordGenerator();
  private static final GenericRecord SINGLE_GET_VALUE_RESPONSE = (GenericRecord) rrg.randomGeneric(STORE_VALUE_SCHEMA);
  private static final String STORE_NAME = "test_store";
  private static final Set<String> BATCH_GET_KEYS = new HashSet<>();
  private static final Map<String, GenericRecord> BATCH_GET_VALUE_RESPONSE = new HashMap<>();
  private static final Map<String, GenericRecord> BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE = new HashMap<>();

  private static final Schema COMPUTE_PROJECTION_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileAsString("TestRecordProjection.avsc"));
  private static final Set<String> COMPUTE_REQUEST_KEYS = new HashSet<>();
  private static final Map<String, ComputeGenericRecord> COMPUTE_REQUEST_VALUE_RESPONSE = new HashMap<>();
  private static final Map<String, ComputeGenericRecord> COMPUTE_REQUEST_VALUE_RESPONSE_KEY_NOT_FOUND_CASE =
      new HashMap<>();

  private TimeoutProcessor timeoutProcessor;
  private ClientConfig.ClientConfigBuilder clientConfigBuilder;
  private GetRequestContext getRequestContext;
  private BatchGetRequestContext batchGetRequestContext;
  private ClientConfig clientConfig;
  private RetriableAvroGenericStoreClient<String, GenericRecord> retriableClient;
  private StatsAvroGenericStoreClient<String, GenericRecord> statsAvroGenericStoreClient;
  private Map<String, ? extends Metric> metrics;

  private static final Object[] FASTCLIENT_REQUEST_TYPES = { RequestType.SINGLE_GET, RequestType.MULTI_GET,
      RequestType.MULTI_GET_STREAMING, RequestType.COMPUTE, RequestType.COMPUTE_STREAMING };

  @DataProvider(name = "FastClient-RequestTypes")
  public Object[][] fcRequestTypes() {
    return DataProviderUtils.allPermutationGenerator(FASTCLIENT_REQUEST_TYPES);
  }

  @DataProvider(name = "FastClient-RequestTypes-And-Two-Boolean")
  public Object[][] fcRequestTypesAndTwoBoolean() {
    return DataProviderUtils
        .allPermutationGenerator(FASTCLIENT_REQUEST_TYPES, DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN);
  }

  protected boolean isRetryBudgetEnabled() {
    return false;
  }

  @BeforeClass
  public void setUp() {
    // Use a faster ticking interval (10ms instead of default 1000ms) and more threads so that
    // the long-tail retry fires promptly even on slow CI runners. The default 1000ms tick means
    // the retry can be delayed by up to 1 full second, which compounds with JDK 8's slower
    // ForkJoinPool scheduling to push metric recording beyond test timeouts.
    timeoutProcessor = new TimeoutProcessor(null, 10, true, 2);
    clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery")
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(
            (int) TimeUnit.MILLISECONDS.toMicros(LONG_TAIL_RETRY_THRESHOLD_IN_MS))
        // Configure batch-get long tail retry thresholds explicitly (2 keys -> 15ms)
        .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-5:15,6-20:30,21-150:50,151-500:100,501-:500")
        .setLongTailRetryEnabledForCompute(true)
        .setRetryBudgetEnabled(isRetryBudgetEnabled())
        .setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds("1-5:15,6-20:30,21-150:50,151-500:100,501-:500");
    BATCH_GET_KEYS.add("test_key_1");
    BATCH_GET_KEYS.add("test_key_2");
    GenericRecord value1 = (GenericRecord) rrg.randomGeneric(STORE_VALUE_SCHEMA);
    GenericRecord value2 = (GenericRecord) rrg.randomGeneric(STORE_VALUE_SCHEMA);
    BATCH_GET_VALUE_RESPONSE.put("test_key_1", value1);
    BATCH_GET_VALUE_RESPONSE.put("test_key_2", value2);
    BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE.put("test_key_2", value2);

    COMPUTE_REQUEST_KEYS.add("test_key_1");
    COMPUTE_REQUEST_KEYS.add("test_key_2");
    GenericRecord projectionResultForKey1 = new GenericData.Record(COMPUTE_PROJECTION_VALUE_SCHEMA);
    projectionResultForKey1.put("name", "TEST_NAME_1");
    projectionResultForKey1.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
    ComputeGenericRecord computeGenericRecordForProjectionKey1 =
        new ComputeGenericRecord(projectionResultForKey1, STORE_VALUE_SCHEMA);
    COMPUTE_REQUEST_VALUE_RESPONSE.put("test_key_1", computeGenericRecordForProjectionKey1);

    GenericRecord projectionResultForKey2 = new GenericData.Record(COMPUTE_PROJECTION_VALUE_SCHEMA);
    projectionResultForKey2.put("name", "TEST_NAME_2");
    projectionResultForKey2.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
    ComputeGenericRecord computeGenericRecordForProjectionKey2 =
        new ComputeGenericRecord(projectionResultForKey2, STORE_VALUE_SCHEMA);
    COMPUTE_REQUEST_VALUE_RESPONSE.put("test_key_2", computeGenericRecordForProjectionKey2);

    COMPUTE_REQUEST_VALUE_RESPONSE_KEY_NOT_FOUND_CASE.put("test_key_2", computeGenericRecordForProjectionKey2);
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
      boolean noReplicaFound,
      ClientConfig clientConfig) {
    StoreMetadata mockMetadata = mock(StoreMetadata.class);
    doReturn(STORE_NAME).when(mockMetadata).getStoreName();
    doReturn(1).when(mockMetadata).getLatestValueSchemaId();
    doReturn(STORE_VALUE_SCHEMA).when(mockMetadata).getValueSchema(1);
    return new DispatchingAvroGenericStoreClient<Object, Object>(mockMetadata, clientConfig) {
      private int requestCnt = 0;

      @Override
      protected CompletableFuture get(GetRequestContext<Object> requestContext, Object key)
          throws VeniceClientException {
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
            } else if (noReplicaFound) {
              requestContext.noAvailableReplica = true;
              originalRequestFuture
                  .completeExceptionally(new VeniceClientException("At least one route did not complete"));
            } else if (keyNotFound) {
              originalRequestFuture.complete(null);
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
              if (noReplicaFound) {
                requestContext.noAvailableReplica = true;
                retryRequestFuture
                    .completeExceptionally(new VeniceClientException("At least one route did not complete"));
              } else if (keyNotFound) {
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
            requestContext.complete();
            if (originalRequestThrowException) {
              callback.onCompletion(Optional.of(new VeniceClientException("Original request exception")));
            } else if (noReplicaFound) {
              requestContext.noAvailableReplica = true;
              callback.onCompletion(Optional.of(new VeniceClientException("At least one route did not complete")));
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
            requestContext.complete();
            if (retryRequestThrowException) {
              callback.onCompletion(Optional.of(new VeniceClientException("Retry request exception")));
            } else if (noReplicaFound) {
              requestContext.noAvailableReplica = true;
              callback.onCompletion(Optional.of(new VeniceClientException("At least one route did not complete")));
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

      @Override
      protected void compute(
          ComputeRequestContext requestContext,
          ComputeRequestWrapper computeRequest,
          Set keys,
          Schema resultSchema,
          StreamingCallback callback,
          long preRequestTimeInNS) throws VeniceClientException {
        InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
        doReturn(timeoutProcessor).when(instanceHealthMonitor).getTimeoutProcessor();
        requestContext.instanceHealthMonitor = instanceHealthMonitor;

        ++requestCnt;
        if (requestCnt == 1) {
          // Mock the original request
          scheduledExecutor.schedule(() -> {
            requestContext.complete();
            if (originalRequestThrowException) {
              callback.onCompletion(Optional.of(new VeniceClientException("Original request exception")));
            } else if (noReplicaFound) {
              requestContext.noAvailableReplica = true;
              callback.onCompletion(Optional.of(new VeniceClientException("At least one route did not complete")));
            } else {
              COMPUTE_REQUEST_KEYS.forEach(key -> {
                if (key.equals("test_key_1") && keyNotFound) {
                  callback.onRecordReceived(key, null);
                } else {
                  callback.onRecordReceived(key, COMPUTE_REQUEST_VALUE_RESPONSE.get(key));
                }
              });
              callback.onCompletion(Optional.empty());
            }
          }, originalRequestDelayMs, TimeUnit.MILLISECONDS);
        } else if (requestCnt == 2) {
          // Mock the retry request
          scheduledExecutor.schedule(() -> {
            requestContext.complete();
            if (retryRequestThrowException) {
              callback.onCompletion(Optional.of(new VeniceClientException("Retry request exception")));
            } else if (noReplicaFound) {
              requestContext.noAvailableReplica = true;
              callback.onCompletion(Optional.of(new VeniceClientException("At least one route did not complete")));
            } else {
              COMPUTE_REQUEST_KEYS.forEach(key -> {
                if (key.equals("test_key_1") && keyNotFound) {
                  callback.onRecordReceived(key, null);
                } else {
                  callback.onRecordReceived(key, COMPUTE_REQUEST_VALUE_RESPONSE.get(key));
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
      boolean keyNotFound,
      boolean noReplicaFound) throws ExecutionException, InterruptedException {
    getRequestContext = new GetRequestContext();
    try {
      GenericRecord value = (GenericRecord) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      if (bothOriginalAndRetryFails || noReplicaFound) {
        fail("An ExecutionException should be thrown here");
      }
      if (keyNotFound) {
        assertNull(value);
      } else {
        assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      if (!(bothOriginalAndRetryFails || noReplicaFound)) {
        throw e;
      }
    }

    validateMetrics(RequestType.SINGLE_GET, errorRetry, longTailRetry, retryWin, noReplicaFound);
  }

  private void testBatchGetAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound,
      boolean noReplicaFound) throws ExecutionException, InterruptedException {
    batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
    try {
      Map<String, GenericRecord> value =
          (Map<String, GenericRecord>) statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();

      if (bothOriginalAndRetryFails || noReplicaFound) {
        fail("An ExecutionException should be thrown here");
      }

      if (keyNotFound) {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE);
      } else {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      if (!(bothOriginalAndRetryFails || noReplicaFound)) {
        throw e;
      }
    }

    validateMetrics(RequestType.MULTI_GET, false, longTailRetry, retryWin, noReplicaFound);
  }

  private void testStreamingBatchGetAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound,
      boolean noReplicaFound) throws ExecutionException, InterruptedException {
    batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
    try {
      VeniceResponseMap<String, GenericRecord> value =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();

      if (bothOriginalAndRetryFails || noReplicaFound) {
        assertFalse(value.isFullResponse());
        assertTrue(value.isEmpty());
      } else if (keyNotFound) {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE_KEY_NOT_FOUND_CASE);
      } else {
        assertEquals(value, BATCH_GET_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      throw e;
    }

    validateMetrics(RequestType.MULTI_GET_STREAMING, false, longTailRetry, retryWin, noReplicaFound);
  }

  private void testComputeAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound,
      boolean noReplicaFound) throws ExecutionException, InterruptedException {
    try {
      VeniceResponseMap<String, ComputeGenericRecord> value =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .execute(COMPUTE_REQUEST_KEYS)
              .get();

      if (bothOriginalAndRetryFails || noReplicaFound) {
        fail("An ExecutionException should be thrown here");
      }

      if (keyNotFound) {
        assertEquals(value, COMPUTE_REQUEST_VALUE_RESPONSE_KEY_NOT_FOUND_CASE);
      } else {
        assertEquals(value, COMPUTE_REQUEST_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      if (!(bothOriginalAndRetryFails || noReplicaFound)) {
        throw e;
      }
    }

    validateMetrics(RequestType.COMPUTE_STREAMING, false, longTailRetry, retryWin, noReplicaFound);
  }

  private void testStreamingComputeAndValidateMetrics(
      boolean bothOriginalAndRetryFails,
      boolean longTailRetry,
      boolean retryWin,
      boolean keyNotFound,
      boolean noReplicaFound) throws ExecutionException, InterruptedException {
    try {
      VeniceResponseMap<String, ComputeGenericRecord> value =
          statsAvroGenericStoreClient.compute().project("name").streamingExecute(COMPUTE_REQUEST_KEYS).get();

      if (bothOriginalAndRetryFails || noReplicaFound) {
        assertFalse(value.isFullResponse());
        assertTrue(value.isEmpty());
      } else if (keyNotFound) {
        assertEquals(value, COMPUTE_REQUEST_VALUE_RESPONSE_KEY_NOT_FOUND_CASE);
      } else {
        assertEquals(value, COMPUTE_REQUEST_VALUE_RESPONSE);
      }
    } catch (ExecutionException e) {
      throw e;
    }

    validateMetrics(RequestType.COMPUTE_STREAMING, false, longTailRetry, retryWin, noReplicaFound);
  }

  /**
   * Note that DispatchingAvroGenericStoreClient is mocked in this test and so the counters
   * @param errorRetry request is retried because the original request results in exception. Only applicable
   *                   for single gets.
   * @param longTailRetry request is retried because the original request is taking more time
   * @param retryWin retry request wins
   */
  private void validateMetrics(
      RequestType requestType,
      boolean errorRetry,
      boolean longTailRetry,
      boolean retryWin,
      boolean noReplicaFound) {
    String metricsPrefix = ClientTestUtils.getMetricPrefix(STORE_NAME, requestType);

    boolean singleGet = requestType == RequestType.SINGLE_GET;
    boolean batchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    boolean computeRequest = requestType == RequestType.COMPUTE || requestType == RequestType.COMPUTE_STREAMING;

    metrics = getStats(clientConfig);
    double expectedKeyCount = (batchGet || computeRequest) ? 2.0 : 1.0;

    String finalMetricsPrefix = metricsPrefix;
    // All metric assertions must be inside waitForNonDeterministicAssertion because metrics are
    // recorded asynchronously in completion callbacks. The request future resolves before all
    // metrics are fully recorded, causing race conditions with synchronous assertions.
    // Metrics are recorded in completion callbacks that execute after the request future resolves.
    // With the test's faster TimeoutProcessor (10ms tick vs 1000ms default), retries fire promptly
    // and metrics should be recorded within a few seconds.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(metrics.get(finalMetricsPrefix + "request.OccurrenceRate").value() > 0);
      assertEquals(metrics.get(finalMetricsPrefix + "request_key_count.Max").value(), expectedKeyCount);

      if (noReplicaFound) {
        assertTrue(metrics.get(metricsPrefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
      } else if (errorRetry || longTailRetry) {
        assertFalse(metrics.get(metricsPrefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
        if (errorRetry) {
          assertTrue(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
          assertFalse(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
        } else {
          assertFalse(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
          assertTrue(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
        }
        assertTrue(metrics.get(metricsPrefix + "retry_request_key_count.Rate").value() > 0);
        assertEquals(metrics.get(metricsPrefix + "retry_request_key_count.Max").value(), expectedKeyCount);
      } else {
        assertFalse(metrics.get(metricsPrefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
        assertFalse(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
        assertFalse(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
        assertFalse(metrics.get(metricsPrefix + "retry_request_key_count.Rate").value() > 0);
        assertFalse(metrics.get(metricsPrefix + "retry_request_key_count.Max").value() > 0);
      }

      // errorRetry is only for single gets
      if (singleGet) {
        if (errorRetry) {
          assertTrue(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
          assertTrue(getRequestContext.retryContext.errorRetryRequestTriggered);
        } else {
          assertFalse(metrics.get(metricsPrefix + "error_retry_request.OccurrenceRate").value() > 0);
          assertTrue(
              getRequestContext.retryContext == null || !getRequestContext.retryContext.errorRetryRequestTriggered);
        }
      }

      // longTailRetry is for both single get, batch gets and compute
      if (longTailRetry) {
        assertTrue(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
        if (batchGet) {
          assertNotNull(batchGetRequestContext.retryContext.retryRequestContext);
          assertEquals(
              batchGetRequestContext.retryContext.retryRequestContext.numKeysInRequest,
              (int) expectedKeyCount);

          // Check retry budget metrics
          String batchGetRetryBudgetMetricName =
              "." + RetriableAvroGenericStoreClient.MULTI_KEY_LONG_TAIL_RETRY_STATS_PREFIX + clientConfig.getStoreName()
                  + "--retry_limit_per_seconds.Gauge";
          // Batch get retry manager is always initialized now
          assertNotNull(metrics.get(batchGetRetryBudgetMetricName), "Retry limit per second metric should not be null");
        } else if (singleGet) {
          assertTrue(getRequestContext.retryContext.longTailRetryRequestTriggered);
          // Check retry budget metrics
          String singleGetRetryBudgetMetricName =
              "." + RetriableAvroGenericStoreClient.SINGLE_KEY_LONG_TAIL_RETRY_STATS_PREFIX
                  + clientConfig.getStoreName() + "--retry_limit_per_seconds.Gauge";
          if (isRetryBudgetEnabled()) {
            assertNotNull(
                metrics.get(singleGetRetryBudgetMetricName),
                "Retry limit per second metric should not be null");
          }
        }
      } else {
        assertFalse(metrics.get(metricsPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
        if (batchGet) {
          assertNull(batchGetRequestContext.retryContext.retryRequestContext);
        } else if (singleGet) {
          assertTrue(
              getRequestContext.retryContext == null || !getRequestContext.retryContext.longTailRetryRequestTriggered);
        }
      }

      if (!noReplicaFound) {
        if (retryWin) {
          assertTrue(metrics.get(metricsPrefix + "retry_request_win.OccurrenceRate").value() > 0);
          assertEquals(metrics.get(metricsPrefix + "retry_request_success_key_count.Max").value(), expectedKeyCount);
          if (batchGet) {
            assertTrue(batchGetRequestContext.retryContext.retryRequestContext.numKeysCompleted.get() > 0);
          } else if (singleGet) {
            assertTrue(getRequestContext.retryContext.retryWin);
          }
        } else {
          assertFalse(metrics.get(metricsPrefix + "retry_request_win.OccurrenceRate").value() > 0);
          assertFalse(metrics.get(metricsPrefix + "retry_request_success_key_count.Max").value() > 0);
          if (batchGet) {
            assertTrue(
                batchGetRequestContext.retryContext.retryRequestContext == null
                    || batchGetRequestContext.retryContext.retryRequestContext.numKeysCompleted.get() == 0);
          } else if (singleGet) {
            assertTrue(getRequestContext.retryContext == null || !getRequestContext.retryContext.retryWin);
          }
        }
      }
    });
  }

  /**
   * Original request is faster than retry threshold.
   * For single get: threshold is 100ms
   * For batch get with 2 keys: threshold is 15ms (from "1-5:15" range in dynamic config)
   */
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testGetWithoutTriggeringLongTailRetry(boolean batchGet, boolean keyNotFound)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    // Use appropriate threshold based on request type
    long thresholdMs = batchGet ? BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS : LONG_TAIL_RETRY_THRESHOLD_IN_MS;

    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            thresholdMs / 2, // Original request completes faster than threshold
            false,
            thresholdMs * 2, // Retry would be slower (but won't be triggered)
            keyNotFound,
            false,
            clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (!batchGet) {
      testSingleGetAndValidateMetrics(false, false, false, false, keyNotFound, false);
    } else {
      testBatchGetAndValidateMetrics(false, false, false, keyNotFound, false);
    }
  }

  /**
   * Original request latency is higher than retry threshold, but still faster than retry request
   * Uses dynamic thresholds: 100ms for single get, 15ms for batch get (2 keys), 100ms for compute
   */
  @Test(dataProvider = "FastClient-RequestTypes", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndOriginalWins(RequestType requestType)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    // Use appropriate threshold based on request type
    boolean isBatchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    long thresholdMs = isBatchGet ? BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS : LONG_TAIL_RETRY_THRESHOLD_IN_MS;

    long slowOriginalDelayMs = Math.max(thresholdMs * 10, MIN_SLOW_REQUEST_DELAY_MS);
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            slowOriginalDelayMs, // Original request exceeds threshold but completes
            false,
            slowOriginalDelayMs * 5, // Retry is slower, so original wins
            false,
            false,
            clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (requestType.equals(RequestType.SINGLE_GET)) {
      testSingleGetAndValidateMetrics(false, false, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      testStreamingBatchGetAndValidateMetrics(false, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      testBatchGetAndValidateMetrics(false, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      testStreamingComputeAndValidateMetrics(false, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE)) {
      testComputeAndValidateMetrics(false, true, false, false, false);
    }
  }

  /**
   * Original request latency is higher than retry threshold and slower than the retry request
   * Uses dynamic thresholds: 100ms for single get, 15ms for batch get (2 keys), 100ms for compute
   */
  @Test(dataProvider = "FastClient-RequestTypes-And-Two-Boolean", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndRetryWins(
      RequestType requestType,
      boolean keyNotFound,
      boolean noReplicaFound) throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    // Use appropriate threshold based on request type
    boolean isBatchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    long thresholdMs = isBatchGet ? BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS : LONG_TAIL_RETRY_THRESHOLD_IN_MS;

    long slowOriginalDelayMs = Math.max(thresholdMs * 10, MIN_SLOW_REQUEST_DELAY_MS);
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            slowOriginalDelayMs, // Original request is slow, exceeds threshold
            false,
            thresholdMs / 2, // Retry is fast and wins
            keyNotFound,
            noReplicaFound,
            clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (requestType.equals(RequestType.SINGLE_GET)) {
      testSingleGetAndValidateMetrics(false, false, true, true, keyNotFound, noReplicaFound);
    } else if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      testStreamingBatchGetAndValidateMetrics(false, true, true, keyNotFound, noReplicaFound);
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      testBatchGetAndValidateMetrics(false, true, true, keyNotFound, noReplicaFound);
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      testStreamingComputeAndValidateMetrics(false, true, true, keyNotFound, noReplicaFound);
    } else if (requestType.equals(RequestType.COMPUTE)) {
      testComputeAndValidateMetrics(false, true, true, keyNotFound, noReplicaFound);
    }
  }

  /**
   * Original request fails and retry succeeds.
   */
  @Test(dataProvider = "FastClient-RequestTypes", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringErrorRetryAndRetryWins(RequestType requestType)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, false, LONG_TAIL_RETRY_THRESHOLD_IN_MS, false, false, clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (requestType.equals(RequestType.SINGLE_GET)) {
      testSingleGetAndValidateMetrics(false, true, false, true, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      testStreamingBatchGetAndValidateMetrics(false, true, true, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      testBatchGetAndValidateMetrics(false, true, true, false, false);
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      testStreamingComputeAndValidateMetrics(false, true, true, false, false);
    } else if (requestType.equals(RequestType.COMPUTE)) {
      testComputeAndValidateMetrics(false, true, true, false, false);
    }
  }

  /**
   * Original request latency exceeds the retry threshold but succeeds and the retry fails.
   * Uses dynamic thresholds: 100ms for single get, 15ms for batch get (2 keys), 100ms for compute
   */
  @Test(dataProvider = "FastClient-RequestTypes", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndRetryFails(RequestType requestType)
      throws ExecutionException, InterruptedException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    // Use appropriate threshold based on request type
    boolean isBatchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    long thresholdMs = isBatchGet ? BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS : LONG_TAIL_RETRY_THRESHOLD_IN_MS;

    long slowOriginalDelayMs = Math.max(10 * thresholdMs, MIN_SLOW_REQUEST_DELAY_MS);
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(false, slowOriginalDelayMs, true, 0, false, false, clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    if (requestType.equals(RequestType.SINGLE_GET)) {
      testSingleGetAndValidateMetrics(false, false, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      testStreamingBatchGetAndValidateMetrics(false, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      testBatchGetAndValidateMetrics(false, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      testStreamingComputeAndValidateMetrics(false, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE)) {
      testComputeAndValidateMetrics(false, true, false, false, false);
    }
  }

  /**
   * Original request latency exceeds the retry threshold, and both the original request and the retry fails.
   * Uses dynamic thresholds: 100ms for single get, 15ms for batch get (2 keys), 100ms for compute
   */
  @Test(dataProvider = "FastClient-RequestTypes", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndBothFailsV1(RequestType requestType)
      throws InterruptedException, ExecutionException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    // Use appropriate threshold based on request type
    boolean isBatchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    long thresholdMs = isBatchGet ? BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS : LONG_TAIL_RETRY_THRESHOLD_IN_MS;

    long slowOriginalDelayMs = Math.max(10 * thresholdMs, MIN_SLOW_REQUEST_DELAY_MS);
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, slowOriginalDelayMs, true, 0, false, false, clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true,
     *  but requestContext values should be checked.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    if (requestType.equals(RequestType.SINGLE_GET)) {
      testSingleGetAndValidateMetrics(true, false, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      testStreamingBatchGetAndValidateMetrics(true, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      testBatchGetAndValidateMetrics(true, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      testStreamingComputeAndValidateMetrics(true, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE)) {
      testComputeAndValidateMetrics(true, true, false, false, false);
    }
  }

  /**
   * Original request latency is lower than the retry threshold, and both the original request and the retry fails.
   */
  @Test(dataProvider = "FastClient-RequestTypes", timeOut = TEST_TIMEOUT)
  public void testGetWithTriggeringLongTailRetryAndBothFailsV2(RequestType requestType)
      throws InterruptedException, ExecutionException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();
    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 0, true, 0, false, false, clientConfig),
        clientConfig,
        timeoutProcessor);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(retriableClient, clientConfig);
    /**
     *  When the request is closed exceptionally (when both original request and the retry throws exception),
     *  only unhealthy counters gets incremented, so not checking for retry related metrics being true,
     *  but requestContext values should be checked.
     *  Check {@link StatsAvroGenericStoreClient#recordRequestMetrics} for more details.
     */
    if (requestType.equals(RequestType.SINGLE_GET)) {
      testSingleGetAndValidateMetrics(true, true, false, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
      testStreamingBatchGetAndValidateMetrics(true, true, false, false, false);
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      testBatchGetAndValidateMetrics(true, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE_STREAMING)) {
      testStreamingComputeAndValidateMetrics(true, true, false, false, false);
    } else if (requestType.equals(RequestType.COMPUTE)) {
      testComputeAndValidateMetrics(true, true, false, false, false);
    }
  }

  /**
   * BUG REPRODUCTION: When the long-tail retry fires but the retry budget is exhausted
   * (isRetryAllowed() returns false), the retryTask does nothing and retryFuture is never completed.
   * Later, when the original request fails, the error retry path is skipped because
   * timeoutFuture.isDone() is true (long-tail already consumed it). This leaves finalFuture
   * incomplete forever.
   *
   * This is the exact scenario observed in the mirror heap dump: instance returns 500 on heartbeat,
   * many requests time out exhausting the retry budget, subsequent requests hang forever.
   *
   * This test uses the {@code setSingleKeyLongTailRetryManager(...)} test hook to install a
   * {@link RetryManager} that always denies retries, ensuring deterministic reproduction
   * of this scenario regardless of timing.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testFinalFutureHangsWhenRetryBudgetExhaustedAndOriginalFails() throws Exception {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    StoreMetadata mockMetadata = mock(StoreMetadata.class);
    doReturn(STORE_NAME).when(mockMetadata).getStoreName();
    doReturn(1).when(mockMetadata).getLatestValueSchemaId();
    doReturn(STORE_VALUE_SCHEMA).when(mockMetadata).getValueSchema(1);

    // Original request is slow: completes AFTER long-tail retry threshold with an error.
    // This ensures the long-tail retry fires first, then original fails.
    InternalAvroStoreClient dispatchingClient =
        new DispatchingAvroGenericStoreClient<Object, Object>(mockMetadata, clientConfig) {
          @Override
          protected CompletableFuture get(GetRequestContext requestContext, Object key) throws VeniceClientException {
            InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
            doReturn(timeoutProcessor).when(instanceHealthMonitor).getTimeoutProcessor();
            requestContext.instanceHealthMonitor = instanceHealthMonitor;

            final CompletableFuture originalRequestFuture = new CompletableFuture();
            scheduledExecutor.schedule(
                () -> originalRequestFuture
                    .completeExceptionally(new VeniceClientException("Instance unhealthy, 500 error")),
                LONG_TAIL_RETRY_THRESHOLD_IN_MS * 3, // 300ms >> 100ms threshold
                TimeUnit.MILLISECONDS);
            return originalRequestFuture;
          }
        };

    retriableClient = new RetriableAvroGenericStoreClient<>(dispatchingClient, clientConfig, timeoutProcessor);

    // Replace the internal singleKeyLongTailRetryManager with a mock
    // that always returns false for isRetryAllowed() (simulating exhausted budget)
    RetryManager mockRetryManager = mock(RetryManager.class);
    doReturn(false).when(mockRetryManager).isRetryAllowed();
    doReturn(false).when(mockRetryManager).isRetryAllowed(anyInt());
    retriableClient.setSingleKeyLongTailRetryManager(mockRetryManager);

    GetRequestContext ctx = new GetRequestContext();
    CompletableFuture<GenericRecord> result = retriableClient.get(ctx, "test_key");

    try {
      result.get(2, TimeUnit.SECONDS);
      fail("Expected an ExecutionException");
    } catch (TimeoutException e) {
      // BUG CONFIRMED: finalFuture never completed.
      // Timeline: long-tail retry fires at 100ms → retryTask runs → isRetryAllowed()=false → does nothing →
      // retryFuture stays incomplete → original fails at 300ms → timeoutFuture.isDone()=true → error retry skipped
      // → allOf(original, retryFuture) never completes → finalFuture hangs forever.
      fail(
          "BUG: finalFuture hangs forever when retry budget is exhausted and original request fails after "
              + "long-tail retry timer fires. retryFuture is never completed because retryTask does nothing "
              + "when isRetryAllowed() returns false, and the error retry path is skipped because "
              + "timeoutFuture.isDone() is true.");
    } catch (ExecutionException e) {
      // CORRECT: future completed with an exception (this means the bug is fixed)
      assertTrue(e.getCause() instanceof VeniceClientException);
    }
  }

  /**
   * BUG REPRODUCTION: When original request fails with HTTP 429 (Too Many Requests),
   * the error retry is intentionally skipped (429 should not be retried). But when the
   * long-tail retry timer has NOT yet fired, the timer is cancelled, and retryFuture is
   * never completed. CompletableFuture.allOf(original, retry) never completes, so
   * finalFuture hangs forever.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testFinalFutureHangsWhenOriginalFailsWith429BeforeLongTailRetry()
      throws InterruptedException, ExecutionException, TimeoutException {
    clientConfigBuilder.setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    clientConfig = clientConfigBuilder.build();

    StoreMetadata mockMetadata = mock(StoreMetadata.class);
    doReturn(STORE_NAME).when(mockMetadata).getStoreName();
    doReturn(1).when(mockMetadata).getLatestValueSchemaId();
    doReturn(STORE_VALUE_SCHEMA).when(mockMetadata).getValueSchema(1);

    // Original request fails fast with 429 (BEFORE long-tail retry threshold)
    InternalAvroStoreClient dispatchingClient =
        new DispatchingAvroGenericStoreClient<Object, Object>(mockMetadata, clientConfig) {
          @Override
          protected CompletableFuture get(GetRequestContext requestContext, Object key) throws VeniceClientException {
            InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
            doReturn(timeoutProcessor).when(instanceHealthMonitor).getTimeoutProcessor();
            requestContext.instanceHealthMonitor = instanceHealthMonitor;

            final CompletableFuture future = new CompletableFuture();
            // Fail immediately with 429 - well before the 100ms long-tail retry threshold
            scheduledExecutor.schedule(
                () -> future.completeExceptionally(new VeniceClientRateExceededException("Too many requests")),
                5,
                TimeUnit.MILLISECONDS);
            return future;
          }
        };

    retriableClient = new RetriableAvroGenericStoreClient<>(dispatchingClient, clientConfig, timeoutProcessor);

    GetRequestContext ctx = new GetRequestContext();
    CompletableFuture<GenericRecord> result = retriableClient.get(ctx, "test_key");

    try {
      // If the bug exists, this will timeout because retryFuture is never completed
      result.get(2, TimeUnit.SECONDS);
      fail("Expected an ExecutionException from 429");
    } catch (TimeoutException e) {
      // BUG CONFIRMED: finalFuture hangs because:
      // 1. Original fails with 429 → savedException set, timeoutFuture cancelled
      // 2. isExceptionCausedByTooManyRequests(429) → true → error retry skipped
      // 3. retryFuture never completed
      // 4. allOf(original, retry) never fires → finalFuture incomplete forever
      fail(
          "BUG: finalFuture hangs forever when original request fails with 429 before long-tail retry fires. "
              + "The retry timer is cancelled, 429 check skips error retry, and retryFuture is never completed.");
    } catch (ExecutionException e) {
      // CORRECT: future completed with the 429 exception
      assertTrue(e.getCause() instanceof VeniceClientRateExceededException);
    }
  }

  /**
   * BUG REPRODUCTION (variant with shorter threshold): Same as above but uses a 20ms threshold
   * to clearly demonstrate the timing: long-tail fires at 20ms (budget denied → does nothing),
   * original fails at 200ms (timeoutFuture.isDone()=true → error retry skipped).
   * Uses {@code setSingleKeyLongTailRetryManager(...)} to inject a mock RetryManager for deterministic reproduction.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testFinalFutureHangsWhenLongTailFiresBeforeOriginalFailsAndBudgetExhausted() throws Exception {
    int shortThresholdMicros = (int) TimeUnit.MILLISECONDS.toMicros(20); // 20ms

    ClientConfig.ClientConfigBuilder builder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(com.linkedin.r2.transport.common.Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery")
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(shortThresholdMicros)
        .setMetricsRepository(getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true));
    ClientConfig testConfig = builder.build();

    StoreMetadata mockMetadata = mock(StoreMetadata.class);
    doReturn(STORE_NAME).when(mockMetadata).getStoreName();
    doReturn(1).when(mockMetadata).getLatestValueSchemaId();
    doReturn(STORE_VALUE_SCHEMA).when(mockMetadata).getValueSchema(1);

    InternalAvroStoreClient dispatchingClient =
        new DispatchingAvroGenericStoreClient<Object, Object>(mockMetadata, testConfig) {
          @Override
          protected CompletableFuture get(GetRequestContext requestContext, Object key) throws VeniceClientException {
            InstanceHealthMonitor instanceHealthMonitor = mock(InstanceHealthMonitor.class);
            doReturn(timeoutProcessor).when(instanceHealthMonitor).getTimeoutProcessor();
            requestContext.instanceHealthMonitor = instanceHealthMonitor;

            final CompletableFuture future = new CompletableFuture();
            // Fail slowly: 200ms >> 20ms threshold → long-tail fires first
            scheduledExecutor.schedule(
                () -> future.completeExceptionally(new VeniceClientException("Instance 500 error")),
                200,
                TimeUnit.MILLISECONDS);
            return future;
          }
        };

    RetriableAvroGenericStoreClient<String, GenericRecord> retryClient =
        new RetriableAvroGenericStoreClient<>(dispatchingClient, testConfig, timeoutProcessor);

    // Replace the RetryManager with a mock that always denies retries
    RetryManager mockRetryManager = mock(RetryManager.class);
    doReturn(false).when(mockRetryManager).isRetryAllowed();
    doReturn(false).when(mockRetryManager).isRetryAllowed(anyInt());
    retryClient.setSingleKeyLongTailRetryManager(mockRetryManager);

    GetRequestContext bugCtx = new GetRequestContext();
    CompletableFuture<GenericRecord> result = retryClient.get(bugCtx, "test_key");

    try {
      result.get(2, TimeUnit.SECONDS);
      fail("Expected an ExecutionException");
    } catch (TimeoutException e) {
      fail(
          "BUG: finalFuture hangs forever. Long-tail retry fired at 20ms but budget was exhausted (retryTask did "
              + "nothing). Original request failed at 200ms but error retry was skipped because "
              + "timeoutFuture.isDone()==true. retryFuture was never completed, blocking allOf() forever.");
    } catch (ExecutionException e) {
      // CORRECT behavior: future completed with exception
      assertTrue(e.getCause() instanceof VeniceClientException);
    }
  }

  @DataProvider
  public Object[][] contextInstallationFailures() {
    return new Object[][] { { "get", 1, 0 }, { "batch", 1, 0 }, { "batch", 2, 1 }, { "compute", 1, 0 },
        { "compute", 2, 0 }, { "compute", 3, 1 }, { "get", 0, 0 }, { "batch", 0, 0 }, { "compute", 0, 0 } };
  }

  @Test(dataProvider = "contextInstallationFailures", timeOut = TEST_TIMEOUT)
  public void testContextInstallationFailureCompletesRequest(String operation, int failedScope, int deliveredRecords)
      throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      fixture.failedScope = failedScope;
      boolean withRetry = failedScope == 0;
      ContextCallback callback = fixture.startRequest("request", operation, withRetry);
      if (withRetry) {
        fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      }
      fixture.complete(0, fixture.response(operation));
      callback.completion.get(5, TimeUnit.SECONDS);
      assertSame(rootCause(callback.failure.get()), fixture.contextFailure);
      assertEquals(callback.completionCount, 1, "Failure notification must not replay application callbacks");
      assertEquals(callback.recordCount, deliveredRecords, "Failed work must not run without context");
      fixture.assertFailedRequests();
      assertEquals(fixture.dispatchContexts, Collections.singletonList("request"));
      assertEquals(fixture.contextCaptures.get(), 1);
      assertEquals(fixture.scheduled.size(), withRetry ? 1 : 0);
    }
  }

  @DataProvider
  public Object[][] executionContextWinners() {
    return DataProviderUtils.allPermutationGenerator(
        new Object[] { "get", "batch", "compute" },
        new Object[] { "direct", "original-only", "original-wins", "retry-wins", "error-retry",
            "retry-install-failure" });
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testStreamingMissingRecordsKeepExecutionContext(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture(2, 1)) {
      ContextCallback callback = fixture.startRequest("request", compute ? "compute" : "batch", false);
      fixture.complete(0, fixture.streamingResponse(compute, 1));
      assertEquals(callback.completion.get(5, TimeUnit.SECONDS), "request");
      assertEquals(callback.failure, Optional.empty());
      assertEquals(callback.completionCount, 1);
      assertEquals(callback.recordCount, 2);
      assertEquals(callback.records.keySet(), fixture.keys);
      assertNotNull(callback.records.get("test_key-0"));
      assertNull(callback.records.get("test_key-1"));
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testRetryCleanupFailureDoesNotCountRetryTwice(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      fixture.failedCleanupScope = 1;
      ContextCallback callback = fixture.startRequest("request", compute ? "compute" : "batch", true);
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      fixture.onWorker(() -> fixture.attempts.get(1).completeExceptionally(new VeniceClientException("retry")));
      fixture.drainDeserialization();
      assertFalse(callback.completion.isDone(), "The original request can still succeed");
      assertTrue(fixture.requests.get(1).resultFuture.isCompletedExceptionally());
      fixture.complete(0, fixture.streamingResponse(compute));
      callback.assertSuccess();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testComputeResponseCleanupFailureDoesNotDeliverQueuedRecordsAfterCompletion() throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      fixture.failedCleanupScope = 1;
      ContextCallback callback = fixture.startRequest("request", "compute", false);
      fixture.onWorker(() -> fixture.attempts.get(0).complete(fixture.streamingResponse(true)));
      assertFalse(fixture.deserialization.isEmpty(), "The decoder must remain queued when response cleanup fails");
      callback.assertFailure();
      assertSame(rootCause(callback.failure.get()), fixture.contextFailure);
      fixture.drainDeserialization();
      callback.assertFailure();
      assertEquals(fixture.contextCaptures.get(), 1);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testRetryCleanupFailureDoesNotDeliverDelayedRecordsAfterCompletion(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      ContextCallback callback = fixture.startRequest("request", compute ? "compute" : "batch", true);
      fixture.onWorker(() -> fixture.attempts.get(0).completeExceptionally(new VeniceClientException("original")));
      fixture.failedCleanupScope = fixture.contextScopes.get() + 1;
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      callback.completion.get(5, TimeUnit.SECONDS);
      assertEquals(callback.completionCount, 1);
      assertEquals(callback.recordCount, 0);
      assertSame(rootCause(callback.failure.get()), fixture.contextFailure);
      assertFalse(fixture.attempts.get(1).isDone(), "The retry transport must still be active");
      fixture.complete(1, fixture.streamingResponse(compute));
      assertEquals(callback.recordCount, 0, "A delayed retry must not deliver records after terminal failure");
      assertEquals(callback.completion.get(5, TimeUnit.SECONDS), "worker-prior-context");
      assertEquals(callback.completionCount, 1, "Late successful transport completion must not replay completion");
      assertEquals(fixture.dispatchContexts, Collections.nCopies(2, "request"));
      assertEquals(fixture.contextCaptures.get(), 1);
    }
  }

  private static Throwable rootCause(Throwable failure) {
    while (failure.getCause() != null) {
      failure = failure.getCause();
    }
    return failure;
  }

  @Test(dataProvider = "executionContextWinners", timeOut = TEST_TIMEOUT)
  public void testRequestExecutionContext(String operation, String winner) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      boolean withRetry = !"direct".equals(winner);
      fixture.failedScope = "retry-install-failure".equals(winner) ? 1 : -1;
      ContextCallback callback = fixture.startRequest("request", operation, withRetry);
      callback.throwOnCompletion = !"get".equals(operation) && "direct".equals(winner);
      boolean retry = withRetry && !"original-only".equals(winner);
      if ("error-retry".equals(winner)) {
        fixture.onWorker(() -> fixture.attempts.get(0).completeExceptionally(new VeniceClientException("original")));
        fixture.drainDeserialization();
      }
      if (retry) {
        fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      }
      int attempts = retry && fixture.failedScope == -1 ? 2 : 1;
      assertEquals(fixture.dispatchContexts, Collections.nCopies(attempts, "request"));
      int winningAttempt = "error-retry".equals(winner) || "retry-wins".equals(winner) ? 1 : 0;
      fixture.complete(winningAttempt, fixture.response(operation));
      callback.assertSuccess();
      assertFalse(fixture.requests.get(winningAttempt).resultFuture.isCompletedExceptionally());
      if (!"compute".equals(operation)) {
        assertEquals(fixture.deserializationContexts, Collections.singletonList("request"));
      }
      if (attempts == 2 && !"error-retry".equals(winner)) {
        fixture.complete(1 - winningAttempt, fixture.response(operation));
        callback.assertSuccess();
      }
      if (withRetry) {
        assertTrue(fixture.scheduled.get(0).done.get());
      } else {
        assertTrue(fixture.scheduled.isEmpty());
      }
      assertEquals(fixture.contextCaptures.get(), 1, "Original and retry must share one snapshot");
      if ("get".equals(operation) && fixture.failedScope == -1) {
        assertEquals(fixture.contextScopes.get(), retry ? 3 : 1);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompletingHelperCanRunPlainFutureContinuationOutsideClientScope() throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      CompletableFuture<GenericRecord> response = fixture.startGet("request", true);
      CompletableFuture<String> downstream = response.thenApply(value -> {
        assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
        return fixture.context.get();
      });
      CompletableFuture<Void> blocked = new CompletableFuture<>();
      CompletableFuture<Void> release = new CompletableFuture<>();
      try {
        CompletableFuture<Void> blockingCallback = response.thenRun(() -> {
          assertEquals(fixture.context.get(), "request");
          blocked.complete(null);
          release.join();
        });
        fixture.onWorker(() -> fixture.attempts.get(0).complete(fixture.singleResponse()));
        Future<?> completing = fixture.submitWorker(() -> fixture.deserialization.remove().run());
        blocked.get(5, TimeUnit.SECONDS);
        // Even a losing complete() call can help drain a plain future's synchronous continuations.
        fixture.inContext("helper-caller", () -> assertFalse(response.complete(SINGLE_GET_VALUE_RESPONSE)));
        assertEquals(downstream.get(5, TimeUnit.SECONDS), "helper-caller");
        release.complete(null);
        completing.get(5, TimeUnit.SECONDS);
        blockingCallback.get(5, TimeUnit.SECONDS);
      } finally {
        release.complete(null);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSingleGetCancellationKeepsExecutionContext() throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      CompletableFuture<GenericRecord> cancelled = fixture.startGet("cancelled-request", true);
      assertTrue(cancelled.cancel(false));
      expectThrows(CancellationException.class, cancelled::join);
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      fixture.complete(1, fixture.singleResponse());
      fixture.complete(0, fixture.singleResponse());
      assertTrue(cancelled.isCancelled());
      assertEquals(fixture.dispatchContexts, Collections.nCopies(2, "cancelled-request"));
    }
  }

  @DataProvider
  public Object[][] contextRequestFailures() {
    return new Object[][] { { "get", "transport" }, { "get", "malformed" }, { "get", "cancel-both" },
        { "get", "cancel-original" }, { "batch", "transport" }, { "compute", "transport" },
        { "batch", "cancel-routes" }, { "compute", "cancel-routes" }, { "batch", "retry-dispatch" },
        { "compute", "retry-dispatch" } };
  }

  @Test(dataProvider = "contextRequestFailures", timeOut = TEST_TIMEOUT)
  public void testRequestFailureAndWorkerReuseKeepExecutionContext(String operation, String failure) throws Exception {
    boolean singleGet = "get".equals(operation);
    boolean dispatchFailure = "retry-dispatch".equals(failure);
    try (ContextFixture fixture = new ContextFixture(singleGet ? 1 : 10, singleGet ? 1 : 2)) {
      fixture.retryDispatchFailure = dispatchFailure ? new IllegalArgumentException("retry dispatch") : null;
      for (String request: new String[] { "request-a", "request-b" }) {
        int offset = fixture.attempts.size();
        int dispatched = fixture.dispatchContexts.size();
        ContextCallback callback = fixture.startRequest(request, operation, true);
        callback.throwOnCompletion = !singleGet;
        if (dispatchFailure) {
          fixture.onWorker(
              () -> fixture.attempts.subList(offset, fixture.attempts.size())
                  .forEach(attempt -> attempt.completeExceptionally(new VeniceClientException("original"))));
          fixture.drainDeserialization();
          assertFalse(callback.completion.isDone(), "The original failure must still allow a retry");
        }
        fixture.onWorker(() -> fixture.scheduled.get(fixture.scheduled.size() - 1).fire());
        fixture.onWorker(() -> {
          for (int attempt = offset; !dispatchFailure && attempt < fixture.attempts.size(); attempt++) {
            if (failure.startsWith("cancel")) {
              if (attempt == offset || !"cancel-original".equals(failure)) {
                assertTrue(fixture.attempts.get(attempt).cancel(false));
              }
            } else if ("malformed".equals(failure)) {
              fixture.attempts.get(attempt)
                  .complete(new TransportClientResponse(1, CompressionStrategy.NO_OP, new byte[0]));
            } else {
              fixture.attempts.get(attempt).completeExceptionally(new VeniceClientException("transport"));
            }
          }
        });
        fixture.drainDeserialization();
        if ("cancel-original".equals(failure)) {
          fixture.complete(offset + 1, fixture.response(operation));
          callback.assertSuccess();
        } else {
          callback.assertFailure();
          fixture.assertFailedRequests();
        }
        if (dispatchFailure) {
          assertSame(rootCause(callback.failure.get()), fixture.retryDispatchFailure);
          assertEquals(fixture.attempts.size(), offset + 2, "Synchronous dispatch must not create a retry transport");
        }
        assertEquals(
            fixture.dispatchContexts.subList(dispatched, fixture.dispatchContexts.size()),
            Collections.nCopies(singleGet ? 2 : dispatchFailure ? 3 : 4, request));
      }
      if ("malformed".equals(failure)) {
        assertEquals(fixture.deserializationContexts, fixture.dispatchContexts);
      }
      assertEquals(fixture.contextCaptures.get(), 2);
    }
  }

  @DataProvider
  public Object[][] streamingContextWorkloads() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.BOOLEAN, new Object[] { 1, 10, 1000 });
  }

  @Test(dataProvider = "streamingContextWorkloads", timeOut = TEST_TIMEOUT)
  public void testStreamingContextScopesDoNotScaleWithRecordCount(boolean compute, int keyCount) throws Exception {
    try (ContextFixture fixture = new ContextFixture(keyCount, 1)) {
      ContextCallback callback = fixture.startRequest("request", compute ? "compute" : "batch", true);
      fixture.complete(0, fixture.streamingResponse(compute));
      callback.assertSuccess();
      assertEquals(fixture.dispatchContexts, Collections.singletonList("request"));
      assertEquals(fixture.scheduled.size(), 1);
      assertEquals(fixture.contextCaptures.get(), 1);
      assertEquals(
          fixture.contextScopes.get(),
          compute ? 3 : 2,
          "Scope response, aggregate completion and (for compute) decoder task, not each record");
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testConcurrentBatchesShareContextOnlyWithinTheirOwnRequest(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture(10, 2)) {
      List<ContextCallback> callbacks = new ArrayList<>();
      for (String request: new String[] { "request-a", "request-b" }) {
        callbacks.add(fixture.startRequest(request, compute ? "compute" : "batch", true));
      }
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      fixture.onWorker(() -> fixture.scheduled.get(1).fire());
      assertEquals(
          fixture.dispatchContexts,
          Arrays.asList(
              "request-a",
              "request-a",
              "request-b",
              "request-b",
              "request-a",
              "request-a",
              "request-b",
              "request-b"));
      // Interleave request A's winning retry with request B's winning original on the same worker.
      for (int attempt: new int[] { 4, 2, 5, 3, 0, 6, 1, 7 }) {
        fixture.complete(attempt, fixture.streamingResponse(compute, fixture.attemptKeyCounts.get(attempt)));
      }
      for (ContextCallback callback: callbacks) {
        callback.assertSuccess();
      }
      assertEquals(fixture.contextCaptures.get(), 2, "Separate requests need separate snapshots");
      assertEquals(fixture.contextScopes.get(), compute ? 22 : 14);
    }
  }

  private static class ScheduledContextRetry {
    private final Runnable task;
    private final AtomicBoolean done = new AtomicBoolean();

    ScheduledContextRetry(Runnable task) {
      this.task = task;
    }

    void fire() {
      if (done.compareAndSet(false, true)) {
        task.run();
      }
    }
  }

  private static class ContextCallback implements StreamingCallback<String, Object> {
    private final ContextFixture fixture;
    private final String request;
    private final boolean compute;
    private final Map<String, Object> records = new HashMap<>();
    private final CompletableFuture<String> completion = new CompletableFuture<>();
    private Optional<Exception> failure;
    private int recordCount;
    private int completionCount;
    private boolean throwOnCompletion;

    ContextCallback(ContextFixture fixture, String request, boolean compute) {
      this.fixture = fixture;
      this.request = request;
      this.compute = compute;
    }

    @Override
    public void onRecordReceived(String key, Object value) {
      recordCount++;
      assertEquals(fixture.context.get(), request);
      assertNull(records.put(key, value), "Each record must be delivered exactly once");
      fixture.inContext("callback-local", () -> assertEquals(fixture.context.get(), "callback-local"));
      assertEquals(fixture.context.get(), request);
    }

    @Override
    public void onCompletion(Optional<Exception> exception) {
      completionCount++;
      failure = exception;
      completion.complete(fixture.context.get());
      if (throwOnCompletion) {
        throw new IllegalStateException("application callback");
      }
    }

    void assertSuccess() throws Exception {
      assertEquals(completion.get(5, TimeUnit.SECONDS), request);
      assertEquals(completionCount, 1);
      assertEquals(failure, Optional.empty());
      assertEquals(records.keySet(), fixture.keys);
      assertEquals(recordCount, fixture.keys.size());
      for (Object value: records.values()) {
        if (compute) {
          assertEquals(((ComputeGenericRecord) value).get("name").toString(), "TEST_NAME_1");
        } else {
          assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
        }
      }
    }

    void assertFailure() throws Exception {
      assertEquals(completion.get(5, TimeUnit.SECONDS), request);
      assertEquals(completionCount, 1);
      assertTrue(failure.isPresent());
      assertEquals(recordCount, 0, "Failed requests must not return records");
    }
  }

  private static class ContextFixture implements AutoCloseable {
    private final ThreadLocal<String> context = new ThreadLocal<>();
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private final Queue<Runnable> deserialization = new ArrayDeque<>();
    private final List<CompletableFuture<TransportClientResponse>> attempts = new ArrayList<>();
    private final List<Integer> attemptKeyCounts = new ArrayList<>();
    private final Map<String, Integer> routeKeyCounts = new HashMap<>();
    private final Set<String> keys = new LinkedHashSet<>();
    private final AtomicInteger contextCaptures = new AtomicInteger();
    private final AtomicInteger contextScopes = new AtomicInteger();
    private final IllegalStateException contextFailure = new IllegalStateException("context installation");
    private int failedScope = -1;
    private int failedCleanupScope = -1;
    private RuntimeException retryDispatchFailure;
    private final List<RequestContext> requests = new ArrayList<>();
    private final List<String> dispatchContexts = new ArrayList<>();
    private final List<String> deserializationContexts = new ArrayList<>();
    private final List<ScheduledContextRetry> scheduled = new ArrayList<>();
    private final DispatchingAvroGenericStoreClient<String, GenericRecord> dispatchClient;
    private final RetriableAvroGenericStoreClient<String, GenericRecord> retryClient;
    private final MetricsRepository repository = new MetricsRepository();

    ContextFixture() throws IOException {
      this(1, 1);
    }

    ContextFixture(int keyCount, int routeCount) throws IOException {
      for (int i = 0; i < keyCount; i++) {
        keys.add(keyCount == 1 ? "test_key" : "test_key-" + i);
      }
      ExecutionContextProvider provider = () -> {
        contextCaptures.incrementAndGet();
        String captured = context.get();
        return task -> {
          int scope = contextScopes.incrementAndGet();
          if (scope == failedScope || failedScope == 0) {
            throw contextFailure;
          }
          inContext(captured, task);
          if (scope == failedCleanupScope) {
            throw contextFailure;
          }
        };
      };
      ClientConfig<String, GenericRecord, ?> config =
          new ClientConfig.ClientConfigBuilder<String, GenericRecord, org.apache.avro.specific.SpecificRecord>()
              .setStoreName(STORE_NAME)
              .setR2Client(mock(Client.class))
              .setD2Client(mock(D2Client.class))
              .setClusterDiscoveryD2Service("discovery")
              .setInstanceHealthMonitor(mock(InstanceHealthMonitor.class))
              .setMetricsRepository(repository)
              .setExecutionContextProvider(provider)
              .setDeserializationExecutor(deserialization::add)
              .build();
      StoreMetadata metadata = mock(StoreMetadata.class);
      doReturn(true).when(metadata).isReady();
      doReturn(STORE_NAME).when(metadata).getStoreName();
      doReturn(Schema.create(Schema.Type.STRING)).when(metadata).getKeySchema();
      doReturn(STORE_VALUE_SCHEMA).when(metadata).getValueSchema(1);
      doReturn(1000).when(metadata).getBatchGetLimit();
      NoopCompressor compressor = new NoopCompressor() {
        @Override
        public ByteBuffer decompress(ByteBuffer data) {
          deserializationContexts.add(context.get());
          return data;
        }
      };
      doReturn(compressor).when(metadata).getCompressor(CompressionStrategy.NO_OP, 1);
      doAnswer(invocation -> {
        RequestContext request = invocation.getArgument(0);
        requests.add(request);
        request.setCurrentVersion(1);
        String route = request.isRetryRequest() ? "http://retry" : "http://original";
        routeKeyCounts.clear();
        if (request instanceof GetRequestContext) {
          GetRequestContext<?> get = (GetRequestContext<?>) request;
          get.setRoute(route);
          get.setPartitionId(0);
          get.setSerializedKey(new byte[] { 1 });
          routeKeyCounts.put(route, 1);
        } else {
          MultiKeyRequestContext<String, GenericRecord> multi = invocation.getArgument(0);
          int index = 0;
          for (String key: multi.getKeys()) {
            String keyRoute = routeCount == 1 ? route : route + "-" + index++ % routeCount;
            multi.addKey(keyRoute, key, new byte[] { 1 }, 0);
            routeKeyCounts.merge(keyRoute, 1, Integer::sum);
          }
        }
        return null;
      }).when(metadata).routeRequest(any(), any());
      doAnswer(invocation -> {
        CompletableFuture<Integer> status = new CompletableFuture<>();
        return new ChainedCompletableFuture<>(status, status);
      }).when(metadata).trackHealthBasedOnRequestToInstance(any(), anyInt(), anyInt(), any());
      TransportClient transport = mock(TransportClient.class);
      doAnswer(invocation -> newAttempt(invocation.getArgument(0))).when(transport).get(any());
      doAnswer(invocation -> newAttempt(invocation.getArgument(0))).when(transport).post(any(), any(), any());
      TimeoutProcessor timer = mock(TimeoutProcessor.class);
      doAnswer(invocation -> {
        ScheduledContextRetry retry = new ScheduledContextRetry(invocation.getArgument(0));
        scheduled.add(retry);
        TimeoutProcessor.TimeoutFuture future = mock(TimeoutProcessor.TimeoutFuture.class);
        doAnswer(ignored -> retry.done.get()).when(future).isDone();
        doAnswer(ignored -> retry.done.compareAndSet(false, true)).when(future).cancel();
        return future;
      }).when(timer).schedule(any(), org.mockito.ArgumentMatchers.anyLong(), any());
      dispatchClient = new DispatchingAvroGenericStoreClient<>(metadata, config, transport);
      retryClient = new RetriableAvroGenericStoreClient<>(dispatchClient, config, timer);
    }

    private CompletableFuture<TransportClientResponse> newAttempt(String url) {
      dispatchContexts.add(context.get());
      if (retryDispatchFailure != null && url.startsWith("http://retry")) {
        throw retryDispatchFailure;
      }
      for (Map.Entry<String, Integer> entry: routeKeyCounts.entrySet()) {
        if (url.startsWith(entry.getKey() + "/")) {
          attemptKeyCounts.add(entry.getValue());
          break;
        }
      }
      CompletableFuture<TransportClientResponse> future = new CompletableFuture<>();
      attempts.add(future);
      assertEquals(attemptKeyCounts.size(), attempts.size());
      return future;
    }

    void inContext(String value, Runnable task) {
      String previous = context.get();
      context.set(value);
      try {
        task.run();
      } finally {
        context.set(previous);
      }
    }

    Future<?> submitWorker(Runnable task) {
      return worker.submit(() -> inContext("worker-prior-context", () -> {
        try {
          task.run();
        } finally {
          assertEquals(context.get(), "worker-prior-context");
        }
      }));
    }

    void onWorker(Runnable task) throws Exception {
      submitWorker(task).get(5, TimeUnit.SECONDS);
      assertNull(context.get());
    }

    void drainDeserialization() throws Exception {
      onWorker(() -> {
        while (!deserialization.isEmpty()) {
          deserialization.remove().run();
          assertEquals(context.get(), "worker-prior-context");
        }
      });
    }

    void complete(int attempt, TransportClientResponse response) throws Exception {
      onWorker(() -> attempts.get(attempt).complete(response));
      drainDeserialization();
    }

    void assertFailedRequests() {
      for (RequestContext request: requests) {
        assertTrue(request.resultFuture.isCompletedExceptionally());
        request.routeRequestMap.values().forEach(route -> assertTrue(route.isDone()));
      }
    }

    TransportClientResponse singleResponse() {
      return new TransportClientResponse(
          1,
          CompressionStrategy.NO_OP,
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(STORE_VALUE_SCHEMA)
              .serialize(SINGLE_GET_VALUE_RESPONSE));
    }

    CompletableFuture<GenericRecord> startGet(String request, boolean withRetry) {
      AtomicReference<CompletableFuture<GenericRecord>> response = new AtomicReference<>();
      inContext(request, () -> response.set((withRetry ? retryClient : dispatchClient).get("test_key")));
      return response.get();
    }

    TransportClientResponse response(String operation) {
      return "get".equals(operation) ? singleResponse() : streamingResponse("compute".equals(operation));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    ContextCallback startRequest(String request, String operation, boolean withRetry) {
      ContextCallback callback = new ContextCallback(this, request, "compute".equals(operation));
      if ("get".equals(operation)) {
        startGet(request, withRetry).whenComplete((value, error) -> {
          if (error == null) {
            callback.onRecordReceived("test_key", value);
          }
          callback.onCompletion(Optional.ofNullable((Exception) error));
        });
      } else {
        inContext(request, () -> {
          InternalAvroStoreClient<String, GenericRecord> client = withRetry ? retryClient : dispatchClient;
          if ("compute".equals(operation)) {
            ComputeRequestWrapper compute = new ComputeRequestWrapper(
                1,
                STORE_VALUE_SCHEMA,
                COMPUTE_PROJECTION_VALUE_SCHEMA,
                loadSchemaFileAsString("TestRecordProjection.avsc"),
                Collections.emptyList(),
                true);
            client.compute(
                new ComputeRequestContext<>(keys.size(), false),
                compute,
                keys,
                COMPUTE_PROJECTION_VALUE_SCHEMA,
                (StreamingCallback) callback,
                System.nanoTime());
          } else {
            client.streamingBatchGet(
                new BatchGetRequestContext<>(keys.size(), false),
                keys,
                (StreamingCallback) callback);
          }
        });
      }
      return callback;
    }

    TransportClientResponse streamingResponse(boolean compute) {
      return streamingResponse(compute, keys.size());
    }

    TransportClientResponse streamingResponse(boolean compute, int keyCount) {
      Schema envelope = compute ? ComputeResponseRecordV1.SCHEMA$ : MultiGetResponseRecordV1.SCHEMA$;
      GenericRecord value = compute ? COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1") : SINGLE_GET_VALUE_RESPONSE;
      byte[] serializedValue =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(value.getSchema()).serialize(value);
      List<GenericRecord> records = new ArrayList<>();
      for (int i = 0; i < keyCount; i++) {
        GenericRecord record = new GenericData.Record(envelope);
        record.put("keyIndex", i);
        record.put("value", ByteBuffer.wrap(serializedValue));
        if (!compute) {
          record.put("schemaId", 1);
        }
        records.add(record);
      }
      return new TransportClientResponse(
          1,
          CompressionStrategy.NO_OP,
          FastSerializerDeserializerFactory.<GenericRecord>getFastAvroGenericSerializer(envelope)
              .serializeObjects(records));
    }

    @Override
    public void close() throws Exception {
      retryClient.close();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
      repository.close();
    }
  }
}
