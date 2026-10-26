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
  public Object[][] executionContextWinners() {
    return new Object[][] { { "original-only" }, { "original-wins" }, { "retry-wins" }, { "error-retry" } };
  }

  @Test(dataProvider = "executionContextWinners", timeOut = TEST_TIMEOUT)
  public void testSingleGetExecutionContext(String winner) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      AtomicReference<CompletableFuture<GenericRecord>> response = new AtomicReference<>();
      fixture.inContext("request", () -> response.set(fixture.retryClient.get("test_key")));
      CompletableFuture<String> downstream = response.get().thenApply(value -> {
        assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
        return fixture.context.get();
      });
      boolean retry = !"original-only".equals(winner);
      if ("error-retry".equals(winner)) {
        fixture.onWorker(() -> fixture.attempts.get(0).completeExceptionally(new VeniceClientException("original")));
        fixture.drainDeserialization();
        assertTrue(fixture.scheduled.get(0).done.get());
      } else if (retry) {
        fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      }
      assertEquals(fixture.dispatchContexts, Collections.nCopies(retry ? 2 : 1, "request"));
      int winningAttempt = "error-retry".equals(winner) || "retry-wins".equals(winner) ? 1 : 0;
      fixture.complete(winningAttempt, fixture.singleResponse());
      assertEquals(downstream.get(5, TimeUnit.SECONDS), "request");
      assertEquals(fixture.deserializationContexts, Collections.singletonList("request"));
      if (retry && !"error-retry".equals(winner)) {
        fixture.complete(1 - winningAttempt, fixture.singleResponse());
        assertEquals(response.get().get(5, TimeUnit.SECONDS), SINGLE_GET_VALUE_RESPONSE);
      }
      assertTrue(fixture.scheduled.get(0).done.get());
      assertEquals(fixture.contextCaptures.get(), 1, "Original and retry must share one snapshot");
      assertEquals(fixture.contextScopes.get(), retry ? ("error-retry".equals(winner) ? 2 : 3) : 1);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSingleGetFailureCancellationAndWorkerReuseKeepExecutionContext() throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      for (String request: new String[] { "request-a", "request-b" }) {
        int offset = fixture.attempts.size();
        AtomicReference<CompletableFuture<GenericRecord>> response = new AtomicReference<>();
        fixture.inContext(request, () -> response.set(fixture.retryClient.get("test_key")));
        CompletableFuture<String> completionContext = response.get().handle((value, failure) -> {
          assertTrue(failure != null);
          return fixture.context.get();
        });
        fixture.onWorker(() -> fixture.scheduled.get(fixture.scheduled.size() - 1).fire());
        fixture.onWorker(() -> {
          fixture.attempts.get(offset).completeExceptionally(new VeniceClientException("original"));
          fixture.attempts.get(offset + 1).completeExceptionally(new VeniceClientException("retry"));
        });
        fixture.drainDeserialization();
        assertEquals(completionContext.get(5, TimeUnit.SECONDS), request);
      }
      assertEquals(
          fixture.dispatchContexts,
          java.util.Arrays.asList("request-a", "request-a", "request-b", "request-b"));
      AtomicReference<CompletableFuture<GenericRecord>> cancelled = new AtomicReference<>();
      fixture.inContext("cancelled-request", () -> cancelled.set(fixture.retryClient.get("test_key")));
      assertTrue(cancelled.get().cancel(false));
      expectThrows(CancellationException.class, () -> cancelled.get().join());
      fixture.onWorker(() -> fixture.scheduled.get(2).fire());
      fixture.complete(5, fixture.singleResponse());
      fixture.complete(4, fixture.singleResponse());
      assertTrue(cancelled.get().isCancelled());
      assertEquals(fixture.dispatchContexts.subList(4, 6), Collections.nCopies(2, "cancelled-request"));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSingleGetDeserializationFailureKeepsExecutionContext() throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      AtomicReference<CompletableFuture<GenericRecord>> response = new AtomicReference<>();
      fixture.inContext("request", () -> response.set(fixture.retryClient.get("test_key")));
      CompletableFuture<String> downstream = response.get().handle((value, failure) -> {
        assertTrue(failure != null);
        return fixture.context.get();
      });
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      TransportClientResponse malformed = new TransportClientResponse(1, CompressionStrategy.NO_OP, new byte[0]);
      fixture.complete(0, malformed);
      fixture.complete(1, malformed);
      assertEquals(downstream.get(5, TimeUnit.SECONDS), "request");
      assertEquals(fixture.deserializationContexts, Collections.nCopies(2, "request"));
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testSingleGetTransportCancellationKeepsExecutionContext(boolean cancelBoth) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      AtomicReference<CompletableFuture<GenericRecord>> response = new AtomicReference<>();
      fixture.inContext("request", () -> response.set(fixture.retryClient.get("test_key")));
      CompletableFuture<String> downstream = response.get().handle((value, failure) -> {
        if (cancelBoth) {
          assertNotNull(failure);
        } else {
          assertNull(failure);
          assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
        }
        return fixture.context.get();
      });
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      fixture.onWorker(() -> {
        assertTrue(fixture.attempts.get(0).cancel(false));
        if (cancelBoth) {
          assertTrue(fixture.attempts.get(1).cancel(false));
        }
      });
      fixture.drainDeserialization();
      if (!cancelBoth) {
        fixture.complete(1, fixture.singleResponse());
      }
      assertEquals(downstream.get(5, TimeUnit.SECONDS), "request");
      assertEquals(fixture.dispatchContexts, Collections.nCopies(2, "request"));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSingleGetWithoutRetryLayerKeepsExecutionContext() throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      AtomicReference<CompletableFuture<GenericRecord>> response = new AtomicReference<>();
      fixture.inContext("request", () -> response.set(fixture.dispatchClient.get("test_key")));
      CompletableFuture<String> downstream = response.get().thenApply(value -> {
        assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
        return fixture.context.get();
      });
      fixture.complete(0, fixture.singleResponse());
      assertEquals(downstream.get(5, TimeUnit.SECONDS), "request");
      assertEquals(fixture.dispatchContexts, Collections.singletonList("request"));
      assertEquals(fixture.deserializationContexts, Collections.singletonList("request"));
      assertTrue(fixture.scheduled.isEmpty());
      assertEquals(fixture.contextCaptures.get(), 1);
      assertEquals(fixture.contextScopes.get(), 1);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testStreamingFailureAndWorkerReuseKeepExecutionContext(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      for (String request: new String[] { "request-a", "request-b" }) {
        int offset = fixture.attempts.size();
        CompletableFuture<String> completion = new CompletableFuture<>();
        fixture.inContext(request, () -> fixture.startStreaming(compute, new StreamingCallback<String, Object>() {
          @Override
          public void onRecordReceived(String key, Object value) {
            fail("Failed requests must not return a record");
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            assertTrue(exception.isPresent());
            completion.complete(fixture.context.get());
            throw new IllegalStateException("application callback");
          }
        }, false));
        fixture.onWorker(() -> fixture.scheduled.get(fixture.scheduled.size() - 1).fire());
        fixture.onWorker(() -> {
          fixture.attempts.get(offset).completeExceptionally(new VeniceClientException("original"));
          fixture.attempts.get(offset + 1).completeExceptionally(new VeniceClientException("retry"));
        });
        fixture.drainDeserialization();
        assertEquals(completion.get(5, TimeUnit.SECONDS), request);
      }
      assertEquals(
          fixture.dispatchContexts,
          java.util.Arrays.asList("request-a", "request-a", "request-b", "request-b"));
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testStreamingWithoutRetryLayerKeepsExecutionContext(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      CompletableFuture<String> completion = new CompletableFuture<>();
      AtomicBoolean recordReceived = new AtomicBoolean();
      fixture.inContext("request", () -> fixture.startStreaming(compute, new StreamingCallback<String, Object>() {
        @Override
        public void onRecordReceived(String key, Object value) {
          assertEquals(fixture.context.get(), "request");
          assertEquals(key, "test_key");
          if (compute) {
            assertEquals(((ComputeGenericRecord) value).get("name").toString(), "context-value");
          } else {
            assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
          }
          assertFalse(recordReceived.getAndSet(true));
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          assertFalse(exception.isPresent());
          completion.complete(fixture.context.get());
        }
      }, true, false));
      fixture.complete(0, fixture.streamingResponse(compute));
      assertEquals(completion.get(5, TimeUnit.SECONDS), "request");
      assertTrue(recordReceived.get());
      assertEquals(fixture.dispatchContexts, Collections.singletonList("request"));
      assertTrue(fixture.scheduled.isEmpty());
    }
  }

  @DataProvider
  public Object[][] streamingExecutionContextWinners() {
    return DataProviderUtils.allPermutationGenerator(
        new Object[] { false, true },
        new Object[] { "original-only", "original-wins", "retry-wins", "original-error" });
  }

  @Test(dataProvider = "streamingExecutionContextWinners", timeOut = TEST_TIMEOUT)
  public void testStreamingExecutionContext(boolean compute, String winner) throws Exception {
    try (ContextFixture fixture = new ContextFixture()) {
      List<String> callbackContexts = new ArrayList<>();
      List<Object> records = new ArrayList<>();
      CompletableFuture<String> completion = new CompletableFuture<>();
      StreamingCallback<String, Object> callback = new StreamingCallback<String, Object>() {
        @Override
        public void onRecordReceived(String key, Object value) {
          assertEquals(key, "test_key");
          callbackContexts.add(fixture.context.get());
          records.add(value);
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          assertEquals(exception, Optional.empty());
          completion.complete(fixture.context.get());
        }
      };
      fixture.inContext("request", () -> fixture.startStreaming(compute, callback));
      boolean retry = !"original-only".equals(winner);
      if ("original-error".equals(winner)) {
        fixture.onWorker(() -> fixture.attempts.get(0).completeExceptionally(new VeniceClientException("original")));
      }
      if (retry) {
        fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      }
      int winningAttempt = "retry-wins".equals(winner) || "original-error".equals(winner) ? 1 : 0;
      TransportClientResponse response = fixture.streamingResponse(compute);
      fixture.complete(winningAttempt, response);
      assertEquals(completion.get(5, TimeUnit.SECONDS), "request");
      assertEquals(callbackContexts, Collections.singletonList("request"));
      assertEquals(records.size(), 1);
      if (compute) {
        assertEquals(((ComputeGenericRecord) records.get(0)).get("name").toString(), "context-value");
      } else {
        assertEquals(records.get(0), SINGLE_GET_VALUE_RESPONSE);
        assertEquals(fixture.deserializationContexts, Collections.singletonList("request"));
      }
      assertEquals(fixture.dispatchContexts, Collections.nCopies(retry ? 2 : 1, "request"));
      if (retry && !"original-error".equals(winner)) {
        fixture.complete(1 - winningAttempt, response);
        assertEquals(records.size(), 1, "The losing attempt must not deliver another record");
      }
      assertTrue(fixture.scheduled.get(0).done.get());
      assertEquals(fixture.contextCaptures.get(), 1, "Routes, decoder tasks and retry must share one snapshot");
    }
  }

  @DataProvider
  public Object[][] streamingContextWorkloads() {
    return DataProviderUtils.allPermutationGenerator(
        new Object[] { false, true },
        new Object[] { 1, 10, 1000 },
        new Object[] { false, true });
  }

  @Test(dataProvider = "streamingContextWorkloads", timeOut = TEST_TIMEOUT)
  public void testStreamingContextScopesDoNotScaleWithRecordCount(boolean compute, int keyCount, boolean withRetry)
      throws Exception {
    try (ContextFixture fixture = new ContextFixture(keyCount, 1)) {
      Map<String, Object> records = new HashMap<>();
      CompletableFuture<String> completion = new CompletableFuture<>();
      fixture.inContext("request", () -> fixture.startStreaming(compute, new StreamingCallback<String, Object>() {
        @Override
        public void onRecordReceived(String key, Object value) {
          assertEquals(fixture.context.get(), "request");
          assertNull(records.put(key, value), "Each record must be delivered exactly once");
          fixture.inContext("callback-local", () -> assertEquals(fixture.context.get(), "callback-local"));
          assertEquals(fixture.context.get(), "request");
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          assertFalse(exception.isPresent());
          assertTrue(completion.complete(fixture.context.get()));
        }
      }, true, withRetry));
      fixture.complete(0, fixture.streamingResponse(compute));
      assertEquals(completion.get(5, TimeUnit.SECONDS), "request");
      assertEquals(records.keySet(), fixture.keys);
      for (Object value: records.values()) {
        if (compute) {
          assertEquals(((ComputeGenericRecord) value).get("name").toString(), "context-value");
        } else {
          assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
        }
      }
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
      Map<String, Map<String, Object>> results = new HashMap<>();
      Map<String, CompletableFuture<String>> completions = new HashMap<>();
      for (String request: new String[] { "request-a", "request-b" }) {
        Map<String, Object> records = new HashMap<>();
        CompletableFuture<String> completion = new CompletableFuture<>();
        results.put(request, records);
        completions.put(request, completion);
        fixture.inContext(request, () -> fixture.startStreaming(compute, new StreamingCallback<String, Object>() {
          @Override
          public void onRecordReceived(String key, Object value) {
            assertEquals(fixture.context.get(), request);
            assertNull(records.put(key, value));
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            assertFalse(exception.isPresent());
            assertTrue(completion.complete(fixture.context.get()));
          }
        }));
      }
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      fixture.onWorker(() -> fixture.scheduled.get(1).fire());
      assertEquals(
          fixture.dispatchContexts,
          java.util.Arrays.asList(
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
      for (String request: results.keySet()) {
        assertEquals(completions.get(request).get(5, TimeUnit.SECONDS), request);
        assertEquals(results.get(request).keySet(), fixture.keys);
        for (Object value: results.get(request).values()) {
          if (compute) {
            assertEquals(((ComputeGenericRecord) value).get("name").toString(), "context-value");
          } else {
            assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
          }
        }
      }
      assertEquals(fixture.contextCaptures.get(), 2, "Separate requests need separate snapshots");
      assertEquals(fixture.contextScopes.get(), compute ? 22 : 14);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testMultiRouteTransportCancellationKeepsRequestContext(boolean compute) throws Exception {
    try (ContextFixture fixture = new ContextFixture(10, 2)) {
      CompletableFuture<String> completion = new CompletableFuture<>();
      fixture.inContext("request", () -> fixture.startStreaming(compute, new StreamingCallback<String, Object>() {
        @Override
        public void onRecordReceived(String key, Object value) {
          fail("Cancelled transports must not return records");
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          assertTrue(exception.isPresent());
          completion.complete(fixture.context.get());
        }
      }, false));
      fixture.onWorker(() -> fixture.scheduled.get(0).fire());
      for (CompletableFuture<TransportClientResponse> attempt: fixture.attempts) {
        fixture.onWorker(() -> assertTrue(attempt.cancel(false)));
        fixture.drainDeserialization();
      }
      assertEquals(completion.get(5, TimeUnit.SECONDS), "request");
      assertEquals(fixture.dispatchContexts, Collections.nCopies(4, "request"));
      assertEquals(fixture.contextCaptures.get(), 1);
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
          contextScopes.incrementAndGet();
          inContext(captured, task);
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
      if (value == null) {
        context.remove();
      } else {
        context.set(value);
      }
      try {
        task.run();
      } finally {
        if (previous == null) {
          context.remove();
        } else {
          context.set(previous);
        }
      }
    }

    void onWorker(Runnable task) throws Exception {
      worker.submit(() -> inContext("worker-prior-context", () -> {
        try {
          task.run();
        } finally {
          assertEquals(context.get(), "worker-prior-context");
        }
      })).get(5, TimeUnit.SECONDS);
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

    TransportClientResponse singleResponse() {
      return new TransportClientResponse(
          1,
          CompressionStrategy.NO_OP,
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(STORE_VALUE_SCHEMA)
              .serialize(SINGLE_GET_VALUE_RESPONSE));
    }

    void startStreaming(boolean compute, StreamingCallback<String, Object> callback) {
      startStreaming(compute, callback, true);
    }

    void startStreaming(boolean compute, StreamingCallback<String, Object> callback, boolean partialSuccessAllowed) {
      startStreaming(compute, callback, partialSuccessAllowed, true);
    }

    void startStreaming(
        boolean compute,
        StreamingCallback<String, Object> callback,
        boolean partialSuccessAllowed,
        boolean withRetry) {
      InternalAvroStoreClient<String, GenericRecord> client = withRetry ? retryClient : dispatchClient;
      if (compute) {
        ComputeRequestWrapper request = new ComputeRequestWrapper(
            1,
            STORE_VALUE_SCHEMA,
            COMPUTE_PROJECTION_VALUE_SCHEMA,
            loadSchemaFileAsString("TestRecordProjection.avsc"),
            Collections.emptyList(),
            true);
        client.compute(
            new ComputeRequestContext<>(keys.size(), partialSuccessAllowed),
            request,
            keys,
            COMPUTE_PROJECTION_VALUE_SCHEMA,
            new StreamingCallback<String, ComputeGenericRecord>() {
              @Override
              public void onRecordReceived(String key, ComputeGenericRecord value) {
                callback.onRecordReceived(key, value);
              }

              @Override
              public void onCompletion(Optional<Exception> exception) {
                callback.onCompletion(exception);
              }
            },
            System.nanoTime());
      } else {
        client.streamingBatchGet(
            new BatchGetRequestContext<>(keys.size(), partialSuccessAllowed),
            keys,
            new StreamingCallback<String, GenericRecord>() {
              @Override
              public void onRecordReceived(String key, GenericRecord value) {
                callback.onRecordReceived(key, value);
              }

              @Override
              public void onCompletion(Optional<Exception> exception) {
                callback.onCompletion(exception);
              }
            });
      }
    }

    TransportClientResponse streamingResponse(boolean compute) {
      return streamingResponse(compute, keys.size());
    }

    TransportClientResponse streamingResponse(boolean compute, int keyCount) {
      byte[] body;
      if (compute) {
        GenericRecord value = new GenericData.Record(COMPUTE_PROJECTION_VALUE_SCHEMA);
        value.put("name", "context-value");
        value.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
        byte[] serializedValue =
            FastSerializerDeserializerFactory.getFastAvroGenericSerializer(COMPUTE_PROJECTION_VALUE_SCHEMA)
                .serialize(value);
        List<ComputeResponseRecordV1> records = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
          ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
          record.keyIndex = i;
          record.value = ByteBuffer.wrap(serializedValue);
          records.add(record);
        }
        body = FastSerializerDeserializerFactory
            .<ComputeResponseRecordV1>getFastAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$)
            .serializeObjects(records);
      } else {
        byte[] serializedValue = singleResponse().getBody();
        List<MultiGetResponseRecordV1> records = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
          MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
          record.keyIndex = i;
          record.schemaId = 1;
          record.value = ByteBuffer.wrap(serializedValue);
          records.add(record);
        }
        body = FastSerializerDeserializerFactory
            .<MultiGetResponseRecordV1>getFastAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$)
            .serializeObjects(records);
      }
      return new TransportClientResponse(1, CompressionStrategy.NO_OP, body);
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
