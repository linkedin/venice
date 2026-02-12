package com.linkedin.venice.fastclient;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;
import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.schema.Utils.loadSchemaFileAsString;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
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
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
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
import java.util.concurrent.TimeoutException;
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
  private static final int TEST_TIMEOUT = 5 * Time.MS_PER_SECOND;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private static final long LONG_TAIL_RETRY_THRESHOLD_IN_MS = 100L;// 100ms for single get
  // Batch get uses dynamic thresholds: "1-5:15,6-20:30,21-150:50,151-500:100,501-:500"
  // For 2 keys (BATCH_GET_KEYS size), threshold is 15ms (from "1-5:15" range)
  private static final long BATCH_GET_LONG_TAIL_RETRY_THRESHOLD_IN_MS = 15L;
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
    timeoutProcessor = new TimeoutProcessor(null, true, 1);
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
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      assertTrue(metrics.get(finalMetricsPrefix + "request.OccurrenceRate").value() > 0);
    });
    assertEquals(metrics.get(metricsPrefix + "request_key_count.Max").value(), expectedKeyCount);

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
        assertEquals(batchGetRequestContext.retryContext.retryRequestContext.numKeysInRequest, (int) expectedKeyCount);

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
            "." + RetriableAvroGenericStoreClient.SINGLE_KEY_LONG_TAIL_RETRY_STATS_PREFIX + clientConfig.getStoreName()
                + "--retry_limit_per_seconds.Gauge";
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

    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            thresholdMs * 10, // Original request exceeds threshold but completes
            false,
            thresholdMs * 50, // Retry is slower, so original wins
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

    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(
            false,
            thresholdMs * 10, // Original request is slow, exceeds threshold
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

    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(false, 10 * thresholdMs, true, 0, false, false, clientConfig),
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

    retriableClient = new RetriableAvroGenericStoreClient<>(
        prepareDispatchingClient(true, 10 * thresholdMs, true, 0, false, false, clientConfig),
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
   * We use reflection to replace the internal RetryManager with a mock that always denies retries,
   * ensuring deterministic reproduction regardless of timing.
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

    // Use reflection to replace the internal singleKeyLongTailRetryManager with a mock
    // that always returns false for isRetryAllowed() (simulating exhausted budget)
    com.linkedin.venice.meta.RetryManager mockRetryManager = mock(com.linkedin.venice.meta.RetryManager.class);
    doReturn(false).when(mockRetryManager).isRetryAllowed();
    doReturn(false).when(mockRetryManager).isRetryAllowed(org.mockito.ArgumentMatchers.anyInt());

    java.lang.reflect.Field retryManagerField =
        RetriableAvroGenericStoreClient.class.getDeclaredField("singleKeyLongTailRetryManager");
    retryManagerField.setAccessible(true);
    retryManagerField.set(retriableClient, mockRetryManager);

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
   * Uses reflection to mock the RetryManager for deterministic reproduction.
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

    // Use reflection to replace the RetryManager with a mock that always denies retries
    com.linkedin.venice.meta.RetryManager mockRetryManager = mock(com.linkedin.venice.meta.RetryManager.class);
    doReturn(false).when(mockRetryManager).isRetryAllowed();
    doReturn(false).when(mockRetryManager).isRetryAllowed(org.mockito.ArgumentMatchers.anyInt());

    java.lang.reflect.Field retryManagerField =
        RetriableAvroGenericStoreClient.class.getDeclaredField("singleKeyLongTailRetryManager");
    retryManagerField.setAccessible(true);
    retryManagerField.set(retryClient, mockRetryManager);

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
}
