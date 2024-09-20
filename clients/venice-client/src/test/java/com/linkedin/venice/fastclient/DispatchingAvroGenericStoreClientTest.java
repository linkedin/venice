package com.linkedin.venice.fastclient;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.KEY_SCHEMA;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.REPLICA1_NAME;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.REPLICA2_NAME;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.getMockR2Client;
import static com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils.getMockRouterBackedSchemaReader;
import static com.linkedin.venice.schema.Utils.loadSchemaFileAsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.fastclient.meta.RequestBasedMetadataTestUtils;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.TransportClientResponseForRoute;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class DispatchingAvroGenericStoreClientTest {
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final Schema STORE_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileAsString("TestRecord.avsc"));
  private static final RandomRecordGenerator rrg = new RandomRecordGenerator();
  private static final GenericRecord SINGLE_GET_VALUE_RESPONSE = (GenericRecord) rrg.randomGeneric(STORE_VALUE_SCHEMA);
  private static final String STORE_NAME = "test_store";
  private static final Set<String> BATCH_GET_KEYS = new HashSet<>();
  private static final Set<String> BATCH_GET_PARTIAL_KEYS_1 = new HashSet<>();
  private static final Set<String> BATCH_GET_PARTIAL_KEYS_2 = new HashSet<>();
  private static final Map<String, GenericRecord> BATCH_GET_VALUE_RESPONSE = new HashMap<>();
  private static final RecordSerializer VALUE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(STORE_VALUE_SCHEMA);
  private static final RecordSerializer MULTI_GET_RESPONSE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);

  private static final Schema COMPUTE_PROJECTION_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(loadSchemaFileAsString("TestRecordProjection.avsc"));
  private static final Set<String> COMPUTE_REQUEST_KEYS = new HashSet<>();
  private static final Set<String> COMPUTE_REQUEST_PARTIAL_KEYS_1 = new HashSet<>();
  private static final Set<String> COMPUTE_REQUEST_PARTIAL_KEYS_2 = new HashSet<>();
  private static final Map<String, GenericRecord> COMPUTE_REQUEST_VALUE_RESPONSE = new HashMap<>();
  private static final RecordSerializer COMPUTE_RESPONSE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);

  private ClientConfig.ClientConfigBuilder clientConfigBuilder;
  private ClientConfig clientConfig;
  private DispatchingAvroGenericStoreClient dispatchingAvroGenericStoreClient;
  private StatsAvroGenericStoreClient statsAvroGenericStoreClient = null;
  private Map<String, ? extends Metric> metrics;
  private StoreMetadata storeMetadata = null;
  private TransportClient mockedTransportClient;

  @BeforeClass
  public void setUp() {
    BATCH_GET_KEYS.add("test_key_1");
    BATCH_GET_KEYS.add("test_key_2");
    BATCH_GET_PARTIAL_KEYS_1.add("test_key_1");
    BATCH_GET_PARTIAL_KEYS_2.add("test_key_2");
    GenericRecord value1 = (GenericRecord) rrg.randomGeneric(STORE_VALUE_SCHEMA);
    GenericRecord value2 = (GenericRecord) rrg.randomGeneric(STORE_VALUE_SCHEMA);
    BATCH_GET_VALUE_RESPONSE.put("test_key_1", value1);
    BATCH_GET_VALUE_RESPONSE.put("test_key_2", value2);
    COMPUTE_REQUEST_KEYS.add("test_key_1");
    COMPUTE_REQUEST_KEYS.add("test_key_2");
    COMPUTE_REQUEST_PARTIAL_KEYS_1.add("test_key_1");
    COMPUTE_REQUEST_PARTIAL_KEYS_2.add("test_key_2");

    GenericRecord projectionResultForKey1 = new GenericData.Record(COMPUTE_PROJECTION_VALUE_SCHEMA);
    projectionResultForKey1.put("name", "TEST_NAME_1");
    projectionResultForKey1.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
    COMPUTE_REQUEST_VALUE_RESPONSE.put("test_key_1", projectionResultForKey1);

    GenericRecord projectionResultForKey2 = new GenericData.Record(COMPUTE_PROJECTION_VALUE_SCHEMA);
    projectionResultForKey2.put("name", "TEST_NAME_2");
    projectionResultForKey2.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
    COMPUTE_REQUEST_VALUE_RESPONSE.put("test_key_2", projectionResultForKey2);
  }

  private void setUpClient() throws InterruptedException {
    setUpClient(false, false, false);
  }

  private void setUpClient(
      boolean transportClientThrowsException,
      boolean transportClientThrowsPartialException,
      boolean transportClientPartialIncomplete) throws InterruptedException {
    setUpClient(
        transportClientThrowsException,
        transportClientThrowsPartialException,
        transportClientPartialIncomplete,
        true,
        TimeUnit.SECONDS.toMillis(30));
  }

  /**
   * @param transportClientThrowsException throws exception for both the keys
   * @param transportClientThrowsPartialException responds correct value for the 1st key and throws exception for the 2nd key
   * @param transportClientPartialIncomplete responds correct value for the 1st key and not do anything for 2nd key
   * @param mockTransportClient mock the transport client to be able to respond with the actual values or exception.
   *                            If not, the hosts won't be reachable as it's not setup to be reachable.
   * @param routingLeakedRequestCleanupThresholdMS time to set routingLeakedRequestCleanupThresholdMS client config.
   */
  private void setUpClient(
      boolean transportClientThrowsException,
      boolean transportClientThrowsPartialException,
      boolean transportClientPartialIncomplete,
      boolean mockTransportClient,
      long routingLeakedRequestCleanupThresholdMS) throws InterruptedException {

    clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(getMockR2Client(false))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery")
        .setMetadataRefreshIntervalInSeconds(1L)
        .setRoutingLeakedRequestCleanupThresholdMS(routingLeakedRequestCleanupThresholdMS)
        .setRoutingPendingRequestCounterInstanceBlockThreshold(1);

    MetricsRepository metricsRepository = new MetricsRepository();
    metrics = metricsRepository.metrics();

    clientConfigBuilder.setMetricsRepository(metricsRepository);
    clientConfig = clientConfigBuilder.build();

    storeMetadata = RequestBasedMetadataTestUtils.getMockMetaData(
        clientConfig,
        STORE_NAME,
        getMockRouterBackedSchemaReader(),
        false,
        false,
        false,
        null,
        AvroCompatibilityHelper.parse(KEY_SCHEMA),
        STORE_VALUE_SCHEMA);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();

    mockedTransportClient = null;
    if (mockTransportClient) {
      mockedTransportClient = mock(TransportClient.class);
      dispatchingAvroGenericStoreClient =
          new DispatchingAvroGenericStoreClient(storeMetadata, clientConfig, mockedTransportClient);
    } else {
      dispatchingAvroGenericStoreClient = new DispatchingAvroGenericStoreClient(storeMetadata, clientConfig);
    }
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(dispatchingAvroGenericStoreClient, clientConfig);
    statsAvroGenericStoreClient.start();
    // metadata should be initialized after start()
    dispatchingAvroGenericStoreClient.verifyMetadataInitialized();

    if (mockTransportClient) {
      // mock get()
      doReturn(valueFuture).when(mockedTransportClient).get(any());
      if (transportClientThrowsException) {
        valueFuture.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
      } else {
        // not doing anything for transportClientThrowsPartialException for batchGet with useStreamingBatchGetAsDefault
        // case
        TransportClientResponse singleGetResponse = new TransportClientResponse(
            1,
            CompressionStrategy.NO_OP,
            SerializerDeserializerFactory.getAvroGenericSerializer(STORE_VALUE_SCHEMA)
                .serialize(SINGLE_GET_VALUE_RESPONSE));
        valueFuture.complete(singleGetResponse);
      }

      // mock post()
      CompletableFuture<TransportClientResponseForRoute> batchGetValueFuture0 = new CompletableFuture<>();
      CompletableFuture<TransportClientResponseForRoute> batchGetValueFuture1 = new CompletableFuture<>();
      TransportClientResponseForRoute batchGetResponse0, batchGetResponse1;

      CompletableFuture<TransportClientResponseForRoute> computeResponseValueFuture0 = new CompletableFuture<>();
      CompletableFuture<TransportClientResponseForRoute> computeResponseValueFuture1 = new CompletableFuture<>();
      TransportClientResponseForRoute computeResponse0, computeResponse1;

      if (transportClientThrowsException) {
        doReturn(batchGetValueFuture0).when(mockedTransportClient).post(any(), any(), any());
        batchGetValueFuture0.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
        batchGetValueFuture1.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
        computeResponseValueFuture0
            .completeExceptionally(new VeniceClientException("Exception for client to return 503"));
        computeResponseValueFuture1
            .completeExceptionally(new VeniceClientException("Exception for client to return 503"));
      } else if (transportClientThrowsPartialException || transportClientPartialIncomplete) {
        // return valid response for 1 route(1 key) and exception for the other
        batchGetResponse0 = new TransportClientResponseForRoute(
            "0",
            1,
            CompressionStrategy.NO_OP,
            serializeBatchGetResponse(BATCH_GET_PARTIAL_KEYS_1),
            mock(CompletableFuture.class));
        doReturn(batchGetValueFuture0).when(mockedTransportClient)
            .post(eq(REPLICA1_NAME + "/storage/test_store_v1"), any(), any());
        batchGetValueFuture0.complete(batchGetResponse0);
        doReturn(batchGetValueFuture1).when(mockedTransportClient)
            .post(eq(REPLICA2_NAME + "/storage/test_store_v1"), any(), any());

        computeResponse0 = new TransportClientResponseForRoute(
            "0",
            1,
            CompressionStrategy.NO_OP,
            serializeComputeResponse(COMPUTE_REQUEST_PARTIAL_KEYS_1),
            mock(CompletableFuture.class));
        doReturn(computeResponseValueFuture0).when(mockedTransportClient)
            .post(eq(REPLICA1_NAME + "/compute/test_store_v1"), any(), any());
        computeResponseValueFuture0.complete(computeResponse0);
        doReturn(computeResponseValueFuture1).when(mockedTransportClient)
            .post(eq(REPLICA2_NAME + "/compute/test_store_v1"), any(), any());

        if (!transportClientPartialIncomplete) {
          batchGetValueFuture1.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
          computeResponseValueFuture1
              .completeExceptionally(new VeniceClientException("Exception for client to return 503"));
        }
      } else {
        batchGetResponse0 = new TransportClientResponseForRoute(
            "0",
            1,
            CompressionStrategy.NO_OP,
            serializeBatchGetResponse(BATCH_GET_PARTIAL_KEYS_1),
            mock(CompletableFuture.class));
        batchGetResponse1 = new TransportClientResponseForRoute(
            "1",
            1,
            CompressionStrategy.NO_OP,
            serializeBatchGetResponse(BATCH_GET_PARTIAL_KEYS_2),
            mock(CompletableFuture.class));
        doReturn(batchGetValueFuture0).when(mockedTransportClient)
            .post(eq(REPLICA1_NAME + "/storage/test_store_v1"), any(), any());
        batchGetValueFuture0.complete(batchGetResponse0);
        doReturn(batchGetValueFuture1).when(mockedTransportClient)
            .post(eq(REPLICA2_NAME + "/storage/test_store_v1"), any(), any());
        batchGetValueFuture1.complete(batchGetResponse1);

        computeResponse0 = new TransportClientResponseForRoute(
            "0",
            1,
            CompressionStrategy.NO_OP,
            serializeComputeResponse(COMPUTE_REQUEST_PARTIAL_KEYS_1),
            mock(CompletableFuture.class));
        computeResponse1 = new TransportClientResponseForRoute(
            "1",
            1,
            CompressionStrategy.NO_OP,
            serializeComputeResponse(COMPUTE_REQUEST_PARTIAL_KEYS_2),
            mock(CompletableFuture.class));
        doReturn(computeResponseValueFuture0).when(mockedTransportClient)
            .post(eq(REPLICA1_NAME + "/compute/test_store_v1"), any(), any());
        computeResponseValueFuture0.complete(computeResponse0);
        doReturn(computeResponseValueFuture1).when(mockedTransportClient)
            .post(eq(REPLICA2_NAME + "/compute/test_store_v1"), any(), any());
        computeResponseValueFuture1.complete(computeResponse1);
      }
    }
  }

  private void tearDown() throws IOException {
    if (storeMetadata != null) {
      storeMetadata.close();
      storeMetadata = null;
    }
    if (statsAvroGenericStoreClient != null) {
      statsAvroGenericStoreClient.close();
      statsAvroGenericStoreClient = null;
    }
  }

  /**
   * Not using retry in these tests, so none of the retry metrics should be incremented
   */
  private void validateRetryMetrics(
      GetRequestContext getRequestContext,
      BatchGetRequestContext batchGetRequestContext,
      boolean batchGet,
      boolean computeRequest,
      String metricPrefix) {
    if (batchGet) {
      assertNull(batchGetRequestContext.retryContext);
    } else if (computeRequest) {
      // Do nothing since we don't have the ComputeRequestContext to test
    } else {
      assertNull(getRequestContext.retryContext);
    }

    assertFalse(metrics.get(metricPrefix + "error_retry_request.OccurrenceRate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "retry_request_key_count.Rate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "retry_request_success_key_count.Rate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "retry_request_win.OccurrenceRate").value() > 0);
  }

  private void validateSingleGetMetrics(GetRequestContext getRequestContext, boolean healthyRequest) {
    validateMetrics(getRequestContext, null, healthyRequest, false, RequestType.SINGLE_GET, false, 1, 0);
  }

  private void validateMultiGetMetrics(
      BatchGetRequestContext batchGetRequestContext,
      boolean healthyRequest,
      boolean partialHealthyRequest,
      RequestType requestType,
      boolean noAvailableReplicas,
      int numKeys) {

    validateMetrics(
        null,
        batchGetRequestContext,
        healthyRequest,
        partialHealthyRequest,
        requestType,
        noAvailableReplicas,
        numKeys,
        2);
  }

  private void validateMultiGetMetrics(
      BatchGetRequestContext batchGetRequestContext,
      boolean healthyRequest,
      boolean partialHealthyRequest,
      RequestType requestType,
      boolean noAvailableReplicas,
      int numKeys,
      double numBlockedReplicas) {
    validateMetrics(
        null,
        batchGetRequestContext,
        healthyRequest,
        partialHealthyRequest,
        requestType,
        noAvailableReplicas,
        numKeys,
        numBlockedReplicas);
  }

  private void validateComputeRequestMetrics(
      boolean healthyRequest,
      boolean partialHealthyRequest,
      RequestType requestType,
      boolean noAvailableReplicas,
      int numKeys,
      double numBlockedReplicas) {
    validateMetrics(
        null,
        null,
        healthyRequest,
        partialHealthyRequest,
        requestType,
        noAvailableReplicas,
        numKeys,
        numBlockedReplicas);
  }

  private void checkRouteMetricForSingleGet(
      String metricName1,
      String metricName2,
      Consumer<Double> conditionCheckFunc) {
    assertTrue(
        metrics.containsKey(metricName1) || metrics.containsKey(metricName2),
        "At least one of the metric must exist for single get metric validation: " + metricName1 + ", " + metricName2);

    for (String metricName: Arrays.asList(metricName1, metricName2)) {
      Metric metric = metrics.get(metricName);
      if (metric != null) {
        conditionCheckFunc.accept(metric.value());
      }
    }
  }

  private void checkRouteMetricForBatchGet(
      String metricName1,
      String metricName2,
      Consumer<Double> conditionCheckFunc) {
    assertTrue(
        metrics.containsKey(metricName1) && metrics.containsKey(metricName2),
        "Metrics must exist for batch get metric validation: " + metricName1 + ", " + metricName2);

    for (String metricName: Arrays.asList(metricName1, metricName2)) {
      Metric metric = metrics.get(metricName);
      if (metric != null) {
        conditionCheckFunc.accept(metric.value());
      }
    }
  }

  private void validateMetrics(
      GetRequestContext getRequestContext,
      BatchGetRequestContext batchGetRequestContext,
      boolean healthyRequest,
      boolean partialHealthyRequest,
      RequestType requestType,
      boolean noAvailableReplicas,
      int numKeys,
      double numBlockedReplicas) {
    String metricPrefix = ClientTestUtils.getMetricPrefix(STORE_NAME, requestType);
    boolean batchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    boolean computeRequest = requestType == RequestType.COMPUTE || requestType == RequestType.COMPUTE_STREAMING;

    String routeMetricsPrefix = "." + STORE_NAME;
    double successKeyCount;
    double requestKeyCount = numKeys;
    if (partialHealthyRequest) {
      // batchGet and partialHealthyRequest: 1 request is unsuccessful
      successKeyCount = numKeys - 1;
    } else {
      successKeyCount = numKeys;
    }
    assertTrue(metrics.get(metricPrefix + "request.OccurrenceRate").value() > 0);
    assertEquals(metrics.get(metricPrefix + "request_key_count.Max").value(), requestKeyCount);
    if (healthyRequest) {
      assertTrue(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get(metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      assertEquals(metrics.get(metricPrefix + "success_request_key_count.Max").value(), successKeyCount);
      if (batchGet) {
        assertTrue(metrics.get(metricPrefix + "response_ttfr.Avg").value() > 0);
        assertTrue(metrics.get(metricPrefix + "response_tt50pr.Avg").value() > 0);
        assertTrue(metrics.get(metricPrefix + "response_tt90pr.Avg").value() > 0);
        assertTrue(metrics.get(metricPrefix + "response_tt95pr.Avg").value() > 0);
        assertTrue(metrics.get(metricPrefix + "response_tt99pr.Avg").value() > 0);
        assertEquals(batchGetRequestContext.successRequestKeyCount.get(), (int) successKeyCount);

        // Check route metrics for batch-get
        String replica1RouterMetricPrefix =
            "." + RequestBasedMetadataTestUtils.SERVER_D2_SERVICE + "_" + REPLICA1_NAME + "--multiget_streaming_";
        String replica2RouterMetricPrefix =
            "." + RequestBasedMetadataTestUtils.SERVER_D2_SERVICE + "_" + REPLICA2_NAME + "--multiget_streaming_";

        checkRouteMetricForBatchGet(
            replica1RouterMetricPrefix + "healthy_request_count.OccurrenceRate",
            replica2RouterMetricPrefix + "healthy_request_count.OccurrenceRate",
            v -> assertTrue(v > 0));
        checkRouteMetricForBatchGet(
            replica1RouterMetricPrefix + "response_waiting_time.99thPercentile",
            replica2RouterMetricPrefix + "response_waiting_time.99thPercentile",
            v -> assertTrue(v > 0));
        checkRouteMetricForBatchGet(
            replica1RouterMetricPrefix + "quota_exceeded_request_count.OccurrenceRate",
            replica2RouterMetricPrefix + "quota_exceeded_request_count.OccurrenceRate",
            v -> assertTrue(v == 0.0d));

      } else if (computeRequest) {
        // Do nothing since we don't have the ComputeRequestContext to test
      } else {
        assertEquals(getRequestContext.successRequestKeyCount.get(), (int) successKeyCount);

        // Check route metrics for single-get
        String replica1RouterMetricPrefix = "." + RequestBasedMetadataTestUtils.SERVER_D2_SERVICE + "_" + REPLICA1_NAME;
        String replica2RouterMetricPrefix = "." + RequestBasedMetadataTestUtils.SERVER_D2_SERVICE + "_" + REPLICA2_NAME;
        checkRouteMetricForSingleGet(
            replica1RouterMetricPrefix + "--healthy_request_count.OccurrenceRate",
            replica2RouterMetricPrefix + "--healthy_request_count.OccurrenceRate",
            v -> assertTrue(v > 0));
        checkRouteMetricForSingleGet(
            replica1RouterMetricPrefix + "--response_waiting_time.99thPercentile",
            replica2RouterMetricPrefix + "--response_waiting_time.99thPercentile",
            v -> assertTrue(v > 0));
        checkRouteMetricForSingleGet(
            replica1RouterMetricPrefix + "--quota_exceeded_request_count.OccurrenceRate",
            replica2RouterMetricPrefix + "--quota_exceeded_request_count.OccurrenceRate",
            v -> assertTrue(v == 0.0d));
      }
    } else if (partialHealthyRequest) {
      assertFalse(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertTrue(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get(metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      // as partial healthy request is still considered unhealthy, not incrementing the below metric
      assertFalse(metrics.get(metricPrefix + "success_request_key_count.Max").value() > 0);
      if (batchGet) {
        assertTrue(metrics.get(metricPrefix + "response_ttfr.Avg").value() > 0);
        assertEquals(batchGetRequestContext.successRequestKeyCount.get(), (int) successKeyCount);
      } else if (computeRequest) {
        // Do nothing since we don't have the ComputeRequestContext to test
      } else {
        assertEquals(getRequestContext.successRequestKeyCount.get(), (int) successKeyCount);
      }
    } else {
      assertFalse(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertTrue(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get(metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get(metricPrefix + "success_request_key_count.Max").value() > 0);
      if (batchGet) {
        assertEquals(batchGetRequestContext.successRequestKeyCount.get(), 0);
      } else if (computeRequest) {
        // Do nothing since we don't have the ComputeRequestContext to test
      } else {
        assertEquals(getRequestContext.successRequestKeyCount.get(), 0);
      }
    }

    if (noAvailableReplicas) {
      assertTrue(metrics.get(metricPrefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
      assertEquals(metrics.get(routeMetricsPrefix + "--blocked_instance_count.Max").value(), numBlockedReplicas);
      if (batchGet) {
        assertTrue(batchGetRequestContext.noAvailableReplica);
      } else if (computeRequest) {
        // Do nothing since we don't have the ComputeRequestContext to test
      } else {
        assertTrue(getRequestContext.noAvailableReplica);
      }
    } else {
      assertFalse(metrics.get(metricPrefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
      if (batchGet) {
        assertFalse(batchGetRequestContext.noAvailableReplica);
      } else if (computeRequest) {
        // Do nothing since we don't have the ComputeRequestContext to test
      } else {
        assertFalse(getRequestContext.noAvailableReplica);
      }
    }
    validateRetryMetrics(getRequestContext, batchGetRequestContext, batchGet, computeRequest, metricPrefix);
  }

  private byte[] serializeBatchGetResponse(Set<String> Keys) {
    List<MultiGetResponseRecordV1> routerRequestValues = new ArrayList<>(Keys.size());
    AtomicInteger count = new AtomicInteger();
    Keys.stream().forEach(key -> {
      MultiGetResponseRecordV1 routerRequestValue = new MultiGetResponseRecordV1();
      byte[] valueBytes = VALUE_SERIALIZER.serialize(BATCH_GET_VALUE_RESPONSE.get(key));
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueBytes);
      routerRequestValue.setValue(valueByteBuffer);
      routerRequestValue.keyIndex = count.getAndIncrement();
      routerRequestValues.add(routerRequestValue);
      routerRequestValue.setSchemaId(1);
    });
    return MULTI_GET_RESPONSE_SERIALIZER.serializeObjects(routerRequestValues);
  }

  private byte[] serializeComputeResponse(Set<String> Keys) {
    // Mock a transport client response
    RecordSerializer<GenericRecord> resultSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(COMPUTE_PROJECTION_VALUE_SCHEMA);
    List<ComputeResponseRecordV1> routerRequestValues = new ArrayList<>(Keys.size());
    AtomicInteger count = new AtomicInteger();
    Keys.stream().forEach(key -> {
      ComputeResponseRecordV1 routerRequestValue = new ComputeResponseRecordV1();
      byte[] valueBytes = resultSerializer.serialize(COMPUTE_REQUEST_VALUE_RESPONSE.get(key));
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueBytes);
      routerRequestValue.setValue(valueByteBuffer);
      routerRequestValue.keyIndex = count.getAndIncrement();
      routerRequestValues.add(routerRequestValue);
    });
    return COMPUTE_RESPONSE_SERIALIZER.serializeObjects(routerRequestValues);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGet() throws ExecutionException, InterruptedException, IOException {
    try {
      setUpClient();
      GetRequestContext getRequestContext = new GetRequestContext();
      GenericRecord value = (GenericRecord) statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
      validateSingleGetMetrics(getRequestContext, true);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetWithExceptionFromTransportLayer() throws IOException {
    GetRequestContext getRequestContext = null;
    try {
      setUpClient(true, false, false);
      getRequestContext = new GetRequestContext();
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get().toString();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("Exception for client to return 503"), e.getMessage());
      validateSingleGetMetrics(getRequestContext, false);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetToUnreachableClient() throws IOException {
    GetRequestContext getRequestContext = null;
    try {
      setUpClient(false, false, false, false, 2 * Time.MS_PER_SECOND);
      getRequestContext = new GetRequestContext();
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("http status: 410, Request timed out"), e.getMessage());
      validateSingleGetMetrics(getRequestContext, false);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchGet() throws ExecutionException, InterruptedException, IOException {
    try {
      setUpClient();
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      Map<String, GenericRecord> value =
          (Map<String, GenericRecord>) statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();
      BATCH_GET_KEYS.stream().forEach(key -> {
        assertTrue(BATCH_GET_VALUE_RESPONSE.get(key).equals(value.get(key)));
      });
      validateMultiGetMetrics(batchGetRequestContext, true, false, RequestType.MULTI_GET, false, 2);
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testBatchGetWithEmptyKeys(boolean streamingBatchGet)
      throws ExecutionException, InterruptedException, IOException {
    try {
      setUpClient();
      BatchGetRequestContext batchGetRequestContext;
      Map<String, GenericRecord> value;
      if (streamingBatchGet) {
        batchGetRequestContext = new BatchGetRequestContext<>(0, true);
        value = (Map<String, GenericRecord>) statsAvroGenericStoreClient
            .streamingBatchGet(batchGetRequestContext, Collections.emptySet())
            .get();
      } else {
        batchGetRequestContext = new BatchGetRequestContext<>(0, false);
        value = (Map<String, GenericRecord>) statsAvroGenericStoreClient
            .batchGet(batchGetRequestContext, Collections.emptySet())
            .get();
      }
      assertTrue(value.isEmpty());
      String metricPrefix = "." + STORE_NAME + "--multiget_streaming_";
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertTrue(metrics.get(metricPrefix + "request.OccurrenceRate").value() > 0);
        assertTrue(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
        assertFalse(metrics.get(metricPrefix + "request_key_count.Max").value() > 0);
        assertFalse(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      });
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchGetWithMoreKeysThanMaxSize() throws InterruptedException, IOException {
    try {
      setUpClient();
      int numKeysInRequest = Store.DEFAULT_BATCH_GET_LIMIT + 1;
      Set<String> keys = new HashSet<>(numKeysInRequest);
      for (int i = 0; i < numKeysInRequest; ++i) {
        keys.add("testKey" + i);
      }
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(numKeysInRequest, false);
      CompletableFuture future = statsAvroGenericStoreClient.batchGet(batchGetRequestContext, keys);
      try {
        future.get();
        fail(VeniceKeyCountLimitException.class.getSimpleName() + " should be thrown");
      } catch (Exception e) {
        assertEquals(VeniceKeyCountLimitException.class, e.getCause().getClass());
      }
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchGetWithExceptionFromTransportLayer() throws IOException {
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(true, false, false);
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      validateMultiGetMetrics(batchGetRequestContext, false, false, RequestType.MULTI_GET, false, 2);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchGetWithExceptionFromTransportLayerForOneRoute() throws IOException {
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(false, true, false);
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET, false, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * Condition to test: batchGet API either returns full results or exception, but no partial results.
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: this test calls batchGet().get() without timeout, so waits till routingLeakedRequestCleanupThresholdMS
   *           times out and returns exception with "At least one route did not complete".
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*VeniceClientException: At least one route did not complete")
  public void testBatchGetWithTimeoutV1() throws IOException, ExecutionException, InterruptedException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          });
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET, false, 2);
      tearDown();
    }
  }

  /**
   * Condition to test: batchGet API either returns full results or exception, but no partial results.
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: routingLeakedRequestCleanupThresholdMS times out before batchGet().get(timeout),
   *            so returns exception with "At least one route did not complete".
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*VeniceClientException: At least one route did not complete")
  public void testBatchGetWithTimeoutV2()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get(2, TimeUnit.SECONDS);
      fail();
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          });
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET, false, 2);
      tearDown();
    }
  }

  /**
   * Condition to test: batchGet API either returns full results or exception, but no partial results.
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior:  batchGet().get(timeout) times out before routingLeakedRequestCleanupThresholdMS,
   *            so AppTimeOutTrackingCompletableFuture returns TimeoutException confirming no partial returns.
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = TimeoutException.class)
  public void testBatchGetWithTimeoutV3()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(2);
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get(1, TimeUnit.SECONDS);
      fail();
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          });
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET, false, 2);
      tearDown();
    }
  }

  /**
   * 1st batchGet(1 key) blocks one replica and 2nd batchGet(2 keys) returns value for only 1 key as the other
   * route is blocked, so batchGet() return an exception.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchGetWithTimeoutV4() throws IOException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      BatchGetRequestContext batchGetRequestContext =
          new BatchGetRequestContext<>(BATCH_GET_PARTIAL_KEYS_2.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_PARTIAL_KEYS_2).get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      BatchGetRequestContext batchGetRequestContext = null;
      try {
        batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
        statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
        fail();
      } catch (Exception e1) {
        assertTrue(e1.getMessage().endsWith("At least one route did not complete"), e1.getMessage());
        assertTrue(
            e1.getCause().getCause().getMessage().contains("No available route for"),
            e1.getCause().getCause().getMessage());
        // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
        TestUtils.waitForNonDeterministicAssertion(
            routingLeakedRequestCleanupThresholdMS + 1000,
            TimeUnit.MILLISECONDS,
            () -> {
              assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
            });
        validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET, true, 2, 1);
      }
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetHeaders() throws IOException, InterruptedException, ExecutionException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      CompletableFuture<VeniceResponseMap<String, GenericRecord>> future =
          statsAvroGenericStoreClient.streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS);
      VeniceResponseMap<String, GenericRecord> response = future.get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(response.get("test_key_1"), BATCH_GET_VALUE_RESPONSE.get("test_key_1"));
      ArgumentCaptor<Map<String, String>> headerCaptor = ArgumentCaptor.forClass(Map.class);
      verify(mockedTransportClient, atLeastOnce()).post(any(), headerCaptor.capture(), any());
      assertTrue(headerCaptor.getValue().containsKey(HttpConstants.VENICE_KEY_COUNT));
      assertEquals(
          headerCaptor.getValue().get(HttpConstants.VENICE_KEY_COUNT),
          Integer.toString(BATCH_GET_KEYS.size()));
    } finally {
      tearDown();
    }
  }

  /**
   * Condition to test: streamingBatchGet(keys) API returns partial results in case of future.get(timeout)
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: this test calls streamingBatchGet().get() without timeout, so waits till routingLeakedRequestCleanupThresholdMS
   *           times out and returns exception with "At least one route did not complete".
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetWithTimeoutV1() throws IOException, ExecutionException, InterruptedException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      CompletableFuture<VeniceResponseMap<String, GenericRecord>> future =
          statsAvroGenericStoreClient.streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS);
      VeniceResponseMap<String, GenericRecord> response = future.get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(response.get("test_key_1"), BATCH_GET_VALUE_RESPONSE.get("test_key_1"));
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          });
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET_STREAMING, false, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * Condition to test: streamingBatchGet(keys) API returns partial results in case of future.get(timeout)
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: routingLeakedRequestCleanupThresholdMS times out before streamingBatchGet().get(timeout),
   *            so returns exception with "At least one route did not complete".
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetWithTimeoutV2()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      CompletableFuture<VeniceResponseMap<String, GenericRecord>> future =
          statsAvroGenericStoreClient.streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS);
      VeniceResponseMap<String, GenericRecord> response = future.get(2, TimeUnit.SECONDS);
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(response.get("test_key_1"), BATCH_GET_VALUE_RESPONSE.get("test_key_1"));
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          });
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET_STREAMING, false, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * Condition to test: streamingBatchGet(keys) API returns partial results in case of future.get(timeout)
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: streamingBatchGet().get(timeout) times out before routingLeakedRequestCleanupThresholdMS,
   *           so returns partial response.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetWithTimeoutV3()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    try {
      long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(2);
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      CompletableFuture<VeniceResponseMap<String, GenericRecord>> future =
          statsAvroGenericStoreClient.streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS);
      VeniceResponseMap<String, GenericRecord> value = future.get(1, TimeUnit.SECONDS);
      assertEquals(value.size(), 1);
      assertFalse(value.isFullResponse());
      assertEquals(BATCH_GET_VALUE_RESPONSE.get("test_key_1"), value.get("test_key_1"));
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          });
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET_STREAMING, false, 2);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchGetToUnreachableClient() throws IOException {
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(false, false, false, false, TimeUnit.SECONDS.toMillis(1));
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } catch (Exception e) {
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      validateMultiGetMetrics(batchGetRequestContext, false, false, RequestType.MULTI_GET, false, 2);

      BatchGetRequestContext batchGetRequestContext2 = null;
      try {
        // the second batchGet is not going to find any routes (as the instances
        // are blocked) and fail instantly
        batchGetRequestContext2 = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), false);
        statsAvroGenericStoreClient.batchGet(batchGetRequestContext2, BATCH_GET_KEYS).get();
        fail();
      } catch (Exception e1) {
        assertTrue(e1.getMessage().endsWith("At least one route did not complete"), e1.getMessage());
        assertTrue(
            e1.getCause().getCause().getMessage().contains("No available route for store"),
            e1.getCause().getCause().getMessage());
        validateMultiGetMetrics(batchGetRequestContext2, false, false, RequestType.MULTI_GET, true, 2);
      }
    } finally {
      tearDown();
    }
  }

  /**
   * 1. 1st streamingBatchGet(2 keys) will result in both the routes marked as
   *    blocked(routingPendingRequestCounterInstanceBlockThreshold is 1).
   * 2. 2nd streamingBatchGet(2 keys) will result in both route getting no replica
   *    found leading to exception.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetToUnreachableClient() throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, false, false, false, TimeUnit.SECONDS.toMillis(1));
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      VeniceResponseMap<String, GenericRecord> response =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateMultiGetMetrics(batchGetRequestContext, false, false, RequestType.MULTI_GET_STREAMING, false, 2);

      // the second batchGet is not going to find any routes (as the instances
      // are blocked) and fail instantly
      BatchGetRequestContext batchGetRequestContext2 = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      VeniceResponseMap<String, GenericRecord> response2 =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext2, BATCH_GET_KEYS)
              .get();
      assertFalse(response2.isFullResponse());
      assertEquals(response2.getTotalEntryCount(), 0);
      validateMultiGetMetrics(batchGetRequestContext2, false, false, RequestType.MULTI_GET_STREAMING, true, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * 1. first streamingBatchGet() with 1 key getting timed out leading to the route getting
   *    blocked (routingPendingRequestCounterInstanceBlockThreshold = 1)
   * 2. second streamingBatchGet() with 2 keys (one key (same as above) failing with no available
   *    replica and one key getting timed out) => exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetToUnreachableClientV1()
      throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, false, false, false, TimeUnit.SECONDS.toMillis(1));
      BatchGetRequestContext batchGetRequestContext =
          new BatchGetRequestContext<>(BATCH_GET_PARTIAL_KEYS_1.size(), true);
      VeniceResponseMap<String, GenericRecord> response =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext, BATCH_GET_PARTIAL_KEYS_1)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateMultiGetMetrics(batchGetRequestContext, false, false, RequestType.MULTI_GET_STREAMING, false, 1);

      BatchGetRequestContext batchGetRequestContext2 = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      VeniceResponseMap<String, GenericRecord> response2 =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext2, BATCH_GET_KEYS)
              .get();
      assertFalse(response2.isFullResponse());
      assertEquals(response2.getTotalEntryCount(), 0);
      validateMultiGetMetrics(batchGetRequestContext2, false, false, RequestType.MULTI_GET_STREAMING, true, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * 1. first streamingBatchGet() with 1 key getting timed out leading to the route getting
   *    blocked (routingPendingRequestCounterInstanceBlockThreshold = 1)
   * 2. second streamingBatchGet() with 2 keys (one key (same as above) failing with no available
   *    replica and one key returning value) => no exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetToUnreachableClientV2()
      throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, false, true, true, TimeUnit.SECONDS.toMillis(1));
      BatchGetRequestContext batchGetRequestContext =
          new BatchGetRequestContext<>(BATCH_GET_PARTIAL_KEYS_2.size(), true);
      VeniceResponseMap<String, GenericRecord> response =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext, BATCH_GET_PARTIAL_KEYS_2)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateMultiGetMetrics(batchGetRequestContext, false, false, RequestType.MULTI_GET_STREAMING, false, 1);
      BatchGetRequestContext batchGetRequestContext2 = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      CompletableFuture<VeniceResponseMap<String, GenericRecord>> future =
          statsAvroGenericStoreClient.streamingBatchGet(batchGetRequestContext2, BATCH_GET_KEYS);
      VeniceResponseMap<String, GenericRecord> value = future.get();
      assertFalse(value.isFullResponse());
      // TODO metric validation: 1st get increments unhealthy metrics and the second get increments healthy
      // metrics
      // validateMultiGetMetrics(true, true, true, true, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * same as testStreamingBatchGetToUnreachableClientV2: transportClientThrowsPartialException instead of
   * transportClientPartialIncomplete.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetToUnreachableClientV3()
      throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, true, false, true, TimeUnit.SECONDS.toMillis(1));
      BatchGetRequestContext batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      VeniceResponseMap<String, GenericRecord> response =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(response.get("test_key_1"), BATCH_GET_VALUE_RESPONSE.get("test_key_1"));
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateMultiGetMetrics(batchGetRequestContext, false, true, RequestType.MULTI_GET_STREAMING, false, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * same as testStreamingBatchGetToUnreachableClientV3 but the transport mock throws exception
   * for both the routes.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingBatchGetToUnreachableClientV4()
      throws IOException, ExecutionException, InterruptedException {
    BatchGetRequestContext batchGetRequestContext = null;
    try {
      setUpClient(true, false, false, true, TimeUnit.SECONDS.toMillis(1));
      batchGetRequestContext = new BatchGetRequestContext<>(BATCH_GET_KEYS.size(), true);
      VeniceResponseMap<String, GenericRecord> response =
          (VeniceResponseMap<String, GenericRecord>) statsAvroGenericStoreClient
              .streamingBatchGet(batchGetRequestContext, BATCH_GET_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateMultiGetMetrics(batchGetRequestContext, false, false, RequestType.MULTI_GET_STREAMING, false, 2);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCompute(boolean streamingCompute) throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient();
      ComputeRequestBuilder requestBuilder = statsAvroGenericStoreClient.compute().project("name");
      Map<String, ComputeGenericRecord> computeResponse;
      if (streamingCompute) {
        computeResponse =
            (Map<String, ComputeGenericRecord>) requestBuilder.streamingExecute(COMPUTE_REQUEST_KEYS).get();
      } else {
        computeResponse = (Map<String, ComputeGenericRecord>) requestBuilder.execute(COMPUTE_REQUEST_KEYS).get();
      }

      Assert.assertEquals(computeResponse.size(), 2);
      Assert.assertEquals(
          computeResponse.get("test_key_1").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1").get("name"));
      Assert.assertEquals(
          computeResponse.get("test_key_2").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_2").get("name"));

      validateComputeRequestMetrics(
          true,
          false,
          streamingCompute ? RequestType.COMPUTE_STREAMING : RequestType.COMPUTE,
          false,
          2,
          0);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testComputeWithEmptyKeys(boolean streamingCompute)
      throws ExecutionException, InterruptedException, IOException {
    try {
      setUpClient();
      ComputeRequestBuilder requestBuilder = statsAvroGenericStoreClient.compute().project("name");
      Map<String, GenericRecord> value;
      if (streamingCompute) {
        value = (Map<String, GenericRecord>) requestBuilder.streamingExecute(Collections.emptySet()).get();
      } else {
        value = (Map<String, GenericRecord>) requestBuilder.execute(Collections.emptySet()).get();
      }
      assertTrue(value.isEmpty());
      String metricPrefix = "." + STORE_NAME + "--compute_streaming_";
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertTrue(metrics.get(metricPrefix + "request.OccurrenceRate").value() > 0);
        assertTrue(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
        assertFalse(metrics.get(metricPrefix + "request_key_count.Max").value() > 0);
        assertFalse(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      });
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testComputeWithMoreKeysThanMaxSize(boolean streamingCompute) throws InterruptedException, IOException {
    try {
      setUpClient();
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < Store.DEFAULT_BATCH_GET_LIMIT + 1; ++i) {
        keys.add("testKey" + i);
      }
      ComputeRequestBuilder requestBuilder = statsAvroGenericStoreClient.compute().project("name");
      CompletableFuture future;
      if (streamingCompute) {
        future = requestBuilder.streamingExecute(keys);
      } else {
        future = requestBuilder.execute(keys);
      }
      try {
        future.get();
        fail(VeniceKeyCountLimitException.class.getSimpleName() + " should be thrown");
      } catch (Exception e) {
        assertEquals(VeniceKeyCountLimitException.class, e.getCause().getClass());
      }
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testComputeWithExceptionFromTransportLayer() throws IOException {
    try {
      setUpClient(true, false, false);
      statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_KEYS).get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE, false, 2, 2);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testComputeWithExceptionFromTransportLayerForOneRoute() throws IOException {
    try {
      setUpClient(false, true, false);
      statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_KEYS).get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE, false, 2, 1);
    } finally {
      tearDown();
    }
  }

  /**
   * Conditions to test: blocking compute API either returns full results or exception, but no partial results.
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: this test calls compute().execute().get() without timeout, so waits till routingLeakedRequestCleanupThresholdMS
   *    times out and returns exception with "At least one route did not complete".
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*VeniceClientException: At least one route did not complete")
  public void testComputeWithTimeoutV1() throws IOException, ExecutionException, InterruptedException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_KEYS).get();
      fail();
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
          });
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE, false, 2, 2);
      tearDown();
    }
  }

  /**
   * Condition to test: blocking compute API either returns full results or exception, but no partial results.
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: routingLeakedRequestCleanupThresholdMS times out before compute().execute().get(timeout)
   *    so returns exception with "At least one route did not complete".
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*VeniceClientException: At least one route did not complete")
  public void testComputeWithTimeoutV2()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_KEYS).get(2, TimeUnit.SECONDS);
      fail();
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
          });
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE, false, 2, 2);
      tearDown();
    }
  }

  /**
   * Condition to test: blocking compute API either returns full results or exception, but no partial results.
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: compute().execute().get(timeout) times out before routingLeakedRequestCleanupThresholdMS,
   *    so AppTimeOutTrackingCompletableFuture returns TimeoutException confirming no partial returns.
   */
  @Test(timeOut = TEST_TIMEOUT, expectedExceptions = TimeoutException.class)
  public void testComputeWithTimeoutV3()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(2);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      ComputeRequestBuilder requestBuilder = statsAvroGenericStoreClient.compute().project("name");
      requestBuilder.execute(COMPUTE_REQUEST_KEYS).get(1, TimeUnit.SECONDS);
      fail();
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
          });
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE, false, 2, 2);
      tearDown();
    }
  }

  /**
   * 1st compute().execute() (with 1 key) blocks one replica and 2nd compute().execute() (with 2 keys) returns value for
   * only 1 key as the other route is blocked, so compute().execute() return an exception.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testComputeWithTimeoutV4() throws IOException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_PARTIAL_KEYS_2).get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      try {
        statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_KEYS).get();
        fail();
      } catch (Exception e1) {
        assertTrue(e1.getMessage().endsWith("At least one route did not complete"), e1.getMessage());
        assertTrue(
            e1.getCause().getCause().getMessage().contains("No available route for"),
            e1.getCause().getCause().getMessage());
        // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
        TestUtils.waitForNonDeterministicAssertion(
            routingLeakedRequestCleanupThresholdMS + 1000,
            TimeUnit.MILLISECONDS,
            () -> {
              assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
            });
        validateComputeRequestMetrics(false, true, RequestType.COMPUTE, true, 2, 1);
      }
    } finally {
      tearDown();
    }
  }

  /**
   * 1. 1st compute request (2 keys) will result in both the routes marked as
   *    blocked(routingPendingRequestCounterInstanceBlockThreshold is 1).
   * 2. 2nd compute request (2 keys) will result in both route getting no replica
   *    found leading to exception.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testComputeToUnreachableClient() throws IOException {
    try {
      setUpClient(false, false, false, false, TimeUnit.SECONDS.toMillis(1));
      statsAvroGenericStoreClient.compute().project("name").execute(COMPUTE_REQUEST_KEYS).get();
      fail();
    } catch (Exception e) {
      // First compute request fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      assertTrue(e.getMessage().endsWith("At least one route did not complete"), e.getMessage());
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE, false, 2, 2);

      try {
        // the second compute request is not going to find any routes (as the instances
        // are blocked) and fail instantly
        ComputeRequestBuilder requestBuilder = statsAvroGenericStoreClient.compute().project("name");
        requestBuilder.execute(COMPUTE_REQUEST_KEYS).get();
        fail();
      } catch (Exception e1) {
        assertTrue(e1.getMessage().endsWith("At least one route did not complete"), e1.getMessage());
        assertTrue(
            e1.getCause().getCause().getMessage().contains("No available route for store"),
            e1.getCause().getCause().getMessage());

        validateComputeRequestMetrics(false, false, RequestType.COMPUTE, true, 2, 2);
      }
    } finally {
      tearDown();
    }
  }

  /**
   * 1. 1st compute request (2 keys) will result in both the routes marked as
   *    blocked(routingPendingRequestCounterInstanceBlockThreshold is 1).
   * 2. 2nd compute request (2 keys) will result in both route getting no replica
   *    found leading to partial response.
   */
  @Test
  public void testStreamingComputeToUnreachableClient() throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, false, false, false, TimeUnit.SECONDS.toMillis(1));
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First compute request fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 2, 2);

      // the second compute request is not going to find any routes (as the instances
      // are blocked) and fail instantly
      VeniceResponseMap<String, ComputeGenericRecord> response2 =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response2.isFullResponse());
      assertEquals(response2.getTotalEntryCount(), 0);
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, true, 2, 2);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeWithExceptionFromTransportLayer()
      throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(true, false, false);
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 2, 2);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeWithExceptionFromTransportLayerForOneRoute()
      throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, true, false);
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(
          response.get("test_key_1").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1").get("name"));
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 2, 1);
    } finally {
      tearDown();
    }
  }

  /**
   * Conditions to test: streaming compute API returns partial results in case of future.get(timeout)
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: this test calls compute().streamingExecute().get() without timeout, so waits till routingLeakedRequestCleanupThresholdMS
   *    times out and returns a partial response.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeWithTimeoutV1() throws IOException, ExecutionException, InterruptedException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(
          response.get("test_key_1").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1").get("name"));
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
          });
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE_STREAMING, false, 2, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * Condition to test: streaming compute API returns partial results in case of future.get(timeout)
   * In this test:
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: routingLeakedRequestCleanupThresholdMS times out before compute().streamingExecute().get(timeout),
   *    so returns a partial response.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeWithTimeoutV2()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(1);
    try {
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get(2, TimeUnit.SECONDS);
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(
          response.get("test_key_1").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1").get("name"));
    } finally {
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
          });
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE_STREAMING, false, 2, 2);
      tearDown();
    }
  }

  /**
   * Condition to test: streaming compute API returns partial results in case of future.get(timeout)
   * setup: 1 key returns valid value and the other key doesn't return anything.
   * Behavior: streamingBatchGet().get(timeout) times out before routingLeakedRequestCleanupThresholdMS,
   *           so returns partial response.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeWithTimeoutV3()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    try {
      long routingLeakedRequestCleanupThresholdMS = TimeUnit.SECONDS.toMillis(2);
      setUpClient(false, false, true, true, routingLeakedRequestCleanupThresholdMS);
      ComputeRequestBuilder requestBuilder = statsAvroGenericStoreClient.compute().project("name");
      CompletableFuture<VeniceResponseMap<String, ComputeGenericRecord>> future =
          requestBuilder.streamingExecute(COMPUTE_REQUEST_KEYS);
      VeniceResponseMap<String, ComputeGenericRecord> value = future.get(1, TimeUnit.SECONDS);
      assertEquals(value.size(), 1);
      assertFalse(value.isFullResponse());
      Assert.assertEquals(
          value.get("test_key_1").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1").get("name"));
      // wait for routingLeakedRequestCleanupThresholdMS for the metrics to be increased
      TestUtils.waitForNonDeterministicAssertion(
          routingLeakedRequestCleanupThresholdMS + 1000,
          TimeUnit.MILLISECONDS,
          () -> {
            assertTrue(metrics.get("." + STORE_NAME + "--compute_streaming_request.OccurrenceRate").value() > 0);
          });
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE_STREAMING, false, 2, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * 1. first compute().streamingExecute() with 1 key getting timed out leading to the route getting
   *    blocked (routingPendingRequestCounterInstanceBlockThreshold = 1)
   * 2. second compute().streamingExecute() with 2 keys (one key (same as above) failing with no available
   *    replica and one key getting timed out) => partial response.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeToUnreachableClientV1() throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, false, false, false, TimeUnit.SECONDS.toMillis(1));
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_PARTIAL_KEYS_1)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 1, 2);

      VeniceResponseMap<String, ComputeGenericRecord> response2 =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response2.isFullResponse());
      assertEquals(response2.getTotalEntryCount(), 0);
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, true, 2, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * 1. first compute().streamingExecute() with 1 key getting timed out leading to the route getting
   *    blocked (routingPendingRequestCounterInstanceBlockThreshold = 1)
   * 2. second compute().streamingExecute() with 2 keys (one key (same as above) failing with no available
   *    replica and one key returning value) => no exception
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeToUnreachableClientV2() throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, false, true, true, TimeUnit.SECONDS.toMillis(1));
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_PARTIAL_KEYS_2)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 1, 2);
      // First compute fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 1, 2);
      CompletableFuture<VeniceResponseMap<String, ComputeGenericRecord>> future =
          statsAvroGenericStoreClient.compute().project("name").streamingExecute(COMPUTE_REQUEST_KEYS);
      VeniceResponseMap<String, ComputeGenericRecord> value = future.get();
      assertFalse(value.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // TODO metric validation: 1st get increments unhealthy metrics and the second get increments healthy
      // metrics
      // validateMultiGetMetrics(true, true, true, true, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * same as testStreamingComputeToUnreachableClientV2: transportClientThrowsPartialException instead of
   * transportClientPartialIncomplete.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeToUnreachableClientV3() throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(false, true, false, true, TimeUnit.SECONDS.toMillis(1));
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 1);
      assertEquals(
          response.get("test_key_1").get("name").toString(),
          COMPUTE_REQUEST_VALUE_RESPONSE.get("test_key_1").get("name"));
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateComputeRequestMetrics(false, true, RequestType.COMPUTE_STREAMING, false, 2, 2);
    } finally {
      tearDown();
    }
  }

  /**
   * same as testStreamingComputeToUnreachableClientV3 but the transport mock throws exception
   * for both the routes.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testStreamingComputeToUnreachableClientV4() throws IOException, ExecutionException, InterruptedException {
    try {
      setUpClient(true, false, false, true, TimeUnit.SECONDS.toMillis(1));
      VeniceResponseMap<String, ComputeGenericRecord> response =
          (VeniceResponseMap<String, ComputeGenericRecord>) statsAvroGenericStoreClient.compute()
              .project("name")
              .streamingExecute(COMPUTE_REQUEST_KEYS)
              .get();
      assertFalse(response.isFullResponse());
      assertEquals(response.getTotalEntryCount(), 0);
      // First batchGet fails with unreachable host after timeout and this adds the hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      validateComputeRequestMetrics(false, false, RequestType.COMPUTE_STREAMING, false, 2, 2);
    } finally {
      tearDown();
    }
  }
}
