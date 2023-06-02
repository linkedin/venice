package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.transport.TransportClientResponseForRoute;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DispatchingAvroGenericStoreClientTest {
  private static final String SINGLE_GET_VALUE_RESPONSE = "test_value";
  private static final String STORE_NAME = "test_store";
  private static final Set<String> BATCH_GET_KEYS = new HashSet<>();
  private static final Map<String, String> BATCH_GET_VALUE_RESPONSE = new HashMap<>();
  private ClientConfig.ClientConfigBuilder clientConfigBuilder;
  private GetRequestContext getRequestContext;
  private BatchGetRequestContext batchGetRequestContext;
  private ClientConfig clientConfig;
  private DispatchingAvroGenericStoreClient dispatchingAvroGenericStoreClient;
  private StatsAvroGenericStoreClient statsAvroGenericStoreClient = null;
  private Map<String, ? extends Metric> metrics;
  private StoreMetadata mockMetadata = null;

  @BeforeClass
  public void setUp() {
    BATCH_GET_KEYS.add("test_key_1");
    BATCH_GET_KEYS.add("test_key_2");
    BATCH_GET_VALUE_RESPONSE.put("test_key_1", "test_value_1");
    BATCH_GET_VALUE_RESPONSE.put("test_key_2", "test_value_2");
  }

  private void setUpClient() {
    setUpClient(false, false);
  }

  private void setUpClient(boolean useStreamingBatchGetAsDefault) {
    setUpClient(useStreamingBatchGetAsDefault, false);
  }

  private void setUpClient(boolean useStreamingBatchGetAsDefault, boolean transportClientThrowsException) {
    clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setUseStreamingBatchGetAsDefault(useStreamingBatchGetAsDefault)
        .setMetadataRefreshIntervalInSeconds(1L);

    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();

    mockMetadata = RequestBasedMetadataTestUtils.getMockMetaData(clientConfig, STORE_NAME);
    TransportClient mockTransportClient = mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();

    dispatchingAvroGenericStoreClient =
        new DispatchingAvroGenericStoreClient(mockMetadata, clientConfig, mockTransportClient);
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(dispatchingAvroGenericStoreClient, clientConfig);
    statsAvroGenericStoreClient.start();

    // Initialize metadata
    dispatchingAvroGenericStoreClient.verifyMetadataInitialized();
    // mock get()
    doReturn(valueFuture).when(mockTransportClient).get(any());
    if (!transportClientThrowsException) {
      TransportClientResponse singleGetResponse = new TransportClientResponse(
          1,
          CompressionStrategy.NO_OP,
          SerializerDeserializerFactory
              .getAvroGenericSerializer(Schema.parse(RequestBasedMetadataTestUtils.VALUE_SCHEMA))
              .serialize(SINGLE_GET_VALUE_RESPONSE));
      valueFuture.complete(singleGetResponse);
    } else {
      valueFuture.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
    }

    // mock post()
    CompletableFuture<TransportClientResponseForRoute> batchGetValueFuture = new CompletableFuture<>();
    TransportClientResponseForRoute batchGetResponse;
    if (!transportClientThrowsException) {
      batchGetResponse = new TransportClientResponseForRoute(
          "0",
          1,
          CompressionStrategy.NO_OP,
          serializeBatchGetResponse(BATCH_GET_KEYS));
      doReturn(batchGetValueFuture).when(mockTransportClient).post(any(), any(), any());
      batchGetValueFuture.complete(batchGetResponse);
    } else {
      doReturn(batchGetValueFuture).when(mockTransportClient).post(any(), any(), any());
      batchGetValueFuture.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
    }
  }

  private void tearDown() throws IOException {
    if (mockMetadata != null) {
      mockMetadata.close();
      mockMetadata = null;
    }
    if (statsAvroGenericStoreClient != null) {
      statsAvroGenericStoreClient.close();
      statsAvroGenericStoreClient = null;
    }
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
   * Not using retry in these tests, so none of the retry metrics should be incremented
   */
  private void validateRetryMetrics() {
    validateRetryMetrics(false, "", false);
  }

  private void validateRetryMetrics(boolean batchGet, String metricPrefix, boolean useStreamingBatchGetAsDefault) {
    if (batchGet) {
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
      if (useStreamingBatchGetAsDefault) {
        assertFalse(batchGetRequestContext.longTailRetryTriggered);
      }
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "retry_request_key_count.Rate").value() > 0);
      assertFalse(batchGetRequestContext.numberOfKeysSentInRetryRequest > 0);
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "retry_request_success_key_count.Rate").value() > 0);
      assertFalse(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get() > 0);
    } else {
      assertFalse(metrics.get("." + STORE_NAME + "--error_retry_request.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.errorRetryRequestTriggered);
      assertFalse(metrics.get("." + STORE_NAME + "--long_tail_retry_request.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.longTailRetryRequestTriggered);
      assertFalse(metrics.get("." + STORE_NAME + "--retry_request_win.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.retryWin);
    }
  }

  private byte[] serializeBatchGetResponse(Set<String> Keys) {
    List<MultiGetResponseRecordV1> routerRequestValues = new ArrayList<>(Keys.size());
    AtomicInteger count = new AtomicInteger();
    Keys.stream().forEach(key -> {
      MultiGetResponseRecordV1 routerRequestValue = new MultiGetResponseRecordV1();
      byte[] valueBytes =
          dispatchingAvroGenericStoreClient.getKeySerializer().serialize(BATCH_GET_VALUE_RESPONSE.get(key));
      ByteBuffer valueByteBuffer = ByteBuffer.wrap(valueBytes);
      routerRequestValue.setValue(valueByteBuffer);
      routerRequestValue.keyIndex = count.getAndIncrement();
      routerRequestValues.add(routerRequestValue);
    });
    return dispatchingAvroGenericStoreClient.getMultiGetSerializer().serializeObjects(routerRequestValues);
  }

  @Test
  public void testGet() throws ExecutionException, InterruptedException, IOException {
    try {
      setUpClient();
      getRequestContext = new GetRequestContext();
      String value = statsAvroGenericStoreClient.get(getRequestContext, "test_key").get().toString();
      assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
      metrics = getStats(clientConfig);
      assertTrue(metrics.get("." + STORE_NAME + "--healthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get("." + STORE_NAME + "--healthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--unhealthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--unhealthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--no_available_replica_request_count.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.noAvailableReplica);
      assertEquals(metrics.get("." + STORE_NAME + "--request_key_count.Max").value(), 1.0);
      assertEquals(metrics.get("." + STORE_NAME + "--success_request_key_count.Max").value(), 1.0);
      assertEquals(getRequestContext.successRequestKeyCount.get(), 1);

      validateRetryMetrics();
    } finally {
      tearDown();
    }
  }

  @Test
  public void testGetWithExceptionFromTransportLayer() throws IOException {
    try {
      setUpClient(false, true);
      getRequestContext = new GetRequestContext();
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get().toString();
      fail();
    } catch (Exception e) {
      metrics = getStats(clientConfig);
      assertFalse(metrics.get("." + STORE_NAME + "--healthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--healthy_request_latency.Avg").value() > 0);
      assertTrue(metrics.get("." + STORE_NAME + "--unhealthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get("." + STORE_NAME + "--unhealthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + "--no_available_replica_request_count.OccurrenceRate").value() > 0);
      assertFalse(getRequestContext.noAvailableReplica);
      assertEquals(metrics.get("." + STORE_NAME + "--request_key_count.Max").value(), 1.0);
      assertFalse(metrics.get("." + STORE_NAME + "--success_request_key_count.Max").value() > 0);
      assertEquals(getRequestContext.successRequestKeyCount.get(), 0);

      validateRetryMetrics();
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBatchGet(boolean useStreamingBatchGetAsDefault)
      throws ExecutionException, InterruptedException, IOException {

    try {
      setUpClient(useStreamingBatchGetAsDefault);
      batchGetRequestContext = new BatchGetRequestContext<>();
      Map<String, String> value =
          (Map<String, String>) statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      if (useStreamingBatchGetAsDefault) {
        BATCH_GET_KEYS.stream().forEach(key -> {
          assertTrue(BATCH_GET_VALUE_RESPONSE.get(key).contentEquals(value.get(key)));
        });
      } else {
        // uses single get, so based on the mock, any key will return SINGLE_GET_VALUE_RESPONSE as the value.
        // also: batchGetRequestContext is not usable anymore
        BATCH_GET_KEYS.stream().forEach(key -> {
          assertTrue(SINGLE_GET_VALUE_RESPONSE.contentEquals(value.get(key)));
        });
      }
      metrics = getStats(clientConfig, RequestType.MULTI_GET);
      String metricPrefix = useStreamingBatchGetAsDefault ? "--multiget_" : "--";
      assertTrue(metrics.get("." + STORE_NAME + metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get("." + STORE_NAME + metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      assertFalse(
          metrics.get("." + STORE_NAME + metricPrefix + "no_available_replica_request_count.OccurrenceRate")
              .value() > 0);
      if (useStreamingBatchGetAsDefault) {
        assertFalse(batchGetRequestContext.noAvailableReplica);
      } // else: locally created single get context will be used internally and not batchGetRequestContext
      if (useStreamingBatchGetAsDefault) {
        assertEquals(metrics.get("." + STORE_NAME + metricPrefix + "request_key_count.Max").value(), 2.0);
        assertEquals(metrics.get("." + STORE_NAME + metricPrefix + "success_request_key_count.Max").value(), 2.0);
        assertEquals(batchGetRequestContext.successRequestKeyCount.get(), 2);
      } else {
        // using single get: so 1 will be recorded twice rather than 2
        assertEquals(metrics.get("." + STORE_NAME + metricPrefix + "request_key_count.Max").value(), 1.0);
        assertEquals(metrics.get("." + STORE_NAME + metricPrefix + "success_request_key_count.Max").value(), 1.0);
      }

      validateRetryMetrics(true, metricPrefix, useStreamingBatchGetAsDefault);
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBatchGetWithExceptionFromTransportLayer(boolean useStreamingBatchGetAsDefault) throws IOException {
    try {
      setUpClient(useStreamingBatchGetAsDefault, true);
      batchGetRequestContext = new BatchGetRequestContext<>();
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } catch (Exception e) {
      metrics = getStats(clientConfig, RequestType.MULTI_GET);
      String metricPrefix = useStreamingBatchGetAsDefault ? "--multiget_" : "--";
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertTrue(metrics.get("." + STORE_NAME + metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get("." + STORE_NAME + metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      if (useStreamingBatchGetAsDefault) {
        assertEquals(metrics.get("." + STORE_NAME + metricPrefix + "request_key_count.Max").value(), 2.0);
        assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "success_request_key_count.Max").value() > 0);
        assertEquals(batchGetRequestContext.successRequestKeyCount.get(), 0);
      } else {
        // using single get: so 1 will be recorded twice rather than 2
        assertEquals(metrics.get("." + STORE_NAME + metricPrefix + "request_key_count.Max").value(), 1.0);
        assertFalse(metrics.get("." + STORE_NAME + metricPrefix + "success_request_key_count.Max").value() > 0);
      }

      validateRetryMetrics(true, metricPrefix, useStreamingBatchGetAsDefault);
    } finally {
      tearDown();
    }
  }
}
