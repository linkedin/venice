package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import com.linkedin.venice.utils.Time;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DispatchingAvroGenericStoreClientTest {
  private static final int TEST_TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final String SINGLE_GET_VALUE_RESPONSE = "test_value";
  private static final String STORE_NAME = "test_store";
  private static final Set<String> BATCH_GET_KEYS = new HashSet<>();
  private static final Set<String> BATCH_GET_PARTIAL_KEYS_1 = new HashSet<>();
  private static final Set<String> BATCH_GET_PARTIAL_KEYS_2 = new HashSet<>();
  private static final Map<String, String> BATCH_GET_VALUE_RESPONSE = new HashMap<>();
  private ClientConfig.ClientConfigBuilder clientConfigBuilder;
  private GetRequestContext getRequestContext;
  private BatchGetRequestContext batchGetRequestContext;
  private ClientConfig clientConfig;
  private DispatchingAvroGenericStoreClient dispatchingAvroGenericStoreClient;
  private StatsAvroGenericStoreClient statsAvroGenericStoreClient = null;
  private Map<String, ? extends Metric> metrics;
  private StoreMetadata storeMetadata = null;

  @BeforeClass
  public void setUp() {
    BATCH_GET_KEYS.add("test_key_1");
    BATCH_GET_KEYS.add("test_key_2");
    BATCH_GET_PARTIAL_KEYS_1.add("test_key_1");
    BATCH_GET_PARTIAL_KEYS_2.add("test_key_2");
    BATCH_GET_VALUE_RESPONSE.put("test_key_1", "test_value_1");
    BATCH_GET_VALUE_RESPONSE.put("test_key_2", "test_value_2");
  }

  private void setUpClient() {
    setUpClient(false);
  }

  private void setUpClient(boolean useStreamingBatchGetAsDefault) {
    setUpClient(useStreamingBatchGetAsDefault, false, false);
  }

  private void setUpClient(
      boolean useStreamingBatchGetAsDefault,
      boolean transportClientThrowsException,
      boolean transportClientThrowsPartialException) {
    setUpClient(
        useStreamingBatchGetAsDefault,
        transportClientThrowsException,
        transportClientThrowsPartialException,
        true,
        TimeUnit.SECONDS.toMillis(30));
  }

  private void setUpClient(
      boolean useStreamingBatchGetAsDefault,
      boolean transportClientThrowsException,
      boolean transportClientThrowsPartialException, // only applicable for useStreamingBatchGetAsDefault
      boolean mockTransportClient,
      long routingLeakedRequestCleanupThresholdMS) {
    clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setUseStreamingBatchGetAsDefault(useStreamingBatchGetAsDefault)
        .setMetadataRefreshIntervalInSeconds(1L)
        .setRoutingLeakedRequestCleanupThresholdMS(routingLeakedRequestCleanupThresholdMS)
        .setRoutingPendingRequestCounterInstanceBlockThreshold(1);

    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfig = clientConfigBuilder.build();

    storeMetadata = RequestBasedMetadataTestUtils.getMockMetaData(clientConfig, STORE_NAME);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();

    TransportClient mockedTransportClient = null;
    if (mockTransportClient) {
      mockedTransportClient = mock(TransportClient.class);
      dispatchingAvroGenericStoreClient =
          new DispatchingAvroGenericStoreClient(storeMetadata, clientConfig, mockedTransportClient);
    } else {
      dispatchingAvroGenericStoreClient = new DispatchingAvroGenericStoreClient(storeMetadata, clientConfig);
    }
    statsAvroGenericStoreClient = new StatsAvroGenericStoreClient(dispatchingAvroGenericStoreClient, clientConfig);
    statsAvroGenericStoreClient.start();

    // Wait till metadata is initialized
    while (true) {
      try {
        dispatchingAvroGenericStoreClient.verifyMetadataInitialized();
        break;
      } catch (VeniceClientException e) {
        if (e.getMessage().endsWith("metadata is not ready, attempting to re-initialize")) {
          // retry until its initialized
          continue;
        }
        throw e;
      }
    }

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
            SerializerDeserializerFactory
                .getAvroGenericSerializer(Schema.parse(RequestBasedMetadataTestUtils.VALUE_SCHEMA))
                .serialize(SINGLE_GET_VALUE_RESPONSE));
        valueFuture.complete(singleGetResponse);
      }

      // mock post()
      CompletableFuture<TransportClientResponseForRoute> batchGetValueFuture0 = new CompletableFuture<>();
      CompletableFuture<TransportClientResponseForRoute> batchGetValueFuture1 = new CompletableFuture<>();
      TransportClientResponseForRoute batchGetResponse0, batchGetResponse1;
      if (transportClientThrowsException) {
        doReturn(batchGetValueFuture0).when(mockedTransportClient).post(any(), any(), any());
        batchGetValueFuture0.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
      } else if (transportClientThrowsPartialException) {
        // return valid response for 1 route(1 key) and exception for the other
        batchGetResponse0 = new TransportClientResponseForRoute(
            "0",
            1,
            CompressionStrategy.NO_OP,
            serializeBatchGetResponse(BATCH_GET_PARTIAL_KEYS_1),
            mock(CompletableFuture.class));
        doReturn(batchGetValueFuture0).when(mockedTransportClient)
            .post(eq("host1/storage/test_store_v1"), any(), any());
        batchGetValueFuture0.complete(batchGetResponse0);
        doReturn(batchGetValueFuture1).when(mockedTransportClient)
            .post(eq("host2/storage/test_store_v1"), any(), any());
        batchGetValueFuture1.completeExceptionally(new VeniceClientException("Exception for client to return 503"));
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
            .post(eq("host1/storage/test_store_v1"), any(), any());
        batchGetValueFuture0.complete(batchGetResponse0);
        doReturn(batchGetValueFuture1).when(mockedTransportClient)
            .post(eq("host2/storage/test_store_v1"), any(), any());
        batchGetValueFuture1.complete(batchGetResponse1);
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
  private void validateRetryMetrics(boolean batchGet, String metricPrefix, boolean useStreamingBatchGetAsDefault) {
    if (batchGet) {
      if (useStreamingBatchGetAsDefault) {
        assertFalse(batchGetRequestContext.longTailRetryTriggered);
        assertFalse(batchGetRequestContext.numberOfKeysSentInRetryRequest > 0);
        assertFalse(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get() > 0);
      } // else: locally created single get context will be used internally and not batchGetRequestContext
    } else {
      assertFalse(getRequestContext.errorRetryRequestTriggered);
      assertFalse(getRequestContext.longTailRetryRequestTriggered);
      assertFalse(getRequestContext.retryWin);
    }

    assertFalse(metrics.get(metricPrefix + "error_retry_request.OccurrenceRate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "long_tail_retry_request.OccurrenceRate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "retry_request_key_count.Rate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "retry_request_success_key_count.Rate").value() > 0);
    assertFalse(metrics.get(metricPrefix + "retry_request_win.OccurrenceRate").value() > 0);
  }

  private void validateSingleGetMetrics(boolean healthyRequest) {
    validateMetrics(healthyRequest, false, false, false, false);
  }

  private void validateMultiGetMetrics(
      boolean healthyRequest,
      boolean partialHealthyRequest,
      boolean useStreamingBatchGetAsDefault,
      boolean noAvailableReplicas) {
    validateMetrics(healthyRequest, partialHealthyRequest, true, useStreamingBatchGetAsDefault, noAvailableReplicas);
  }

  private void validateMetrics(
      boolean healthyRequest,
      boolean partialHealthyRequest,
      boolean batchGet,
      boolean useStreamingBatchGetAsDefault,
      boolean noAvailableReplicas) {
    metrics = getStats(clientConfig);
    String metricPrefix = "." + STORE_NAME + ((batchGet && useStreamingBatchGetAsDefault) ? "--multiget_" : "--");
    double requestKeyCount;
    double successKeyCount;
    if (useStreamingBatchGetAsDefault) {
      if (partialHealthyRequest) {
        // batchGet and partialHealthyRequest: 2 request tried but only 1 request is successful
        requestKeyCount = 2.0;
        successKeyCount = 1.0;
      } else {
        requestKeyCount = 2.0;
        successKeyCount = 2.0;
      }
    } else {
      // single get: 1
      // batchGet using single get: 1 will be recorded twice rather than 2
      requestKeyCount = 1.0;
      successKeyCount = 1.0;
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
        if (useStreamingBatchGetAsDefault) {
          assertEquals(batchGetRequestContext.successRequestKeyCount.get(), (int) successKeyCount);
        } // else: locally created single get context will be used internally and not batchGetRequestContext
      } else {
        assertEquals(getRequestContext.successRequestKeyCount.get(), (int) successKeyCount);
      }
    } else if (partialHealthyRequest) {
      assertFalse(metrics.get(metricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertFalse(metrics.get(metricPrefix + "healthy_request_latency.Avg").value() > 0);
      assertTrue(metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate").value() > 0);
      assertTrue(metrics.get(metricPrefix + "unhealthy_request_latency.Avg").value() > 0);
      assertFalse(metrics.get(metricPrefix + "success_request_key_count.Max").value() > 0);
      if (batchGet) {
        if (useStreamingBatchGetAsDefault) {
          assertEquals(batchGetRequestContext.successRequestKeyCount.get(), (int) successKeyCount);
        } // else: locally created single get context will be used internally and not batchGetRequestContext
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
        if (useStreamingBatchGetAsDefault) {
          assertEquals(batchGetRequestContext.successRequestKeyCount.get(), 0);
        } // else: locally created single get context will be used internally and not batchGetRequestContext
      } else {
        assertEquals(getRequestContext.successRequestKeyCount.get(), 0);
      }
    }

    // the below counter will always fail as we never increment them
    assertFalse(metrics.get(metricPrefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
    if (noAvailableReplicas) {
      if (batchGet) {
        if (useStreamingBatchGetAsDefault) {
          assertTrue(batchGetRequestContext.noAvailableReplica);
        } // else: locally created single get context will be used internally and not batchGetRequestContext
      } else {
        assertTrue(getRequestContext.noAvailableReplica);
      }
    } else {
      if (batchGet) {
        if (useStreamingBatchGetAsDefault) {
          assertFalse(batchGetRequestContext.noAvailableReplica);
        } // else: locally created single get context will be used internally and not batchGetRequestContext
      } else {
        assertFalse(getRequestContext.noAvailableReplica);
      }
    }

    validateRetryMetrics(batchGet, metricPrefix, useStreamingBatchGetAsDefault);
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testGet() throws ExecutionException, InterruptedException, IOException {
    try {
      setUpClient();
      getRequestContext = new GetRequestContext();
      String value = statsAvroGenericStoreClient.get(getRequestContext, "test_key").get().toString();
      assertEquals(value, SINGLE_GET_VALUE_RESPONSE);
      validateSingleGetMetrics(true);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetWithExceptionFromTransportLayer() throws IOException {
    try {
      setUpClient(false, true, false);
      getRequestContext = new GetRequestContext();
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get().toString();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("Exception for client to return 503"));
      metrics = getStats(clientConfig);
      validateSingleGetMetrics(false);
    } finally {
      tearDown();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetToUnreachableClient() throws IOException {
    try {
      setUpClient(false, false, false, false, 2 * Time.MS_PER_SECOND);
      getRequestContext = new GetRequestContext();
      statsAvroGenericStoreClient.get(getRequestContext, "test_key").get();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().endsWith("http status: 410, Request timed out"));
      validateSingleGetMetrics(false);
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
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
      validateMultiGetMetrics(true, false, useStreamingBatchGetAsDefault, false);
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testBatchGetWithExceptionFromTransportLayer(boolean useStreamingBatchGetAsDefault) throws IOException {
    try {
      setUpClient(useStreamingBatchGetAsDefault, true, false);
      batchGetRequestContext = new BatchGetRequestContext<>();
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } catch (Exception e) {
      if (useStreamingBatchGetAsDefault) {
        assertTrue(e.getMessage().endsWith("At least one route did not complete"));
      } else {
        assertTrue(e.getMessage().endsWith("Exception for client to return 503"));
      }
      validateMultiGetMetrics(false, false, useStreamingBatchGetAsDefault, false);
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testBatchGetWithExceptionFromTransportLayerForOneRoute(boolean useStreamingBatchGetAsDefault)
      throws IOException {
    try {
      setUpClient(useStreamingBatchGetAsDefault, false, true);
      batchGetRequestContext = new BatchGetRequestContext<>();
      Map<String, String> value =
          (Map<String, String>) statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      if (useStreamingBatchGetAsDefault) {
        fail();
        /* assertEquals(value.size(), 1);
        assertTrue(BATCH_GET_VALUE_RESPONSE.get("test_key_2").contentEquals(value.get("test_key_2")));*/
      } else {
        // uses single get, so based on the mock, any key will return SINGLE_GET_VALUE_RESPONSE as the value.
        // also: batchGetRequestContext is not usable anymore
        BATCH_GET_KEYS.stream().forEach(key -> {
          assertTrue(SINGLE_GET_VALUE_RESPONSE.contentEquals(value.get(key)));
        });
        validateMultiGetMetrics(true, false, useStreamingBatchGetAsDefault, false);
      }
    } catch (Exception e) {
      if (useStreamingBatchGetAsDefault) {
        assertTrue(e.getMessage().endsWith("At least one route did not complete"));
      } else {
        fail();
      }
      validateMultiGetMetrics(false, true, useStreamingBatchGetAsDefault, false);
    } finally {
      tearDown();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testBatchGetToUnreachableClient(boolean useStreamingBatchGetAsDefault) throws IOException {
    try {
      setUpClient(useStreamingBatchGetAsDefault, false, false, false, 7 * Time.MS_PER_SECOND);
      batchGetRequestContext = new BatchGetRequestContext<>();
      statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
      fail();
    } catch (Exception e) {
      // First batchGet fails with unreachable host after wait time and this add these hosts
      // as blocked due to setRoutingPendingRequestCounterInstanceBlockThreshold(1)
      if (useStreamingBatchGetAsDefault) {
        assertTrue(e.getMessage().endsWith("At least one route did not complete"));
      } else {
        assertTrue(e.getMessage().endsWith("http status: 410, Request timed out"));
      }
      validateMultiGetMetrics(false, false, useStreamingBatchGetAsDefault, false);

      try {
        // the second batchGet is not going to find any routes (as the instances
        // are blocked) and fail due to that
        batchGetRequestContext = new BatchGetRequestContext<>();
        statsAvroGenericStoreClient.batchGet(batchGetRequestContext, BATCH_GET_KEYS).get();
        fail();
      } catch (Exception e1) {
        System.out.println("Exception is: " + e1.getMessage());
        if (useStreamingBatchGetAsDefault) {
          assertTrue(e1.getMessage().endsWith("Response was not complete"));
        } else {
          assertTrue(e1.getMessage().matches(".*No available route for store.*"));
        }
        validateMultiGetMetrics(false, false, useStreamingBatchGetAsDefault, true);
      }
    } finally {
      tearDown();
    }
  }
}
