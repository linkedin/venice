package com.linkedin.davinci.client;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.DAVINCI_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.Metric;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;


public class StatsAvroGenericDaVinciClientTest {
  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testGet(boolean reuseApi) throws ExecutionException, InterruptedException {
    String storeName = "test_store";
    AvroGenericDaVinciClient mockClient = mock(AvroGenericDaVinciClient.class);
    CompletableFuture<String> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new RuntimeException("mock_exception_thrown_by_async_future"));
    CompletableFuture<String> okFuture = new CompletableFuture<>();
    String okResult = "test_result";
    okFuture.complete(okResult);
    when(mockClient.get(any(), any())).thenReturn(okFuture)
        .thenReturn(errorFuture)
        .thenThrow(new RuntimeException("mock_exception_by_function_directly"));
    when(mockClient.getStoreName()).thenReturn(storeName);

    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(DAVINCI_CLIENT, CLIENT_METRIC_ENTITIES, true);
    StatsAvroGenericDaVinciClient statsClient = new StatsAvroGenericDaVinciClient(
        mockClient,
        new ClientConfig(storeName).setMetricsRepository(metricsRepository));

    Object result;
    if (reuseApi) {
      result = statsClient.get("test_key", null).get();
    } else {
      result = statsClient.get("test_key").get();
    }
    assertEquals(result, okResult);

    if (reuseApi) {
      assertThrows(ExecutionException.class, () -> statsClient.get("test_key", null).get());
      assertThrows(RuntimeException.class, () -> statsClient.get("test_key", null));
    } else {
      assertThrows(ExecutionException.class, () -> statsClient.get("test_key").get());
      assertThrows(RuntimeException.class, () -> statsClient.get("test_key"));
    }
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(metrics.get(".test_store--healthy_request.OccurrenceRate").value() > 0);
    assertTrue(metrics.get(".test_store--unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(metrics.get(".test_store--healthy_request_latency.Avg").value() > 0);
    // we have 2 requests, one success and one failure and we would record the key count for the success request as 1
    // and the key count for the failure request as 0.
    assertEquals(metrics.get(".test_store--success_request_key_count.Avg").value(), 1.0 / 2);
    assertEquals(metrics.get(".test_store--success_request_key_count.Max").value(), 1.0);
    assertTrue(metrics.get(".test_store--success_request_ratio.SimpleRatioStat").value() < 1.0);
    assertTrue(metrics.get(".test_store--success_request_key_ratio.SimpleRatioStat").value() < 1.0);
  }

  @Test
  public void testBatchGet() throws ExecutionException, InterruptedException {
    String storeName = "test_store";
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3"));
    AvroGenericDaVinciClient mockClient = mock(AvroGenericDaVinciClient.class);
    DaVinciBackend mockBackend = mock(DaVinciBackend.class, RETURNS_DEEP_STUBS);
    when(mockClient.getStoreName()).thenReturn(storeName);
    when(mockBackend.getCachedStore(anyString()).isEnableReads()).thenReturn(true);
    when(mockBackend.getCachedStore(anyString()).isEnableWrites()).thenReturn(true);
    CompletableFuture<String> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new RuntimeException("mock_exception_thrown_by_async_future"));
    CompletableFuture<Map<String, String>> okFuture = new CompletableFuture<>();
    Map<String, String> resultMap = new HashMap<>();
    resultMap.put("key1", "value1");
    resultMap.put("key2", "value2");
    okFuture.complete(resultMap);
    when(mockClient.batchGetImplementation(any())).thenReturn(okFuture)
        .thenReturn(errorFuture)
        .thenThrow(new RuntimeException("mock_exception_by_function_directly"));
    doCallRealMethod().when(mockClient).batchGet(any());
    doCallRealMethod().when(mockClient).streamingBatchGet(any(), any());
    when(mockClient.getDaVinciBackend()).thenReturn(mockBackend);

    VeniceMetricsRepository metricsRepository =
        getVeniceMetricsRepository(DAVINCI_CLIENT, CLIENT_METRIC_ENTITIES, true);
    StatsAvroGenericDaVinciClient statsClient = new StatsAvroGenericDaVinciClient(
        mockClient,
        new ClientConfig(storeName).setMetricsRepository(metricsRepository));

    // No exception should happen
    assertEquals(statsClient.batchGet(keys).get(), resultMap);

    assertThrows(ExecutionException.class, () -> statsClient.batchGet(keys).get());
    assertThrows(RuntimeException.class, () -> statsClient.batchGet(keys));
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();

    assertTrue(metrics.get(".test_store--multiget_healthy_request.OccurrenceRate").value() > 0);
    assertTrue(metrics.get(".test_store--multiget_unhealthy_request.OccurrenceRate").value() > 0);
    assertTrue(metrics.get(".test_store--multiget_healthy_request_latency.Avg").value() > 0);
    // We have 3 batch get requests, one success with 2 keys, one failure, and one with run time exception.
    // Key count for the success one is 2, failure one is 0, and the run time exception one is never recorded.
    assertEquals(metrics.get(".test_store--multiget_success_request_key_count.Avg").value(), 2.0 / 2);
    assertEquals(metrics.get(".test_store--multiget_success_request_key_count.Max").value(), 2.0);
    assertTrue(metrics.get(".test_store--multiget_success_request_ratio.SimpleRatioStat").value() < 1.0);
    assertTrue(metrics.get(".test_store--multiget_success_request_key_ratio.SimpleRatioStat").value() < 1.0);
  }
}
