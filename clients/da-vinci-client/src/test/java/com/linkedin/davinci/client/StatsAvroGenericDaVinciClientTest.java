package com.linkedin.davinci.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
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

    MetricsRepository metricsRepository = new MetricsRepository();
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
    assertEquals(metrics.get(".test_store--success_request_key_count.Avg").value(), 1.0);
    assertEquals(metrics.get(".test_store--success_request_key_count.Max").value(), 1.0);
    assertTrue(metrics.get(".test_store--success_request_ratio.SimpleRatioStat").value() < 1.0);
    assertTrue(metrics.get(".test_store--success_request_key_ratio.SimpleRatioStat").value() < 1.0);
  }

  @Test
  public void testBatchGet() throws ExecutionException, InterruptedException {
    String storeName = "test_store";
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3"));
    AvroGenericDaVinciClient mockClient = mock(AvroGenericDaVinciClient.class);
    CompletableFuture<String> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new RuntimeException("mock_exception_thrown_by_async_future"));
    CompletableFuture<Map<String, String>> okFuture = new CompletableFuture<>();
    Map<String, String> resultMap = new HashMap<>();
    resultMap.put("key1", "value1");
    resultMap.put("key2", "value2");
    okFuture.complete(resultMap);
    when(mockClient.batchGet(any())).thenReturn(okFuture)
        .thenReturn(errorFuture)
        .thenThrow(new RuntimeException("mock_exception_by_function_directly"));
    when(mockClient.getStoreName()).thenReturn(storeName);

    MetricsRepository metricsRepository = new MetricsRepository();
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
    assertEquals(metrics.get(".test_store--multiget_success_request_key_count.Avg").value(), 2.0);
    assertEquals(metrics.get(".test_store--multiget_success_request_key_count.Max").value(), 2.0);
    assertTrue(metrics.get(".test_store--multiget_success_request_ratio.SimpleRatioStat").value() < 1.0);
    assertTrue(metrics.get(".test_store--multiget_success_request_key_ratio.SimpleRatioStat").value() < 1.0);
  }
}
