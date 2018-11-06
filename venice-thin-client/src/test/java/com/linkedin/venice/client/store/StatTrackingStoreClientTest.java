package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class StatTrackingStoreClientTest {
  private InternalAvroStoreClient<String, Object> mockStoreClient;
  private String metricPrefix;

  @BeforeTest
  public void setUp() {
    mockStoreClient = mock(InternalAvroStoreClient.class);

    String storeName = TestUtils.getUniqueString("store");
    doReturn(storeName).when(mockStoreClient).getStoreName();
    metricPrefix = "." + storeName;
  }


  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.complete(mockReturnObject);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });

    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    statTrackingStoreClient.get("key").get();

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--unhealthy_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
  }

  @Test
  public void testMultiGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Map<String, String> result = new HashMap<>();
    Set<String> keySet = new HashSet<>();
    String keyPrefix = "key_";
    for (int i = 0; i < 5; ++i) {
      result.put(keyPrefix + i, "value_" + i);
    }
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    mockInnerFuture.complete(result);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });
    doReturn(mockInnerFuture).when(mockStoreClient).batchGet(any(), any(), anyLong());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    statTrackingStoreClient.batchGet(keySet).get();

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--multiget_request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--multiget_healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--multiget_unhealthy_request.OccurrenceRate");
    Metric keyCountMetric = metrics.get(metricPrefix + "--multiget_request_key_count.Avg");
    Metric successKeyCountMetric = metrics.get(metricPrefix + "--multiget_success_request_key_count.Avg");
    Metric successKeyRatioMetric = metrics.get(metricPrefix + "--multiget_success_request_key_ratio.SimpleRatioStat");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
    Assert.assertEquals(keyCountMetric.value(), 10.0);
    Assert.assertEquals(successKeyCountMetric.value(), 5.0);
    Assert.assertTrue(successKeyRatioMetric.value() > 0, "Success Key Ratio should be positive");
  }

  @Test
  public void testGetWithException() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException("Inner mock exception", HttpStatus.BAD_REQUEST_400));
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
        InternalAvroStoreClient.handleStoreExceptionInternally(throwable);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });
    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    try {
      statTrackingStoreClient.get("key").get();
      Assert.fail("ExecutionException should be thrown");
    } catch (ExecutionException e) {
      System.out.println(e);
      // expected
    }

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--unhealthy_request.OccurrenceRate");
    Metric http400RequestMetric = metrics.get(metricPrefix + "--http_400_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertEquals(healthyRequestMetric.value(), 0.0);
    Assert.assertTrue(unhealthyRequestMetric.value() > 0.0);
    Assert.assertTrue(http400RequestMetric.value() > 0.0);
  }
}
