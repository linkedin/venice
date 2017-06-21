package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class StatTrackingStoreClientTest {

  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    InternalAvroStoreClient<String, Object> mockStoreClient = mock(InternalAvroStoreClient.class);
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
    doReturn(mockInnerFuture).when(mockStoreClient).get(any());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    statTrackingStoreClient.get(new String("123")).get();

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(".venice_client--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(".venice_client--healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(".venice_client--unhealthy_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
  }

  @Test
  public void testMultiGet() throws ExecutionException, InterruptedException {
    InternalAvroStoreClient<String, Object> mockStoreClient = mock(InternalAvroStoreClient.class);
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
    doReturn(mockInnerFuture).when(mockStoreClient).multiGet(any());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    statTrackingStoreClient.multiGet(keySet).get();

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(".venice_client--multiget_request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(".venice_client--multiget_healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(".venice_client--multiget_unhealthy_request.OccurrenceRate");
    Metric keyCountMetric = metrics.get(".venice_client--multiget_request_key_count.Avg");
    Metric successKeyCountMetric = metrics.get(".venice_client--multiget_success_request_key_count.Avg");
    Metric successKeyRatioMetric = metrics.get(".venice_client--multiget_success_request_key_ratio.SimpleRatioStat");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
    Assert.assertEquals(keyCountMetric.value(), 10.0);
    Assert.assertEquals(successKeyCountMetric.value(), 5.0);
    Assert.assertTrue(successKeyRatioMetric.value() > 0, "Success Key Ratio should be positive");
  }

  @Test
  public void testGetWithException() throws ExecutionException, InterruptedException {
    InternalAvroStoreClient<String, Object> mockStoreClient = mock(InternalAvroStoreClient.class);
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
    doReturn(mockInnerFuture).when(mockStoreClient).get(any());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    try {
      statTrackingStoreClient.get(new String("123")).get();
      Assert.fail("ExecutionException should be thrown");
    } catch (ExecutionException e) {
      System.out.println(e);
      // expected
    }

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(".venice_client--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(".venice_client--healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(".venice_client--unhealthy_request.OccurrenceRate");
    Metric http400RequestMetric = metrics.get(".venice_client--http_400_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertEquals(healthyRequestMetric.value(), 0.0);
    Assert.assertTrue(unhealthyRequestMetric.value() > 0.0);
    Assert.assertTrue(http400RequestMetric.value() > 0.0);
  }
}
