package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
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
