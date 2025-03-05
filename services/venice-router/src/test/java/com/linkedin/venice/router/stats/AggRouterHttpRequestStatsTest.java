package com.linkedin.venice.router.stats;

import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.TehutiException;
import io.tehuti.metrics.Sensor;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


public class AggRouterHttpRequestStatsTest {
  private VeniceMetricsRepository metricsRepository;
  private MockTehutiReporter reporter;
  private ReadOnlyStoreRepository storeMetadataRepository;

  @BeforeSuite
  public void setUp() {
    this.metricsRepository = new VeniceMetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
    storeMetadataRepository = mock(ReadOnlyStoreRepository.class);
  }

  @Test
  public void testAggRouterMetrics() {
    AggRouterHttpRequestStats stats = new AggRouterHttpRequestStats(
        "test-cluster",
        metricsRepository,
        RequestType.SINGLE_GET,
        false,
        storeMetadataRepository,
        true,
        mock(Sensor.class));

    stats.recordRequest("store5");
    Assert.assertEquals(reporter.query(".total--request.Count").value(), 1d);

    stats.recordRequest("store1");
    Assert.assertEquals(reporter.query(".total--request.Count").value(), 2d);
    Assert.assertNotNull(metricsRepository.getMetric(".store1--request.Count"));
    Assert.assertEquals(reporter.query(".store1--request.Count").value(), 1d);

    stats.recordThrottledRequest("store1", 1.0, TOO_MANY_REQUESTS, 1);
    stats.recordThrottledRequest("store2", 1.0, TOO_MANY_REQUESTS, 1);
    stats.recordErrorRetryCount("store1");
    Assert.assertEquals(reporter.query(".total--request.Count").value(), 2d);
    Assert.assertEquals(reporter.query(".store1--request.Count").value(), 1d);
    Assert.assertEquals(reporter.query(".store1--error_retry.Count").value(), 1d);

    for (int i = 1; i <= 100; i += 1) {
      stats.recordLatency("store2", i);
    }

    Assert.assertEquals((int) reporter.query(".total--latency.50thPercentile").value(), 50);
    Assert.assertEquals((int) reporter.query(".total--latency.95thPercentile").value(), 95);
    Assert.assertEquals((int) reporter.query(".total--latency.99thPercentile").value(), 99);
    Assert.assertEquals((int) reporter.query(".store2--latency.50thPercentile").value(), 50);

    stats.handleStoreDeleted("store1");
    Assert.assertNull(metricsRepository.getMetric(".store1--request.Count"));
  }

  @Test
  public void testProfilingMetrics() {
    AggRouterHttpRequestStats stats = new AggRouterHttpRequestStats(
        "test-cluster",
        metricsRepository,
        RequestType.COMPUTE,
        true,
        storeMetadataRepository,
        true,
        mock(Sensor.class));

    for (int i = 1; i <= 100; i += 1) {
      stats.recordKeySize(i);
      stats.recordResponseSize("store1", i);
    }

    Assert.assertEquals((int) reporter.query(".total--compute_key_size_in_byte.1thPercentile").value(), 1);
    Assert.assertEquals((int) reporter.query(".total--compute_key_size_in_byte.2thPercentile").value(), 2);
    Assert.assertEquals((int) reporter.query(".total--compute_key_size_in_byte.3thPercentile").value(), 3);
    Assert.assertEquals((int) reporter.query(".total--compute_key_size_in_byte.4thPercentile").value(), 4);
    Assert.assertEquals((int) reporter.query(".total--compute_response_size.1thPercentile").value(), 1);
    Assert.assertEquals((int) reporter.query(".total--compute_response_size.2thPercentile").value(), 2);
    Assert.assertEquals((int) reporter.query(".total--compute_response_size.3thPercentile").value(), 3);
    Assert.assertEquals((int) reporter.query(".total--compute_response_size.4thPercentile").value(), 4);
  }

  @Test
  public void testDisableMultiGetStoreMetrics() {
    String clusterName = "test-cluster";
    AggRouterHttpRequestStats multiGetStats = new AggRouterHttpRequestStats(
        clusterName,
        metricsRepository,
        RequestType.MULTI_GET,
        false,
        storeMetadataRepository,
        true,
        mock(Sensor.class));
    AggRouterHttpRequestStats streamingMultiGetStats = new AggRouterHttpRequestStats(
        clusterName,
        metricsRepository,
        RequestType.MULTI_GET_STREAMING,
        false,
        storeMetadataRepository,
        true,
        mock(Sensor.class));
    String storeName = Utils.getUniqueString("test-store");
    multiGetStats.recordRequest(storeName);
    streamingMultiGetStats.recordRequest(storeName);
    multiGetStats.recordHealthyRequest(storeName, 10, HttpResponseStatus.OK, 1);
    streamingMultiGetStats.recordHealthyRequest(storeName, 10, HttpResponseStatus.OK, 1);
    // Total stats should exist for streaming and non-streaming multi-get
    Assert.assertEquals((int) reporter.query(".total--multiget_request.Count").value(), 1);
    Assert.assertEquals((int) reporter.query(".total--multiget_streaming_request.Count").value(), 1);
    Assert.assertEquals((int) reporter.query(".total--multiget_healthy_request_latency.Max").value(), 10);
    Assert.assertEquals((int) reporter.query(".total--multiget_streaming_healthy_request_latency.Max").value(), 10);
    // Store level stats should only exist for streaming multi-get
    Assert.assertEquals((int) reporter.query("." + storeName + "--multiget_streaming_request.Count").value(), 1);
    Assert.assertEquals(
        (int) reporter.query("." + storeName + "--multiget_streaming_healthy_request_latency.Max").value(),
        10);
    TehutiException exception =
        Assert.expectThrows(TehutiException.class, () -> reporter.query("." + storeName + "--multiget_request.Count"));
    Assert.assertTrue(exception.getMessage().contains("does not exist"));
    exception = Assert.expectThrows(
        TehutiException.class,
        () -> reporter.query("." + storeName + "--multiget_healthy_request_latency.Max"));
    Assert.assertTrue(exception.getMessage().contains("does not exist"));
  }
}
