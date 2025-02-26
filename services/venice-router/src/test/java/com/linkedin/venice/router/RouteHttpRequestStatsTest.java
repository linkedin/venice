package com.linkedin.venice.router;

import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_METRIC_PREFIX;
import static com.linkedin.venice.router.RouterServer.ROUTER_SERVICE_NAME;
import static com.linkedin.venice.router.RouterServer.TOTAL_INFLIGHT_REQUEST_COUNT;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterHttpRequestStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


public class RouteHttpRequestStatsTest {
  private MockTehutiReporter reporter;
  private RouteHttpRequestStats stats;
  private RouterHttpRequestStats routerHttpRequestStats;

  private VeniceMetricsRepository localMetricRepo;

  @BeforeSuite
  public void setUp() {
    MetricsRepository metrics = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);
    MetricConfig metricConfig = new MetricConfig().timeWindow(1, TimeUnit.SECONDS);
    localMetricRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setServiceName(ROUTER_SERVICE_NAME)
            .setMetricPrefix(ROUTER_SERVICE_METRIC_PREFIX)
            .setTehutiMetricConfig(metricConfig)
            .build());
    Sensor totalInflightRequestSensor = localMetricRepo.sensor("total_inflight_request");
    totalInflightRequestSensor.add(TOTAL_INFLIGHT_REQUEST_COUNT, new Rate());
    stats = new RouteHttpRequestStats(metrics, mock(StorageNodeClient.class));
    routerHttpRequestStats = new RouterHttpRequestStats(
        metrics,
        "test-store",
        "test-cluster",
        RequestType.SINGLE_GET,
        mock(ScatterGatherStats.class),
        false,
        totalInflightRequestSensor);
  }

  @Test
  public void routerMetricsTest() {
    stats.recordPendingRequest("my_host1");
    stats.recordPendingRequest("my_host2");

    Assert.assertEquals(stats.getPendingRequestCount("my_host1"), 1);

    stats.recordPendingRequest("my_host1");
    Assert.assertEquals(stats.getPendingRequestCount("my_host1"), 2);

    stats.recordFinishedRequest("my_host1");
    stats.recordFinishedRequest("my_host2");

    Assert.assertEquals(stats.getPendingRequestCount("my_host1"), 1);
    Assert.assertEquals(stats.getPendingRequestCount("my_host2"), 0);
  }

  @Test
  public void routerInFlightMetricTest() {
    routerHttpRequestStats.recordIncomingRequest();
    Assert.assertTrue(getInFlightRequestRate() > 0.0);
    // After waiting for metric time window, it wil be reset back to 0
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      Assert.assertEquals(getInFlightRequestRate(), 0.0);
    });
  }

  public double getInFlightRequestRate() {
    Metric metric = localMetricRepo.getMetric(TOTAL_INFLIGHT_REQUEST_COUNT);
    // max return -infinity when there are no samples. validate only against finite value
    return Double.isFinite(metric.value()) ? metric.value() : 0.0;
  }
}
