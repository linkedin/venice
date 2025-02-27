package com.linkedin.venice.router;

import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterHttpRequestStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


public class RouteHttpRequestStatsTest {
  private MockTehutiReporter reporter;
  private RouteHttpRequestStats stats;
  private RouterHttpRequestStats routerHttpRequestStats;
  private InFlightRequestStat inFlightRequestStat;

  @BeforeSuite
  public void setUp() {
    MetricsRepository metrics = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);
    VeniceRouterConfig mockConfig = mock(VeniceRouterConfig.class);
    doReturn(1).when(mockConfig).getRouterInFlightMetricWindowSeconds();
    inFlightRequestStat = new InFlightRequestStat(mockConfig);
    Sensor totalInflightRequestSensor = inFlightRequestStat.getTotalInflightRequestSensor();
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
    Assert.assertTrue(inFlightRequestStat.getInFlightRequestRate() > 0.0);
    // After waiting for metric time window, it wil be reset back to 0
    waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      Assert.assertEquals(inFlightRequestStat.getInFlightRequestRate(), 0.0);
    });
  }
}
