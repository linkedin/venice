package com.linkedin.venice.router;

import static org.mockito.Mockito.mock;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterHttpRequestStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


public class RouteHttpRequestStatsTest {
  private MockTehutiReporter reporter;
  private RouteHttpRequestStats stats;
  private RouterHttpRequestStats routerHttpRequestStats;

  @BeforeSuite
  public void setUp() {
    MetricsRepository metrics = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);

    stats = new RouteHttpRequestStats(metrics, mock(StorageNodeClient.class));
    routerHttpRequestStats =
        new RouterHttpRequestStats(metrics, "", RequestType.SINGLE_GET, mock(ScatterGatherStats.class), false);
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
    routerHttpRequestStats.recordRequest();
    Assert.assertTrue(RouterHttpRequestStats.hasInFlightRequests());
  }
}
