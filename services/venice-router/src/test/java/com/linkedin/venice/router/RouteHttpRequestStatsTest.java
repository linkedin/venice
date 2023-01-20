package com.linkedin.venice.router;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


public class RouteHttpRequestStatsTest {
  private MockTehutiReporter reporter;
  private RouteHttpRequestStats stats;

  @BeforeSuite
  public void setUp() {
    MetricsRepository metrics = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);

    stats = new RouteHttpRequestStats(metrics, mock(StorageNodeClient.class));
  }

  @Test
  public void routerMetricsTest() {
    stats.recordPendingRequest("my_host1");
    stats.recordPendingRequest("my_host2");

    Assert.assertEquals(reporter.query(".my_host1--pending_request_count.Gauge").value(), 1d);

    stats.recordPendingRequest("my_host1");
    Assert.assertEquals(reporter.query(".my_host1--pending_request_count.Gauge").value(), 2d);

    stats.recordFinishedRequest("my_host1");
    stats.recordFinishedRequest("my_host2");

    Assert.assertEquals(reporter.query(".my_host1--pending_request_count.Gauge").value(), 1d);
    Assert.assertEquals(reporter.query(".my_host2--pending_request_count.Gauge").value(), 0d);
  }
}
