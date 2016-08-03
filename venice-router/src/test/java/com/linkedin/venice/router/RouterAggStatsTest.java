package com.linkedin.venice.router;

import com.linkedin.venice.router.stats.RouterAggStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class RouterAggStatsTest {

  private MockTehutiReporter reporter;
  private RouterAggStats stats;

  @BeforeSuite
  public void setup() {
    MetricsRepository metrics = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);

    RouterAggStats.init(metrics);
    stats = RouterAggStats.getStats();
  }

  @Test
  public void RouterMetricsTest() {
    stats.addRequest();
    Assert.assertEquals(reporter.query(".total_request.count").value(), 1d);

    stats.addRequest("store1");
    Assert.assertEquals(reporter.query(".total_request.count").value(), 2d);
    Assert.assertEquals(reporter.query(".store1_request.count").value(), 1d);

    for (int i = 1; i <= 100; i += 1) {
        stats.addLatency("store2", i);
    }

    Assert.assertEquals((int)reporter.query(".total_latency.50thPercentile").value(), 50);
    Assert.assertEquals((int)reporter.query(".total_latency.95thPercentile").value(), 95);
    Assert.assertEquals((int)reporter.query(".total_latency.99thPercentile").value(), 99);

    Assert.assertEquals((int)reporter.query(".store2_latency.50thPercentile").value(), 50);
  }

  @AfterSuite
  public void cleanup() {
      stats.close();
  }
}
