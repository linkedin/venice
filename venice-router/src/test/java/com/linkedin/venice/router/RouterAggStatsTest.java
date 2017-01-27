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
    stats = RouterAggStats.getInstance();
  }

  @Test
  public void RouterMetricsTest() {
    stats.recordRequest("store5");
    Assert.assertEquals(reporter.query(".total.request.Count").value(), 1d);

    stats.recordRequest("store1");
    Assert.assertEquals(reporter.query(".total.request.Count").value(), 2d);
    Assert.assertEquals(reporter.query(".store1.request.Count").value(), 1d);

    for (int i = 1; i <= 100; i += 1) {
        stats.recordLatency("store2", i);
    }

    Assert.assertEquals((int)reporter.query(".total.latency.50thPercentile").value(), 50);
    Assert.assertEquals((int)reporter.query(".total.latency.95thPercentile").value(), 95);
    Assert.assertEquals((int)reporter.query(".total.latency.99thPercentile").value(), 99);

    Assert.assertEquals((int)reporter.query(".store2.latency.50thPercentile").value(), 50);
  }

  @AfterSuite
  public void cleanup() {
      stats.close();
  }
}
