package com.linkedin.venice.router;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class AggRouterHttpRequestStatsTest {
  MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;
  private AggRouterHttpRequestStats stats;

  @BeforeSuite
  public void setUp() {
    this.metricsRepository = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);
  }

  @Test
  public void RouterMetricsTest() {
    AggRouterHttpRequestStats stats = new AggRouterHttpRequestStats(metricsRepository, RequestType.SINGLE_GET);

    stats.recordRequest("store5");
    Assert.assertEquals(reporter.query(".total--request.Count").value(), 1d);

    stats.recordRequest("store1");
    Assert.assertEquals(reporter.query(".total--request.Count").value(), 2d);
    Assert.assertEquals(reporter.query(".store1--request.Count").value(), 1d);

    stats.recordThrottledRequest("store1", 1.0);
    stats.recordThrottledRequest("store2", 1.0);
    Assert.assertEquals(reporter.query(".total--request.Count").value(), 2d);
    Assert.assertEquals(reporter.query(".store1--request.Count").value(), 1d);


    for (int i = 1; i <= 100; i += 1) {
      stats.recordLatency("store2", i);
    }

    System.out.println(reporter.query(".total--latency.50thPercentile").value());

    Assert.assertEquals((int) reporter.query(".total--latency.50thPercentile").value(), 50);
    Assert.assertEquals((int) reporter.query(".total--latency.95thPercentile").value(), 95);
    Assert.assertEquals((int) reporter.query(".total--latency.99thPercentile").value(), 99);

    Assert.assertEquals((int) reporter.query(".store2--latency.50thPercentile").value(), 50);
  }

  @Test
  public void TestProfilingMetrics() {
    AggRouterHttpRequestStats stats =
        new AggRouterHttpRequestStats(metricsRepository, RequestType.COMPUTE, true);

    for (int i = 1; i <= 100; i += 1) {
      stats.recordKeySize("store1", i);
    }

    Assert.assertEquals((int) reporter.query(".total--compute_key_size_in_byte.1thPercentile").value(), 1);
    Assert.assertEquals((int) reporter.query(".total--compute_key_size_in_byte.2thPercentile").value(), 2);
    Assert.assertEquals((int) reporter.query(".store1--compute_key_size_in_byte.3thPercentile").value(), 3);
    Assert.assertEquals((int) reporter.query(".store1--compute_key_size_in_byte.4thPercentile").value(), 4);
  }
}
