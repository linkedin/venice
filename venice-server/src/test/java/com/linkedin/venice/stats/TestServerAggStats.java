package com.linkedin.venice.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Created by sdwu on 8/31/16.
 */
public class TestServerAggStats {
  private MockTehutiReporter reporter;
  private ServerAggStats stats;

  @BeforeSuite
  public void setup() {
    MetricsRepository metrics = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);

    ServerAggStats.init(metrics);
    stats = ServerAggStats.getInstance();
  }

  @Test
  public void ServerMetricsTest() {
    stats.recordSuccessRequest("store1");
    stats.recordSuccessRequest("store2");
    stats.recordErrorRequest("store1");
    stats.recordErrorRequest();

    Assert.assertEquals(reporter.query(".store1_success_request.SampledCount").value(), 1d);
    Assert.assertEquals(reporter.query(".total_error_request.SampledCount").value(), 2d);
    Assert.assertEquals(reporter.query(".total_success_request_ratio.LambdaStat").value(), 0.5d);
  }

  @AfterSuite
  public void cleanup() {
    stats.close();
  }
}