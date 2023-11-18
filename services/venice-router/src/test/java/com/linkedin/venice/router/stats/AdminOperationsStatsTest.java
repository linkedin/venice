package com.linkedin.venice.router.stats;

import static org.mockito.Mockito.*;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.AsyncGaugeConfig;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminOperationsStatsTest {
  @Test
  public void testAdminOperationsStats() {
    MetricsRepository metrics = new MetricsRepository(
        new MetricConfig(new AsyncGaugeConfig(Executors.newSingleThreadExecutor(), TimeUnit.MINUTES.toMillis(1), 100)));
    MockTehutiReporter reporter = new MockTehutiReporter();
    metrics.addReporter(reporter);
    VeniceRouterConfig mockConfig = mock(VeniceRouterConfig.class);
    when(mockConfig.isReadThrottlingEnabled()).thenReturn(true);
    when(mockConfig.isEarlyThrottleEnabled()).thenReturn(true);
    new AdminOperationsStats(metrics, "testAdminOperationsStats", mockConfig);
    Assert.assertEquals(reporter.query(".testAdminOperationsStats--read_quota_throttle.Gauge").value(), 1d);
    when(mockConfig.isReadThrottlingEnabled()).thenReturn(false);
    when(mockConfig.isEarlyThrottleEnabled()).thenReturn(false);
    Assert.assertEquals(reporter.query(".testAdminOperationsStats--read_quota_throttle.Gauge").value(), 0d);
  }
}
