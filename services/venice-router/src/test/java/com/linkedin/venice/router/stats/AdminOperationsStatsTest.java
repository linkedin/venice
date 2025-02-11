package com.linkedin.venice.router.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminOperationsStatsTest {
  @Test
  public void testAdminOperationsStats() {
    MetricsRepository metrics = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository();
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
