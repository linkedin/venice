package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.stats.AsyncGauge;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionStatsTest {
  @Test
  public void testIngestionStatsGauge() throws Exception {
    // Use a dedicated executor to avoid contention with the shared DEFAULT_ASYNC_GAUGE_EXECUTOR in CI
    try (AsyncGauge.AsyncGaugeExecutor executor = new AsyncGauge.AsyncGaugeExecutor.Builder().build()) {
      AbstractVeniceStatsReporter mockReporter = mock(AbstractVeniceStatsReporter.class);
      doReturn(mock(IngestionStats.class)).when(mockReporter).getStats();
      IngestionStatsReporter.IngestionStatsGauge gauge =
          new IngestionStatsReporter.IngestionStatsGauge(mockReporter, () -> 1.0, "testIngestionStatsGauge");
      Assert.assertEquals(gauge.measure(new MetricConfig(executor), System.currentTimeMillis()), 1.0);
    }
  }
}
