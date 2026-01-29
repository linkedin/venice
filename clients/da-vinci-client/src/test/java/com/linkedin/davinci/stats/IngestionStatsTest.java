package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.tehuti.metrics.MetricConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionStatsTest {
  @Test
  public void testIngestionStatsGauge() {
    AbstractVeniceStatsReporter mockReporter = mock(AbstractVeniceStatsReporter.class);
    doReturn(mock(IngestionStats.class)).when(mockReporter).getStats();
    IngestionStatsReporter.IngestionStatsGauge gauge =
        new IngestionStatsReporter.IngestionStatsGauge(mockReporter, () -> 1.0, "testIngestionStatsGauge");
    Assert.assertEquals(gauge.measure(new MetricConfig(), System.currentTimeMillis()), 1.0);
  }
}
