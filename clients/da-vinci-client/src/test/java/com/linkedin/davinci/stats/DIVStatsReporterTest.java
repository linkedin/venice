package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.Test;


public class DIVStatsReporterTest {
  @Test
  public void testDIVReporterCanReport() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    String storeName = Utils.getUniqueString("store");
    DIVStatsReporter divStatsReporter = new DIVStatsReporter(metricsRepository, storeName);

    assertEquals(
        reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);

    DIVStats stats = new DIVStats();
    stats.recordSuccessMsg();
    divStatsReporter.setStats(stats);
    assertEquals(reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(), 1d);

    divStatsReporter.setStats(null);
    assertEquals(
        reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);
  }

  @Test
  public void testDIVStatsCounter() {
    /**
     * Test {@link DIVStatsReporter#DIVStatsCounter}
     */
    DIVStatsReporter mockDIVStatsReporter = mock(DIVStatsReporter.class);
    doReturn(mock(DIVStats.class)).when(mockDIVStatsReporter).getStats();
    DIVStatsReporter.DIVStatsCounter counter =
        new DIVStatsReporter.DIVStatsCounter("testDIVStatsCounter", mockDIVStatsReporter, () -> 1L);
    assertEquals(counter.measure(new MetricConfig(), System.currentTimeMillis()), 1.0);

  }
}
