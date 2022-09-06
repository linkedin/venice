package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

import com.linkedin.davinci.stats.DIVStats;
import com.linkedin.davinci.stats.DIVStatsReporter;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DIVStatsReporterTest {
  @Test
  public void testDIVReporterCanReport() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    String storeName = Utils.getUniqueString("store");
    DIVStatsReporter divStatsReporter = new DIVStatsReporter(metricsRepository, storeName);

    Assert.assertEquals(
        reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);

    DIVStats stats = new DIVStats();
    stats.recordSuccessMsg();
    divStatsReporter.setStats(stats);
    Assert.assertEquals(reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(), 1d);

    divStatsReporter.setStats(null);
    Assert.assertEquals(
        reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);
  }
}
