package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_DIV_STATS;

import com.linkedin.davinci.stats.DIVStats;
import com.linkedin.davinci.stats.DIVStatsReporter;
import com.linkedin.davinci.stats.VeniceVersionedStatsReporter;
import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VersionedDIVStatsReporterTest {
  @Test
  public void testVersionedDIVStatsReporterCanReport() {

    VeniceVersionedStatsReporter.resetStats();

    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    String storeName = Utils.getUniqueString("store");
    VeniceVersionedStatsReporter<DIVStats, DIVStatsReporter> statsReporter =
        new VeniceVersionedStatsReporter<>(metricsRepository, storeName, (mr, name) -> new DIVStatsReporter(mr, name));
    DIVStats stats = new DIVStats();

    stats.recordCurrentIdleTime();
    statsReporter.setFutureStats(1, stats);
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(), 1d);

    statsReporter.setFutureStats(0, null);
    stats.recordCurrentIdleTime();
    statsReporter.setCurrentStats(1, stats);
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 0d);
    Assert.assertEquals(
        reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(),
        (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);
  }
}
