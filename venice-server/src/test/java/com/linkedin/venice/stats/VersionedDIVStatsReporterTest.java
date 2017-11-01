package com.linkedin.venice.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.stats.StatsErrorCode.*;


public class VersionedDIVStatsReporterTest {
  @Test
  public void testVersionedDIVStatsReporterCanReport() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    String storeName = TestUtils.getUniqueString("store");
    VersionedDIVStatsReporter statsReporter = new VersionedDIVStatsReporter(metricsRepository, storeName,
        (mr, name) -> new DIVStatsReporter(mr, name));
    DIVStats stats = new DIVStats();

    stats.recordCurrentIdleTime();
    statsReporter.setFutureStats(1, stats);
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(), 1d);

    statsReporter.setFutureStats(0, null);
    stats.recordCurrentIdleTime();
    statsReporter.setCurrentStats(1, stats);
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "_future--current_idle_time.DIVStatsCounter").value(), (double) NULL_DIV_STATS.code);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
    Assert.assertEquals(reporter.query("." + storeName + "_current--current_idle_time.DIVStatsCounter").value(), 2d);
  }
}
