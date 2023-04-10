package com.linkedin.venice.stats;

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
        new VeniceVersionedStatsReporter<>(metricsRepository, storeName, DIVStatsReporter::new);
    DIVStats stats = new DIVStats();

    statsReporter.setFutureStats(1, stats);
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 1d);

    statsReporter.setFutureStats(0, null);
    statsReporter.setCurrentStats(1, stats);
    Assert.assertEquals(reporter.query("." + storeName + "--future_version.VersionStat").value(), 0d);
    Assert.assertEquals(reporter.query("." + storeName + "--current_version.VersionStat").value(), 1d);
  }
}
