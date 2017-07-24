package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;


public class VersionedDIVStatsReporter extends AbstractVersionedReporter<DIVStatsReporter> {
  private final DIVStatsReporter totalStatsReporter;

  public VersionedDIVStatsReporter(MetricsRepository metricsRepository, String storeName,
      StatsSupplier<DIVStatsReporter> statsSupplier) {
    super(metricsRepository, storeName, statsSupplier);

    totalStatsReporter = new DIVStatsReporter(metricsRepository, storeName + "_total");
  }

  public void setCurrentStats(int version, DIVStats stats) {
    setCurrentVersion(version);
    getCurrentReporter().setDIVStats(stats);
  }

  public void setFutureStats(int version, DIVStats stats) {
    setFutureVersion(version);
    getFutureReporter().setDIVStats(stats);
  }

  public void setBackupStats(int version, DIVStats stats) {
    setBackupVersion(version);
    getBackupReporter().setDIVStats(stats);
  }

  public void setTotalStats(DIVStats totalStats) {
    totalStatsReporter.setDIVStats(totalStats);
  }
}
