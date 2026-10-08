package com.linkedin.davinci.stats;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;


public class VeniceVersionedStatsReporter<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>>
    extends AbstractVeniceStats {
  // Published as one snapshot so record-time role classification sees a consistent current/future pair.
  private volatile VersionInfo versionInfo = VersionInfo.NON_EXISTING;

  private final STATS_REPORTER currentStatsReporter;
  private final STATS_REPORTER futureStatsReporter;
  private final STATS_REPORTER totalStatsReporter;
  private final boolean isSystemStore;

  public VeniceVersionedStatsReporter(
      MetricsRepository metricsRepository,
      String storeName,
      StatsSupplier<STATS_REPORTER> statsSupplier) {
    super(metricsRepository, storeName);

    this.isSystemStore = VeniceSystemStoreUtils.isSystemStore(storeName);

    registerSensor(
        "current_version",
        new AsyncGauge((ignored1, ignored2) -> versionInfo.getCurrentVersion(), "current_version"));
    registerSensor(
        "future_version",
        new AsyncGauge((ignored1, ignored2) -> versionInfo.getFutureVersion(), "future_version"));

    this.currentStatsReporter = statsSupplier.get(metricsRepository, storeName + "_current", (String) null);
    if (!isSystemStore) {
      this.futureStatsReporter = statsSupplier.get(metricsRepository, storeName + "_future", (String) null);
      this.totalStatsReporter = statsSupplier.get(metricsRepository, storeName + "_total", (String) null);
    } else {
      this.futureStatsReporter = null;
      this.totalStatsReporter = null;
    }
  }

  public void registerConditionalStats() {
    this.currentStatsReporter.registerConditionalStats();
    if (!isSystemStore) {
      this.futureStatsReporter.registerConditionalStats();
      this.totalStatsReporter.registerConditionalStats();
    }
  }

  public void unregisterStats() {
    this.currentStatsReporter.unregisterStats();
    if (!isSystemStore) {
      this.futureStatsReporter.unregisterStats();
      this.totalStatsReporter.unregisterStats();
    }
    super.unregisterAllSensors();
  }

  public int getCurrentVersion() {
    return versionInfo.getCurrentVersion();
  }

  public int getFutureVersion() {
    return versionInfo.getFutureVersion();
  }

  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  public synchronized void setCurrentStats(int version, STATS stats) {
    versionInfo = new VersionInfo(version, versionInfo.getFutureVersion());
    linkStatsWithReporter(currentStatsReporter, stats);
  }

  public synchronized void setFutureStats(int version, STATS stats) {
    versionInfo = new VersionInfo(versionInfo.getCurrentVersion(), version);
    linkStatsWithReporter(futureStatsReporter, stats);
  }

  public void setTotalStats(STATS totalStats) {
    linkStatsWithReporter(totalStatsReporter, totalStats);
  }

  /** Links role-scoped stats once; unlike {@link #setCurrentStats}, they are not re-pointed on swap. */
  public void setRoleStats(STATS currentRoleStats, STATS futureRoleStats) {
    currentStatsReporter.setRoleStats(currentRoleStats);
    if (futureStatsReporter != null) {
      futureStatsReporter.setRoleStats(futureRoleStats);
    }
  }

  private void linkStatsWithReporter(STATS_REPORTER reporter, STATS stats) {
    if (reporter != null) {
      reporter.setStats(stats);
    }
  }
}
