package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;


public class VeniceVersionedStatsReporter<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>>
    extends AbstractVeniceStats {
  private int currentVersion = NON_EXISTING_VERSION;
  private int futureVersion = NON_EXISTING_VERSION;

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

    registerSensor("current_version", new AsyncGauge((ignored1, ignored2) -> currentVersion, "current_version"));
    registerSensor("future_version", new AsyncGauge((ignored1, ignored2) -> futureVersion, "future_version"));

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
    return currentVersion;
  }

  public int getFutureVersion() {
    return futureVersion;
  }

  public void setCurrentStats(int version, STATS stats) {
    currentVersion = version;
    linkStatsWithReporter(currentStatsReporter, stats);
  }

  public void setFutureStats(int version, STATS stats) {
    futureVersion = version;
    linkStatsWithReporter(futureStatsReporter, stats);
  }

  public void setTotalStats(STATS totalStats) {
    linkStatsWithReporter(totalStatsReporter, totalStats);
  }

  private void linkStatsWithReporter(STATS_REPORTER reporter, STATS stats) {
    if (reporter != null) {
      reporter.setStats(stats);
    }
  }
}
