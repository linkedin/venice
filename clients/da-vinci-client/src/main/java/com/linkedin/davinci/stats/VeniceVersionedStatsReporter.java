package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;


public class VeniceVersionedStatsReporter<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>>
    extends AbstractVeniceStats {
  private static final AtomicBoolean IS_VERSION_STATS_SETUP = new AtomicBoolean(false);

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

    /**
     * All stats which are aware of version should line up with Helix listener. It's duplicate
     * to store version metrics for each type of stats. It also causes metric conflict. See
     * {@link MetricsRepository#registerMetric}. Adding an atomic state may not be a good
     * way to deal with it. We might want to refactor the code in the future to remove it. (e.g.
     * we could have a centralized stats version dispatcher that listens to Helix data changes,
     * keeps track of store version and notifies each stats.
     * TODO: get ride of {@link IS_VERSION_STATS_SETUP}
     */
    if (IS_VERSION_STATS_SETUP.compareAndSet(false, true)) {
      registerSensor("current_version", new VersionStat(() -> (double) currentVersion));
      if (!isSystemStore) {
        registerSensor("future_version", new VersionStat(() -> (double) futureVersion));
      }
    }

    this.currentStatsReporter = statsSupplier.get(metricsRepository, storeName + "_current");
    if (!isSystemStore) {
      this.futureStatsReporter = statsSupplier.get(metricsRepository, storeName + "_future");
      this.totalStatsReporter = statsSupplier.get(metricsRepository, storeName + "_total");
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

  /**
   * {@link VersionStat} works exactly the same as {@link Gauge}. The reason we
   * want to keep it here is for maintaining metric name's backward-compatibility. (We
   * suffix class name to metric name to indicate metric's types.)
   */
  private static class VersionStat extends Gauge {
    VersionStat(DoubleSupplier supplier) {
      super(supplier::getAsDouble);
    }
  }

  /**
   * Only for tests, to reset the global state
   * TODO: Fix, VOLDENG-4211
   */
  public static void resetStats() {
    IS_VERSION_STATS_SETUP.set(false);
  }
}
