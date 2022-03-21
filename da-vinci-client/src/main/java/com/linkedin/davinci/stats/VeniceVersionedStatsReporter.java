package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

public class VeniceVersionedStatsReporter<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>>
    extends AbstractVeniceStats {
  private static AtomicBoolean isVersionStatsSetup = new AtomicBoolean(false);

  private int currentVersion = NON_EXISTING_VERSION;
  private int futureVersion = NON_EXISTING_VERSION;
  private int backupVersion = NON_EXISTING_VERSION;

  private final STATS_REPORTER currentStatsReporter;
  private final STATS_REPORTER futureStatsReporter;
  private final STATS_REPORTER backupStatsReporter;
  private final STATS_REPORTER totalStatsReporter;

  public VeniceVersionedStatsReporter(MetricsRepository metricsRepository, String storeName, StatsSupplier<STATS_REPORTER> statsSupplier) {
    super(metricsRepository, storeName);

    /**
     * All stats which are aware of version should line up with Helix listener. It's duplicate
     * to store version metrics for each type of stats. It also causes metric conflict. See
     * {@link MetricsRepository#registerMetric}. Adding an atomic state may not be a good
     * way to deal with it. We might want to refactor the code in the future to remove it. (e.g.
     * we could have a centralized stats version dispatcher that listens to Helix data changes,
     * keeps track of store version and notifies each stats.
     * TODO: get ride of {@link isVersionStatsSetup}
     */
    if (isVersionStatsSetup.compareAndSet(false, true)) {
      registerSensor("current_version", new VersionStat(() -> (double) currentVersion));
      registerSensor("future_version", new VersionStat(() -> (double) futureVersion));
      registerSensor("backup_version", new VersionStat(() -> (double) backupVersion));
    }

    this.currentStatsReporter = statsSupplier.get(metricsRepository, storeName + "_current");
    this.futureStatsReporter = statsSupplier.get(metricsRepository, storeName + "_future");
    this.backupStatsReporter = statsSupplier.get(metricsRepository, storeName + "_backup");
    this.totalStatsReporter = statsSupplier.get(metricsRepository, storeName + "_total");
  }

  public void registerConditionalStats() {
    this.currentStatsReporter.registerConditionalStats();
    this.futureStatsReporter.registerConditionalStats();
    this.backupStatsReporter.registerConditionalStats();
    this.totalStatsReporter.registerConditionalStats();
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  public int getFutureVersion() {
    return futureVersion;
  }

  public int getBackupVersion() {
    return backupVersion;
  }

  public void setCurrentStats(int version, STATS stats) {
    setCurrentVersion(version);
    getCurrentStatsReporter().setStats(stats);
  }

  public void setFutureStats(int version, STATS stats) {
    setFutureVersion(version);
    getFutureStatsReporter().setStats(stats);
  }

  public void setBackupStats(int version, STATS stats) {
    setBackupVersion(version);
    getBackupStatsReporter().setStats(stats);
  }

  protected void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  protected void setFutureVersion(int futureVersion) {
    this.futureVersion = futureVersion;
  }

  protected void setBackupVersion(int backupVersion) {
    this.backupVersion = backupVersion;
  }

  public STATS_REPORTER getCurrentStatsReporter() {
    return currentStatsReporter;
  }

  public STATS_REPORTER getFutureStatsReporter() {
    return futureStatsReporter;
  }

  public STATS_REPORTER getBackupStatsReporter() {
    return backupStatsReporter;
  }

  public void setTotalStats(STATS totalStats) {
    totalStatsReporter.setStats(totalStats);
  }

  /**
   * {@link VersionStat} works exactly the same as {@link Gauge}. The reason we
   * want to keep it here is for maintaining metric name's backward-compatibility. (We
   * suffix class name to metric name to indicate metric's types.)
   */
  private static class VersionStat extends Gauge {
    VersionStat(DoubleSupplier supplier) {
      super(() -> supplier.getAsDouble());
    }
  }

  /**
   * Only for tests, to reset the global state
   * TODO: Fix, VOLDENG-4211
   */
  public static void resetStats(){
    isVersionStatsSetup.set(false);
  }
}
