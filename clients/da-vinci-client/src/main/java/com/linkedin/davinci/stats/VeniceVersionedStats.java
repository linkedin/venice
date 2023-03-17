package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceVersionedStats<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVersionedStats.class);

  private final String storeName;
  private final Int2ObjectMap<STATS> versionedStats;
  private final VeniceVersionedStatsReporter<STATS, STATS_REPORTER> reporters;

  private final Supplier<STATS> statsInitiator;
  private final STATS totalStats;

  public VeniceVersionedStats(
      MetricsRepository metricsRepository,
      String storeName,
      Supplier<STATS> statsInitiator,
      StatsSupplier<STATS_REPORTER> reporterSupplier) {
    this.storeName = storeName;
    this.versionedStats = new Int2ObjectOpenHashMap<>();
    this.reporters = new VeniceVersionedStatsReporter<>(metricsRepository, storeName, reporterSupplier);
    this.statsInitiator = statsInitiator;

    this.totalStats = statsInitiator.get();
    reporters.setTotalStats(totalStats);
  }

  protected STATS getTotalStats() {
    return totalStats;
  }

  public void registerConditionalStats() {
    reporters.registerConditionalStats();
  }

  public void unregisterStats() {
    reporters.unregisterStats();
  }

  public int getCurrentVersion() {
    return reporters.getCurrentVersion();
  }

  public int getFutureVersion() {
    return reporters.getFutureVersion();
  }

  public void setCurrentVersion(int version) {
    reporters.setCurrentStats(version, getStats(version));
  }

  public void setFutureVersion(int version) {
    reporters.setFutureStats(version, getStats(version));
  }

  /**
   * return a deep copy of all version numbers
   */
  public synchronized IntSet getAllVersionNumbers() {
    return new IntOpenHashSet(versionedStats.keySet());
  }

  protected STATS getStats(int version) {
    STATS stats = versionedStats.get(version);
    if (stats == null) {
      LOGGER.warn(
          "Stats has not been created while trying to set it as current version. Store: {}, version: {}",
          storeName,
          version);
      stats = addVersion(version);
    }
    return stats;
  }

  public synchronized STATS addVersion(int version) {
    STATS stats = versionedStats.get(version);
    if (stats == null) {
      stats = statsInitiator.get();
      versionedStats.put(version, stats);
    }
    return stats;
  }

  public synchronized void removeVersion(int version) {
    if (versionedStats.remove(version) == null) {
      LOGGER
          .warn("Stats has already been removed. Something might be wrong. Store: {}, version: {}", storeName, version);
    }
  }
}
