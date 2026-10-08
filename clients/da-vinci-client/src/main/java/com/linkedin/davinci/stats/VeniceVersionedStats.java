package com.linkedin.davinci.stats;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
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
  // Role-scoped stats; null unless role-scoped stats are enabled. See getRoleStats(int).
  private final STATS currentRoleStats;
  private final STATS futureRoleStats;

  public VeniceVersionedStats(
      MetricsRepository metricsRepository,
      String storeName,
      Supplier<STATS> statsInitiator,
      StatsSupplier<STATS_REPORTER> reporterSupplier) {
    this(metricsRepository, storeName, statsInitiator, reporterSupplier, false);
  }

  public VeniceVersionedStats(
      MetricsRepository metricsRepository,
      String storeName,
      Supplier<STATS> statsInitiator,
      StatsSupplier<STATS_REPORTER> reporterSupplier,
      boolean roleScopedStatsEnabled) {
    this.storeName = storeName;
    this.versionedStats = new Int2ObjectOpenHashMap<>();
    this.reporters = new VeniceVersionedStatsReporter<>(metricsRepository, storeName, reporterSupplier);
    this.statsInitiator = statsInitiator;

    this.totalStats = statsInitiator.get();
    reporters.setTotalStats(totalStats);

    if (roleScopedStatsEnabled) {
      this.currentRoleStats = statsInitiator.get();
      // System stores have no future reporter, so there is nothing to read future role stats.
      this.futureRoleStats = VeniceSystemStoreUtils.isSystemStore(storeName) ? null : statsInitiator.get();
      reporters.setRoleStats(currentRoleStats, futureRoleStats);
    } else {
      this.currentRoleStats = null;
      this.futureRoleStats = null;
    }
  }

  protected STATS getTotalStats() {
    return totalStats;
  }

  /**
   * Returns the role-scoped stats for the role {@code version} holds right now, or null if role-scoped stats are
   * disabled or the version is neither current nor future. Mirrors how OTel classifies the version role at record
   * time, so a backlog recorded while a version was future stays attributed to future after it is promoted.
   */
  protected STATS getRoleStats(int version) {
    if (currentRoleStats == null) {
      return null;
    }
    if (version == reporters.getCurrentVersion()) {
      return currentRoleStats;
    }
    if (version == reporters.getFutureVersion()) {
      return futureRoleStats;
    }
    return null;
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
      LOGGER.debug(
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
