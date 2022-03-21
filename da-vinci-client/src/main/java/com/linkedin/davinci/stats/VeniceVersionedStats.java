package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.StatsSupplier;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceVersionedStats<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>> {
  private static final Logger logger = LogManager.getLogger(VeniceVersionedStats.class);

  private final String storeName;
  private final Map<Integer, STATS> versionedStats;
  private final VeniceVersionedStatsReporter<STATS, STATS_REPORTER> reporters;

  private final Supplier<STATS> statsInitiator;
  private final STATS totalStats;

  public VeniceVersionedStats(MetricsRepository metricsRepository, String storeName,
      Supplier<STATS>statsInitiator,  StatsSupplier<STATS_REPORTER> reporterSupplier) {
    this.storeName = storeName;
    this.versionedStats = new HashMap<>();
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

  public int getCurrentVersion() {
    return reporters.getCurrentVersion();
  }

  public int getFutureVersion() {
    return reporters.getFutureVersion();
  }

  public int getBackupVersion() {
    return reporters.getBackupVersion();
  }

  public void setCurrentVersion(int version) {
    reporters.setCurrentStats(version, getStats(version));
  }

  public void setFutureVersion(int version) {
    reporters.setFutureStats(version, getStats(version));
  }

  public void setBackupVersion(int version) {
    reporters.setBackupStats(version, getStats(version));
  }

    /**
     * return a deep copy of all version numbers
     */
  public synchronized Set<Integer> getAllVersionNumbers() {
    Set<Integer> versionNums = new HashSet<>();
    versionedStats.keySet().forEach(key -> versionNums.add(key));
    return versionNums;
  }

  protected STATS getStats(int version) {
    if (!versionedStats.containsKey(version)) {
      logger.warn("Stats has not been created while trying to set it as current version. "
          + "Store: " + storeName + " version: " + version);
      addVersion(version);
    }

    return versionedStats.get(version);
  }

  public boolean containsVersion(int version) {
    return versionedStats.containsKey(version);
  }

  public synchronized void addVersion(int version) {
    if (!versionedStats.containsKey(version)) {
      versionedStats.put(version, statsInitiator.get());
    } else {
      logger.warn("Stats has already been created. "
          + "Store: " + storeName + " version: " + version);
    }
  }

  public synchronized void removeVersion(int version) {
    if (versionedStats.containsKey(version)) {
      versionedStats.remove(version);
    } else {
      logger.warn("Stats has already been removed. Something might be wrong. "
          + "Store: " + storeName + " version: " + version);
    }
  }
}
