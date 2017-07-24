package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class VersionedDIVStats {
  private static final Logger logger = Logger.getLogger(VersionedDIVStats.class);

  private final String storeName;
  private final Map<Integer, DIVStats> versionedStats;
  private final DIVStats totalStats;
  private final VersionedDIVStatsReporter reporters;

  public VersionedDIVStats(MetricsRepository metricsRepository, String storeName) {
    this.versionedStats = new HashMap<>();
    this.storeName = storeName;
    this.reporters = new VersionedDIVStatsReporter(metricsRepository, storeName,
        (mr, name) -> new DIVStatsReporter(mr, name));

    totalStats = new DIVStats();
    reporters.setTotalStats(totalStats);
  }

  public void recordDuplicateMsg(int version) {
    totalStats.recordDuplicateMsg();
    getStats(version).recordDuplicateMsg();
  }

  public void recordMissingMsg(int version) {
    totalStats.recordMissingMsg();
    getStats(version).recordMissingMsg();
  }

  public void recordCorruptedMsg(int version) {
    totalStats.recordCorruptedMsg();
    getStats(version).recordCorruptedMsg();
  }

  public void recordSuccessMsg(int version) {
    totalStats.recordSuccessMsg();
    getStats(version).recordSuccessMsg();
  }

  public void recordCurrentIdleTime(int version) {
    //we don't record current idle time for total stats
    getStats(version).recordCurrentIdleTime();
  }

  public void recordOverallIdleTime(int version) {
    totalStats.recordOverallIdleTime();
    getStats(version).recordOverallIdleTime();
  }

  public void resetCurrentIdleTime(int version) {
    getStats(version).resetCurrentIdleTime();
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

  public synchronized Set<Integer> getAllVersionNumbers() {
    return Collections.unmodifiableSet(versionedStats.keySet());

  }

  private DIVStats getStats(int version) {
    if (!versionedStats.containsKey(version)) {
      logger.warn("DIVStats has not been created while trying to set it as current version. "
          + "Something might be wrong. Version: " + version);
      addVersion(version);
    }

    return versionedStats.get(version);
  }

  public boolean containsVersion(int version) {
    return versionedStats.containsKey(version);
  }

  public synchronized void addVersion(int version) {
    if (!versionedStats.containsKey(version)) {
      versionedStats.put(version, new DIVStats());
    } else {
      logger.warn("DIVStats has already been created. Something might be wrong. "
          + "Store: " + storeName + " version: " + version);
    }
  }

  public synchronized void removeVersion(int version) {
    if (versionedStats.containsKey(version)) {
      versionedStats.remove(version);
    } else {
      logger.warn("DIVStats has already been removed. Something might be wrong. "
          + "Store: " + storeName + " version: " + version);
    }
  }
}
