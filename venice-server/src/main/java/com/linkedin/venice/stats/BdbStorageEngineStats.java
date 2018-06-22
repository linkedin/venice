package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.bdb.BdbSpaceUtilizationSummary;
import com.linkedin.venice.utils.CachedCallable;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;

import static com.linkedin.venice.stats.StatsErrorCode.*;

/**
 * Specific metrics for BDB.
 * TODO: may want to have an abstract storageEngine metrics layer if Venice allows other storage engines.
 */

public class BdbStorageEngineStats {
  private Environment bdbEnvironment = null;
  /**
   * Both {@link Environment} and {@link BdbSpaceUtilizationSummary} take non-trivial creation time.
   * Cache and reuse them to improve the performance.
   */
  private  CachedCallable<EnvironmentStats> fastStats;
  private  CachedCallable<BdbSpaceUtilizationSummary> fastSpaceStats;

  public BdbStorageEngineStats() {
    //in the case bdbEnvironment is null, metrics will return 0
    this.fastStats =
        new CachedCallable<>(() -> bdbEnvironment == null ?
            null : getEnvironmentStats(bdbEnvironment), 5 * Time.MS_PER_SECOND);
    this.fastSpaceStats =
        new CachedCallable<>(() -> bdbEnvironment == null ?
            null : new BdbSpaceUtilizationSummary(bdbEnvironment), 5 * Time.MS_PER_SECOND);
  }

  public void setBdbEnvironment(Environment bdbEnvironment) {
    this.bdbEnvironment = bdbEnvironment;
  }

  public void removeBdbEnvironment() {
    this.bdbEnvironment = null;
  }

  private EnvironmentStats getEnvironmentStats(Environment bdbEnvironment) {
    StatsConfig config = new StatsConfig();
    config.setFast(true);
    return bdbEnvironment.getStats(config);
  }

  public EnvironmentStats getFastStats() {
    try {
      return fastStats.call();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  public BdbSpaceUtilizationSummary getFastSpaceUtilizationSummary() {
    try {
      return fastSpaceStats.call();
    } catch(Exception e) {
      throw new VeniceException(e);
    }
  }

  //Read/Write
  public double getPercentageCacheMisses() {
    return Utils.getRatio(getNumCacheMiss(), getNumReadsTotal() + getNumWritesTotal());
  }

  private long getNumCacheMiss() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNCacheMiss();
  }

  public double getPercentageBINMiss() {
    return Utils.getRatio(getBINFetchMisses(), getBINFetches());
  }

  public double getPercentageINMiss() {
    return Utils.getRatio(getINFetchMisses(), getINFetches());
  }

  private long getBINFetchMisses() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNBINsFetchMiss();
  }

  private long getBINFetches() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNBINsFetch();
  }

  private long getINFetchMisses() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNUpperINsFetchMiss();
  }

  private long getINFetches() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNUpperINsFetch();
  }

  private long getNumReadsTotal() {
    return getNumRandomReads() + getNumSequentialReads();
  }

  private long getNumRandomReads() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNRandomReads();
  }

  private long getNumSequentialReads() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNSequentialReads();
  }

  private long getNumWritesTotal() {
    return getNumRandomWrites() + getNumSequentialWrites();
  }

  private long getNumRandomWrites() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNRandomWrites();
  }

  private long getNumSequentialWrites() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNSequentialWrites();
  }

  //Disk
  public long getAllottedCacheSize() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getCacheTotalBytes();
  }

  public double getPercentageSpaceUtilization() {
    return Utils.getRatio(getTotalSpaceUtilized(), getTotalSpace());
  }

  private long getTotalSpaceUtilized() {
    BdbSpaceUtilizationSummary summary = getFastSpaceUtilizationSummary();
    return summary == null ? NULL_BDB_ENVIRONMENT.code : summary.getTotalSpaceUtilized();
  }

  public long getTotalSpace() {
    BdbSpaceUtilizationSummary summary = getFastSpaceUtilizationSummary();
    return summary == null ? NULL_BDB_ENVIRONMENT.code : summary.getTotalSpaceUsed();
  }

  //cleaning
  public long getNumCleanerRuns() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNCleanerRuns();
  }

  public long getCleanerBacklog() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getCleanerBacklog();
  }

  public long getNumCleanerEntriesRead() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNCleanerEntriesRead();
  }

  public long getNumCleanerDeletions() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? NULL_BDB_ENVIRONMENT.code : stats.getNCleanerDeletions();
  }
}