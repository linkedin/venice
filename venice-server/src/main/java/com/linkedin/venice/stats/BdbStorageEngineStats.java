package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.store.bdb.BdbSpaceUtilizationSummary;
import com.linkedin.venice.utils.CachedCallable;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;

/**
 * Specific metrics for BDB.
 * TODO: may want to have an abstract storageEngine metrics layer if Venice allows other storage engines.
 */

public class BdbStorageEngineStats extends AbstractVeniceStats {
  private Environment bdbEnvironment = null;
  /**
   * Both {@link Environment} and {@link BdbSpaceUtilizationSummary} take non-trivial creation time.
   * Cache and reuse them to improve the performance.
   */
  private  CachedCallable<EnvironmentStats> fastStats;
  private  CachedCallable<BdbSpaceUtilizationSummary> fastSpaceStats;

  public BdbStorageEngineStats(MetricsRepository metricsRepository, String name) {
    this(metricsRepository, name, 5 * Time.MS_PER_SECOND);
  }

  public BdbStorageEngineStats(MetricsRepository metricsRepository,
                               String storeName,
                               long ttlMs) {
    super(metricsRepository, storeName);

    //in the case bdbEnvironment is null, metrics will return 0
    this.fastStats =
        new CachedCallable<>(() -> bdbEnvironment == null ?
            null : getEnvironmentStats(bdbEnvironment), ttlMs);
    this.fastSpaceStats =
        new CachedCallable<>(() -> bdbEnvironment == null ?
            null : new BdbSpaceUtilizationSummary(bdbEnvironment), ttlMs);

    registerSensor("bdb_cache_misses_ratio",
        new CacheMissesRatioStat(this));
    registerSensor("bdb_space_utilization_ratio",
        new SpaceUtilizationRatioStat(this));
    registerSensor("bdb_allotted_cache_size_in_bytes",
        new AllottedCacheSizeStat(this));
    registerSensor("bdb_space_size_in_bytes",
        new SpaceSizeStat(this));
    registerSensor("bdb_disk_cleaner",
        new NumCleanerRunsStat(this));
  }

  public void setBdbEnvironment(Environment bdbEnvironment) {
    this.bdbEnvironment = bdbEnvironment;
  }

  private EnvironmentStats getEnvironmentStats(Environment bdbEnvironment) {
    StatsConfig config = new StatsConfig();
    config.setFast(true);
    return bdbEnvironment.getStats(config);
  }

  //wrapper bdb stats API into Tehuti Lambda stats
  private static class CacheMissesRatioStat extends LambdaStat {
    public CacheMissesRatioStat(BdbStorageEngineStats bdbStorageEngineStats) {
      super(() -> bdbStorageEngineStats.getPercentageCacheMisses());
    }
  }

  private static class SpaceUtilizationRatioStat extends LambdaStat {
    public SpaceUtilizationRatioStat(BdbStorageEngineStats bdbStorageEngineStats) {
      super (() -> bdbStorageEngineStats.getPercentageSpaceUtilization());
    }
  }

  private static class AllottedCacheSizeStat extends LambdaStat {
    public AllottedCacheSizeStat(BdbStorageEngineStats bdbStorageEngineStats) {
      super (() -> bdbStorageEngineStats.getAllottedCacheSize());
    }
  }

  private static class SpaceSizeStat extends LambdaStat {
    public SpaceSizeStat(BdbStorageEngineStats bdbStorageEngineStats) {
      super (() -> bdbStorageEngineStats.getTotalSpace());
    }
  }

  private static class NumCleanerRunsStat extends LambdaStat {
    public NumCleanerRunsStat(BdbStorageEngineStats bdbStorageEngineStats) {
      super (() -> bdbStorageEngineStats.getNumCleanerRuns());
    }
  }

  private EnvironmentStats getFastStats() {
    try {
      return fastStats.call();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  private BdbSpaceUtilizationSummary getFastSpaceUtilizationSummary() {
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

  public long getNumCacheMiss() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getNCacheMiss();
  }

  public long getNumReadsTotal() {
    return getNumRandomReads() + getNumSequentialReads();
  }

  public long getNumRandomReads() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getNRandomReads();
  }

  public long getNumSequentialReads() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getNSequentialReads();
  }

  public long getNumWritesTotal() {
    return getNumRandomWrites() + getNumSequentialWrites();
  }

  public long getNumRandomWrites() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getNRandomWrites();
  }

  public long getNumSequentialWrites() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getNSequentialWrites();
  }

  //Disk
  public long getAllottedCacheSize() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getCacheTotalBytes();
  }

  public double getPercentageSpaceUtilization() {
    return Utils.getRatio(getTotalSpaceUtilized(), getTotalSpace());
  }

  public long getTotalSpaceUtilized() {
    BdbSpaceUtilizationSummary summary = getFastSpaceUtilizationSummary();
    return summary == null ? 0l : summary.getTotalSpaceUtilized();
  }

  public long getTotalSpace() {
    BdbSpaceUtilizationSummary summary = getFastSpaceUtilizationSummary();
    return summary == null ? 0l : summary.getTotalSpaceUsed();
  }

  //cleaning
  public long getNumCleanerRuns() {
    EnvironmentStats stats = getFastStats();
    return stats == null ? 0l : stats.getNCleanerRuns();
  }
}
