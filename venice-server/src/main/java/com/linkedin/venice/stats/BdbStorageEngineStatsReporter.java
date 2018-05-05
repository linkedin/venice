package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.util.function.Supplier;

import static com.linkedin.venice.stats.StatsErrorCode.NULL_BDB_STATS;
public class BdbStorageEngineStatsReporter extends AbstractVeniceStatsReporter<BdbStorageEngineStats> {
  public BdbStorageEngineStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
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
    registerSensor("bdb_BIN_miss_ratio",
        new PercentageBINMiss(this));
    registerSensor("bdb_IN_miss_ratio",
        new PercentageINMiss(this));
    registerSensor("cleaner_backlog",
        new CleanerBacklogStat(this));
    registerSensor("num_cleaner_entries_read",
        new NumCleanerEntriesReadStat(this));
    registerSensor("num_cleaner_deletions",
        new NumCleanerDeletionsStat(this));
  }

  private static class CacheMissesRatioStat extends BDBStatsCounter {
    CacheMissesRatioStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> reporter.getStats().getPercentageCacheMisses());
    }
  }

  private static class SpaceUtilizationRatioStat extends BDBStatsCounter {
    SpaceUtilizationRatioStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> reporter.getStats().getPercentageSpaceUtilization());
    }
  }

  private static class AllottedCacheSizeStat extends BDBStatsCounter {
    AllottedCacheSizeStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getAllottedCacheSize());
    }
  }

  private static class SpaceSizeStat extends BDBStatsCounter {
    SpaceSizeStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getTotalSpace());
    }
  }

  private static class NumCleanerRunsStat extends BDBStatsCounter {
    NumCleanerRunsStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getNumCleanerRuns());
    }
  }

  private static class CleanerBacklogStat extends BDBStatsCounter {
    CleanerBacklogStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getCleanerBacklog());
    }
  }

  private static class NumCleanerEntriesReadStat extends BDBStatsCounter {
    NumCleanerEntriesReadStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getNumCleanerEntriesRead());
    }
  }

  private static class NumCleanerDeletionsStat extends BDBStatsCounter {
    NumCleanerDeletionsStat(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getNumCleanerDeletions());
    }
  }

  private static class PercentageBINMiss extends BDBStatsCounter {
    PercentageBINMiss(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getPercentageBINMiss());
    }
  }

  private static class PercentageINMiss extends BDBStatsCounter {
    PercentageINMiss(BdbStorageEngineStatsReporter reporter) {
      super(reporter, () -> (double) reporter.getStats().getPercentageINMiss());
    }
  }

  private static class BDBStatsCounter extends Gauge {
    BDBStatsCounter(AbstractVeniceStatsReporter reporter, Supplier<Double> supplier) {
      super(() -> reporter.getStats() == null ? NULL_BDB_STATS.code : supplier.get());
    }
  }
}
