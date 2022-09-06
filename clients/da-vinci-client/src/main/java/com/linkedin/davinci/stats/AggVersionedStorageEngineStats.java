package com.linkedin.davinci.stats;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.StatsErrorCode;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AggVersionedStorageEngineStats extends
    AbstractVeniceAggVersionedStats<AggVersionedStorageEngineStats.StorageEngineStats, AggVersionedStorageEngineStats.StorageEngineStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedStorageEngineStats.class);

  public AggVersionedStorageEngineStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository) {
    super(metricsRepository, metadataRepository, StorageEngineStats::new, StorageEngineStatsReporter::new);
  }

  public void setStorageEngine(String topicName, AbstractStorageEngine storageEngine) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(topicName)) {
      LOGGER.warn("Invalid topic name: " + topicName);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromKafkaTopicName(topicName);
    try {
      getStats(storeName, version).setStorageEngine(storageEngine);
    } catch (Exception e) {
      LOGGER.warn("Failed to setup StorageEngine for store: " + storeName + ", version: " + version);
    }
  }

  public void recordRocksDBOpenFailure(String topicName) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(topicName)) {
      LOGGER.warn("Invalid topic name: " + topicName);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromKafkaTopicName(topicName);
    try {
      getStats(storeName, version).recordRocksDBOpenFailure();
    } catch (Exception e) {
      LOGGER.warn("Failed to record open failure for store: " + storeName + ", version: " + version);
    }
  }

  static class StorageEngineStats {
    private AbstractStorageEngine storageEngine;
    private final AtomicInteger rocksDBOpenFailureCount = new AtomicInteger(0);

    public void setStorageEngine(AbstractStorageEngine storageEngine) {
      this.storageEngine = storageEngine;
    }

    public long getDiskUsageInBytes() {
      if (null != storageEngine) {
        return storageEngine.getStoreSizeInBytes();
      }
      return 0;
    }

    public long getRMDDiskUsageInBytes() {
      if (null != storageEngine) {
        return storageEngine.getRMDSizeInBytes();
      }
      return 0;
    }

    public void recordRocksDBOpenFailure() {
      rocksDBOpenFailureCount.incrementAndGet();
    }
  }

  static class StorageEngineStatsReporter extends AbstractVeniceStatsReporter<StorageEngineStats> {
    public StorageEngineStatsReporter(MetricsRepository metricsRepository, String storeName) {
      super(metricsRepository, storeName);
    }

    @Override
    protected void registerStats() {
      registerSensor("disk_usage_in_bytes", new Gauge(() -> {
        StorageEngineStats stats = getStats();
        if (null == stats) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.getDiskUsageInBytes();
        }
      }));
      registerSensor("rmd_disk_usage_in_bytes", new Gauge(() -> {
        StorageEngineStats stats = getStats();
        if (null == stats) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.getRMDDiskUsageInBytes();
        }
      }));
      registerSensor("rocksdb_open_failure_count", new Gauge(() -> {
        StorageEngineStats stats = getStats();
        if (null == stats) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.rocksDBOpenFailureCount.get();
        }
      }));
    }
  }
}
