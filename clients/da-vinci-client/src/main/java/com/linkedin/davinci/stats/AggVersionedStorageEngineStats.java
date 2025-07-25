package com.linkedin.davinci.stats;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.StatsErrorCode;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AggVersionedStorageEngineStats extends
    AbstractVeniceAggVersionedStats<AggVersionedStorageEngineStats.StorageEngineStatsWrapper, AggVersionedStorageEngineStats.StorageEngineStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedStorageEngineStats.class);

  public AggVersionedStorageEngineStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    super(
        metricsRepository,
        metadataRepository,
        StorageEngineStatsWrapper::new,
        StorageEngineStatsReporter::new,
        unregisterMetricForDeletedStoreEnabled);
  }

  public void setStorageEngine(String topicName, StorageEngine storageEngine) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(topicName)) {
      LOGGER.warn("Invalid topic name: {}", topicName);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromKafkaTopicName(topicName);
    try {
      getStats(storeName, version).setStorageEngine(storageEngine);
    } catch (Exception e) {
      LOGGER.warn("Failed to setup StorageEngine for store: {}, version: {}", storeName, version);
    }
  }

  public void recordRocksDBOpenFailure(String topicName) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(topicName)) {
      LOGGER.warn("Invalid topic name: {}", topicName);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromKafkaTopicName(topicName);
    try {
      getStats(storeName, version).recordRocksDBOpenFailure();
    } catch (Exception e) {
      LOGGER.warn("Failed to record open failure for store: {}, version: {}", storeName, version);
    }
  }

  static class StorageEngineStatsWrapper {
    private StorageEngine storageEngine;
    private final AtomicInteger rocksDBOpenFailureCount = new AtomicInteger(0);

    public void setStorageEngine(StorageEngine storageEngine) {
      this.storageEngine = storageEngine;
    }

    public long getDiskUsageInBytes() {
      if (storageEngine != null) {
        return storageEngine.getStats().getStoreSizeInBytes();
      }
      return 0;
    }

    public long getRMDDiskUsageInBytes() {
      if (storageEngine != null) {
        return storageEngine.getStats().getRMDSizeInBytes();
      }
      return 0;
    }

    public long getKeyCountEstimate() {
      if (storageEngine != null) {
        return storageEngine.getStats().getKeyCountEstimate();
      }
      return 0;
    }

    public long getDuplicateKeyCountEstimate() {
      if (storageEngine != null) {
        return storageEngine.getStats().getDuplicateKeyCountEstimate();
      }
      return 0;
    }

    public void recordRocksDBOpenFailure() {
      rocksDBOpenFailureCount.incrementAndGet();
    }
  }

  static class StorageEngineStatsReporter extends AbstractVeniceStatsReporter<StorageEngineStatsWrapper> {
    public StorageEngineStatsReporter(MetricsRepository metricsRepository, String storeName, String clusterName) {
      super(metricsRepository, storeName);
    }

    @Override
    protected void registerStats() {
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        StorageEngineStatsWrapper stats = getStats();
        if (stats == null) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.getDiskUsageInBytes();
        }
      }, "disk_usage_in_bytes"));
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        StorageEngineStatsWrapper stats = getStats();
        if (stats == null) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.getRMDDiskUsageInBytes();
        }
      }, "rmd_disk_usage_in_bytes"));
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        StorageEngineStatsWrapper stats = getStats();
        if (stats == null) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.rocksDBOpenFailureCount.get();
        }
      }, "rocksdb_open_failure_count"));
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        StorageEngineStatsWrapper stats = getStats();
        if (stats == null) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.getKeyCountEstimate();
        }
      }, "rocksdb_key_count_estimate"));
      registerSensor(new AsyncGauge((ignored, ignored2) -> {
        StorageEngineStatsWrapper stats = getStats();
        if (stats == null) {
          return StatsErrorCode.NULL_STORAGE_ENGINE_STATS.code;
        } else {
          return stats.getDuplicateKeyCountEstimate();
        }
      }, "rocksdb_duplicate_key_count_estimate"));
    }
  }
}
