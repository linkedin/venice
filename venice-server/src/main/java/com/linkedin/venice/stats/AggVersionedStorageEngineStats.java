package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.store.AbstractStorageEngine;
import io.tehuti.metrics.MetricsRepository;
import org.apache.log4j.Logger;


public class AggVersionedStorageEngineStats extends AbstractVeniceAggVersionedStats<
    AggVersionedStorageEngineStats.StorageEngineStats,
    AggVersionedStorageEngineStats.StorageEngineStatsReporter> {
  private static final Logger LOGGER = Logger.getLogger(AggVersionedStorageEngineStats.class);

  public AggVersionedStorageEngineStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository metadataRepository) {
    super(metricsRepository, metadataRepository, () -> new StorageEngineStats(), (mr, name) -> new StorageEngineStatsReporter(mr, name));
  }

  public void setStorageEngine(String topicName, AbstractStorageEngine storageEngine) {
    if (!Version.topicIsValidStoreVersion(topicName)) {
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

  static class StorageEngineStats {
    private AbstractStorageEngine storageEngine;

    public void setStorageEngine(AbstractStorageEngine storageEngine) {
      this.storageEngine = storageEngine;
    }

    public long getDiskUsageInBytes() {
      if (null != storageEngine) {
        return storageEngine.getStoreSizeInBytes();
      }
      return 0;
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
    }
  }
}
