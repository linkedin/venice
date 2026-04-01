package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Gauge;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AggVersionedStorageEngineStats extends
    AbstractVeniceAggVersionedStats<AggVersionedStorageEngineStats.StorageEngineStatsWrapper, AggVersionedStorageEngineStats.StorageEngineStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedStorageEngineStats.class);
  private static final double DEFAULT_DISK_SIZE_DROP_ALERT_THRESHOLD = 0.5;
  private static final String DISK_SIZE_DROP_ALERT_METRIC = "version_swap_disk_size_drop_alert";

  private final double diskSizeDropAlertThreshold;
  private final Map<String, Sensor> diskSizeDropAlertSensors = new VeniceConcurrentHashMap<>();

  public AggVersionedStorageEngineStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    this(
        metricsRepository,
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        DEFAULT_DISK_SIZE_DROP_ALERT_THRESHOLD);
  }

  public AggVersionedStorageEngineStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      double diskSizeDropAlertThreshold) {
    super(
        metricsRepository,
        metadataRepository,
        StorageEngineStatsWrapper::new,
        StorageEngineStatsReporter::new,
        unregisterMetricForDeletedStoreEnabled);
    this.diskSizeDropAlertThreshold = diskSizeDropAlertThreshold;
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

  /**
   * Called when a store's version info changes.
   * After the parent updates version info, compares the current version's disk size
   * with the future version's disk size when the future version has completed ingestion
   * (PUSHED status). Records 1 in the alert metric if the future version is significantly
   * smaller, or 0 otherwise.
   */
  @Override
  public void handleStoreChanged(Store store) {
    super.handleStoreChanged(store);
    checkAndRecordDiskSizeAlert(store);
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    try {
      super.handleStoreDeleted(storeName);
    } finally {
      Sensor removed = diskSizeDropAlertSensors.remove(storeName);
      if (removed != null) {
        getMetricsRepository().removeSensor(removed.name());
      }
    }
  }

  /**
   * Compares current vs future version disk sizes and actively records the alert metric.
   * Only performs the comparison when the future version has status PUSHED (ingestion complete),
   * to avoid false positives from partial data during STARTED status.
   */
  void checkAndRecordDiskSizeAlert(Store store) {
    String storeName = store.getName();
    int currentVersion = getCurrentVersion(storeName);
    int futureVersion = getFutureVersion(storeName);

    Sensor sensor = getOrCreateDiskSizeDropAlertSensor(storeName);

    if (currentVersion == NON_EXISTING_VERSION || futureVersion == NON_EXISTING_VERSION) {
      sensor.record(0);
      return;
    }

    // Only check when the future version has completed ingestion (PUSHED status).
    // During STARTED, disk data is partial and would cause false positive alerts.
    Version futureVersionObj = store.getVersion(futureVersion);
    if (futureVersionObj == null || futureVersionObj.getStatus() != VersionStatus.PUSHED) {
      sensor.record(0);
      return;
    }

    try {
      StorageEngineStatsWrapper currentStats = getStats(storeName, currentVersion);
      StorageEngineStatsWrapper futureStats = getStats(storeName, futureVersion);
      long currentSize = currentStats.getDiskUsageInBytes();
      long futureSize = futureStats.getDiskUsageInBytes();

      // Only alert if both versions have meaningful data
      if (currentSize <= 0 || futureSize <= 0) {
        sensor.record(0);
        return;
      }

      if (futureSize < currentSize * diskSizeDropAlertThreshold) {
        LOGGER.warn(
            "Disk size drop detected for store {}: current version {} size = {} bytes, "
                + "future version {} size = {} bytes, threshold = {}",
            storeName,
            currentVersion,
            currentSize,
            futureVersion,
            futureSize,
            diskSizeDropAlertThreshold);
        sensor.record(1);
      } else {
        sensor.record(0);
      }
    } catch (Exception e) {
      LOGGER.warn("Unable to compute disk size drop alert for store: {}", storeName, e);
      sensor.record(0);
    }
  }

  private Sensor getOrCreateDiskSizeDropAlertSensor(String storeName) {
    return diskSizeDropAlertSensors.computeIfAbsent(storeName, s -> {
      String sensorFullName = AbstractVeniceStats.getSensorFullName(s, DISK_SIZE_DROP_ALERT_METRIC);
      Sensor sensor = getMetricsRepository().sensor(sensorFullName);
      sensor.add(sensorFullName + ".Gauge", new Gauge());
      return sensor;
    });
  }

  // Visible for testing
  double getDiskSizeDropAlertThreshold() {
    return diskSizeDropAlertThreshold;
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
    }
  }
}
