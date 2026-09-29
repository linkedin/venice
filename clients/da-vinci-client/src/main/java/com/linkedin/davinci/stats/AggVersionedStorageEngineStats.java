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


/**
 * Aggregated versioned storage engine stats with per-store Tehuti reporters and OTel metrics.
 *
 * <p><b>OTel stats lifecycle:</b> OTel stats are created lazily when a storage engine or open failure is recorded; the
 * shared per-store registry updates their version info and closes them on store deletion, and
 * {@link #removeStorageEngine} closes them once the store's last storage engine leaves this host.
 */
public class AggVersionedStorageEngineStats extends
    AbstractVeniceAggVersionedStats<AggVersionedStorageEngineStats.StorageEngineStatsWrapper, AggVersionedStorageEngineStats.StorageEngineStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedStorageEngineStats.class);
  private static final double DEFAULT_DISK_SIZE_DROP_ALERT_THRESHOLD = 0.5;
  private static final String DISK_SIZE_DROP_ALERT_METRIC = "version_swap_disk_size_drop_alert";

  private final double diskSizeDropAlertThreshold;
  private final Map<String, Sensor> diskSizeDropAlertSensors = new VeniceConcurrentHashMap<>();

  private final PerStoreVersionedOtelStats<StorageEngineOtelStats> otelStats;
  private final String clusterName;

  public AggVersionedStorageEngineStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      String clusterName) {
    this(
        metricsRepository,
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled,
        DEFAULT_DISK_SIZE_DROP_ALERT_THRESHOLD,
        clusterName);
  }

  public AggVersionedStorageEngineStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      double diskSizeDropAlertThreshold,
      String clusterName) {
    super(
        metricsRepository,
        metadataRepository,
        StorageEngineStatsWrapper::new,
        StorageEngineStatsReporter::new,
        unregisterMetricForDeletedStoreEnabled);
    this.diskSizeDropAlertThreshold = diskSizeDropAlertThreshold;
    this.clusterName = clusterName;
    this.otelStats = createPerStoreOtelStats(
        storeName -> new StorageEngineOtelStats(getMetricsRepository(), storeName, clusterName));
  }

  public void setStorageEngine(String topicName, StorageEngine storageEngine) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(topicName)) {
      LOGGER.warn("Invalid topic name: {}", topicName);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromKafkaTopicName(topicName);
    try {
      StorageEngineStatsWrapper wrapper = getStats(storeName, version);
      wrapper.setStorageEngine(storageEngine);
      otelStats.compute(storeName, stats -> stats.setStatsWrapper(version, wrapper));
    } catch (Exception e) {
      LOGGER.warn("Failed to setup StorageEngine for store: {}, version: {}", storeName, version, e);
    }
  }

  /**
   * Stops the OTel gauges of a version whose storage engine left this host, and closes the store's OTel metrics once
   * none of its storage engines remain. Tehuti keeps reading the engine as before.
   */
  public void removeStorageEngine(String topicName) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(topicName)) {
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromKafkaTopicName(topicName);
    try {
      otelStats.removeIf(storeName, stats -> stats.onVersionRemoved(version));
    } catch (Exception e) {
      LOGGER.warn("Failed to remove OTel storage engine stats for store: {}, version: {}", storeName, version, e);
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
      getOrCreateOtelStats(storeName).recordRocksDBOpenFailure(version);
    } catch (Exception e) {
      LOGGER.warn("Failed to record open failure for store: {}, version: {}", storeName, version, e);
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
  protected void cleanupVersionResources(String storeName, int version) {
    if (otelStats == null) {
      return;
    }
    StorageEngineOtelStats stats = otelStats.get(storeName);
    if (stats != null) {
      try {
        stats.onVersionRemoved(version);
      } catch (Exception e) {
        LOGGER.error("Failed to remove OTel wrapper for store: {}, version: {}", storeName, version, e);
      }
    }
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

      // Skip if current version has no data (e.g., first version of a store)
      if (currentSize <= 0) {
        LOGGER.info(
            "Skipping disk size drop check for store {}: current version {} has no data (size = {} bytes)",
            storeName,
            currentVersion,
            currentSize);
        sensor.record(0);
        return;
      }

      // Since we already guard on PUSHED status (ingestion complete), futureSize == 0
      // is a real data loss signal, not an incomplete ingestion artifact.
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

  private StorageEngineOtelStats getOrCreateOtelStats(String storeName) {
    return otelStats.getOrCreate(storeName);
  }

  static class StorageEngineStatsWrapper {
    private StorageEngine storageEngine;
    private final AtomicInteger rocksDBOpenFailureCount = new AtomicInteger(0);

    public void setStorageEngine(StorageEngine storageEngine) {
      this.storageEngine = storageEngine;
    }

    public long getDiskUsageInBytes() {
      return storageEngine != null ? storageEngine.getStats().getStoreSizeInBytes() : 0;
    }

    public long getRMDDiskUsageInBytes() {
      return storageEngine != null ? storageEngine.getStats().getRMDSizeInBytes() : 0;
    }

    public long getKeyCountEstimate() {
      return storageEngine != null ? storageEngine.getStats().getKeyCountEstimate() : 0;
    }

    public void recordRocksDBOpenFailure() {
      rocksDBOpenFailureCount.incrementAndGet();
    }

    public int getRocksDBOpenFailureCount() {
      return rocksDBOpenFailureCount.get();
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
