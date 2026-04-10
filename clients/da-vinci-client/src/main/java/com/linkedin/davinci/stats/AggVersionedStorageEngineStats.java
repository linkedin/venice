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
 * <p><b>OTel stats lifecycle:</b> OTel stats are created lazily by {@link #getOrCreateOtelStats}
 * and updated by {@link #onVersionInfoUpdated} via {@code computeIfPresent}. This class does NOT
 * override {@code loadAllStats()}, and {@code AbstractVeniceAggVersionedStats} always calls it
 * from its own constructor. As a result, {@code onVersionInfoUpdated} and {@code cleanupVersionResources}
 * are called from within {@code super()}, before the subclass constructor has run at all —
 * meaning {@code otelStatsMap} is still {@code null} at that point. This makes the null guards
 * in those overrides mandatory.
 */
public class AggVersionedStorageEngineStats extends
    AbstractVeniceAggVersionedStats<AggVersionedStorageEngineStats.StorageEngineStatsWrapper, AggVersionedStorageEngineStats.StorageEngineStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedStorageEngineStats.class);
  private static final double DEFAULT_DISK_SIZE_DROP_ALERT_THRESHOLD = 0.5;
  private static final String DISK_SIZE_DROP_ALERT_METRIC = "version_swap_disk_size_drop_alert";

  private final double diskSizeDropAlertThreshold;
  private final Map<String, Sensor> diskSizeDropAlertSensors = new VeniceConcurrentHashMap<>();

  /**
   * Per-store OTel stats, keyed by store name. Bounded by the number of stores on this host.
   * Entries are created lazily via {@link #getOrCreateOtelStats(String)} and removed in
   * {@link #handleStoreDeleted(String)}.
   */
  private final Map<String, StorageEngineOtelStats> otelStatsMap = new VeniceConcurrentHashMap<>();
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
      getOrCreateOtelStats(storeName).setStatsWrapper(version, wrapper);
    } catch (Exception e) {
      LOGGER.warn("Failed to setup StorageEngine for store: {}, version: {}", storeName, version, e);
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

  /**
   * Updates version info for existing OTel stats only. Uses {@code computeIfPresent} intentionally:
   * OTel stats are created lazily on first access via {@link #setStorageEngine}/{@link #recordRocksDBOpenFailure},
   * not eagerly here — this avoids the constructor-time re-entrance hazard described in
   * {@link #getOrCreateOtelStats}. Null guard: called from {@code super()} constructor before
   * {@code otelStatsMap} is initialized.
   */
  @Override
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    if (otelStatsMap == null) {
      return;
    }
    otelStatsMap.computeIfPresent(storeName, (k, stats) -> {
      try {
        stats.updateVersionInfo(currentVersion, futureVersion);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to update OTel version info for store: {}, current: {}, future: {}",
            storeName,
            currentVersion,
            futureVersion,
            e);
      }
      return stats;
    });
  }

  @Override
  protected void cleanupVersionResources(String storeName, int version) {
    if (otelStatsMap == null) {
      return;
    }
    otelStatsMap.computeIfPresent(storeName, (k, stats) -> {
      try {
        stats.onVersionRemoved(version);
      } catch (Exception e) {
        LOGGER.error("Failed to remove OTel wrapper for store: {}, version: {}", storeName, version, e);
      }
      return stats;
    });
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
      StorageEngineOtelStats otelStats = otelStatsMap.remove(storeName);
      if (otelStats != null) {
        otelStats.close();
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

  /**
   * Gets or creates OTel stats for a store. {@code getCurrentVersion}/{@code getFutureVersion}
   * are called <b>before</b> {@code computeIfAbsent} to avoid a re-entrance hazard: when the
   * store is not yet in {@code aggStats}, calling these methods inside the lambda would trigger
   * {@code getVersionedStats} -> {@code addStore} -> {@code applyVersionInfo} ->
   * {@code onVersionInfoUpdated} -> {@code otelStatsMap.computeIfPresent}, which re-enters
   * {@code otelStatsMap} from inside the lambda and violates the ConcurrentHashMap contract
   * (JDK-8062841). The {@code get()} fast-path skips the version lookups when stats already exist.
   */
  private StorageEngineOtelStats getOrCreateOtelStats(String storeName) {
    StorageEngineOtelStats existing = otelStatsMap.get(storeName);
    if (existing != null) {
      return existing;
    }
    int currentVersion = getCurrentVersion(storeName);
    int futureVersion = getFutureVersion(storeName);
    return otelStatsMap.computeIfAbsent(storeName, k -> {
      StorageEngineOtelStats stats = new StorageEngineOtelStats(getMetricsRepository(), k, clusterName);
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
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
