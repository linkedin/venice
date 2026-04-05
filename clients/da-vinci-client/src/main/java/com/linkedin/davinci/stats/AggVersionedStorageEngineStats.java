package com.linkedin.davinci.stats;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
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
    super(
        metricsRepository,
        metadataRepository,
        StorageEngineStatsWrapper::new,
        StorageEngineStatsReporter::new,
        unregisterMetricForDeletedStoreEnabled);
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
      StorageEngineOtelStats otelStats = otelStatsMap.remove(storeName);
      if (otelStats != null) {
        otelStats.close();
      }
    }
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
