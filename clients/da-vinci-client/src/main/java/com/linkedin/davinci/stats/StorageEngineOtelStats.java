package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.OtelVersionedStatsUtils.classifyVersion;
import static com.linkedin.davinci.stats.OtelVersionedStatsUtils.getVersionForRole;
import static com.linkedin.davinci.stats.StorageEngineOtelMetricEntity.DISK_USAGE;
import static com.linkedin.davinci.stats.StorageEngineOtelMetricEntity.KEY_COUNT_ESTIMATE;
import static com.linkedin.davinci.stats.StorageEngineOtelMetricEntity.ROCKSDB_OPEN_FAILURE_COUNT;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.davinci.stats.AggVersionedStorageEngineStats.StorageEngineStatsWrapper;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Map;


/**
 * Per-store OTel stats for storage engine metrics.
 *
 * <p>Holds 3 OTel metrics:
 * <ul>
 *   <li>{@code ingestion.disk.used} — ASYNC_GAUGE with VERSION_ROLE + RECORD_TYPE dimensions,
 *       implemented via {@link AsyncMetricEntityStateTwoEnums}{@code <VeniceRecordType, VersionRole>}.</li>
 *   <li>{@code rocksdb.key.estimated_count} — ASYNC_GAUGE with VERSION_ROLE dimension</li>
 *   <li>{@code rocksdb.open.failure_count} — COUNTER with VERSION_ROLE dimension</li>
 * </ul>
 *
 * <p>Tehuti metrics are managed separately by
 * {@link AggVersionedStorageEngineStats.StorageEngineStatsReporter}.
 */
public class StorageEngineOtelStats implements Closeable {
  private final boolean emitOtelMetrics;

  /**
   * Current and future version numbers for this store. Volatile + immutable: all updates replace
   * the entire reference atomically via {@link #updateVersionInfo}. Initial sentinel value of
   * {@code (NON_EXISTING_VERSION, NON_EXISTING_VERSION)} means ASYNC_GAUGE callbacks return 0
   * before the first {@link #updateVersionInfo} call.
   */
  private volatile VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);

  /**
   * Per-version wrapper map, keyed by version number. Bounded by the number of active versions
   * per store (typically 2-3: current, future, backup). Entries are removed via
   * {@link #onVersionRemoved(int)} when versions are cleaned up.
   */
  private final Map<Integer, StorageEngineStatsWrapper> wrappersByVersion = new VeniceConcurrentHashMap<>();

  /**
   * Disk usage ASYNC_GAUGE metrics — one callback per (VeniceRecordType, VersionRole) pair.
   * Bounded by VeniceRecordType.values().length × VersionRole.values().length (2 × 3 = 6).
   */
  private final AsyncMetricEntityStateTwoEnums<VeniceRecordType, VersionRole> diskUsageMetrics;

  /** Key count ASYNC_GAUGE with VersionRole dimension */
  private final AsyncMetricEntityStateOneEnum<VersionRole> keyCountMetric;

  /** RocksDB open failure COUNTER with VersionRole dimension */
  private final MetricEntityStateOneEnum<VersionRole> openFailureMetric;

  public StorageEngineOtelStats(MetricsRepository metricsRepository, String storeName, String clusterName) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .build();

    this.emitOtelMetrics = otelSetup.emitOpenTelemetryMetrics();

    if (emitOtelMetrics) {
      VeniceOpenTelemetryMetricsRepository otelRepository = otelSetup.getOtelRepository();
      Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelSetup.getBaseDimensionsMap();

      this.diskUsageMetrics = AsyncMetricEntityStateTwoEnums.create(
          DISK_USAGE.getMetricEntity(),
          otelRepository,
          baseDimensionsMap,
          VeniceRecordType.class,
          VersionRole.class,
          (recordType, role) -> () -> getDiskUsageForRole(role, recordType));

      this.keyCountMetric = AsyncMetricEntityStateOneEnum.create(
          KEY_COUNT_ESTIMATE.getMetricEntity(),
          otelRepository,
          baseDimensionsMap,
          VersionRole.class,
          role -> () -> getKeyCountForRole(role));

      // RocksDB open failure count: COUNTER with VersionRole dimension
      this.openFailureMetric = MetricEntityStateOneEnum
          .create(ROCKSDB_OPEN_FAILURE_COUNT.getMetricEntity(), otelRepository, baseDimensionsMap, VersionRole.class);
    } else {
      this.diskUsageMetrics = null;
      this.keyCountMetric = null;
      this.openFailureMetric = null;
    }
  }

  /**
   * Updates the current and future version for this store.
   */
  public void updateVersionInfo(int currentVersion, int futureVersion) {
    VersionInfo current = this.versionInfo;
    if (current.getCurrentVersion() == currentVersion && current.getFutureVersion() == futureVersion) {
      return;
    }
    this.versionInfo = new VersionInfo(currentVersion, futureVersion);
  }

  /**
   * Registers a wrapper for a specific version, enabling ASYNC_GAUGE callbacks to resolve data.
   */
  public void setStatsWrapper(int version, StorageEngineStatsWrapper wrapper) {
    if (!emitOtelMetrics) {
      return;
    }
    if (wrapper == null) {
      throw new IllegalArgumentException("wrapper must not be null");
    }
    wrappersByVersion.put(version, wrapper);
  }

  /**
   * Removes a version's wrapper when the version is cleaned up.
   */
  public void onVersionRemoved(int version) {
    if (!emitOtelMetrics) {
      return;
    }
    wrappersByVersion.remove(version);
  }

  /**
   * Records a RocksDB open failure for a specific version as a COUNTER increment.
   *
   * <p>Semantic divergence: OTel COUNTER increments at recording time, permanently labeling
   * each failure with the version's role at that moment. Tehuti AsyncGauge polls
   * {@code rocksDBOpenFailureCount} on the {@link AggVersionedStorageEngineStats.StorageEngineStatsWrapper}
   * for the version currently bound to the reporter's role slot. Wrappers are per-version (not
   * per-role): the failure count accumulates in the wrapper for the version's lifetime. When a
   * role reporter re-binds to a new version's wrapper, the Tehuti gauge reflects that version's
   * cumulative count. Old wrapper counts are no longer polled but are not reset.
   */
  public void recordRocksDBOpenFailure(int version) {
    if (!emitOtelMetrics) {
      return;
    }
    VersionRole role = classifyVersion(version, versionInfo);
    openFailureMetric.record(1, role);
  }

  /**
   * Resolves the wrapper for a given VersionRole, or null if the version is not found or has no wrapper.
   */
  private StorageEngineStatsWrapper getWrapperForRole(VersionRole role) {
    VersionInfo snapshot = versionInfo;
    int version = getVersionForRole(role, snapshot, wrappersByVersion.keySet());
    if (version == NON_EXISTING_VERSION) {
      return null;
    }
    return wrappersByVersion.get(version);
  }

  /**
   * ASYNC_GAUGE callback: resolves disk usage (data or RMD) for a specific VersionRole.
   * Returns 0 when the version/wrapper/engine is unavailable.
   */
  private long getDiskUsageForRole(VersionRole role, VeniceRecordType recordType) {
    StorageEngineStatsWrapper wrapper = getWrapperForRole(role);
    if (wrapper == null) {
      return 0;
    }
    switch (recordType) {
      case DATA:
        return wrapper.getDiskUsageInBytes();
      case REPLICATION_METADATA:
        return wrapper.getRMDDiskUsageInBytes();
      default:
        throw new IllegalArgumentException("Unknown record type: " + recordType);
    }
  }

  /**
   * ASYNC_GAUGE callback: resolves key count estimate for a specific VersionRole.
   * Returns 0 when the version/wrapper is unavailable.
   */
  private long getKeyCountForRole(VersionRole role) {
    StorageEngineStatsWrapper wrapper = getWrapperForRole(role);
    return wrapper == null ? 0 : wrapper.getKeyCountEstimate();
  }

  /**
   * Clears internal wrapper references so ASYNC_GAUGE callbacks return 0 (not stale values).
   * OTel instruments registered with the SDK are NOT deregistered — they remain
   * registered and will continue to be polled until the SDK is shut down.
   */
  @Override
  public void close() {
    wrappersByVersion.clear();
  }
}
