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
   * {@code (NON_EXISTING_VERSION, NON_EXISTING_VERSION)} means no role has a version backing it, so
   * each async-gauge's {@code liveStateResolver} returns {@code null} and no data points are emitted.
   */
  private volatile VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);

  /**
   * Per-version wrapper map, keyed by version number. Bounded by the number of active versions
   * per store (typically 2-3: current, future, backup). Entries are removed via
   * {@link #onVersionRemoved(int)} when versions are cleaned up.
   */
  private final Map<Integer, StorageEngineStatsWrapper> wrappersByVersion = new VeniceConcurrentHashMap<>();

  /** Disk usage ASYNC_GAUGE with VeniceRecordType and VersionRole dimensions */
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

      // Two-callback contract: the liveStateResolver returns the wrapper or null (null -> dormant,
      // no emission); the valueResolver reads the metric value from that wrapper. The null return
      // is the liveness signal, enforced by the API.
      this.diskUsageMetrics = AsyncMetricEntityStateTwoEnums.create(
          DISK_USAGE.getMetricEntity(),
          otelRepository,
          baseDimensionsMap,
          VeniceRecordType.class,
          VersionRole.class,
          (recordType, role) -> getWrapperForRole(role),
          (wrapper, recordType, role) -> diskUsage(wrapper, recordType));

      this.keyCountMetric = AsyncMetricEntityStateOneEnum.create(
          KEY_COUNT_ESTIMATE.getMetricEntity(),
          otelRepository,
          baseDimensionsMap,
          VersionRole.class,
          role -> getWrapperForRole(role),
          (wrapper, role) -> wrapper.getKeyCountEstimate());

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
   * Updates the current and future version for this store. Async gauges pick up the new role
   * classification automatically on the next OTel collection.
   */
  public void updateVersionInfo(int currentVersion, int futureVersion) {
    this.versionInfo = new VersionInfo(currentVersion, futureVersion);
  }

  /**
   * Registers a wrapper for a specific version, enabling ASYNC_GAUGE callbacks to resolve data on
   * subsequent collections.
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

  /** Reads disk usage (data or RMD) from a resolved wrapper. */
  private static long diskUsage(StorageEngineStatsWrapper wrapper, VeniceRecordType recordType) {
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
   * Clears internal wrapper references. On subsequent collections each async-gauge's
   * {@code liveStateResolver} will return {@code null} for every role and no data points will be
   * emitted. The SDK instruments themselves are NOT deregistered — they remain registered and
   * are polled until the SDK is shut down.
   */
  @Override
  public void close() {
    wrappersByVersion.clear();
  }
}
