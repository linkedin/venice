package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelMetricEntity.INGESTION_RECORD_DELAY;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import com.linkedin.venice.stats.metrics.MetricEntityStateFiveEnums;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;


/**
 * OpenTelemetry metrics for record-level delay monitoring.
 * Tracks delays for regular data records (not heartbeat control messages).
 *
 * <p>SLO classification dimensions ({@link VeniceStoreWriteType}, {@link VeniceChunkingStatus},
 * {@link VeniceRegionLocality}) are supplied by the caller on every record so they reflect the
 * version-level configuration the caller resolved (rather than store-level approximations baked
 * in at construction time). Write-type and chunking are emitted as dynamic dimensions; locality
 * stays a base dimension because it is a deterministic function of the source region — caller
 * provides the resolved enum on the first call per region, and it is cached in that region's
 * {@link MetricEntityStateFiveEnums} instance.
 *
 * <p>Note: Tehuti metrics are managed separately in {@link HeartbeatStatReporter}.
 */
public class RecordLevelDelayOtelStats {
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /**
   * Per-region metric entity states, keyed by region name. Grows lazily via {@code computeIfAbsent} and is bounded
   * by the number of distinct regions in the deployment. Entries are not evicted individually.
   */
  private final Map<String, MetricEntityStateFiveEnums<VersionRole, ReplicaType, ReplicaState, VeniceStoreWriteType, VeniceChunkingStatus>> metricsByRegion;

  // Version info cache for classifying versions as CURRENT/FUTURE/BACKUP
  private volatile VersionInfo versionInfo = VersionInfo.NON_EXISTING;

  public RecordLevelDelayOtelStats(MetricsRepository metricsRepository, String storeName, String clusterName) {
    this.metricsByRegion = new VeniceConcurrentHashMap<>();

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .build();

    this.emitOtelMetrics = otelSetup.emitOpenTelemetryMetrics();
    this.otelRepository = otelSetup.getOtelRepository();
    this.baseDimensionsMap = otelSetup.getBaseDimensionsMap();
  }

  /**
   * Returns true if OTel metrics are emitted.
   */
  public boolean emitOtelMetrics() {
    return emitOtelMetrics;
  }

  /**
   * Updates the current and future version for this store.
   *
   * @param currentVersion The current serving version
   * @param futureVersion The future/upcoming version
   */
  public void updateVersionInfo(int currentVersion, int futureVersion) {
    this.versionInfo = new VersionInfo(currentVersion, futureVersion);
  }

  /**
   * Records a record-level delay with all dimensional attributes to OTel metrics.
   * Returns early if OTel metrics are disabled or version is invalid.
   *
   * @param version The version number (used to classify VersionRole as CURRENT/FUTURE/BACKUP)
   * @param region The record's source region
   * @param replicaType The replica type {@link ReplicaType}
   * @param replicaState The replica state {@link ReplicaState}
   * @param writeType Pre-resolved write-type label (REGULAR / WRITE_COMPUTE)
   * @param chunkingStatus Pre-resolved chunking label (CHUNKED / UNCHUNKED)
   * @param locality Pre-resolved locality label (LOCAL / REMOTE) — applied to the per-region metric state on first
   *          call per region; subsequent calls reuse the cached state
   * @param delayMs The delay in milliseconds
   */
  public void recordRecordDelayOtelMetrics(
      int version,
      String region,
      ReplicaType replicaType,
      ReplicaState replicaState,
      VeniceStoreWriteType writeType,
      VeniceChunkingStatus chunkingStatus,
      VeniceRegionLocality locality,
      long delayMs) {
    if (!emitOtelMetrics()) {
      return;
    }
    VersionRole versionRole = OtelVersionedStatsUtils.classifyVersion(version, this.versionInfo);

    MetricEntityStateFiveEnums<VersionRole, ReplicaType, ReplicaState, VeniceStoreWriteType, VeniceChunkingStatus> metricState =
        getOrCreateMetricState(region, locality);

    // Records to OTel metrics only
    metricState.record(delayMs, versionRole, replicaType, replicaState, writeType, chunkingStatus);
  }

  /**
   * Gets or creates a metric entity state for a specific region. Region locality is supplied by the
   * caller and baked into the per-region base dimensions on first call — every subsequent record
   * for the same region reuses the cached {@link MetricEntityStateFiveEnums}. Locality is a
   * deterministic function of source region for a given server, so once-per-region caching is safe.
   */
  private MetricEntityStateFiveEnums<VersionRole, ReplicaType, ReplicaState, VeniceStoreWriteType, VeniceChunkingStatus> getOrCreateMetricState(
      String region,
      VeniceRegionLocality locality) {
    return metricsByRegion.computeIfAbsent(region, r -> {
      // Add region name and locality to base dimensions
      Map<VeniceMetricsDimensions, String> regionBaseDimensions = new HashMap<>(baseDimensionsMap);
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_NAME, r);
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_LOCALITY, locality.getDimensionValue());

      return MetricEntityStateFiveEnums.create(
          INGESTION_RECORD_DELAY.getMetricEntity(),
          otelRepository,
          regionBaseDimensions,
          VersionRole.class,
          ReplicaType.class,
          ReplicaState.class,
          VeniceStoreWriteType.class,
          VeniceChunkingStatus.class);
    });
  }

  @VisibleForTesting
  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  /**
   * Clears the per-region metric state map, releasing references to MetricEntityState objects.
   * Does not deregister OTel instruments from the metrics repository — they will be
   * cleaned up when the Meter/MeterProvider is closed or the SDK shuts down.
   */
  public void close() {
    metricsByRegion.clear();
  }

}
