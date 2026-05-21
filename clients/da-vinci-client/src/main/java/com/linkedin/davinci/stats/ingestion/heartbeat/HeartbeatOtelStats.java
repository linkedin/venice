package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelMetricEntity.INGESTION_HEARTBEAT_DELAY;

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
import com.linkedin.venice.stats.metrics.AbstractStatsCloseable;
import com.linkedin.venice.stats.metrics.MetricEntityStateFiveEnums;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;


/**
 * OpenTelemetry metrics for heartbeat monitoring.
 * Note: Tehuti metrics are managed separately in {@link HeartbeatStatReporter}.
 */
public class HeartbeatOtelStats extends AbstractStatsCloseable {
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /**
   * Per-region metric entity states, keyed by region name. Grows lazily via {@code computeIfAbsent} and is bounded
   * by the number of distinct regions in the deployment. Entries are not evicted individually. Locality is baked
   * into the per-region state's base dimensions on first call (deterministic function of source region for a
   * given server), matching the layout of {@link RecordLevelDelayOtelStats}.
   */
  private final Map<String, MetricEntityStateFiveEnums<VersionRole, ReplicaType, ReplicaState, VeniceStoreWriteType, VeniceChunkingStatus>> metricsByRegion;

  // Version info cache for classifying versions as CURRENT/FUTURE/BACKUP
  private volatile VersionInfo versionInfo = VersionInfo.NON_EXISTING;

  public HeartbeatOtelStats(MetricsRepository metricsRepository, String storeName, String clusterName) {
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
   * Records a heartbeat delay with all dimensional attributes to OTel metrics.
   * Returns early if OTel metrics are disabled.
   *
   * @param version The version number
   * @param region The region name
   * @param replicaType The replica type {@link ReplicaType}
   * @param replicaState The replica state {@link ReplicaState}
   * @param writeType Pre-resolved store write-type label (REGULAR / WRITE_COMPUTE)
   * @param chunkingStatus Pre-resolved version-level chunking label (CHUNKED / UNCHUNKED)
   * @param locality Pre-resolved locality label (LOCAL / REMOTE) — applied to the per-region metric state on first
   *          call per region; subsequent calls reuse the cached state. Null is coerced to REMOTE: from the
   *          perspective of an unconfigured server, every source region is "not the local region".
   * @param delayMs The delay in milliseconds
   */
  public void recordHeartbeatDelayOtelMetrics(
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

    /*
     * Locality can be null when the server's local region is unconfigured (PCS / HMS leave it
     * null in that case rather than mislabel everything as REMOTE). For the OTel emit path we
     * still need a concrete label, so default to REMOTE.
     */
    VeniceRegionLocality resolvedLocality = locality != null ? locality : VeniceRegionLocality.REMOTE;

    MetricEntityStateFiveEnums<VersionRole, ReplicaType, ReplicaState, VeniceStoreWriteType, VeniceChunkingStatus> metricState =
        getOrCreateMetricState(region, resolvedLocality);

    metricState.record(delayMs, versionRole, replicaType, replicaState, writeType, chunkingStatus);
  }

  /**
   * Gets or creates a metric entity state for a specific region. Region locality is supplied by the
   * caller and baked into the per-region base dimensions on first call — every subsequent record
   * for the same region reuses the cached state.
   */
  private MetricEntityStateFiveEnums<VersionRole, ReplicaType, ReplicaState, VeniceStoreWriteType, VeniceChunkingStatus> getOrCreateMetricState(
      String region,
      VeniceRegionLocality locality) {
    return metricsByRegion.computeIfAbsent(region, r -> {
      Map<VeniceMetricsDimensions, String> regionBaseDimensions = new HashMap<>(baseDimensionsMap);
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_NAME, r);
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_LOCALITY, locality.getDimensionValue());

      return MetricEntityStateFiveEnums.create(
          INGESTION_HEARTBEAT_DELAY.getMetricEntity(),
          otelRepository,
          regionBaseDimensions,
          VersionRole.class,
          ReplicaType.class,
          ReplicaState.class,
          VeniceStoreWriteType.class,
          VeniceChunkingStatus.class,
          statsCloseables);
    });
  }

  @VisibleForTesting
  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  /**
   * Closes the {@link CompositeCloseable} — releasing wrapper-side state for the sync HISTOGRAM
   * recorded by this class. The per-region map is then cleared so wrappers become eligible for
   * GC. The SDK aggregator persists until the MeterProvider is closed; this call only releases
   * wrapper memory.
   */
  @Override
  public void close() {
    super.close();
    metricsByRegion.clear();
  }

}
