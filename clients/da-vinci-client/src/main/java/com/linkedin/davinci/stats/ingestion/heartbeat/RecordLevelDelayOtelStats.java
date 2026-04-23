package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelMetricEntity.INGESTION_RECORD_DELAY;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

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
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;


/**
 * OpenTelemetry metrics for record-level delay monitoring.
 * Tracks delays for regular data records (not heartbeat control messages).
 * Note: Tehuti metrics are managed separately in {@link HeartbeatStatReporter}.
 */
public class RecordLevelDelayOtelStats {
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** Local region name for computing region locality (LOCAL vs REMOTE). */
  private final String localRegionName;

  /**
   * Per-region metric entity states, keyed by region name. Grows lazily via {@code computeIfAbsent} and is bounded
   * by the number of distinct regions in the deployment. Entries are not evicted individually.
   */
  private final Map<String, MetricEntityStateThreeEnums<VersionRole, ReplicaType, ReplicaState>> metricsByRegion;

  // Version info cache for classifying versions as CURRENT/FUTURE/BACKUP
  private volatile VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);

  public RecordLevelDelayOtelStats(
      MetricsRepository metricsRepository,
      String storeName,
      String clusterName,
      String localRegionName,
      boolean partialUpdateEnabled,
      boolean chunkingEnabled) {
    this.metricsByRegion = new VeniceConcurrentHashMap<>();
    this.localRegionName = localRegionName;

    VeniceStoreWriteType writeType =
        partialUpdateEnabled ? VeniceStoreWriteType.WRITE_COMPUTE : VeniceStoreWriteType.REGULAR;
    VeniceChunkingStatus chunkStatus = chunkingEnabled ? VeniceChunkingStatus.CHUNKED : VeniceChunkingStatus.UNCHUNKED;

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .addCustomDimension(writeType)
            .addCustomDimension(chunkStatus)
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
   * @param version The version number
   * @param region The region name
   * @param replicaType The replica type {@link ReplicaType}
   * @param replicaState The replica state {@link ReplicaState}
   * @param delayMs The delay in milliseconds
   */
  public void recordRecordDelayOtelMetrics(
      int version,
      String region,
      ReplicaType replicaType,
      ReplicaState replicaState,
      long delayMs) {
    if (!emitOtelMetrics()) {
      return;
    }
    VersionRole versionRole = OtelVersionedStatsUtils.classifyVersion(version, this.versionInfo);

    MetricEntityStateThreeEnums<VersionRole, ReplicaType, ReplicaState> metricState = getOrCreateMetricState(region);

    // Records to OTel metrics only
    metricState.record(delayMs, versionRole, replicaType, replicaState);
  }

  /**
   * Gets or creates a metric entity state for a specific region. Region locality (LOCAL vs REMOTE)
   * is computed once per region at creation time by comparing the record's source region against
   * this server's local region.
   */
  private MetricEntityStateThreeEnums<VersionRole, ReplicaType, ReplicaState> getOrCreateMetricState(String region) {
    return metricsByRegion.computeIfAbsent(region, r -> {
      // Add region name and locality to base dimensions
      Map<VeniceMetricsDimensions, String> regionBaseDimensions = new HashMap<>(baseDimensionsMap);
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_NAME, r);
      VeniceRegionLocality locality =
          r.equals(localRegionName) ? VeniceRegionLocality.LOCAL : VeniceRegionLocality.REMOTE;
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_LOCALITY, locality.getDimensionValue());

      return MetricEntityStateThreeEnums.create(
          INGESTION_RECORD_DELAY.getMetricEntity(),
          otelRepository,
          regionBaseDimensions,
          VersionRole.class,
          ReplicaType.class,
          ReplicaState.class);
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
