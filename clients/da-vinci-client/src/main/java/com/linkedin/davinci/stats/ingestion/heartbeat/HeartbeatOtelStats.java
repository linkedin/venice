package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_HEARTBEAT_DELAY;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.davinci.stats.ServerMetricEntity;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaState;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * OpenTelemetry metrics for heartbeat monitoring.
 * Note: Tehuti metrics are managed separately in {@link HeartbeatStatReporter}.
 */
public class HeartbeatOtelStats {
  public static final Collection<MetricEntity> SERVER_METRIC_ENTITIES =
      getUniqueMetricEntities(ServerMetricEntity.class);
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  // Per-region metric entity states
  private final Map<String, MetricEntityStateThreeEnums<VersionRole, ReplicaType, ReplicaState>> metricsByRegion;

  // Version info cache for classifying versions as CURRENT/FUTURE/BACKUP
  private volatile VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);

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
   * Returns early if OTel metrics are disabled or version is invalid.
   *
   * @param version The version number
   * @param region The region name
   * @param replicaType The replica type {@link ReplicaType}
   * @param replicaState The replica state {@link ReplicaState}
   * @param delayMs The delay in milliseconds
   */
  public void recordHeartbeatDelayOtelMetrics(
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
   * Gets or creates a metric entity state for a specific region.
   */
  private MetricEntityStateThreeEnums<VersionRole, ReplicaType, ReplicaState> getOrCreateMetricState(String region) {
    return metricsByRegion.computeIfAbsent(region, r -> {
      // Add region to base dimensions
      Map<VeniceMetricsDimensions, String> regionBaseDimensions = new HashMap<>(baseDimensionsMap);
      regionBaseDimensions.put(VeniceMetricsDimensions.VENICE_REGION_NAME, r);

      return MetricEntityStateThreeEnums.create(
          INGESTION_HEARTBEAT_DELAY.getMetricEntity(),
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
}
