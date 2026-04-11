package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.VeniceVersionedStatsOtelMetricEntity.STORE_VERSION;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;


/**
 * Emits a single OTel {@code ASYNC_GAUGE} per store for the current and future version numbers.
 *
 * <p>Metric: {@code store.version} with dimensions {@code CLUSTER_NAME}, {@code STORE_NAME},
 * and {@code VERSION_ROLE}. Only {@link VersionRole#CURRENT CURRENT} and {@link VersionRole#FUTURE FUTURE}
 * are emitted — backup version number is not tracked.
 *
 * <p>This class implements {@link StoreDataChangedListener} and is registered once per process
 * on the metadata repository. Per-store state is created lazily on first store change
 * and bounded by the number of stores on the host.
 *
 * <p><b>Cleanup limitation:</b> OTel async gauge callbacks cannot be deregistered (OTel SDK
 * limitation). On store deletion, version info is reset to {@link VersionInfo#NON_EXISTING}
 * rather than removed — this avoids duplicate callback registration if the store is re-created.
 * Callbacks remain registered until the {@link MetricsRepository} is closed.
 */
public class StoreVersionOtelStats implements StoreDataChangedListener {
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** Per-store version info. Written by metadata-change thread, read by OTel collection thread. */
  private final Map<String, AtomicReference<VersionInfo>> perStoreVersions = new VeniceConcurrentHashMap<>();

  public StoreVersionOtelStats(MetricsRepository metricsRepository, String clusterName) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
  }

  @Override
  public void handleStoreCreated(Store store) {
    handleStoreChanged(store);
  }

  @Override
  public void handleStoreChanged(Store store) {
    String storeName = store.getName();
    int currentVersion = store.getCurrentVersion();
    int futureVersion = OtelVersionedStatsUtils.computeFutureVersion(store.getVersions());

    AtomicReference<VersionInfo> versionInfoRef = perStoreVersions.computeIfAbsent(storeName, k -> {
      AtomicReference<VersionInfo> newVersionInfoRef = new AtomicReference<>(VersionInfo.NON_EXISTING);
      registerOtelGauge(k, newVersionInfoRef);
      return newVersionInfoRef;
    });
    versionInfoRef.set(new VersionInfo(currentVersion, futureVersion));
  }

  /**
   * Resets version info to {@link VersionInfo#NON_EXISTING} rather than removing the map entry.
   * OTel async gauge callbacks cannot be deregistered, so removing and re-creating the entry
   * on store re-creation would produce duplicate callbacks. Keeping the entry with reset values
   * ensures the gauge reports 0 for deleted stores and reuses the same callback on re-creation.
   */
  @Override
  public void handleStoreDeleted(String storeName) {
    AtomicReference<VersionInfo> versionInfoRef = perStoreVersions.get(storeName);
    if (versionInfoRef != null) {
      versionInfoRef.set(VersionInfo.NON_EXISTING);
    }
  }

  /**
   * Registers two ASYNC_GAUGE callbacks: one for CURRENT and one for FUTURE.
   * Only these two roles are tracked — backup version number is not tracked.
   */
  private void registerOtelGauge(String storeName, AtomicReference<VersionInfo> versionInfoRef) {
    if (otelRepository == null) {
      return;
    }
    Map<VeniceMetricsDimensions, String> storeDims = new HashMap<>(baseDimensionsMap);
    storeDims.put(VeniceMetricsDimensions.VENICE_STORE_NAME, OpenTelemetryMetricsSetup.sanitizeStoreName(storeName));
    registerRoleGauge(storeDims, VersionRole.CURRENT, () -> versionInfoRef.get().getCurrentVersion());
    registerRoleGauge(storeDims, VersionRole.FUTURE, () -> versionInfoRef.get().getFutureVersion());
  }

  private void registerRoleGauge(
      Map<VeniceMetricsDimensions, String> storeDims,
      VersionRole role,
      LongSupplier callback) {
    Map<VeniceMetricsDimensions, String> dims = new HashMap<>(storeDims);
    dims.put(VeniceMetricsDimensions.VENICE_VERSION_ROLE, role.getDimensionValue());
    Attributes attrs = otelRepository.createAttributes(STORE_VERSION.getMetricEntity(), dims);
    AsyncMetricEntityStateBase.create(STORE_VERSION.getMetricEntity(), otelRepository, dims, attrs, callback);
  }
}
