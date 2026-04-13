package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.VeniceVersionedStatsOtelMetricEntity.STORE_VERSION;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
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
 * Registers OTel {@code ASYNC_GAUGE} callbacks per store for the current and future version numbers.
 *
 * <p>Metric: {@code store.version} with dimensions {@code CLUSTER_NAME}, {@code STORE_NAME},
 * and {@code VERSION_ROLE}. Only {@link VersionRole#CURRENT CURRENT} and {@link VersionRole#FUTURE FUTURE}
 * are emitted — backup version number is not tracked.
 *
 * <p>This class implements {@link StoreDataChangedListener} and should be registered once per process
 * on the metadata repository via {@link #register(ReadOnlyStoreRepository)}. Per-store state is
 * created lazily on first store change and bounded by the number of distinct store names ever
 * observed by the process (entries are not removed on deletion — see cleanup limitation below).
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

  /**
   * Creates and registers a {@link StoreVersionOtelStats} listener on the given metadata repository.
   * Initializes gauges for all pre-existing stores. This is the preferred entry point — it combines
   * construction and registration to prevent the listener from existing in an unregistered state.
   */
  public static StoreVersionOtelStats create(
      MetricsRepository metricsRepository,
      String clusterName,
      ReadOnlyStoreRepository metadataRepository) {
    StoreVersionOtelStats stats = new StoreVersionOtelStats(metricsRepository, clusterName);
    stats.register(metadataRepository);
    return stats;
  }

  StoreVersionOtelStats(MetricsRepository metricsRepository, String clusterName) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
  }

  /**
   * Registers this listener on the metadata repository and initializes gauges for all
   * pre-existing stores. Prefer {@link #create} which combines construction and registration.
   */
  void register(ReadOnlyStoreRepository metadataRepository) {
    metadataRepository.registerStoreDataChangedListener(this);
    if (otelRepository == null) {
      return;
    }
    // Initialize gauges for pre-existing stores. Uses computeIfAbsent only (no unconditional
    // set) so that a concurrent ZK event with newer data is never overwritten by the snapshot.
    metadataRepository.getAllStores().forEach(this::initializeStoreIfAbsent);
  }

  @Override
  public void handleStoreCreated(Store store) {
    handleStoreChanged(store);
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (otelRepository == null) {
      return;
    }
    String storeName = store.getName();
    VersionInfo newInfo = computeVersionInfo(store);

    // computeIfAbsent for first-time gauge registration (matches the pattern in other OTel stats
    // classes like ParticipantStoreConsumptionStats, AggVersionedDIVStats). The unconditional
    // set() after updates existing entries with the latest data from the ZK event.
    perStoreVersions.computeIfAbsent(storeName, k -> {
      AtomicReference<VersionInfo> newRef = new AtomicReference<>(newInfo);
      registerOtelGauge(k, newRef);
      return newRef;
    }).set(newInfo);
  }

  /** Initializes a store entry if absent. Does NOT update existing entries — avoids overwriting
   *  concurrent ZK events with stale snapshot data during {@link #register} initialization. */
  private void initializeStoreIfAbsent(Store store) {
    String storeName = store.getName();
    VersionInfo newInfo = computeVersionInfo(store);
    perStoreVersions.computeIfAbsent(storeName, k -> {
      AtomicReference<VersionInfo> newRef = new AtomicReference<>(newInfo);
      registerOtelGauge(k, newRef);
      return newRef;
    });
  }

  private static VersionInfo computeVersionInfo(Store store) {
    return new VersionInfo(
        store.getCurrentVersion(),
        OtelVersionedStatsUtils.computeFutureVersion(store.getVersions()));
  }

  /**
   * Resets version info to {@link VersionInfo#NON_EXISTING} rather than removing the map entry.
   * OTel async gauge callbacks cannot be deregistered, so removing and re-creating the entry
   * on store re-creation would produce duplicate callbacks. Keeping the entry with reset values
   * ensures the gauge reports NON_EXISTING_VERSION for deleted stores and reuses the same
   * callback on re-creation.
   */
  @Override
  public void handleStoreDeleted(String storeName) {
    if (otelRepository == null) {
      return;
    }
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
