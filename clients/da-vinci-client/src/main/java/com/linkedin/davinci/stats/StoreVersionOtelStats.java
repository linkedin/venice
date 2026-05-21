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
import com.linkedin.venice.stats.metrics.CompositeCloseable;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
 * observed by the process. Entries are not removed on deletion — see {@link #handleStoreDeleted}.
 */
public class StoreVersionOtelStats implements StoreDataChangedListener, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(StoreVersionOtelStats.class);
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** Per-store version info. Written by metadata-change thread, read by OTel collection thread. */
  private final Map<String, AtomicReference<VersionInfo>> perStoreVersions = new VeniceConcurrentHashMap<>();

  /**
   * The metadata repository this listener is registered on. Set when {@link #register} succeeds;
   * null otherwise (OTel disabled or never registered). Used by {@link #close} to deregister.
   */
  private ReadOnlyStoreRepository registeredMetadataRepository;

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
   *
   * <p>When OTel is disabled the listener is NOT registered — every event handler would early-return
   * anyway, so registering would just add no-op dispatch overhead on every store create/change/delete.
   */
  void register(ReadOnlyStoreRepository metadataRepository) {
    if (otelRepository == null) {
      LOGGER.info("OTel metrics disabled; skipping StoreVersionOtelStats listener registration.");
      return;
    }
    this.registeredMetadataRepository = metadataRepository;
    metadataRepository.registerStoreDataChangedListener(this);
    // Initialize gauges for pre-existing stores. Uses computeIfAbsent only (no unconditional
    // set) so that a concurrent ZK event with newer data is never overwritten by the snapshot.
    metadataRepository.getAllStores().forEach(this::initializeStoreIfAbsent);
  }

  /** Unregisters this OTel listener from the metadata repository. Idempotent. */
  @Override
  public void close() {
    if (registeredMetadataRepository != null) {
      registeredMetadataRepository.unregisterStoreDataChangedListener(this);
      registeredMetadataRepository = null;
    }
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
      registerOtelGauges(k, newRef);
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
      registerOtelGauges(k, newRef);
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
   * The ASYNC_GAUGE callback closes over the {@link AtomicReference}, so removing the entry would
   * orphan the SDK-side callback (still polling, with no clean way to deregister it through the
   * current wrapper). Resetting keeps one live callback pointed at sentinel values
   * ({@code NON_EXISTING_VERSION}) which dashboards can filter; map cardinality stays bounded by
   * the host's store count across delete→re-create cycles.
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

  /** Registers ASYNC_GAUGE callbacks for both CURRENT and FUTURE roles for a single store. */
  private void registerOtelGauges(String storeName, AtomicReference<VersionInfo> versionInfoRef) {
    Map<VeniceMetricsDimensions, String> storeDims =
        OpenTelemetryMetricsSetup.buildStoreDimensionsMap(baseDimensionsMap, storeName);
    registerRoleGauge(storeDims, VersionRole.CURRENT, () -> versionInfoRef.get().getCurrentVersion());
    registerRoleGauge(storeDims, VersionRole.FUTURE, () -> versionInfoRef.get().getFutureVersion());
  }

  private AsyncMetricEntityStateBase registerRoleGauge(
      Map<VeniceMetricsDimensions, String> storeDims,
      VersionRole role,
      LongSupplier callback) {
    Map<VeniceMetricsDimensions, String> dims = new HashMap<>(storeDims);
    dims.put(VeniceMetricsDimensions.VENICE_VERSION_ROLE, role.getDimensionValue());
    Attributes attrs = otelRepository.createAttributes(STORE_VERSION.getMetricEntity(), dims);
    return AsyncMetricEntityStateBase
        .create(STORE_VERSION.getMetricEntity(), otelRepository, dims, attrs, callback, CompositeCloseable.NONE);
  }
}
