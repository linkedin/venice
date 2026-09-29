package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class AbstractVeniceAggVersionedStats<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>>
    implements StoreDataChangedListener {
  private static final Logger LOGGER = LogManager.getLogger(AbstractVeniceAggVersionedStats.class);

  private final Supplier<STATS> statsInitiator;
  private final StatsSupplier<STATS_REPORTER> reporterSupplier;

  protected final ReadOnlyStoreRepository metadataRepository;
  private final MetricsRepository metricsRepository;

  private final Map<String, VeniceVersionedStats<STATS, STATS_REPORTER>> aggStats;
  private final List<PerStoreVersionedOtelStats<?>> perStoreOtelStats = new CopyOnWriteArrayList<>();
  private final boolean unregisterMetricForDeletedStoreEnabled;

  protected MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public AbstractVeniceAggVersionedStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      Supplier<STATS> statsInitiator,
      StatsSupplier<STATS_REPORTER> reporterSupplier,
      boolean unregisterMetricForDeletedStoreEnabled) {
    this.metadataRepository = metadataRepository;
    this.metricsRepository = metricsRepository;
    this.statsInitiator = statsInitiator;
    this.reporterSupplier = reporterSupplier;

    this.aggStats = new VeniceConcurrentHashMap<>();
    this.unregisterMetricForDeletedStoreEnabled = unregisterMetricForDeletedStoreEnabled;
    metadataRepository.registerStoreDataChangedListener(this);
    loadAllStats();
  }

  public synchronized void loadAllStats() {
    metadataRepository.getAllStores().forEach(store -> {
      addStore(store);
      updateTotalStats(store.getName());
    });
  }

  protected void recordVersionedAndTotalStat(String storeName, int version, Consumer<STATS> function) {
    VeniceVersionedStats<STATS, STATS_REPORTER> stats = getVersionedStats(storeName);
    Utils.computeIfNotNull(stats.getTotalStats(), function);
    Utils.computeIfNotNull(stats.getStats(version), function);
  }

  protected STATS getTotalStats(String storeName) {
    return getVersionedStats(storeName).getTotalStats();
  }

  protected STATS getStats(String storeName, int version) {
    return getVersionedStats(storeName).getStats(version);
  }

  protected void registerConditionalStats(String storeName) {
    getVersionedStats(storeName).registerConditionalStats();
  }

  private VeniceVersionedStats<STATS, STATS_REPORTER> getVersionedStats(String storeName) {
    VeniceVersionedStats<STATS, STATS_REPORTER> stats = aggStats.get(storeName);
    if (stats == null) {
      Store store = metadataRepository.getStoreOrThrow(storeName);
      stats = addStore(store);
      updateTotalStats(storeName);
    }
    return stats;
  }

  /**
   * Applies version info to a VeniceVersionedStats object. This is the core logic shared by both
   * {@link #addStore(Store)} (initialization) and {@link #updateStatsVersionInfo(String, List, int)}
   * (updates).
   *
   * <p>Guards both setCurrentVersion and setFutureVersion with equality checks. This is critical
   * because both methods have side effects: they call {@link VeniceVersionedStats#getStats(int)}
   * which creates a new stats entry if absent, then wire that entry to the reporter via
   * setCurrentStats/setFutureStats. Calling either with NON_EXISTING_VERSION would create a
   * stats entry with version 0.
   */
  private void applyVersionInfo(
      VeniceVersionedStats<STATS, STATS_REPORTER> versionedStats,
      String storeName,
      List<Version> existingVersions,
      int newCurrentVersion) {
    if (newCurrentVersion != versionedStats.getCurrentVersion()) {
      versionedStats.setCurrentVersion(newCurrentVersion);
    }

    List<Integer> existingVersionNumbers =
        existingVersions.stream().map(Version::getNumber).collect(Collectors.toList());

    // remove old versions except version 0. Version 0 is the default version when a store is created. Since no one will
    // report to it, it is always "empty". We use it to reset reporters. eg. when a topic goes from in-flight to
    // current, we reset in-flight reporter to version 0.
    versionedStats.getAllVersionNumbers()
        .stream()
        .filter(versionNum -> !existingVersionNumbers.contains(versionNum) && versionNum != NON_EXISTING_VERSION)
        .forEach(versionNum -> {
          versionedStats.removeVersion(versionNum);
          cleanupVersionResources(storeName, versionNum);
        });

    for (Version version: existingVersions) {
      versionedStats.addVersion(version.getNumber());
    }
    int futureVersion = OtelVersionedStatsUtils.computeFutureVersion(existingVersions);

    if (futureVersion != versionedStats.getFutureVersion()) {
      versionedStats.setFutureVersion(futureVersion);
    }

    onVersionInfoUpdated(storeName, versionedStats.getCurrentVersion(), versionedStats.getFutureVersion());
  }

  /**
   * Adds a store and initializes its version info. Uses computeIfAbsent for thread-safety,
   * ensuring version info is always initialized exactly once when the store is first added.
   */
  protected VeniceVersionedStats<STATS, STATS_REPORTER> addStore(Store store) {
    return aggStats.computeIfAbsent(store.getName(), s -> {
      VeniceVersionedStats<STATS, STATS_REPORTER> newStats =
          new VeniceVersionedStats<>(metricsRepository, s, statsInitiator, reporterSupplier);
      applyVersionInfo(newStats, store.getName(), store.getVersions(), store.getCurrentVersion());
      return newStats;
    });
  }

  protected void updateStatsVersionInfo(String storeName, List<Version> existingVersions, int newCurrentVersion) {
    VeniceVersionedStats<STATS, STATS_REPORTER> versionedStats = getVersionedStats(storeName);
    applyVersionInfo(versionedStats, storeName, existingVersions, newCurrentVersion);
    updateTotalStats(storeName);
  }

  @Override
  public void handleStoreCreated(Store store) {
    addStore(store);
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    VeniceVersionedStats<STATS, STATS_REPORTER> stats = aggStats.remove(storeName);
    if (stats == null) {
      LOGGER.debug("Trying to delete stats but store '{}' is not in the metric list.", storeName);
    } else if (unregisterMetricForDeletedStoreEnabled) {
      stats.unregisterStats();
    }
    perStoreOtelStats.forEach(registry -> registry.remove(storeName));
  }

  @Override
  public void handleStoreChanged(Store store) {
    updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
  }

  /**
   * return {@link Store#NON_EXISTING_VERSION} if future version doesn't exist.
   */
  protected int getFutureVersion(String storeName) {
    return getVersionedStats(storeName).getFutureVersion();
  }

  /**
   * return {@link Store#NON_EXISTING_VERSION} if current version doesn't exist.
   */
  protected int getCurrentVersion(String storeName) {
    return getVersionedStats(storeName).getCurrentVersion();
  }

  /**
   * Some versioned stats might always increasing; in this case, the value in the total stats should be updated with
   * the aggregated values across the new version list.
   */
  protected void updateTotalStats(String storeName) {
    // no-op
  }

  /**
   * Hook method called when version info is updated for a store. Updates every per-store OTel registry, so overrides
   * must call {@code super}.
   *
   * <p><b>WARNING:</b> This method may be called from within {@code aggStats.computeIfAbsent}
   * in {@link #addStore(Store)}. Implementations MUST NOT call {@link #getCurrentVersion},
   * {@link #getFutureVersion}, or {@link #getVersionedStats} — these re-enter {@code aggStats}
   * and cause a deadlock or IllegalStateException.
   */
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    perStoreOtelStats.forEach(registry -> registry.updateVersionInfo(storeName, currentVersion, futureVersion));
  }

  /**
   * Hook method for subclasses to clean up version-specific resources (e.g., OTel stats)
   * when a version is removed. Same re-entrance warning as {@link #onVersionInfoUpdated}.
   */
  protected void cleanupVersionResources(String storeName, int version) {
    // no-op by default
  }

  /**
   * Creates a per-store OTel registry whose stats get the store's versions and are closed on store deletion. Call it
   * from the subclass constructor; hooks may run earlier, so overrides that use the registry must null-check it.
   */
  protected final <OTEL_STATS extends StoreOtelStats> PerStoreVersionedOtelStats<OTEL_STATS> createPerStoreOtelStats(
      Function<String, OTEL_STATS> statsFactory) {
    PerStoreVersionedOtelStats<OTEL_STATS> registry = new PerStoreVersionedOtelStats<>(statsFactory);
    perStoreOtelStats.add(registry);
    return registry;
  }

  /**
   * Per-store OTel stats managed by a {@link AbstractVeniceAggVersionedStats.PerStoreVersionedOtelStats}. Both methods
   * run under the registry's per-store lock, so they must not call back into this class.
   */
  public interface StoreOtelStats extends AutoCloseable {
    void updateVersionInfo(int currentVersion, int futureVersion);

    @Override
    void close();
  }

  /**
   * One {@link StoreOtelStats} per store. Registries can only be created through
   * {@link AbstractVeniceAggVersionedStats#createPerStoreOtelStats}, so every store's stats get version updates and
   * are closed on store deletion.
   */
  protected final class PerStoreVersionedOtelStats<OTEL_STATS extends StoreOtelStats> {
    private final Map<String, OTEL_STATS> statsByStore = new VeniceConcurrentHashMap<>();
    private final Function<String, OTEL_STATS> statsFactory;

    private PerStoreVersionedOtelStats(Function<String, OTEL_STATS> statsFactory) {
      this.statsFactory = statsFactory;
    }

    public OTEL_STATS get(String storeName) {
      return statsByStore.get(storeName);
    }

    /** Reads the versions before entering the map, since reading them can update this map. */
    public OTEL_STATS getOrCreate(String storeName) {
      OTEL_STATS existing = statsByStore.get(storeName);
      if (existing != null) {
        return existing;
      }
      int currentVersion = getCurrentVersion(storeName);
      int futureVersion = getFutureVersion(storeName);
      return statsByStore.computeIfAbsent(storeName, name -> {
        OTEL_STATS stats = statsFactory.apply(name);
        stats.updateVersionInfo(currentVersion, futureVersion);
        return stats;
      });
    }

    /** Runs {@code action} on the store's stats, creating them if absent, atomically with {@link #removeIf}. */
    public void compute(String storeName, Consumer<OTEL_STATS> action) {
      int currentVersion = getCurrentVersion(storeName);
      int futureVersion = getFutureVersion(storeName);
      statsByStore.compute(storeName, (name, existing) -> {
        OTEL_STATS stats = existing;
        if (stats == null) {
          stats = statsFactory.apply(name);
          stats.updateVersionInfo(currentVersion, futureVersion);
        }
        action.accept(stats);
        return stats;
      });
    }

    /** Closes and removes the store's stats if {@code predicate} holds, atomically with {@link #compute}. */
    public void removeIf(String storeName, Predicate<OTEL_STATS> predicate) {
      statsByStore.computeIfPresent(storeName, (name, stats) -> {
        if (predicate.test(stats)) {
          stats.close();
          return null;
        }
        return stats;
      });
    }

    /** Visible for testing. */
    public Map<String, OTEL_STATS> getStatsByStore() {
      return statsByStore;
    }

    private void updateVersionInfo(String storeName, int currentVersion, int futureVersion) {
      statsByStore.computeIfPresent(storeName, (name, stats) -> {
        stats.updateVersionInfo(currentVersion, futureVersion);
        return stats;
      });
    }

    private void remove(String storeName) {
      statsByStore.computeIfPresent(storeName, (name, stats) -> {
        stats.close();
        return null;
      });
    }
  }
}
