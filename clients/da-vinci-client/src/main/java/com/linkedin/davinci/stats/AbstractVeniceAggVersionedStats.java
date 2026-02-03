package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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
      addStore(store.getName());
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
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
      // Use computeIfAbsent to atomically add the store and initialize version info.
      // This prevents returning partially initialized stats and duplicate initialization
      // when multiple threads try to get stats for a new store concurrently.
      stats = aggStats.computeIfAbsent(storeName, s -> {
        VeniceVersionedStats<STATS, STATS_REPORTER> newStats =
            new VeniceVersionedStats<>(metricsRepository, s, statsInitiator, reporterSupplier);
        initializeVersionInfo(newStats, store);
        return newStats;
      });
    }
    return stats;
  }

  /** Initializes version info for a newly created VeniceVersionedStats */
  private void initializeVersionInfo(VeniceVersionedStats<STATS, STATS_REPORTER> versionedStats, Store store) {
    versionedStats.setCurrentVersion(store.getCurrentVersion());

    List<Version> existingVersions = store.getVersions();
    int futureVersion = NON_EXISTING_VERSION;
    for (Version version: existingVersions) {
      int versionNum = version.getNumber();
      versionedStats.addVersion(versionNum);

      VersionStatus status = version.getStatus();
      if (status == VersionStatus.STARTED || status == VersionStatus.PUSHED) {
        if (futureVersion < versionNum) {
          futureVersion = versionNum;
        }
      }
    }

    // Set directly without checking - this is a newly created stats object with default values
    versionedStats.setFutureVersion(futureVersion);

    // Notify subclasses that version info has been initialized
    onVersionInfoUpdated(store.getName(), versionedStats.getCurrentVersion(), versionedStats.getFutureVersion());
  }

  protected VeniceVersionedStats<STATS, STATS_REPORTER> addStore(String storeName) {
    return aggStats.computeIfAbsent(
        storeName,
        s -> new VeniceVersionedStats<>(metricsRepository, storeName, statsInitiator, reporterSupplier));
  }

  protected void updateStatsVersionInfo(String storeName, List<Version> existingVersions, int newCurrentVersion) {
    VeniceVersionedStats<STATS, STATS_REPORTER> versionedStats = getVersionedStats(storeName);

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

    int futureVersion = NON_EXISTING_VERSION;
    for (Version version: existingVersions) {
      int versionNum = version.getNumber();

      // add this version to stats if it is absent
      versionedStats.addVersion(versionNum);

      VersionStatus status = version.getStatus();
      if (status == VersionStatus.STARTED || status == VersionStatus.PUSHED) {
        if (futureVersion != NON_EXISTING_VERSION) {
          LOGGER.warn(
              "Multiple versions have been marked as STARTED PUSHING. There might be a parallel push. Store: {}",
              storeName);
        }

        // in case there is a parallel push, record the largest version as future version
        if (futureVersion < versionNum) {
          futureVersion = versionNum;
        }
      }
    }

    if (futureVersion != versionedStats.getFutureVersion()) {
      versionedStats.setFutureVersion(futureVersion);
    }

    // Notify subclasses that version info has changed
    onVersionInfoUpdated(storeName, versionedStats.getCurrentVersion(), versionedStats.getFutureVersion());

    /**
     * Since versions are changed, update the total stats accordingly.
     */
    updateTotalStats(storeName);
  }

  @Override
  public void handleStoreCreated(Store store) {
    addStore(store.getName());
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    VeniceVersionedStats<STATS, STATS_REPORTER> stats = aggStats.remove(storeName);
    if (stats == null) {
      LOGGER.debug("Trying to delete stats but store '{}' is not in the metric list.", storeName);
    } else if (unregisterMetricForDeletedStoreEnabled) {
      stats.unregisterStats();
    }
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
   * return {@link Store#NON_EXISTING_VERSION} if backup version doesn't exist.
   */

  /**
   * Some versioned stats might always increasing; in this case, the value in the total stats should be updated with
   * the aggregated values across the new version list.
   */
  protected void updateTotalStats(String storeName) {
    // no-op
  }

  /**
   * Hook method called when version info is updated for a store.
   * Subclasses can override this to react to version changes.
   *
   * @param storeName The store whose version info changed
   * @param currentVersion The new current version
   * @param futureVersion The new future version
   */
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    // no-op by default
  }

  /**
   * Hook method for subclasses to clean up their own version-specific resources
   * (e.g., OTel stats) when a version is removed. This is called after the internal
   * versioned stats have been removed via {@link VeniceVersionedStats#removeVersion}.
   *
   * @param storeName The store whose version was removed
   * @param version The version number that was removed
   */
  protected void cleanupVersionResources(String storeName, int version) {
    // no-op by default
  }
}
