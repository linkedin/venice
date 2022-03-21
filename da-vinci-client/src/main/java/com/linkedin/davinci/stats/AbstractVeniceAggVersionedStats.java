package com.linkedin.davinci.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.stats.StatsSupplier;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.meta.Store.*;


public abstract class AbstractVeniceAggVersionedStats<STATS, STATS_REPORTER extends AbstractVeniceStatsReporter<STATS>>
    implements StoreDataChangedListener {
  private static final Logger logger = LogManager.getLogger(AbstractVeniceAggVersionedStats.class);

  private final Supplier<STATS> statsInitiator;
  private final StatsSupplier<STATS_REPORTER> reporterSupplier;

  private final ReadOnlyStoreRepository metadataRepository;
  private final MetricsRepository metricsRepository;

  private final Map<String, VeniceVersionedStats<STATS, STATS_REPORTER>> aggStats;

  public AbstractVeniceAggVersionedStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository metadataRepository,
      Supplier<STATS> statsInitiator, StatsSupplier<STATS_REPORTER> reporterSupplier) {
    this.metadataRepository = metadataRepository;
    this.metricsRepository = metricsRepository;
    this.statsInitiator = statsInitiator;
    this.reporterSupplier = reporterSupplier;

    this.aggStats = new VeniceConcurrentHashMap<>();
    metadataRepository.registerStoreDataChangedListener(this);
    loadAllStats();
  }

  public synchronized void loadAllStats() {
    metadataRepository.getAllStores().forEach(store -> {
      addStore(store.getName());
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
    });
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
    if (!aggStats.containsKey(storeName)) {
      addStore(storeName);
      Store store = metadataRepository.getStoreOrThrow(storeName);
      updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
    }
    return aggStats.get(storeName);
  }

  private synchronized void addStore(String storeName) {
    if (!aggStats.containsKey(storeName)) {
      aggStats.put(storeName, new VeniceVersionedStats<>(metricsRepository, storeName, statsInitiator, reporterSupplier));
    } else {
      logger.warn("VersionedStats has already been created. Something might be wrong. "
          + "Store: " + storeName);
    }
  }

  protected void updateStatsVersionInfo(String storeName, List<Version> existingVersions, int newCurrentVersion) {
    VeniceVersionedStats<STATS, STATS_REPORTER> versionedDIVStats = getVersionedStats(storeName);

    if (newCurrentVersion != versionedDIVStats.getCurrentVersion()) {
      versionedDIVStats.setCurrentVersion(newCurrentVersion);
    }

    List<Integer> existingVersionNumbers =
        existingVersions.stream().map(Version::getNumber).collect(Collectors.toList());

    //remove old versions except version 0. Version 0 is the default version when a store is created. Since no one will
    //report to it, it is always "empty". We use it to reset reporters. eg. when a topic goes from in-flight to current,
    //we reset in-flight reporter to version 0.
    versionedDIVStats.getAllVersionNumbers().stream()
        .filter(versionNum -> !existingVersionNumbers.contains(versionNum) && versionNum != NON_EXISTING_VERSION)
        .forEach(versionNum -> versionedDIVStats.removeVersion(versionNum));

    int futureVersion = NON_EXISTING_VERSION;
    int backupVersion = NON_EXISTING_VERSION;
    for (Version version : existingVersions) {
      int versionNum = version.getNumber();

      //add this version to stats if it is absent
      if (!versionedDIVStats.containsVersion(versionNum)) {
        versionedDIVStats.addVersion(versionNum);
      }

      VersionStatus status = version.getStatus();
      if (status == VersionStatus.STARTED || status == VersionStatus.PUSHED) {
        if (futureVersion != NON_EXISTING_VERSION) {
          logger.warn(
              "Multiple versions have been marked as STARTED PUSHING. " + "There might be a parallel push. Store: " + storeName);
        }

        //in case there is a parallel push, record the largest version as future version
        if (futureVersion < versionNum) {
          futureVersion = versionNum;
        }
      } else {
        //check past version
        if (status == VersionStatus.ONLINE && versionNum != newCurrentVersion) {
          if (backupVersion != 0) {
            logger.warn("There are more than 1 backup versions. Something might be wrong." + "Store: " + storeName);
          }

          backupVersion = versionNum;
        }
      }
    }

    if (futureVersion != versionedDIVStats.getFutureVersion()) {
      versionedDIVStats.setFutureVersion(futureVersion);
    }
    if (backupVersion != versionedDIVStats.getBackupVersion()) {
      versionedDIVStats.setBackupVersion(backupVersion);
    }

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
    if (!aggStats.containsKey(storeName)) {
      logger.warn("Trying to delete stats but store: " + storeName + "is not in the metric list. Something might be wrong.");
    }

    //aggStats.remove(storeName); //TODO: sdwu to make a more permanent solution
  }

  @Override
  public void handleStoreChanged(Store store) {
    updateStatsVersionInfo(store.getName(), store.getVersions(), store.getCurrentVersion());
  }

  public boolean isFutureVersion(String storeName, int version) {
    VeniceVersionedStats<STATS, STATS_REPORTER> versionedStats = getVersionedStats(storeName);
    return versionedStats.getFutureVersion() == version;
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
  protected int getBackupVersion(String storeName) {
    return getVersionedStats(storeName).getBackupVersion();
  }

  /**
   * Some versioned stats might always increasing; in this case, the value in the total stats should be updated with
   * the aggregated values across the new version list.
   */
  protected void updateTotalStats(String storeName) {
    // no-op
  }
}
