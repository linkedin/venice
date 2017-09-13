package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

//TODO: if other stats adapt this multi-version pattern, it'd be better to consider abstracting this class.
public class AggVersionedDIVStats implements StoreDataChangedListener {
  private static final Logger logger = Logger.getLogger(AggVersionedDIVStats.class);

  private final ReadOnlyStoreRepository metadataRepository;
  private final MetricsRepository metricsRepository;

  private final Map<String, VersionedDIVStats> aggStats;

  public AggVersionedDIVStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository metaRepository) {
    this.metricsRepository = metricsRepository;
    this.metadataRepository = metaRepository;

    aggStats = new HashMap<>();
    metaRepository.registerStoreDataChangedListener(this);
    /**
     * TODO: load could be very slow, need more investigation,
     * and it might be caused by registering too many sensors/metrics with Tehuti.
     *
     * For now, we will disable the preload, and let {@link AggVersionedDIVStats} users gradually
     * load/register metrics for the stores when necessary
     */
    //loadAllStats();
  }

  public void recordException(String storeName, int version, DataValidationException e) {
    if (e instanceof DuplicateDataException) {
      recordDuplicateMsg(storeName, version);
    } else if (e instanceof MissingDataException) {
      recordMissingMsg(storeName, version);
    } else if (e instanceof CorruptDataException) {
      recordCorruptedMsg(storeName, version);
    }
  }

  public void recordDuplicateMsg(String storeName, int version) {
    getVersionedStats(storeName).recordDuplicateMsg(version);
  }

  public void recordMissingMsg(String storeName, int version) {
    getVersionedStats(storeName).recordMissingMsg(version);
  }

  public void recordCorruptedMsg(String storeName, int version) {
    getVersionedStats(storeName).recordCorruptedMsg(version);
  }

  public void recordSuccessMsg(String storeName, int version) {
    getVersionedStats(storeName).recordSuccessMsg(version);
  }

  public void recordCurrentIdleTime(String storeName, int version) {
    getVersionedStats(storeName).recordCurrentIdleTime(version);
  }

  public void recordOverallIdleTime(String storeName, int version) {
    getVersionedStats(storeName).recordOverallIdleTime(version);
  }

  public void resetCurrentIdleTime(String storeName, int version) {
    getVersionedStats(storeName).resetCurrentIdleTime(version);
  }

  protected synchronized void loadAllStats() {
    metadataRepository.getAllStores().forEach(store -> {
      addStore(store.getName());
      updateStatsVersionInfo(store);
    });
  }

  private VersionedDIVStats getVersionedStats(String storeName) {
    if (!aggStats.containsKey(storeName)) {
      addStore(storeName);
      Store store = metadataRepository.getStore(storeName);
      if (null == store) {
        throw new VeniceException("Unknown store: " + storeName);
      }
      updateStatsVersionInfo(store);
    }

    return aggStats.get(storeName);
  }

  private synchronized void addStore(String storeName) {
    if (!aggStats.containsKey(storeName)) {
      aggStats.put(storeName, new VersionedDIVStats(metricsRepository, storeName));
    } else {
      logger.warn("VersionedDIVStats has already been created. Something might be wrong. "
          + "Store: " + storeName);
    }
  }

  private void updateStatsVersionInfo(Store store) {
    VersionedDIVStats versionedDIVStats = getVersionedStats(store.getName());

    int newCurrentVersion = store.getCurrentVersion();
    if (newCurrentVersion != versionedDIVStats.getCurrentVersion()) {
      versionedDIVStats.setCurrentVersion(newCurrentVersion);
    }

    List<Version> existingVersions = store.getVersions();
    List<Integer> existingVersionNumbers =
        existingVersions.stream().map(Version::getNumber).collect(Collectors.toList());

    //remove old versions except version 0. Version 0 is the default version when a store is created. Since no one will
    //report to it, it is always "empty". We use it to reset reporters. eg. when a topic goes from in-flight to current,
    //we reset in-flight reporter to version 0.
    versionedDIVStats.getAllVersionNumbers().stream()
        .filter(versionNum -> !existingVersionNumbers.contains(versionNum) && versionNum != 0)
        .forEach(versionNum -> versionedDIVStats.removeVersion(versionNum));

    int futureVersion = 0;
    int backupVersion = 0;
    for (Version version : existingVersions) {
      int versionNum = version.getNumber();

      //add this version to stats if it is absent
      if (!versionedDIVStats.containsVersion(versionNum)) {
        versionedDIVStats.addVersion(versionNum);
      }

      VersionStatus status = version.getStatus();
      if (status == VersionStatus.STARTED || status == VersionStatus.PUSHED) {
        if (futureVersion != 0) {
          logger.warn(
              "Multiple versions have been marked as STARTED PUSHING. " + "There might be a parallel push. Store: " + store.getName());
        }

        //in case there is a parallel push, record the largest version as future version
        if (futureVersion < versionNum) {
          futureVersion = versionNum;
        }
      } else {
        //check past version
        if (status == VersionStatus.ONLINE && versionNum != newCurrentVersion) {
          if (backupVersion != 0) {
            logger.warn("There are more than 1 backup versions. Something might be wrong." + "Store: " + store.getName());
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
  }

  @Override
  public void handleStoreCreated(Store store) {
    addStore(store.getName());
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    if (!aggStats.containsKey(storeName)) {
      logger.warn("Trying to delete versionedDIVStats but store: " + storeName + "is not in the metric list. Something might be wrong.");
    }

    //aggStats.remove(storeName); //TODO: sdwu to make a more permanent solution
  }

  @Override
  public void handleStoreChanged(Store store) {
    updateStatsVersionInfo(store);
  }
}
