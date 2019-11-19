package com.linkedin.venice.router.api;

import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.stats.StaleVersionStats;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;

public class VeniceVersionFinder {
  private static final Logger logger = Logger.getLogger(VeniceVersionFinder.class);

  private final ReadOnlyStoreRepository metadataRepository;
  private final StaleVersionStats stats;
  private ConcurrentMap<String, Integer> lastCurrentVersion = new ConcurrentHashMap<>();

  private OnlineInstanceFinder onlineInstanceFinder;

  public VeniceVersionFinder(ReadOnlyStoreRepository metadataRepository, OnlineInstanceFinder onlineInstanceFinder,
      StaleVersionStats stats) {
    this.metadataRepository = metadataRepository;
    this.onlineInstanceFinder = onlineInstanceFinder;
    this.stats = stats;
  }

  public int getVersion(String store) throws VeniceException {
    /**
     * TODO: clone a store object is too expensive, and we could choose to expose the necessary methods
     * in {@link ReadOnlyStoreRepository}, such as 'isEnableReads' and others.
     */
    Store veniceStore = metadataRepository.getStore(store);
    if (null == veniceStore){
      throw new VeniceNoStoreException(store);
    }
    if (!veniceStore.isEnableReads()) {
      throw new StoreDisabledException(store, "read");
    }

    int metadataCurrentVersion = veniceStore.getCurrentVersion();
    if (!lastCurrentVersion.containsKey(store)){
      lastCurrentVersion.put(store, metadataCurrentVersion);
      if (metadataCurrentVersion == Store.NON_EXISTING_VERSION) {
        /** This should happen at most once per store, since we are adding the mapping to {@link lastCurrentVersion} */
        veniceStore = metadataRepository.refreshOneStore(store);
        metadataCurrentVersion = veniceStore.getCurrentVersion();
      }
    }
    if (lastCurrentVersion.get(store).equals(metadataCurrentVersion)){
      stats.recordNotStale();
      return metadataCurrentVersion;
    }
   //This is a new version change, verify we have online replicas for each partition
    String kafkaTopic = Version.composeKafkaTopic(store, metadataCurrentVersion);
    if (anyOfflinePartitions(kafkaTopic)) {
      VersionStatus lastCurrentVersionStatus = veniceStore.getVersionStatus(lastCurrentVersion.get(store));
      if (lastCurrentVersionStatus.equals(VersionStatus.ONLINE)) {
        logger.warn(
            "Offline partitions for new active version " + kafkaTopic + ", continuing to serve previous version: " + lastCurrentVersion.get(store));
        stats.recordStale(metadataCurrentVersion, lastCurrentVersion.get(store));
        return lastCurrentVersion.get(store);
      } else {
        logger.warn(""
            + "Offline partitions for new active version: " + kafkaTopic
            + ", but previous version :" + lastCurrentVersion.get(store) + " has status: " + lastCurrentVersionStatus.toString()
            + ".  Switching to serve new active version.");
        lastCurrentVersion.put(store, metadataCurrentVersion);
        stats.recordNotStale();
        return metadataCurrentVersion;
      }
    } else { // all partitions are online
      lastCurrentVersion.put(store, metadataCurrentVersion);
      stats.recordNotStale();
      return metadataCurrentVersion;
    }
  }

  private boolean anyOfflinePartitions(String kafkaTopic) {
    int partitionCount = onlineInstanceFinder.getNumberOfPartitions(kafkaTopic);
    for (int p = 0; p < partitionCount; p++) {
      List<Instance> partitionHosts = onlineInstanceFinder.getReadyToServeInstances(kafkaTopic, p);
      if (partitionHosts.isEmpty()) {
        String partitionAssignment;
        try {
          partitionAssignment = onlineInstanceFinder.getAllInstances(kafkaTopic, p).toString();
        } catch (Exception e) {
          logger.warn("Failed to get partition assignment for logging purposes for resource: " + kafkaTopic, e);
          partitionAssignment = "unknown";
        }
        logger.warn("No online replicas for partition " + p + " of " + kafkaTopic + ", partition assignment: " + partitionAssignment);
        return true;
      }
    }
    return false;
  }
}
