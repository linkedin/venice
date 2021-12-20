package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.stats.StaleVersionReason;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceVersionFinder {
  private static final Logger logger = LogManager.getLogger(VeniceVersionFinder.class);
  private static final RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

  private final ReadOnlyStoreRepository metadataRepository;
  private final StaleVersionStats stats;
  private final HelixReadOnlyStoreConfigRepository storeConfigRepo;
  private final Map<String, String> clusterToD2Map;
  private final String clusterName;
  private final ConcurrentMap<String, Integer> lastCurrentVersion = new ConcurrentHashMap<>();
  private final OnlineInstanceFinder onlineInstanceFinder;
  private final CompressorFactory compressorFactory;

  public VeniceVersionFinder(ReadOnlyStoreRepository metadataRepository, OnlineInstanceFinder onlineInstanceFinder,
      StaleVersionStats stats, HelixReadOnlyStoreConfigRepository storeConfigRepo,
      Map<String, String> clusterToD2Map, String clusterName, CompressorFactory compressorFactory) {
    this.metadataRepository = metadataRepository;
    this.onlineInstanceFinder = onlineInstanceFinder;
    this.stats = stats;
    this.storeConfigRepo = storeConfigRepo;
    this.clusterToD2Map = clusterToD2Map;
    this.clusterName = clusterName;
    this.compressorFactory = compressorFactory;
  }

  public int getVersion(String store, BasicFullHttpRequest request) throws VeniceException {
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
    Store storeToCheckMigration = VeniceSystemStoreUtils.getSystemStoreType(store) == VeniceSystemStoreType.METADATA_STORE ?
        metadataRepository.getStore(VeniceSystemStoreUtils.getStoreNameFromSystemStoreName(store)) : veniceStore;
    if (storeToCheckMigration != null && storeToCheckMigration.isMigrating() &&
        request.headers().contains(HttpConstants.VENICE_ALLOW_REDIRECT)) {
      Optional<StoreConfig> config = storeConfigRepo.getStoreConfig(store);
      if (config.isPresent()) {
        String newCluster = config.get().getCluster();
        if (!clusterName.equals(newCluster)) {
          String d2Service = clusterToD2Map.get(newCluster);
          throw new VeniceStoreIsMigratedException(store, newCluster, d2Service);
        }
      }
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
    int prevVersion = lastCurrentVersion.get(store);
    if (prevVersion == metadataCurrentVersion){
      stats.recordNotStale();
      return metadataCurrentVersion;
    }
    //This is a new version change, verify we have online replicas for each partition
    String kafkaTopic = Version.composeKafkaTopic(store, metadataCurrentVersion);

    boolean currentVersionDecompressorReady = isDecompressorReady(veniceStore, metadataCurrentVersion);
    boolean prevVersionDecompressorReady = isDecompressorReady(veniceStore, prevVersion);

    boolean currentVersionHasOfflinePartitions = anyOfflinePartitions(kafkaTopic);
    if (currentVersionHasOfflinePartitions || !currentVersionDecompressorReady) {
      String errorMessage = "Unable to serve new active version: " + kafkaTopic + ".";

      if (currentVersionHasOfflinePartitions) {
        errorMessage += " Offline partitions for new active version.";
        stats.recordStalenessReason(StaleVersionReason.OFFLINE_PARTITIONS);
      }

      if (!currentVersionDecompressorReady) {
        errorMessage += " Decompressor not ready for current version (Has dictionary downloaded?).";
        stats.recordStalenessReason(StaleVersionReason.DICTIONARY_NOT_DOWNLOADED);
      }

      VersionStatus lastCurrentVersionStatus = veniceStore.getVersionStatus(prevVersion);
      if (lastCurrentVersionStatus.equals(VersionStatus.ONLINE) && prevVersionDecompressorReady) {
        String message = errorMessage + " Continuing to serve previous version: " + prevVersion + ".";
        if (!filter.isRedundantException(message)) {
          logger.warn(message);
        }
        stats.recordStale(metadataCurrentVersion, prevVersion);
        return prevVersion;
      } else {
        errorMessage += " Unable to serve previous version: " + prevVersion + ".";

        if (!lastCurrentVersionStatus.equals(VersionStatus.ONLINE)) {
          errorMessage += " Previous version has status: " + lastCurrentVersionStatus.toString() + ".";
        }

        if (!prevVersionDecompressorReady) {
          errorMessage += " Decompressor not ready for previous version (Has dictionary downloaded?).";
        }

        // When the router has only one available version, despite offline partitions, or dictionary not yet downloaded,
        // etc, it will return it as the available version.
        // If the partitions are still unavailable or the dictionary is not downloaded by the time the records needs to
        // be decompressed, then the router will return an error response.
        String message = errorMessage + " Switching to serve new active version.";
        if (!filter.isRedundantException(message)) {
          logger.warn(message);
        }

        lastCurrentVersion.put(store, metadataCurrentVersion);
        stats.recordNotStale();
        return metadataCurrentVersion;
      }
    } else { // all partitions are online and decompressor is initialized with dictionary
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
        String message = "No online replica exists for partition " + p + " of " + kafkaTopic + ", partition assignment: " + partitionAssignment;
        if (!filter.isRedundantException(message)) {
          logger.warn(message);
        }
        return true;
      }
    }
    return false;
  }

  private boolean isDecompressorReady(Store store, int versionNumber) {
    String kafkaTopic = Version.composeKafkaTopic(store.getName(), versionNumber);
    return store.getVersion(versionNumber)
        .map(version -> version.getCompressionStrategy() != CompressionStrategy.ZSTD_WITH_DICT || compressorFactory.versionSpecificCompressorExists(kafkaTopic))
        .orElse(false);
  }
}
