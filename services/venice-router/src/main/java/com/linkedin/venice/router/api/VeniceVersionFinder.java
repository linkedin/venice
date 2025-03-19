package com.linkedin.venice.router.api;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.router.stats.RouterCurrentVersionStats;
import com.linkedin.venice.router.stats.StaleVersionReason;
import com.linkedin.venice.router.stats.StaleVersionStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code VeniceVersionFinder} provides methods to find the version for a given store.
 */
public class VeniceVersionFinder {
  private static final Logger LOGGER = LogManager.getLogger(VeniceVersionFinder.class);
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final ReadOnlyStoreRepository metadataRepository;
  private final StaleVersionStats stats;
  private final ReadOnlyStoreConfigRepository storeConfigRepo;
  private final Map<String, String> clusterToD2Map;
  private final String clusterName;
  private final ConcurrentMap<String, Integer> lastCurrentVersionMap = new ConcurrentHashMap<>();

  protected final Map<String, RouterCurrentVersionStats> storeStats = new VeniceConcurrentHashMap<>();

  private final HelixBaseRoutingRepository routingDataRepository;
  private final CompressorFactory compressorFactory;

  private final MetricsRepository metricsRepository;

  public VeniceVersionFinder(
      ReadOnlyStoreRepository metadataRepository,
      HelixBaseRoutingRepository routingDataRepository,
      StaleVersionStats stats,
      ReadOnlyStoreConfigRepository storeConfigRepo,
      Map<String, String> clusterToD2Map,
      String clusterName,
      CompressorFactory compressorFactory,
      MetricsRepository metricsRepository) {
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
    this.stats = stats;
    this.storeConfigRepo = storeConfigRepo;
    this.clusterToD2Map = clusterToD2Map;
    this.clusterName = clusterName;
    this.compressorFactory = compressorFactory;
    this.metricsRepository = metricsRepository;
  }

  public int getVersion(String storeName, BasicFullHttpRequest request) throws VeniceException {
    /**
     * TODO: clone a store object is too expensive, and we could choose to expose the necessary methods
     * in {@link ReadOnlyStoreRepository}, such as 'isEnableReads' and others.
     */
    Store store = metadataRepository.getStore(storeName);
    if (store == null || store.isMigrating()) {
      // The client might be idle for a long time while the store is migrated. Check for store migration.
      if (request != null && request.headers().contains(HttpConstants.VENICE_ALLOW_REDIRECT)) {
        Optional<StoreConfig> config = storeConfigRepo.getStoreConfig(storeName);
        if (config.isPresent()) {
          String newCluster = config.get().getCluster();
          if (!clusterName.equals(newCluster)) {
            String d2Service = clusterToD2Map.get(newCluster);
            throw new VeniceStoreIsMigratedException(storeName, newCluster, d2Service);
          }
        }
      }
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
    }
    if (!store.isEnableReads()) {
      throw new StoreDisabledException(storeName, "read");
    }

    int metadataCurrentVersion = store.getCurrentVersion();
    if (!lastCurrentVersionMap.containsKey(storeName)) {
      /**
       * new version not ready to serve
       * OR a version of an existing store comes back online */
      if (metadataCurrentVersion == Store.NON_EXISTING_VERSION) {

        store = metadataRepository.refreshOneStore(storeName);
        metadataCurrentVersion = store.getCurrentVersion();
      } else if (!isDecompressorReady(store, metadataCurrentVersion)) {
        // new store ready, but compressor not ready -> fail request
        return Store.NON_EXISTING_VERSION;
      }
    }
    // TODO: put lastCurrentVersion in Map.computeIfAbsent()

    // existing store OR new store version ready to serve
    int lastCurrentVersion = lastCurrentVersionMap.get(storeName);
    if (lastCurrentVersion == metadataCurrentVersion) {
      // no version change
      stats.recordNotStale();
      return metadataCurrentVersion;
    }
    int currentVersion = maybeServeNewCurrentVersion(store, lastCurrentVersion, metadataCurrentVersion);
    // TODO: refactor maybeServeNewCurrentVersion to a smaller function that checks for version readiness, checking 1)
    // status 2) partition 3) decompressor

    String errorMessage = "";
    // TODO check new version
    // TODO log checking new version
    if (!EXCEPTION_FILTER.isRedundantException(errorMessage)) {
      LOGGER.warn(errorMessage);
    }
    // TODO else check old version
    // TODO log checking old version
    if (!EXCEPTION_FILTER.isRedundantException(errorMessage)) {
      LOGGER.warn(errorMessage);
    }

    storeStats.computeIfAbsent(storeName, k -> new RouterCurrentVersionStats(metricsRepository, storeName))
        .updateCurrentVersion(currentVersion);
    return currentVersion;
  }

  private boolean checkVersionReadiness(Store store, int versionNumber, String errorMessage) {
    String storeName = store.getName();
    VersionStatus versionStatus = store.getVersionStatus(versionNumber);
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);
    boolean versionStatusOnline = versionStatus.equals(VersionStatus.ONLINE);
    boolean partitionResourcesReady = isPartitionResourcesReady(kafkaTopic);
    boolean decompressorReady = isDecompressorReady(store, versionNumber);

    // TODO: switch case for if each of the above is not ready, add to error message and emit staleness reason metric
    errorMessage += "Unable to serve version: " + kafkaTopic + ".";

    if (!versionStatusOnline) {
      errorMessage += " Version has status: " + versionStatus + ".";
    }

    if (!partitionResourcesReady) {
      errorMessage += " Partition resources not ready for new active version.";
      stats.recordStalenessReason(StaleVersionReason.OFFLINE_PARTITIONS);
    }

    if (!decompressorReady) {
      errorMessage += " Decompressor not ready for current version (Has dictionary downloaded?).";
      stats.recordStalenessReason(StaleVersionReason.DICTIONARY_NOT_DOWNLOADED);
    }

    return decompressorReady && partitionResourcesReady && versionStatusOnline;
  }

  // existing store, new version
  private int maybeServeNewCurrentVersion(Store store, int lastCurrentVersion, int newCurrentVersion) {
    String storeName = store.getName();
    // This is a new version change, verify we have online replicas for each partition
    String kafkaTopic = Version.composeKafkaTopic(storeName, newCurrentVersion);
    boolean currentVersionDecompressorReady = isDecompressorReady(store, newCurrentVersion);
    boolean currentVersionPartitionResourcesReady = isPartitionResourcesReady(kafkaTopic);
    if (currentVersionPartitionResourcesReady && currentVersionDecompressorReady) {
      // all partitions are online and decompressor is initialized with dictionary
      lastCurrentVersionMap.put(storeName, newCurrentVersion);
      stats.recordNotStale();
      return newCurrentVersion;
    }

    // new version not ready to serve -> serve last current version
    String errorMessage = "Unable to serve new active version: " + kafkaTopic + ".";
    if (!currentVersionPartitionResourcesReady) {
      errorMessage += " Partition resources not ready for new active version.";
      stats.recordStalenessReason(StaleVersionReason.OFFLINE_PARTITIONS);
    }

    if (!currentVersionDecompressorReady) {
      errorMessage += " Decompressor not ready for current version (Has dictionary downloaded?).";
      stats.recordStalenessReason(StaleVersionReason.DICTIONARY_NOT_DOWNLOADED);
    }

    // check last current version still ready to serve/online
    VersionStatus lastCurrentVersionStatus = store.getVersionStatus(lastCurrentVersion);
    boolean prevVersionDecompressorReady = isDecompressorReady(store, lastCurrentVersion);
    if (lastCurrentVersionStatus.equals(VersionStatus.ONLINE) && prevVersionDecompressorReady) {
      // last current version is still ready to serve
      String message = errorMessage + " Continuing to serve previous version: " + lastCurrentVersion + ".";
      if (!EXCEPTION_FILTER.isRedundantException(message)) {
        LOGGER.warn(message);
      }
      stats.recordStale(newCurrentVersion, lastCurrentVersion);
      return lastCurrentVersion;
    } else {
      /**
       * last current version is not ready to serve
       * -> both last current and new version are not ready to serve
       * -> fail request
       */
      lastCurrentVersionMap.remove(storeName, lastCurrentVersion);
      errorMessage += " Unable to serve previous version: " + lastCurrentVersion + ".";

      if (!lastCurrentVersionStatus.equals(VersionStatus.ONLINE)) {
        errorMessage += " Previous version has status: " + lastCurrentVersionStatus + ".";
      }

      if (!prevVersionDecompressorReady) {
        errorMessage += " Decompressor not ready for previous version (Has dictionary downloaded?).";
      }

      String message = errorMessage + " No version ready to serve.";
      if (!EXCEPTION_FILTER.isRedundantException(message)) {
        LOGGER.warn(message);
      }
      return Store.NON_EXISTING_VERSION;
    }
  }

  private boolean isPartitionResourcesReady(String kafkaTopic) {
    if (!routingDataRepository.containsKafkaTopic(kafkaTopic)) {
      return false;
    }
    int partitionCount = routingDataRepository.getNumberOfPartitions(kafkaTopic);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      List<Instance> partitionHosts = routingDataRepository.getReadyToServeInstances(kafkaTopic, partitionId);
      if (partitionHosts.isEmpty()) {
        String partitionAssignment;
        try {
          partitionAssignment = routingDataRepository.getAllInstances(kafkaTopic, partitionId).toString();
        } catch (Exception e) {
          LOGGER.warn("Failed to get partition assignment for resource: {}", kafkaTopic, e);
          partitionAssignment = "unknown";
        }
        String message = "No online replica exists for partition " + partitionId + " of " + kafkaTopic
            + ", partition assignment: " + partitionAssignment;
        if (!EXCEPTION_FILTER.isRedundantException(message)) {
          LOGGER.warn(message);
        }
        return false;
      }
    }
    return true;
  }

  private boolean isDecompressorReady(Store store, int versionNumber) {
    Version version = store.getVersion(versionNumber);
    if (version == null) {
      return false;
    }
    return version.getCompressionStrategy() != CompressionStrategy.ZSTD_WITH_DICT
        || compressorFactory.versionSpecificCompressorExists(Version.composeKafkaTopic(store.getName(), versionNumber));
  }
}
